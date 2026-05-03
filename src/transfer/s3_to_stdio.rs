use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use futures::future::try_join_all;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::sync::{Mutex as TokioMutex, Notify};
use tracing::{debug, warn};

use crate::Config;
use crate::storage::Storage;
use crate::storage::additional_checksum_verify::is_multipart_upload_checksum;
use crate::storage::checksum::AdditionalChecksum;
use crate::storage::e_tag_verify::{
    generate_e_tag_hash, is_multipart_upload_e_tag, normalize_e_tag, verify_e_tag,
};
use crate::transfer::TransferOutcome;
use crate::transfer::first_chunk;
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::{
    SyncStatistics, detect_additional_checksum, detect_additional_checksum_with_head_object,
    is_full_object_checksum,
};

/// Transfer an S3 object to stdout. Dispatches between the serial and
/// parallel pathsｊ based on `--max-parallel-uploads` and object size.
pub async fn transfer(
    config: &Config,
    source: Storage,
    source_key: &str,
    writer: impl tokio::io::AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    // Don't short-circuit to transfer_serial when --auto-chunksize is on:
    // the serial loop only knows uniform `multipart_chunksize` boundaries
    // and would compute a composite ETag whose part layout doesn't match
    // a multipart source's actual parts. transfer_parallel's chunk plan
    // (built from get_object_parts_attributes / get_object_parts) is the
    // only path that honors auto_chunksize — and it works fine with one
    // worker (worker_count clamps to 1 below).
    if config.max_parallel_uploads <= 1 && !config.transfer_config.auto_chunksize {
        return transfer_serial(
            config,
            source,
            source_key,
            writer,
            cancellation_token,
            stats_sender,
        )
        .await;
    }

    let head = source
        .head_object(
            source_key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context(format!("failed to head source object: {source_key}"))?;

    let source_size = head.content_length().unwrap_or(0) as u64;

    // Pick the parallel path when:
    //   - non-auto_chunksize: source crosses multipart_threshold (today's
    //     boundary for "this object would be a multipart upload").
    //   - auto_chunksize: ALWAYS, regardless of size. With auto_chunksize
    //     the source's actual first-part size (from head_object_first_part)
    //     determines the chunk boundary, which is what makes per-part ETag
    //     and additional-checksum verification work for sources with
    //     non-uniform parts. Falling back to transfer_serial here would
    //     use the default multipart_chunksize for hash chunking and
    //     produce a bogus computed ETag whenever the source's actual part
    //     size differs. multipart_threshold is mutually-exclusive with
    //     auto_chunksize at the CLI, so it is not a meaningful gate here.
    let take_parallel = config.transfer_config.auto_chunksize
        || source_size >= config.transfer_config.multipart_threshold;

    if !take_parallel {
        // Small object on the parallel-eligible path — fall back to the
        // existing serial loop. The HEAD result is discarded; serial
        // will issue its own non-ranged GET.
        let _ = head;
        return transfer_serial(
            config,
            source,
            source_key,
            writer,
            cancellation_token,
            stats_sender,
        )
        .await;
    }

    transfer_parallel(
        config,
        source,
        source_key,
        head,
        writer,
        cancellation_token,
        stats_sender,
    )
    .await
}

/// Transfer an S3 object to stdout with inline ETag and checksum verification.
///
/// Downloads the object from S3 via source.get_object(), writes the body
/// to stdout, and computes ETag (MD5) and additional checksums inline.
async fn transfer_serial(
    config: &Config,
    source: Storage,
    source_key: &str,
    mut writer: impl tokio::io::AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    // Get object from S3 source
    let get_object_output = source
        .get_object(
            source_key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context(format!("failed to download source object: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_size = get_object_output.content_length().unwrap_or(0) as u64;
    let source_e_tag = get_object_output.e_tag().map(|e| e.to_string());
    let source_sse = get_object_output.server_side_encryption().cloned();
    // Auto-detect checksum algorithm from source response when --enable-additional-checksum
    // is used. `additional_checksum_algorithm` is rejected at CLI validation when the target
    // isn't S3, so only the mode path applies here.
    let (detected_algorithm, source_final_checksum) = if config.additional_checksum_mode.is_some() {
        detect_additional_checksum(&get_object_output)
            .map(|(a, c)| (Some(a), Some(c)))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let multipart_chunksize = config.transfer_config.multipart_chunksize as usize;
    let multipart_threshold = config.transfer_config.multipart_threshold as usize;

    // Determine if additional checksum verification is needed
    let verify_additional_checksum = config.additional_checksum_mode.is_some()
        && !config.disable_additional_checksum_verify
        && detected_algorithm.is_some()
        && source_final_checksum.is_some();

    // Create additional checksum calculator if needed
    let mut additional_checksum = if verify_additional_checksum {
        Some(AdditionalChecksum::new(
            detected_algorithm.as_ref().unwrap().clone(),
            config.full_object_checksum,
        ))
    } else {
        None
    };

    // Determine the chunk boundary for additional checksum.
    // For singlepart source, treat entire content as one chunk.
    // For multipart source, use multipart_chunksize.
    let checksum_is_multipart = verify_additional_checksum
        && is_multipart_upload_checksum(&source_final_checksum)
        && !is_full_object_checksum(&source_final_checksum);

    // Write body to stdout while computing hashes inline
    let mut body = get_object_output.body.into_async_read();

    // Honor --rate-limit-bandwidth on this path. Other directions apply it via
    // AsyncReadWithCallback in storage/mod.rs; s3_to_stdio reads the raw body
    // directly, so throttle explicitly here using the same leaky_bucket.
    let rate_limit_bandwidth = source.get_rate_limit_bandwidth();

    let mut concatnated_md5_hash: Vec<u8> = Vec::new();
    let mut parts_count: i64 = 0;
    let mut chunk_buffer: Vec<u8> = Vec::new();
    let mut total_bytes = 0u64;

    // For additional checksum: accumulate chunk data
    let mut checksum_chunk_buffer: Vec<u8> = Vec::new();

    let mut buf = vec![0u8; 64 * 1024]; // 64KB read buffer
    loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        let n = body
            .read(&mut buf)
            .await
            .context("s3_to_stdio: failed to read body")?;
        if n == 0 {
            break;
        }

        if let Some(limiter) = &rate_limit_bandwidth {
            limiter.acquire(n).await;
        }

        writer
            .write_all(&buf[..n])
            .await
            .context("s3_to_stdio: failed to write to stdout")?;

        total_bytes += n as u64;
        let _ = stats_sender.send(SyncStatistics::SyncBytes(n as u64)).await;

        // Accumulate data for MD5 (ETag) computation in chunksize-sized blocks
        if !config.disable_etag_verify {
            chunk_buffer.extend_from_slice(&buf[..n]);

            // Process complete chunks
            while chunk_buffer.len() >= multipart_chunksize && total_bytes < source_size {
                let md5_digest = md5::compute(&chunk_buffer[..multipart_chunksize]);
                concatnated_md5_hash.extend_from_slice(md5_digest.as_slice());
                parts_count += 1;
                chunk_buffer = chunk_buffer[multipart_chunksize..].to_vec();
            }
        }

        // Accumulate data for additional checksum computation
        if let Some(ref mut checksum) = additional_checksum {
            if checksum_is_multipart {
                checksum_chunk_buffer.extend_from_slice(&buf[..n]);
                while checksum_chunk_buffer.len() >= multipart_chunksize
                    && total_bytes < source_size
                {
                    checksum.update(&checksum_chunk_buffer[..multipart_chunksize]);
                    checksum.finalize(); // finalize each part
                    checksum_chunk_buffer = checksum_chunk_buffer[multipart_chunksize..].to_vec();
                }
            } else {
                // Singlepart or full-object checksum: just accumulate all data
                checksum_chunk_buffer.extend_from_slice(&buf[..n]);
            }
        }
    }

    // Process remaining data in the MD5 chunk buffer
    if !config.disable_etag_verify && !chunk_buffer.is_empty() {
        let md5_digest = md5::compute(&chunk_buffer);
        concatnated_md5_hash.extend_from_slice(md5_digest.as_slice());
        parts_count += 1;
    }

    // Process remaining data in the additional checksum buffer.
    // For multipart, finalize the last part now (matching the per-chunk pattern
    // inside the read loop). For single-part, leave the hasher un-finalized so
    // the verification block below can call finalize() once to get the full hash.
    if let Some(ref mut checksum) = additional_checksum {
        if !checksum_chunk_buffer.is_empty() {
            checksum.update(&checksum_chunk_buffer);
            if checksum_is_multipart {
                checksum.finalize(); // finalize last part
            }
        }
    }

    writer
        .flush()
        .await
        .context("s3_to_stdio: failed to flush stdout")?;

    // If the read loop broke because the cancellation token fired, we have
    // a truncated body. Return early before ETag / additional-checksum
    // verification so we don't compute hashes over a truncated body and
    // log a spurious mismatch warning. cli/mod.rs observes the cancelled
    // token and maps the run to ExitStatus::Cancelled (exit 130), so the
    // specific error variant here is advisory only — it's the token state
    // that drives the process exit code.
    if cancellation_token.is_cancelled() {
        return Err(anyhow!(S3syncError::Cancelled));
    }

    // ETag verification
    if !config.disable_etag_verify && !source.is_express_onezone_storage() {
        let target_e_tag = if total_bytes < multipart_threshold as u64 {
            Some(generate_e_tag_hash(&concatnated_md5_hash, 0))
        } else {
            Some(generate_e_tag_hash(&concatnated_md5_hash, parts_count))
        };

        let verify_result = verify_e_tag(
            !config.disable_multipart_verify,
            &config.source_sse_c,
            &None,
            &source_sse,
            &source_e_tag,
            &None,
            &target_e_tag,
        );

        match verify_result {
            Some(true) => {
                debug!(
                    key = source_key,
                    source_e_tag = normalize_e_tag(&source_e_tag),
                    target_e_tag = normalize_e_tag(&target_e_tag),
                    "e_tag verified.",
                );
                let _ = stats_sender
                    .send(SyncStatistics::ETagVerified {
                        key: source_key.to_string(),
                    })
                    .await;
            }
            Some(false) => {
                warn!(
                    key = source_key,
                    source_e_tag = normalize_e_tag(&source_e_tag),
                    target_e_tag = normalize_e_tag(&target_e_tag),
                    "e_tag mismatch.",
                );
                let _ = stats_sender
                    .send(SyncStatistics::ETagMismatch {
                        key: source_key.to_string(),
                    })
                    .await;
                // The sync_warning stat is advisory only — the binary reads
                // the source storage's has_warning atomic to pick
                // ExitStatus::Warning. Flip it here so the process exits 3.
                source.set_warning();
            }
            None => {
                debug!(
                    key = source_key,
                    "e_tag verification skipped (SSE-C or unsupported SSE).",
                );
            }
        }
    }

    // Additional checksum verification
    if let Some(ref mut checksum) = additional_checksum {
        let source_checksum = source_final_checksum.as_ref().unwrap();
        let additional_checksum_algorithm = detected_algorithm.as_ref().unwrap().as_str();

        let target_final_checksum = if checksum_is_multipart {
            checksum.finalize_all()
        } else {
            checksum.finalize()
        };

        if *source_checksum == target_final_checksum {
            debug!(
                key = source_key,
                additional_checksum_algorithm = additional_checksum_algorithm,
                source_final_checksum = source_checksum,
                target_final_checksum = target_final_checksum,
                "additional checksum verified."
            );
            let _ = stats_sender
                .send(SyncStatistics::ChecksumVerified {
                    key: source_key.to_string(),
                })
                .await;
        } else {
            // full_object_checksum covers the whole object; a mismatch cannot be
            // explained by chunksize differences and always indicates corruption.
            if is_full_object_checksum(&Some(source_checksum.clone())) {
                return Err(anyhow::anyhow!(
                    "additional checksum mismatch. output data may be corrupted. \
                     key={}, algorithm={}, source_final_checksum={}, target_final_checksum={}",
                    source_key,
                    additional_checksum_algorithm,
                    source_checksum,
                    target_final_checksum
                ));
            }

            warn!(
                key = source_key,
                additional_checksum_algorithm = additional_checksum_algorithm,
                source_final_checksum = source_checksum,
                target_final_checksum = target_final_checksum,
                "additional checksum mismatch. output data may be corrupted."
            );
            let _ = stats_sender
                .send(SyncStatistics::ChecksumMismatch {
                    key: source_key.to_string(),
                })
                .await;
            source.set_warning();
        }
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: source_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
}

/// Shared state across worker tasks during a parallel s3-to-stdio transfer.
///
/// Workers fetch their assigned chunk into a local `Vec<u8>`, then take the
/// `state` mutex to insert it into `ready` and drain any contiguous
/// in-order chunks (starting at `next_to_write`) to `writer`. This keeps
/// the writer single-threaded while the GETs run in parallel.
///
/// `failed` implements first-writer-wins error reporting: the first worker
/// to hit an error stores it here and signals cancellation; peers observe
/// `failed.is_some()` and bail without overwriting.
struct WriterState<W: AsyncWrite + Unpin + Send> {
    writer: W,
    next_to_write: u32,
    ready: HashMap<u32, Vec<u8>>,

    concatnated_md5_hash: Vec<u8>,
    parts_count: i64,
    additional_checksum: Option<AdditionalChecksum>,
    checksum_is_multipart: bool,
    total_bytes: u64,

    failed: Option<anyhow::Error>,
}

/// Issue a ranged GET against `source` for `[offset, offset+size)` and
/// fill a freshly allocated `Vec<u8>` of capacity `size`. Validates the
/// returned `content_range` against the requested range and honors the
/// source's rate-limit bandwidth (applied between reads, not inside
/// poll_read — see body comment for the cancellation reasoning).
async fn ranged_get_into_buffer(
    source: &dyn crate::storage::StorageTrait,
    source_key: &str,
    range: &str,
    size: u64,
    config: &Config,
    cancellation_token: PipelineCancellationToken,
) -> Result<Vec<u8>> {
    // Race the GET issue itself against cancellation — the SDK's send()
    // future has no native is_cancelled hook so we wrap it in select!.
    // (Subsequent body reads use the simpler is_cancelled() polling
    // pattern; see comment below.)
    let get_object_output = tokio::select! {
        _ = cancellation_token.cancelled() => return Err(anyhow!(S3syncError::Cancelled)),
        result = source.get_object(
            source_key,
            config.version_id.clone(),
            None, // checksum_mode: not needed for chunk fetches
            Some(range.to_string()),
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        ) => result.context(format!("failed to download chunk: {source_key} {range}"))?,
    };

    first_chunk::validate_content_range(&get_object_output, range)?;

    // Read the raw body directly (no AsyncReadWithCallback wrapper).
    //
    // Why not use `convert_to_buf_byte_stream_with_callback`: that
    // wrapper applies the leaky-bucket rate limiter from inside
    // `poll_read` via `task::block_in_place + Handle::current().block_on`.
    // While the worker thread is parked synchronously inside that
    // `block_on(acquire(n))`, no `select!` above it can preempt — the
    // cancellation arm only re-polls after `block_in_place` returns.
    // With a 2 MiB/s shared rate limit and 4 workers each cycling
    // through these synchronous waits, SIGINT-to-process-exit latency
    // ballooned to >60s in `cancel_s3_to_stdout_sigint_exits_130`.
    //
    // Mirroring the serial path's pattern fixes it: read the raw body
    // (cancellable async await), then apply the rate limiter as a
    // normal `acquire(n).await` BETWEEN reads. `is_cancelled()` at the
    // top of each loop iteration catches SIGINT within one read cycle
    // (~few ms even under throttling).
    let rate_limit_bandwidth = source.get_rate_limit_bandwidth();
    let mut buf = Vec::<u8>::with_capacity(size as usize);
    let mut reader = BufReader::new(get_object_output.body.into_async_read());
    let mut filled: u64 = 0;
    loop {
        if cancellation_token.is_cancelled() {
            return Err(anyhow!(S3syncError::Cancelled));
        }
        let chunk = reader
            .fill_buf()
            .await
            .context("ranged_get_into_buffer: read failed")?;
        if chunk.is_empty() {
            break;
        }
        let chunk_len = chunk.len();
        let remaining = (size - filled) as usize;
        // S3 ranged GETs must return exactly the requested bytes; an
        // over-read indicates corruption upstream or in transit.
        // Surface it instead of silently truncating to `remaining`
        // (which would both consume and drop the extra bytes, and
        // let a wrong body still verify against a per-part hash
        // computed only over the truncated prefix).
        if chunk_len > remaining {
            return Err(anyhow!(
                "ranged_get_into_buffer: over-read for {range}: \
                 expected {size} bytes, observed at least {} bytes",
                filled + chunk_len as u64
            ));
        }
        buf.extend_from_slice(chunk);
        filled += chunk_len as u64;
        reader.consume(chunk_len);

        // Rate limit BETWEEN reads (cancellable async await), not
        // INSIDE poll_read. The await yields control to the runtime
        // and can be observed by select! / cancellation right after.
        if let Some(limiter) = &rate_limit_bandwidth {
            limiter.acquire(chunk_len).await;
        }

        if filled >= size {
            break;
        }
    }

    if filled != size {
        return Err(anyhow!(
            "ranged_get_into_buffer: short read for {range}: expected {size}, got {filled}"
        ));
    }

    // Defence against over-reads that align exactly to a BufReader chunk
    // boundary: the in-loop check above only catches over-reads when a
    // single read straddles `size`. If chunks happen to land such that
    // we hit `filled == size` on a chunk-end, any extra upstream bytes
    // are still buffered (or pending on the stream); peek once more to
    // surface them as the same protocol-anomaly error.
    let trailing = reader
        .fill_buf()
        .await
        .context("ranged_get_into_buffer: post-read fill_buf failed")?;
    if !trailing.is_empty() {
        return Err(anyhow!(
            "ranged_get_into_buffer: over-read for {range}: \
             expected {size} bytes, observed at least {} additional byte(s)",
            trailing.len()
        ));
    }

    Ok(buf)
}

async fn transfer_parallel(
    config: &Config,
    source: Storage,
    source_key: &str,
    head: HeadObjectOutput,
    writer: impl AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_size = head.content_length().unwrap_or(0) as u64;

    // Zero-byte source: the chunk planner below can produce a
    // (offset=0, size=0) entry on the non-multipart auto_chunksize
    // branch (multipart_chunksize.min(0) == 0), after which the
    // dispatcher's `offset + size - 1` underflows (debug panic;
    // release wraps to u64::MAX → bogus range). The serial path
    // handles empty bodies cleanly (one non-ranged GET, MD5 of the
    // empty body, ETag verify if requested), so delegate.
    if source_size == 0 {
        return transfer_serial(
            config,
            source,
            source_key,
            writer,
            cancellation_token,
            stats_sender,
        )
        .await;
    }

    let source_e_tag = head.e_tag().map(|e| e.to_string());
    let source_sse = head.server_side_encryption().cloned();

    let multipart_chunksize = config.transfer_config.multipart_chunksize;

    // Detect additional checksum from HEAD (mirrors serial's GET-based detection).
    let (detected_algorithm, source_final_checksum) = if config.additional_checksum_mode.is_some() {
        detect_additional_checksum_with_head_object(&head)
            .map(|(a, c)| (Some(a), Some(c)))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let verify_additional_checksum = config.additional_checksum_mode.is_some()
        && !config.disable_additional_checksum_verify
        && detected_algorithm.is_some()
        && source_final_checksum.is_some();

    let additional_checksum = if verify_additional_checksum {
        Some(AdditionalChecksum::new(
            detected_algorithm.as_ref().unwrap().clone(),
            config.full_object_checksum,
        ))
    } else {
        None
    };

    let checksum_is_multipart = verify_additional_checksum
        && is_multipart_upload_checksum(&source_final_checksum)
        && !is_full_object_checksum(&source_final_checksum);

    // Build the chunk plan.
    //
    // For non-auto_chunksize: uniform multipart_chunksize chunks (the
    // first chunk is `chunksize.min(source_size)`; remaining chunks
    // are `multipart_chunksize` each, with the last possibly shorter).
    //
    // For auto_chunksize on a multipart source: ALL chunk boundaries
    // must come from the source's actual part layout. Using
    // head_object_first_part for chunk #1 only (and multipart_chunksize
    // for the rest) was wrong — for a 16 MiB source uploaded with 5
    // MiB parts (4 parts of [5,5,5,1] MiB) it produced [(0,5), (5,8),
    // (13,3)] = 3 chunks, so the per-part MD5/SHA256 boundaries didn't
    // match the source and the composite ETag/checksum mismatched. The
    // only way to get matching boundaries on non-uniform multipart
    // sources is GetObjectAttributes (parts list).
    //
    // For auto_chunksize on a singlepart source: a single chunk
    // covering the whole body is correct (no per-part hashes to align).
    const MAX_PARTS_DEFAULT: i32 = 1000;
    let chunks: Vec<(u64, u64)> =
        if config.transfer_config.auto_chunksize && is_multipart_upload_e_tag(&source_e_tag) {
            // Prefer GetObjectAttributes (single round trip), but it
            // returns the parts list only when the source has per-part
            // metadata — S3 records that only for uploads that used
            // --additional-checksum-algorithm. Without per-part
            // checksums, attributes returns empty; fall back to
            // per-part HEAD ?partNumber=N (one HEAD per part) which
            // works for any multipart object. Mirrors
            // first_chunk::get_object_parts_if_necessary's two-tier
            // strategy.
            let parts = source
                .get_object_parts_attributes(
                    source_key,
                    config.version_id.clone(),
                    MAX_PARTS_DEFAULT,
                    config.source_sse_c.clone(),
                    config.source_sse_c_key.clone(),
                    config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("auto_chunksize: get_object_parts_attributes failed")?;
            let parts = if parts.is_empty() {
                source
                    .get_object_parts(
                        source_key,
                        config.version_id.clone(),
                        config.source_sse_c.clone(),
                        config.source_sse_c_key.clone(),
                        config.source_sse_c_key_md5.clone(),
                    )
                    .await
                    .context("auto_chunksize: get_object_parts fallback failed")?
            } else {
                parts
            };
            if parts.is_empty() {
                // Both attributes and per-part HEAD returned empty
                // even though the source ETag claims multipart.
                // Pathological (e.g., source overwritten as
                // singlepart between our HEAD and now, or both APIs
                // mis-reported). DO NOT fall back to a single chunk
                // covering the whole body: that would allocate
                // `source_size` bytes in one Vec — for a multi-GiB
                // (or 50 TB) source this is OOM-grade. Hard-error
                // instead and let the user pick a workaround
                // (rerun, or remove --auto-chunksize and set
                // --multipart-chunksize manually).
                return Err(anyhow!(
                    "auto_chunksize: source ETag indicates multipart but neither \
                     get_object_parts_attributes nor get_object_parts returned a \
                     parts list (source_size={source_size}). Refusing to fall back \
                     to a single-chunk download because that would allocate the \
                     entire object in memory. Retry the operation, or remove \
                     --auto-chunksize and set --multipart-chunksize to match the \
                     source's part size."
                ));
            }
            let mut chunks_vec = Vec::with_capacity(parts.len());
            let mut offset = 0u64;
            for p in &parts {
                let size = p.size().unwrap() as u64;
                chunks_vec.push((offset, size));
                offset += size;
            }
            // The parts list is the only thing telling us where the per-part
            // hash boundaries are; HEAD's content_length is the only thing
            // telling us how many bytes the source actually contains. If
            // those disagree, downloading the parts-list range would emit a
            // truncated body to stdout while still producing a per-part /
            // composite ETag that verifies against the source. Refuse the
            // transfer so the user sees an error instead of silent
            // truncation under exit 0.
            if offset != source_size {
                return Err(anyhow!(
                    "auto_chunksize: parts list (sum={offset}) does not match \
                     source content_length ({source_size}); refusing to download \
                     to avoid silent truncation."
                ));
            }
            chunks_vec
        } else {
            let first_chunk_size = multipart_chunksize.min(source_size);
            let mut chunks_vec = vec![(0, first_chunk_size)];
            let mut offset = first_chunk_size;
            while offset < source_size {
                let size = (source_size - offset).min(multipart_chunksize);
                chunks_vec.push((offset, size));
                offset += size;
            }
            chunks_vec
        };
    let parts_total = chunks.len() as u32;
    let worker_count = (config.max_parallel_uploads as u32).min(parts_total).max(1);

    let state = Arc::new(TokioMutex::new(WriterState {
        writer,
        next_to_write: 0,
        ready: HashMap::new(),
        concatnated_md5_hash: Vec::new(),
        parts_count: 0,
        additional_checksum,
        checksum_is_multipart,
        total_bytes: 0,
        failed: None,
    }));
    let notify = Arc::new(Notify::new());
    let next_to_dispatch = Arc::new(AtomicU32::new(0));
    let chunks = Arc::new(chunks);

    // Spawn worker futures. We use try_join_all (no tokio::spawn) so the
    // writer doesn't need a 'static bound.
    let mut workers = Vec::with_capacity(worker_count as usize);
    for _ in 0..worker_count {
        let state = state.clone();
        let notify = notify.clone();
        let next_to_dispatch = next_to_dispatch.clone();
        let chunks = chunks.clone();
        let source_clone = dyn_clone::clone_box(&*source);
        let cancellation_token = cancellation_token.clone();
        let stats_sender = stats_sender.clone();
        let config = config.clone();
        let source_key = source_key.to_string();

        workers.push(parallel_worker(
            state,
            notify,
            next_to_dispatch,
            chunks,
            parts_total,
            source_clone,
            source_key,
            config,
            cancellation_token,
            stats_sender,
        ));
    }

    // Workers always return Ok unless they hit a programming error
    // (permit acquire failure, etc.). Real failures are recorded into
    // state.failed; cancellation is observed via cancellation_token.
    // This way the actual root-cause error is preserved instead of being
    // overwritten by the Cancelled error of a peer worker that bailed.
    try_join_all(workers).await?;

    // Take ownership of the WriterState (workers are done, only us holds
    // an Arc now).
    let state = Arc::try_unwrap(state)
        .map_err(|_| anyhow!("internal: workers still hold WriterState"))?
        .into_inner();

    if let Some(e) = state.failed {
        return Err(e);
    }
    if cancellation_token.is_cancelled() {
        return Err(anyhow!(S3syncError::Cancelled));
    }

    // Final flush + verification.
    finalize_parallel(
        config.clone(),
        &*source,
        source_key,
        state,
        source_e_tag,
        source_sse,
        source_final_checksum,
        detected_algorithm,
        cancellation_token,
        stats_sender,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn parallel_worker<W: AsyncWrite + Unpin + Send>(
    state: Arc<TokioMutex<WriterState<W>>>,
    notify: Arc<Notify>,
    next_to_dispatch: Arc<AtomicU32>,
    chunks: Arc<Vec<(u64, u64)>>,
    parts_total: u32,
    source: Storage,
    source_key: String,
    config: Config,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    // Workers return Ok in normal completion AND in failure/cancellation
    // bail-out paths. Failures are recorded into state.failed (the
    // first-writer-wins pattern); the caller drains state.failed after
    // try_join_all to recover the actual error. Returning Err from a
    // worker is reserved for programming errors (e.g. permit acquire
    // failure) that would short-circuit try_join_all.
    loop {
        let part_num = next_to_dispatch.fetch_add(1, Ordering::SeqCst);
        if part_num >= parts_total {
            return Ok(());
        }

        if cancellation_token.is_cancelled() {
            notify.notify_waiters();
            return Ok(());
        }

        // Bail early if another worker has already failed.
        {
            let s = state.lock().await;
            if s.failed.is_some() {
                return Ok(());
            }
        }

        let (offset, size) = chunks[part_num as usize];
        let range = format!("bytes={}-{}", offset, offset + size - 1);

        let permit = config
            .source_client_config
            .as_ref()
            .expect("source_client_config required for parallel s3_to_stdio")
            .parallel_upload_semaphore
            .clone()
            .acquire_owned()
            .await
            .context("acquire parallel_upload_semaphore")?;

        let buf_result = ranged_get_into_buffer(
            &*source,
            &source_key,
            &range,
            size,
            &config,
            cancellation_token.clone(),
        )
        .await;
        drop(permit);

        let buf = match buf_result {
            Ok(b) => b,
            Err(e) => {
                let mut s = state.lock().await;
                if s.failed.is_none() {
                    s.failed = Some(e);
                }
                cancellation_token.cancel();
                drop(s);
                notify.notify_waiters();
                return Ok(());
            }
        };

        // Hand off + drain in part-number order.
        {
            let mut s = state.lock().await;
            if s.failed.is_some() {
                return Ok(());
            }
            s.ready.insert(part_num, buf);

            let WriterState {
                writer,
                next_to_write,
                ready,
                concatnated_md5_hash,
                parts_count,
                additional_checksum,
                checksum_is_multipart,
                total_bytes,
                failed,
                ..
            } = &mut *s;

            while let Some(chunk) = ready.remove(next_to_write) {
                if !config.disable_etag_verify {
                    let md5 = md5::compute(&chunk);
                    concatnated_md5_hash.extend_from_slice(md5.as_slice());
                    *parts_count += 1;
                }
                if let Some(chk) = additional_checksum.as_mut() {
                    chk.update(&chunk);
                    if *checksum_is_multipart {
                        chk.finalize();
                    }
                }

                let chunk_len = chunk.len();
                if let Err(e) = writer.write_all(&chunk).await {
                    *failed = Some(e.into());
                    cancellation_token.cancel();
                    drop(s);
                    notify.notify_waiters();
                    return Ok(());
                }
                // Flush after every write_all for symmetry with the
                // serial path: a process crash between drained chunks
                // doesn't leave bytes stranded in any user-space
                // buffer the caller may have wrapped the writer in
                // (e.g., BufWriter). For tokio::io::stdout() this is
                // a no-op; data integrity > the marginal flush cost.
                if let Err(e) = writer.flush().await {
                    *failed = Some(e.into());
                    cancellation_token.cancel();
                    drop(s);
                    notify.notify_waiters();
                    return Ok(());
                }
                // Emit SyncBytes AFTER both write_all AND flush
                // succeed — bytes have actually reached stdout (and
                // any user-space buffer in front of it has been
                // drained), not just been downloaded into the ready
                // buffer. Sender is unbounded, so this await won't
                // block the drain loop or the lock we still hold.
                let _ = stats_sender
                    .send(SyncStatistics::SyncBytes(chunk_len as u64))
                    .await;
                *total_bytes += chunk_len as u64;
                *next_to_write += 1;
            }
        }
        notify.notify_waiters();

        // Buffer-cap gate: don't claim a new part until OUR chunk has
        // been flushed. If the drain above flushed us, this returns
        // immediately.
        //
        // Concurrency note: `notify.notified()` must be constructed AND
        // registered (via `enable()`) BEFORE we release the state lock,
        // otherwise a peer worker that drains and calls `notify_waiters()`
        // between our drop and our subscribe will not wake us. We also
        // race `notified` against `cancellation_token.cancelled()` so a
        // cancellation arriving while every worker is parked still
        // unblocks all workers.
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let s = state.lock().await;
            if s.failed.is_some() {
                return Ok(());
            }
            if s.next_to_write > part_num {
                break;
            }
            drop(s);

            tokio::select! {
                _ = &mut notified => {}
                _ = cancellation_token.cancelled() => {
                    notify.notify_waiters();
                    return Ok(());
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn finalize_parallel<W: AsyncWrite + Unpin + Send>(
    config: Config,
    source: &dyn crate::storage::StorageTrait,
    source_key: &str,
    mut state: WriterState<W>,
    source_e_tag: Option<String>,
    source_sse: Option<aws_sdk_s3::types::ServerSideEncryption>,
    source_final_checksum: Option<String>,
    detected_algorithm: Option<aws_sdk_s3::types::ChecksumAlgorithm>,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    state
        .writer
        .flush()
        .await
        .context("s3_to_stdio: failed to flush stdout")?;

    if cancellation_token.is_cancelled() {
        return Err(anyhow!(S3syncError::Cancelled));
    }

    // ---- ETag verification ----
    if !config.disable_etag_verify && !source.is_express_onezone_storage() {
        // Pick the target ETag format from the SOURCE's ETag shape, not
        // from `total_bytes < multipart_threshold`. The size-based
        // proxy is wrong for the parallel path's auto_chunksize entry
        // point: a 7 MiB body uploaded as 2-part multipart has size <
        // default 8 MiB threshold but a multipart `-N`-suffixed ETag,
        // and computing a singlepart MD5 here would mismatch. Source's
        // own ETag shape is the authoritative signal for which format
        // we need to reproduce.
        let target_e_tag = if is_multipart_upload_e_tag(&source_e_tag) {
            Some(generate_e_tag_hash(
                &state.concatnated_md5_hash,
                state.parts_count,
            ))
        } else {
            Some(generate_e_tag_hash(&state.concatnated_md5_hash, 0))
        };

        let verify_result = verify_e_tag(
            !config.disable_multipart_verify,
            &config.source_sse_c,
            &None,
            &source_sse,
            &source_e_tag,
            &None,
            &target_e_tag,
        );

        match verify_result {
            Some(true) => {
                debug!(
                    key = source_key,
                    source_e_tag = normalize_e_tag(&source_e_tag),
                    target_e_tag = normalize_e_tag(&target_e_tag),
                    "e_tag verified.",
                );
                let _ = stats_sender
                    .send(SyncStatistics::ETagVerified {
                        key: source_key.to_string(),
                    })
                    .await;
            }
            Some(false) => {
                warn!(
                    key = source_key,
                    source_e_tag = normalize_e_tag(&source_e_tag),
                    target_e_tag = normalize_e_tag(&target_e_tag),
                    "e_tag mismatch.",
                );
                let _ = stats_sender
                    .send(SyncStatistics::ETagMismatch {
                        key: source_key.to_string(),
                    })
                    .await;
                source.set_warning();
            }
            None => {
                debug!(
                    key = source_key,
                    "e_tag verification skipped (SSE-C or unsupported SSE).",
                );
            }
        }
    }

    // ---- Additional checksum verification ----
    if let Some(mut checksum) = state.additional_checksum {
        let source_checksum = source_final_checksum.as_ref().unwrap();
        let additional_checksum_algorithm = detected_algorithm.as_ref().unwrap().as_str();

        let target_final_checksum = if state.checksum_is_multipart {
            checksum.finalize_all()
        } else {
            checksum.finalize()
        };

        if *source_checksum == target_final_checksum {
            debug!(
                key = source_key,
                additional_checksum_algorithm = additional_checksum_algorithm,
                source_final_checksum = source_checksum,
                target_final_checksum = target_final_checksum,
                "additional checksum verified."
            );
            let _ = stats_sender
                .send(SyncStatistics::ChecksumVerified {
                    key: source_key.to_string(),
                })
                .await;
        } else {
            if is_full_object_checksum(&Some(source_checksum.clone())) {
                return Err(anyhow!(
                    "additional checksum mismatch. output data may be corrupted. \
                     key={}, algorithm={}, source_final_checksum={}, target_final_checksum={}",
                    source_key,
                    additional_checksum_algorithm,
                    source_checksum,
                    target_final_checksum
                ));
            }

            warn!(
                key = source_key,
                additional_checksum_algorithm = additional_checksum_algorithm,
                source_final_checksum = source_checksum,
                target_final_checksum = target_final_checksum,
                "additional checksum mismatch. output data may be corrupted."
            );
            let _ = stats_sender
                .send(SyncStatistics::ChecksumMismatch {
                    key: source_key.to_string(),
                })
                .await;
            source.set_warning();
        }
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: source_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
}

#[cfg(test)]
#[allow(dead_code)]
mod tests {
    use super::*;
    use crate::config::TransferConfig;
    use crate::storage::StorageTrait;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{ObjectChecksum, SseCustomerKey, StoragePath};
    use anyhow::{Result, anyhow};
    use async_trait::async_trait;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
    use aws_sdk_s3::primitives::{ByteStream, DateTime};
    use aws_sdk_s3::types::{ChecksumMode, ObjectPart, ServerSideEncryption, Tagging};
    use leaky_bucket::RateLimiter;
    use std::collections::HashMap;
    use std::io;
    use std::path::PathBuf;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use std::task::{Context as TaskContext, Poll};
    use std::time::Duration;

    /// Async sink that captures writes into a `Vec<u8>`. The underlying
    /// buffer is shared via `Arc<StdMutex<...>>` so the test can read what
    /// was written after `transfer()` returns.
    #[derive(Clone)]
    struct VecWriter {
        buf: Arc<StdMutex<Vec<u8>>>,
    }

    impl VecWriter {
        fn new() -> Self {
            Self {
                buf: Arc::new(StdMutex::new(Vec::new())),
            }
        }
        fn buf(&self) -> Arc<StdMutex<Vec<u8>>> {
            self.buf.clone()
        }
    }

    impl tokio::io::AsyncWrite for VecWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut TaskContext<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.buf.lock().unwrap().extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    /// Configurable per-test source. Tests build one of these via the
    /// builder methods, then box it as `Storage` for `transfer()`.
    #[derive(Clone)]
    struct MockSource {
        inner: Arc<MockSourceInner>,
    }

    struct MockSourceInner {
        body: Vec<u8>,
        e_tag: Option<String>,
        sse: Option<ServerSideEncryption>,
        checksum_sha256: Option<String>,
        is_express_onezone: bool,
        head_should_fail: bool,
        head_first_part_size: Option<i64>,
        // Per-part sizes returned by both `get_object_parts_attributes`
        // and `get_object_parts`. Empty by default — production code
        // interprets that as "no parts metadata available". Tests that
        // exercise the auto_chunksize path against a multipart source
        // should set this via `with_parts(&[size1, size2, ...])`.
        parts_sizes: StdMutex<Vec<i64>>,
        // When true, `get_object_parts_attributes` returns Ok(empty)
        // even when `parts_sizes` is configured — used to simulate
        // real-S3 behavior for sources uploaded WITHOUT
        // --additional-checksum-algorithm (S3 returns no per-part
        // attributes in that case). Production code falls back to
        // `get_object_parts` (per-part HEAD), which always returns
        // `parts_sizes` regardless of this flag.
        force_empty_parts_attributes: StdMutex<bool>,
        // Map of "byte offset of failing part" → unit. If a ranged GET's
        // start matches an entry, get_object returns Err.
        fail_get_at_offsets: StdMutex<Vec<u64>>,
        // Map of "byte offset of part" → delay before responding.
        delay_get_at_offsets: StdMutex<HashMap<u64, Duration>>,
        // Number of extra padding bytes appended to every ranged GET
        // body (content_length / content_range stay as the requested
        // values). Simulates a misbehaving upstream that delivers more
        // body bytes than the range it claims to be returning. Default
        // 0 leaves response sizes exact.
        over_read_extra: StdMutex<usize>,
        head_calls: AtomicU32,
        head_first_part_calls: AtomicU32,
        get_object_parts_attributes_calls: AtomicU32,
        get_object_parts_calls: AtomicU32,
        get_calls: AtomicU32,
        in_flight_gets: AtomicU32,
        peak_in_flight_gets: AtomicU32,
        warning: Arc<AtomicBool>,
    }

    impl MockSource {
        fn new(body: Vec<u8>) -> Self {
            Self {
                inner: Arc::new(MockSourceInner {
                    body,
                    e_tag: None,
                    sse: None,
                    checksum_sha256: None,
                    is_express_onezone: false,
                    head_should_fail: false,
                    head_first_part_size: None,
                    parts_sizes: StdMutex::new(Vec::new()),
                    force_empty_parts_attributes: StdMutex::new(false),
                    fail_get_at_offsets: StdMutex::new(Vec::new()),
                    delay_get_at_offsets: StdMutex::new(HashMap::new()),
                    over_read_extra: StdMutex::new(0),
                    head_calls: AtomicU32::new(0),
                    head_first_part_calls: AtomicU32::new(0),
                    get_object_parts_attributes_calls: AtomicU32::new(0),
                    get_object_parts_calls: AtomicU32::new(0),
                    get_calls: AtomicU32::new(0),
                    in_flight_gets: AtomicU32::new(0),
                    peak_in_flight_gets: AtomicU32::new(0),
                    warning: Arc::new(AtomicBool::new(false)),
                }),
            }
        }
        fn with_e_tag(mut self, etag: &str) -> Self {
            Arc::get_mut(&mut self.inner).unwrap().e_tag = Some(etag.to_string());
            self
        }
        fn with_sha256(mut self, sha: &str) -> Self {
            Arc::get_mut(&mut self.inner).unwrap().checksum_sha256 = Some(sha.to_string());
            self
        }
        fn fail_head(mut self) -> Self {
            Arc::get_mut(&mut self.inner).unwrap().head_should_fail = true;
            self
        }
        fn express_onezone(mut self) -> Self {
            Arc::get_mut(&mut self.inner).unwrap().is_express_onezone = true;
            self
        }
        fn with_first_part_size(mut self, size: i64) -> Self {
            Arc::get_mut(&mut self.inner).unwrap().head_first_part_size = Some(size);
            self
        }
        fn head_first_part_calls(&self) -> u32 {
            self.inner.head_first_part_calls.load(Ordering::SeqCst)
        }
        /// Configure the per-part sizes returned by both
        /// `get_object_parts_attributes` and `get_object_parts`. Use
        /// this on tests that exercise the auto_chunksize parallel
        /// path against a multipart source — the production code uses
        /// these sizes to build a chunk plan that aligns with the
        /// source's actual part boundaries.
        fn with_parts(self, sizes: &[i64]) -> Self {
            *self.inner.parts_sizes.lock().unwrap() = sizes.to_vec();
            self
        }
        /// Force `get_object_parts_attributes` to return Ok(empty),
        /// triggering production code's fallback to `get_object_parts`.
        /// Simulates a real-S3 source that was uploaded WITHOUT
        /// `--additional-checksum-algorithm` (S3 returns no per-part
        /// attributes for such uploads). `parts_sizes` (set via
        /// `with_parts`) is still served by `get_object_parts`.
        fn with_empty_parts_attributes(self) -> Self {
            *self.inner.force_empty_parts_attributes.lock().unwrap() = true;
            self
        }
        fn get_object_parts_attributes_calls(&self) -> u32 {
            self.inner
                .get_object_parts_attributes_calls
                .load(Ordering::SeqCst)
        }
        fn get_object_parts_calls(&self) -> u32 {
            self.inner.get_object_parts_calls.load(Ordering::SeqCst)
        }
        fn fail_get_at(self, offset: u64) -> Self {
            self.inner.fail_get_at_offsets.lock().unwrap().push(offset);
            self
        }
        fn delay_get_at(self, offset: u64, delay: Duration) -> Self {
            self.inner
                .delay_get_at_offsets
                .lock()
                .unwrap()
                .insert(offset, delay);
            self
        }
        /// Make every ranged GET return `extra` additional padding
        /// bytes beyond what the range asks for. content_length and
        /// content_range remain set to the requested size — the goal
        /// is to simulate an upstream whose body length exceeds its
        /// own range/content-length headers.
        fn with_over_read(self, extra: usize) -> Self {
            *self.inner.over_read_extra.lock().unwrap() = extra;
            self
        }
        fn head_calls(&self) -> u32 {
            self.inner.head_calls.load(Ordering::SeqCst)
        }
        fn get_calls(&self) -> u32 {
            self.inner.get_calls.load(Ordering::SeqCst)
        }
        fn peak_in_flight_gets(&self) -> u32 {
            self.inner.peak_in_flight_gets.load(Ordering::SeqCst)
        }
        fn warning_set(&self) -> bool {
            self.inner.warning.load(Ordering::SeqCst)
        }
    }

    fn parse_range(range: &str) -> (u64, u64) {
        // "bytes=START-END"
        let s = range.strip_prefix("bytes=").unwrap();
        let (a, b) = s.split_once('-').unwrap();
        (a.parse().unwrap(), b.parse().unwrap())
    }

    #[async_trait]
    impl StorageTrait for MockSource {
        fn is_local_storage(&self) -> bool {
            false
        }
        fn is_express_onezone_storage(&self) -> bool {
            self.inner.is_express_onezone
        }

        async fn get_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<GetObjectOutput> {
            self.inner.get_calls.fetch_add(1, Ordering::SeqCst);
            let now = self.inner.in_flight_gets.fetch_add(1, Ordering::SeqCst) + 1;
            let mut peak = self.inner.peak_in_flight_gets.load(Ordering::SeqCst);
            while now > peak {
                match self.inner.peak_in_flight_gets.compare_exchange(
                    peak,
                    now,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                ) {
                    Ok(_) => break,
                    Err(actual) => peak = actual,
                }
            }

            let (body, content_length, content_range): (Vec<u8>, i64, Option<String>) =
                if let Some(r) = range.as_deref() {
                    let (start, end) = parse_range(r);
                    if self
                        .inner
                        .fail_get_at_offsets
                        .lock()
                        .unwrap()
                        .contains(&start)
                    {
                        self.inner.in_flight_gets.fetch_sub(1, Ordering::SeqCst);
                        return Err(anyhow!("simulated GET failure for range {r}"));
                    }
                    let delay = {
                        self.inner
                            .delay_get_at_offsets
                            .lock()
                            .unwrap()
                            .get(&start)
                            .copied()
                    };
                    if let Some(d) = delay {
                        tokio::time::sleep(d).await;
                    }
                    let len = (end - start + 1) as usize;
                    let mut slice = self.inner.body[start as usize..start as usize + len].to_vec();
                    let extra = *self.inner.over_read_extra.lock().unwrap();
                    if extra > 0 {
                        slice.extend(std::iter::repeat_n(0xAAu8, extra));
                    }
                    let cr = format!("bytes {start}-{end}/{}", self.inner.body.len());
                    (slice, len as i64, Some(cr))
                } else {
                    let body = self.inner.body.clone();
                    let len = body.len() as i64;
                    (body, len, None)
                };

            let mut builder = GetObjectOutput::builder()
                .body(ByteStream::from(body))
                .content_length(content_length)
                .last_modified(DateTime::from_secs(0));
            if let Some(cr) = content_range {
                builder = builder.content_range(cr);
            }
            if let Some(ref e) = self.inner.e_tag {
                builder = builder.e_tag(e.clone());
            }
            if let Some(ref s) = self.inner.checksum_sha256 {
                builder = builder.checksum_sha256(s.clone());
            }
            if let Some(ref sse) = self.inner.sse {
                builder = builder.server_side_encryption(sse.clone());
            }

            self.inner.in_flight_gets.fetch_sub(1, Ordering::SeqCst);
            Ok(builder.build())
        }

        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            unimplemented!()
        }

        async fn head_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            self.inner.head_calls.fetch_add(1, Ordering::SeqCst);
            if self.inner.head_should_fail {
                return Err(anyhow!("simulated HEAD failure"));
            }
            let mut builder = HeadObjectOutput::builder()
                .content_length(self.inner.body.len() as i64)
                .last_modified(DateTime::from_secs(0));
            if let Some(ref e) = self.inner.e_tag {
                builder = builder.e_tag(e.clone());
            }
            if let Some(ref s) = self.inner.checksum_sha256 {
                builder = builder.checksum_sha256(s.clone());
            }
            Ok(builder.build())
        }

        async fn head_object_first_part(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            self.inner
                .head_first_part_calls
                .fetch_add(1, Ordering::SeqCst);
            let size = self
                .inner
                .head_first_part_size
                .unwrap_or(self.inner.body.len() as i64);
            Ok(HeadObjectOutput::builder()
                .content_length(size)
                .last_modified(DateTime::from_secs(0))
                .build())
        }

        async fn get_object_parts(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            self.inner
                .get_object_parts_calls
                .fetch_add(1, Ordering::SeqCst);
            Ok(self
                .inner
                .parts_sizes
                .lock()
                .unwrap()
                .iter()
                .map(|s| ObjectPart::builder().size(*s).build())
                .collect())
        }

        async fn get_object_parts_attributes(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _max_parts: i32,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            self.inner
                .get_object_parts_attributes_calls
                .fetch_add(1, Ordering::SeqCst);
            if *self.inner.force_empty_parts_attributes.lock().unwrap() {
                return Ok(Vec::new());
            }
            Ok(self
                .inner
                .parts_sizes
                .lock()
                .unwrap()
                .iter()
                .map(|s| ObjectPart::builder().size(*s).build())
                .collect())
        }

        async fn put_object(
            &self,
            _key: &str,
            _source: Storage,
            _source_key: &str,
            _source_size: u64,
            _source_additional_checksum: Option<String>,
            _get_object_output_first_chunk: GetObjectOutput,
            _tagging: Option<String>,
            _object_checksum: Option<ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            unimplemented!()
        }

        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
            unimplemented!()
        }

        async fn delete_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<DeleteObjectOutput> {
            unimplemented!()
        }

        fn get_client(&self) -> Option<Arc<Client>> {
            None
        }
        fn get_stats_sender(&self) -> async_channel::Sender<SyncStatistics> {
            async_channel::unbounded().0
        }
        async fn send_stats(&self, _stats: SyncStatistics) {}
        fn get_local_path(&self) -> PathBuf {
            PathBuf::new()
        }
        fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
            None
        }
        fn generate_copy_source_key(&self, _key: &str, _version_id: Option<String>) -> String {
            unimplemented!()
        }
        fn set_warning(&self) {
            self.inner.warning.store(true, Ordering::SeqCst);
        }
    }

    /// Build a Config tuned for tests. `parallel` and `chunksize` and
    /// `threshold` are the knobs each test exercises. Tests that need
    /// other fields (additional_checksum_mode, etc.) call `.modify()`
    /// on the returned value before passing it in.
    fn test_config(parallel: usize, threshold: u64, chunksize: u64) -> Config {
        // Build a ClientConfig with a parallel_upload_semaphore sized to
        // `parallel`. We construct it by hand here — the production code
        // builds it via the CLI parser, which we can't depend on in unit
        // tests.
        use crate::config::{CLITimeoutConfig, ClientConfig, RetryConfig};
        use crate::types::{ClientConfigLocation, S3Credentials};
        use aws_smithy_types::checksum_config::RequestChecksumCalculation;
        use tokio::sync::Semaphore;

        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: S3Credentials::FromEnvironment,
            region: Some("us-east-1".to_string()),
            endpoint_url: None,
            force_path_style: false,
            retry_config: RetryConfig {
                aws_max_attempts: 1,
                initial_backoff_milliseconds: 0,
            },
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(parallel)),
            accelerate: false,
            request_payer: None,
        };

        Config {
            source: StoragePath::S3 {
                bucket: "src".to_string(),
                prefix: String::new(),
            },
            target: StoragePath::Local(PathBuf::from("/tmp")),
            show_progress: false,
            source_client_config: Some(client_config),
            target_client_config: None,
            tracing_config: None,
            transfer_config: TransferConfig {
                multipart_threshold: threshold,
                multipart_chunksize: chunksize,
                auto_chunksize: false,
            },
            disable_tagging: false,
            server_side_copy: false,
            no_guess_mime_type: false,
            disable_multipart_verify: false,
            disable_etag_verify: false,
            disable_additional_checksum_verify: false,
            storage_class: None,
            sse: None,
            sse_kms_key_id: crate::types::SseKmsKeyId { id: None },
            source_sse_c: None,
            source_sse_c_key: SseCustomerKey { key: None },
            source_sse_c_key_md5: None,
            target_sse_c: None,
            target_sse_c_key: SseCustomerKey { key: None },
            target_sse_c_key_md5: None,
            canned_acl: None,
            additional_checksum_mode: None,
            additional_checksum_algorithm: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_type: None,
            expires: None,
            metadata: None,
            no_sync_system_metadata: false,
            no_sync_user_defined_metadata: false,
            website_redirect: None,
            tagging: None,
            put_last_modified_metadata: false,
            disable_payload_signing: false,
            disable_content_md5_header: false,
            full_object_checksum: false,
            source_accelerate: false,
            target_accelerate: false,
            source_request_payer: false,
            target_request_payer: false,
            if_none_match: false,
            disable_stalled_stream_protection: false,
            disable_express_one_zone_additional_checksum: false,
            max_parallel_uploads: parallel as u16,
            rate_limit_bandwidth: None,
            version_id: None,
            is_stdio_source: false,
            is_stdio_target: true,
            no_fail_on_verify_error: false,
            skip_existing: false,
            dry_run: false,
        }
    }

    /// Run `transfer()` against `mock` collecting stdout into a Vec.
    /// Returns (transfer result, captured bytes, captured stat events).
    async fn run_transfer(
        config: Config,
        mock: MockSource,
    ) -> (Result<TransferOutcome>, Vec<u8>, Vec<SyncStatistics>) {
        let writer = VecWriter::new();
        let buf = writer.buf();
        let token = create_pipeline_cancellation_token();
        let (stats_tx, stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let mock_source: Storage = Box::new(mock);
        let result = transfer(&config, mock_source, "k", writer, token, stats_tx).await;

        let mut events = Vec::new();
        while let Ok(s) = stats_rx.try_recv() {
            events.push(s);
        }
        let bytes = buf.lock().unwrap().clone();
        (result, bytes, events)
    }

    #[tokio::test]
    async fn serial_path_writes_full_body_to_stdout_when_max_parallel_uploads_is_one() {
        // 4 KiB body, max_parallel_uploads=1 → existing serial loop runs end-to-end.
        let body: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone()).with_e_tag("\"d41d8cd98f00b204e9800998ecf8427e\"");
        let config = test_config(
            /* parallel */ 1,
            /* threshold */ 8 * 1024 * 1024,
            /* chunksize */ 8 * 1024 * 1024,
        );

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        // No HEAD on the serial path.
        assert_eq!(mock.head_calls(), 0);
        // Single non-ranged GET.
        assert_eq!(mock.get_calls(), 1);
    }

    #[tokio::test]
    async fn dispatcher_falls_back_to_serial_when_object_below_threshold() {
        // size = 1 MiB, threshold = 8 MiB ⇒ HEAD probes size, then serial path.
        let body: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone());
        let config = test_config(
            /* parallel */ 4,
            /* threshold */ 8 * 1024 * 1024,
            /* chunksize */ 8 * 1024 * 1024,
        );

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        // Exactly one HEAD on the parallel-eligible path.
        assert_eq!(mock.head_calls(), 1);
        // Then a single non-ranged GET via serial.
        assert_eq!(mock.get_calls(), 1);
    }

    #[tokio::test]
    async fn dispatcher_skips_head_when_max_parallel_uploads_is_one() {
        // Size larger than threshold but N=1 ⇒ no HEAD, straight to serial.
        let body: Vec<u8> = vec![0x42; 12 * 1024 * 1024];
        let mock = MockSource::new(body.clone());
        let config = test_config(1, 8 * 1024 * 1024, 8 * 1024 * 1024);

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured.len(), body.len());
        assert_eq!(mock.head_calls(), 0);
        assert_eq!(mock.get_calls(), 1);
    }

    #[tokio::test]
    async fn parallel_writes_bytes_in_order_when_chunks_arrive_out_of_order() {
        // 24 MiB body, threshold 8 MiB, chunksize 8 MiB ⇒ 3 chunks.
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = (0..3 * chunksize).map(|i| (i % 251) as u8).collect();
        let mock = MockSource::new(body.clone());
        // Slow chunk 0, fast chunks 1 & 2 — forces drain ordering.
        let mock = mock
            .delay_get_at(0, Duration::from_millis(80))
            .delay_get_at(chunksize as u64, Duration::from_millis(20));
        let config = test_config(
            /* parallel */ 4,
            /* threshold */ 8 * 1024 * 1024,
            /* chunksize */ chunksize as u64,
        );

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(
            captured, body,
            "bytes flushed must equal source body in order"
        );
        assert_eq!(mock.head_calls(), 1);
        assert_eq!(mock.get_calls(), 3);
    }

    #[tokio::test]
    async fn parallel_concurrent_get_count_never_exceeds_max_parallel_uploads() {
        // 8 chunks, N=3, every chunk delayed so more would-be concurrent if
        // gate were absent. Assert peak in_flight ≤ 3.
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0xAB; 8 * chunksize];
        let mut mock = MockSource::new(body.clone());
        for i in 0..8u64 {
            mock = mock.delay_get_at(i * chunksize as u64, Duration::from_millis(40));
        }
        let config = test_config(3, 8 * 1024 * 1024, chunksize as u64);

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured.len(), body.len());
        assert!(
            mock.peak_in_flight_gets() <= 3,
            "peak in-flight gets {} exceeded N=3",
            mock.peak_in_flight_gets()
        );
        assert_eq!(mock.get_calls(), 8);
    }

    #[tokio::test]
    async fn parallel_single_chunk_path_succeeds() {
        // size 6 MiB, threshold 5 MiB, chunksize 16 MiB ⇒ 1 chunk total
        // even though we're on the parallel path.
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i & 0xFF) as u8).collect();
        let mock = MockSource::new(body.clone());
        let config = test_config(4, 5 * 1024 * 1024, 16 * 1024 * 1024);

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert_eq!(mock.head_calls(), 1);
        // Exactly one ranged GET (one chunk).
        assert_eq!(mock.get_calls(), 1);
    }

    /// Compute a multipart-style ETag matching exactly what production
    /// `generate_e_tag_hash` produces. Delegating instead of inlining the
    /// `format!` guarantees byte-equality, so `verify_e_tag` returns
    /// `Some(true)` in the verified-happy-path test.
    fn compute_multipart_etag(parts: &[&[u8]]) -> String {
        let mut concat = Vec::new();
        for p in parts {
            let d = md5::compute(p);
            concat.extend_from_slice(d.as_slice());
        }
        generate_e_tag_hash(&concat, parts.len() as i64)
    }

    #[tokio::test]
    async fn parallel_emits_etag_verified_when_etag_matches() {
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = (0..3 * chunksize).map(|i| (i % 211) as u8).collect();
        let parts: Vec<&[u8]> = body.chunks(chunksize).collect();
        let etag = compute_multipart_etag(&parts);

        let mock = MockSource::new(body.clone()).with_e_tag(&etag);
        let config = test_config(4, 8 * 1024 * 1024, chunksize as u64);

        let (result, captured, events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok());
        assert_eq!(captured, body);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagVerified { key } if key == "k"))
        );
        assert!(!mock.warning_set());
    }

    #[tokio::test]
    async fn parallel_emits_etag_mismatch_warning_when_etag_differs() {
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0x01; 3 * chunksize];
        let mock = MockSource::new(body.clone()).with_e_tag("\"deadbeef-3\"");
        let config = test_config(4, 8 * 1024 * 1024, chunksize as u64);

        let (result, _captured, events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "verify mismatch must NOT fail the transfer");
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagMismatch { key } if key == "k"))
        );
        assert!(
            mock.warning_set(),
            "set_warning() must be called on mismatch"
        );
    }

    #[tokio::test]
    async fn parallel_skips_etag_for_express_onezone() {
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0x55; 2 * chunksize];
        let mock = MockSource::new(body.clone())
            .with_e_tag("\"anything\"")
            .express_onezone();
        let config = test_config(4, 8 * 1024 * 1024, chunksize as u64);

        let (result, _captured, events) = run_transfer(config, mock).await;

        assert!(result.is_ok());
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagVerified { .. }))
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagMismatch { .. }))
        );
    }

    #[tokio::test]
    async fn parallel_disable_etag_verify_skips_compute_and_verify() {
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0x99; 2 * chunksize];
        let mock = MockSource::new(body.clone()).with_e_tag("\"would-mismatch\"");
        let mut config = test_config(4, 8 * 1024 * 1024, chunksize as u64);
        config.disable_etag_verify = true;

        let (result, captured, events) = run_transfer(config, mock).await;

        assert!(result.is_ok());
        assert_eq!(captured, body);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagVerified { .. }))
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ETagMismatch { .. }))
        );
    }

    /// `disable_additional_checksum_verify` for the s3 → stdout
    /// direction. The CLI gates `--disable-additional-checksum-verify`
    /// on `requires = "additional_checksum_algorithm"`, and
    /// `--additional-checksum-algorithm` is rejected when the target
    /// isn't S3 — so the flag combination is unreachable through the
    /// CLI for s3 → stdout (see the comment block above the
    /// `s3_to_stdout_disable_etag_and_multipart_verify_compose_silently`
    /// e2e test). The underlying production gate
    /// (`!config.disable_additional_checksum_verify` inside
    /// transfer_parallel) IS reachable by constructing a Config
    /// directly. This test pins that gate: with both
    /// `additional_checksum_mode = Some(Enabled)` and
    /// `disable_additional_checksum_verify = true`, the parallel
    /// path issues no `ChecksumVerified` / `ChecksumMismatch`
    /// events even when the source has a stored composite SHA256
    /// that would otherwise verify cleanly. ETag verification is
    /// independent and still fires.
    #[tokio::test]
    async fn parallel_disable_additional_checksum_verify_skips_checksum_events() {
        use aws_sdk_s3::types::ChecksumMode;
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = (0..3 * chunksize).map(|i| (i % 191) as u8).collect();
        let parts: Vec<&[u8]> = body.chunks(chunksize).collect();
        let composite = compute_composite_sha256(&parts);

        let mock = MockSource::new(body.clone()).with_sha256(&composite);
        let mut config = test_config(4, 8 * 1024 * 1024, chunksize as u64);
        config.additional_checksum_mode = Some(ChecksumMode::Enabled);
        config.disable_additional_checksum_verify = true;

        let (result, captured, events) = run_transfer(config, mock).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ChecksumVerified { .. })),
            "disable_additional_checksum_verify must suppress ChecksumVerified \
             even when the source's composite SHA256 would have matched"
        );
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ChecksumMismatch { .. })),
            "and must suppress ChecksumMismatch by symmetry"
        );
    }

    /// Compute a multipart-composite SHA256 matching what production
    /// `AdditionalChecksum::finalize_all` for SHA256 emits. The format is
    /// `{base64-STANDARD(SHA256(concat-of-per-part-SHA256-bytes))}-{N}`.
    fn compute_composite_sha256(parts: &[&[u8]]) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use sha2::{Digest, Sha256};
        let mut concat = Vec::new();
        for p in parts {
            let mut h = Sha256::new();
            h.update(p);
            concat.extend_from_slice(&h.finalize());
        }
        let mut h = Sha256::new();
        h.update(&concat);
        let composite = STANDARD.encode(h.finalize());
        format!("{composite}-{}", parts.len())
    }

    /// Compute a full-object SHA256 matching what production
    /// `AdditionalChecksum::finalize` (single-part / full-object mode) emits.
    fn compute_full_sha256(body: &[u8]) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(body);
        STANDARD.encode(h.finalize())
    }

    #[tokio::test]
    async fn parallel_emits_checksum_verified_for_multipart_composite() {
        use aws_sdk_s3::types::ChecksumMode;
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = (0..3 * chunksize).map(|i| (i % 191) as u8).collect();
        let parts: Vec<&[u8]> = body.chunks(chunksize).collect();
        let composite = compute_composite_sha256(&parts);

        let mock = MockSource::new(body.clone()).with_sha256(&composite);
        let mut config = test_config(4, 8 * 1024 * 1024, chunksize as u64);
        config.additional_checksum_mode = Some(ChecksumMode::Enabled);

        let (result, captured, events) = run_transfer(config, mock).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ChecksumVerified { key } if key == "k"))
        );
    }

    #[tokio::test]
    async fn parallel_emits_checksum_verified_for_full_object_checksum() {
        use aws_sdk_s3::types::ChecksumMode;
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = (0..3 * chunksize).map(|i| (i % 173) as u8).collect();
        let full = compute_full_sha256(&body);

        let mock = MockSource::new(body.clone()).with_sha256(&full);
        let mut config = test_config(4, 8 * 1024 * 1024, chunksize as u64);
        config.additional_checksum_mode = Some(ChecksumMode::Enabled);
        config.full_object_checksum = true;

        let (result, captured, events) = run_transfer(config, mock).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert!(
            events
                .iter()
                .any(|e| matches!(e, SyncStatistics::ChecksumVerified { .. }))
        );
    }

    #[tokio::test]
    async fn parallel_returns_err_on_full_object_checksum_mismatch() {
        use aws_sdk_s3::types::ChecksumMode;
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0x01; 3 * chunksize];

        let mock =
            MockSource::new(body).with_sha256("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=");
        let mut config = test_config(4, 8 * 1024 * 1024, chunksize as u64);
        config.additional_checksum_mode = Some(ChecksumMode::Enabled);
        config.full_object_checksum = true;

        let (result, _captured, _events) = run_transfer(config, mock).await;

        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("additional checksum mismatch"),
            "expected hard error on full-object mismatch, got: {err:?}"
        );
    }

    #[tokio::test]
    async fn parallel_returns_err_when_a_chunk_get_fails() {
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0xCC; 4 * chunksize];
        // Fail chunk index 2 (offset = 2 * chunksize).
        let mock = MockSource::new(body.clone()).fail_get_at(2 * chunksize as u64);
        let config = test_config(4, 8 * 1024 * 1024, chunksize as u64);

        let (result, captured, _events) = run_transfer(config, mock).await;

        assert!(result.is_err());
        // Captured bytes (if any) must be a strict prefix of the body.
        assert!(
            body.starts_with(&captured),
            "stdout output is not a prefix of the source body"
        );
    }

    #[tokio::test]
    async fn parallel_returns_err_when_head_fails() {
        let body: Vec<u8> = vec![0; 16 * 1024 * 1024];
        let mock = MockSource::new(body).fail_head();
        let config = test_config(4, 8 * 1024 * 1024, 8 * 1024 * 1024);

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_err());
        assert!(
            captured.is_empty(),
            "no bytes should be flushed when HEAD fails"
        );
        assert_eq!(mock.head_calls(), 1);
        assert_eq!(mock.get_calls(), 0);
    }

    // Test 8 from the spec ("parallel_returns_err_when_content_range_mismatch")
    // is omitted: validating mismatched content_range is the responsibility of
    // first_chunk::validate_content_range (covered by its own tests). Forcing
    // a mismatch here would require a separate mock variant that returns the
    // wrong range — net new coverage is zero, since the validator is the
    // shared component being relied on.

    #[tokio::test]
    async fn parallel_returns_default_outcome_when_cancelled_before_dispatch() {
        let body: Vec<u8> = vec![0; 16 * 1024 * 1024];
        let mock = MockSource::new(body);
        let config = test_config(4, 8 * 1024 * 1024, 8 * 1024 * 1024);

        let writer = VecWriter::new();
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let mock_source: Storage = Box::new(mock.clone());
        let result = transfer(&config, mock_source, "k", writer, token, stats_tx).await;

        // Pre-cancelled token returns the default outcome (matches serial).
        assert!(result.is_ok());
        assert_eq!(mock.head_calls(), 0);
        assert_eq!(mock.get_calls(), 0);
    }

    #[tokio::test]
    async fn parallel_returns_cancelled_when_token_cancelled_mid_download() {
        // 4 chunks. Slow chunk 0 (1 second). Cancel after 100ms.
        let chunksize = 8 * 1024 * 1024usize;
        let body: Vec<u8> = vec![0; 4 * chunksize];
        let mock = MockSource::new(body).delay_get_at(0, Duration::from_secs(1));
        let config = test_config(4, 8 * 1024 * 1024, chunksize as u64);

        let writer = VecWriter::new();
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let mock_source: Storage = Box::new(mock);
        let token_for_canceller = token.clone();
        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            token_for_canceller.cancel();
        });

        let result = transfer(&config, mock_source, "k", writer, token, stats_tx).await;
        canceller.await.unwrap();

        let err = result.expect_err("expected Cancelled");
        assert!(
            err.to_string().to_lowercase().contains("cancel"),
            "expected cancel error, got: {err:?}"
        );
    }

    /// Regression: when --auto-chunksize is set, the dispatcher must
    /// take the parallel path regardless of source_size vs
    /// multipart_threshold. transfer_parallel must then build its
    /// chunk plan from the source's actual part layout (via
    /// `get_object_parts_attributes`), not from
    /// `multipart_chunksize` — non-uniform parts (e.g., a 16 MiB
    /// source uploaded with 5 MiB parts producing [5,5,5,1] MiB)
    /// require ALL chunk boundaries to match the source for the
    /// per-part composite hashes to verify.
    ///
    /// Setup: source_size = 6 MiB (well below the default 8 MiB
    /// threshold), auto_chunksize = true, multipart ETag, mock
    /// configured to return 2 parts of 3 MiB each via
    /// `get_object_parts_attributes`. The parallel path must:
    ///   1. HEAD once (dispatcher).
    ///   2. Call `get_object_parts_attributes` once (chunk plan).
    ///   3. Issue exactly 2 ranged GETs (one per source part).
    /// `head_object_first_part` must NOT be called — the new
    /// chunk-plan logic uses the parts list, not first-part HEAD.
    #[tokio::test]
    async fn dispatcher_takes_parallel_path_when_auto_chunksize_even_below_threshold() {
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone())
            .with_e_tag("\"abcdefabcdefabcdefabcdefabcdefab-2\"")
            .with_parts(&[3 * 1024 * 1024, 3 * 1024 * 1024]);
        let mut config = test_config(/* parallel */ 4, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert_eq!(mock.head_calls(), 1, "dispatcher must HEAD once");
        assert_eq!(
            mock.get_object_parts_attributes_calls(),
            1,
            "auto_chunksize on a multipart source must fetch parts attributes once"
        );
        assert_eq!(
            mock.head_first_part_calls(),
            0,
            "head_object_first_part must NOT be called — chunk plan now uses the parts list"
        );
        assert_eq!(
            mock.get_calls(),
            2,
            "auto_chunksize with 2 parts of 3MiB on a 6MiB body must \
             produce 2 ranged GETs (parallel path), not a single full GET (serial)"
        );
    }

    /// REGRESSION: --auto-chunksize must take effect even when the user
    /// has set --max-parallel-uploads 1. The dispatcher's
    /// `max_parallel_uploads <= 1` short-circuit was unconditional and
    /// landed every such invocation in `transfer_serial`, which only
    /// knows uniform `multipart_chunksize` boundaries. On a multipart
    /// source whose parts don't match `multipart_chunksize` (here: 2
    /// parts of 3 MiB with default 8 MiB chunksize), the serial path
    /// produced a composite hash over the wrong part layout — verify
    /// failed with a bogus target ETag even though the body itself was
    /// byte-correct. Pin the routing: with auto_chunksize on, even at
    /// max_parallel_uploads=1 we must take the parallel path
    /// (worker_count clamps to 1) so chunk boundaries come from the
    /// source's actual parts list.
    #[tokio::test]
    async fn auto_chunksize_takes_parallel_path_even_when_max_parallel_uploads_is_one() {
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone())
            .with_e_tag("\"abcdefabcdefabcdefabcdefabcdefab-2\"")
            .with_parts(&[3 * 1024 * 1024, 3 * 1024 * 1024]);
        let mut config = test_config(/* parallel */ 1, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert_eq!(
            mock.head_calls(),
            1,
            "auto_chunksize must HEAD even at max_parallel_uploads=1 \
             (the serial short-circuit must not swallow auto_chunksize)"
        );
        assert_eq!(
            mock.get_object_parts_attributes_calls(),
            1,
            "auto_chunksize on a multipart source must fetch parts attributes"
        );
        assert_eq!(
            mock.get_calls(),
            2,
            "auto_chunksize with 2 parts must issue 2 ranged GETs even \
             at max_parallel_uploads=1 (1 worker draining 2 chunks), \
             not a single non-ranged GET via the serial path"
        );
    }

    /// REGRESSION: when `get_object_parts_attributes` returns empty
    /// (real S3 behavior for sources uploaded WITHOUT
    /// --additional-checksum-algorithm), the chunk plan must fall
    /// back to `get_object_parts` (per-part HEAD ?partNumber=N) and
    /// build chunks from those sizes. Without this fallback the e2e
    /// tests `s3_to_stdout_auto_chunksize_verifies_etag_for_5mib_parts_source`
    /// and `s3_to_stdout_auto_chunksize_verifies_etag_when_source_below_default_threshold`
    /// fail with sync_complete=0 because the chunk-plan logic
    /// hard-errors on empty attributes.
    #[tokio::test]
    async fn auto_chunksize_falls_back_to_get_object_parts_when_attributes_empty() {
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone())
            .with_e_tag("\"abcdefabcdefabcdefabcdefabcdefab-2\"")
            .with_parts(&[3 * 1024 * 1024, 3 * 1024 * 1024])
            .with_empty_parts_attributes();
        let mut config = test_config(/* parallel */ 4, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert_eq!(
            mock.get_object_parts_attributes_calls(),
            1,
            "attributes is tried first"
        );
        assert_eq!(
            mock.get_object_parts_calls(),
            1,
            "fallback to get_object_parts must fire when attributes returns empty"
        );
        assert_eq!(
            mock.get_calls(),
            2,
            "fallback yields 2 parts → 2 ranged GETs"
        );
    }

    /// Auto-chunksize on a singlepart source: no parts attributes to
    /// fetch (the source ETag has no `-N` suffix), so the chunk plan
    /// is one chunk covering the whole body.
    #[tokio::test]
    async fn auto_chunksize_singlepart_source_uses_single_chunk() {
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mock = MockSource::new(body.clone()).with_e_tag("\"abcdefabcdefabcdefabcdefabcdefab\"");
        let mut config = test_config(/* parallel */ 4, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert_eq!(captured, body);
        assert_eq!(
            mock.get_object_parts_attributes_calls(),
            0,
            "singlepart ETag → skip parts fetch"
        );
        assert_eq!(mock.get_calls(), 1, "single chunk → exactly one ranged GET");
    }

    /// Pathological auto_chunksize: source ETag claims multipart
    /// (`-N` suffix) but BOTH `get_object_parts_attributes` and
    /// `get_object_parts` return empty (e.g., the source was
    /// overwritten as singlepart between our HEAD and the parts
    /// call, or both APIs mis-reported). The transfer MUST error
    /// out — falling back to a single chunk = whole body would
    /// allocate `source_size` bytes in one Vec, which on a multi-GiB
    /// or 50 TB source is OOM-grade. Pin the hard-error behavior
    /// (recoverable: the user retries or removes --auto-chunksize).
    #[tokio::test]
    async fn auto_chunksize_errors_when_multipart_source_has_no_parts_metadata() {
        let body: Vec<u8> = vec![0u8; 6 * 1024 * 1024];
        let mock = MockSource::new(body)
            // Multipart-shaped ETag triggers the parts-fetch path.
            .with_e_tag("\"abcdefabcdefabcdefabcdefabcdefab-2\"")
            // ...but neither parts API has any parts to return.
            // (Default `with_parts(&[])`/no with_parts() ⇒ empty.)
            .with_empty_parts_attributes();
        let mut config = test_config(/* parallel */ 4, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock.clone()).await;

        let err = result.expect_err("must hard-error rather than allocate the whole body");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("auto_chunksize")
                && msg.contains("parts list")
                && msg.contains("Refusing to fall back"),
            "error message must explain the refusal and suggest a workaround, got: {msg}"
        );
        assert!(
            captured.is_empty(),
            "no bytes should be flushed on this path"
        );
        assert_eq!(
            mock.get_object_parts_attributes_calls(),
            1,
            "attributes is tried first"
        );
        assert_eq!(
            mock.get_object_parts_calls(),
            1,
            "fallback to get_object_parts is tried second"
        );
        assert_eq!(
            mock.get_calls(),
            0,
            "no ranged GETs issued — we bail before the worker pool"
        );
    }

    /// `ranged_get_into_buffer` previously took `chunk.len().min(remaining)`
    /// and consumed the whole buffer, silently discarding any bytes the
    /// upstream sent past the requested range. That hides a protocol /
    /// data-source anomaly (S3 ranged GETs MUST return exactly the
    /// requested bytes; an over-read means corruption upstream or in
    /// transit). Pin the over-read guard: the transfer must hard-error
    /// citing the requested range instead of silently truncating.
    #[tokio::test]
    async fn parallel_returns_err_when_ranged_get_returns_more_bytes_than_requested() {
        // Single-chunk path (chunksize 16 MiB ≥ body 6 MiB) so the over-
        // read shows up on exactly one ranged GET.
        let body: Vec<u8> = (0..6 * 1024 * 1024).map(|i| (i & 0xFF) as u8).collect();
        let mock = MockSource::new(body.clone()).with_over_read(1);
        let config = test_config(/* parallel */ 4, 5 * 1024 * 1024, 16 * 1024 * 1024);

        let (result, _captured, _events) = run_transfer(config, mock).await;

        let err =
            result.expect_err("transfer must error when GET returns more bytes than requested");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("over-read"),
            "error must explain the over-read; got: {msg}"
        );
    }

    /// auto_chunksize=true on a non-multipart, zero-byte object built a
    /// chunk plan of `[(0, 0)]`, then the dispatcher computed
    /// `0 + 0 - 1` (debug: panic; release: u64::MAX → bogus range
    /// `bytes=0-18446744073709551615`). Empty-body transfers must
    /// succeed without hitting the parallel chunk-plan path.
    #[tokio::test]
    async fn auto_chunksize_zero_byte_singlepart_source_does_not_underflow() {
        let mock = MockSource::new(Vec::new())
            // Empty-object MD5 — singlepart-shaped (no `-N` suffix), so
            // the multipart parts-fetch branch is skipped and we fall
            // into the size-derived chunk planner.
            .with_e_tag("\"d41d8cd98f00b204e9800998ecf8427e\"");
        let mut config = test_config(/* parallel */ 4, 8 * 1024 * 1024, 8 * 1024 * 1024);
        config.transfer_config.auto_chunksize = true;

        let (result, captured, _events) = run_transfer(config, mock).await;

        assert!(result.is_ok(), "transfer failed: {result:?}");
        assert!(
            captured.is_empty(),
            "captured bytes for an empty source must be empty, got {} byte(s)",
            captured.len()
        );
    }
}
