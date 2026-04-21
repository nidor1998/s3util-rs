use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, warn};

use crate::Config;
use crate::storage::Storage;
use crate::storage::additional_checksum_verify::is_multipart_upload_checksum;
use crate::storage::checksum::AdditionalChecksum;
use crate::storage::e_tag_verify::{generate_e_tag_hash, normalize_e_tag, verify_e_tag};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, detect_additional_checksum, is_full_object_checksum};

/// Transfer an S3 object to stdout with inline ETag and checksum verification.
///
/// Downloads the object from S3 via source.get_object(), writes the body
/// to stdout, and computes ETag (MD5) and additional checksums inline.
pub async fn transfer(
    config: &Config,
    source: Storage,
    source_key: &str,
    mut writer: impl tokio::io::AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
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
        return Ok(());
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
                    .send(SyncStatistics::SyncWarning {
                        key: source_key.to_string(),
                    })
                    .await;
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
                .send(SyncStatistics::SyncWarning {
                    key: source_key.to_string(),
                })
                .await;
        }
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: source_key.to_string(),
        })
        .await;

    Ok(())
}
