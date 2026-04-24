use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::debug;

use crate::Config;
use crate::storage::Storage;
use crate::transfer::{TransferOutcome, first_chunk, translate_source_head_object_error};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, detect_additional_checksum_with_head_object};

/// Transfer an S3 object to the local filesystem.
///
/// Uses first-chunk optimization: for objects at or above `multipart_threshold`,
/// only the first chunk is fetched initially via a range request. Local storage's
/// `put_object_multipart` then fetches remaining chunks in parallel (bounded by
/// `max_parallel_uploads`) and writes them to the destination file at their
/// offsets. For smaller objects, a single non-ranged `get_object` is issued.
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    source_key: &str,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // HEAD the source to learn the full object size and the composite/final
    // checksum. A ranged GET against a composite-multipart object does not
    // return the root composite checksum; HEAD does.
    let head_object_output = source
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
        .map_err(|e| translate_source_head_object_error(e, source_key))?;

    let source_size = head_object_output.content_length().unwrap_or(0);

    // Auto-detect checksum algorithm from HEAD. `additional_checksum_algorithm`
    // is rejected at CLI validation when the target is local, so only the mode
    // path applies.
    let (detected_algorithm, source_additional_checksum) =
        if config.additional_checksum_mode.is_some() {
            detect_additional_checksum_with_head_object(&head_object_output)
                .map(|(a, c)| (Some(a), Some(c)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    // Compute the first-chunk range. Returns None for objects below
    // `multipart_threshold` (or below 5 MiB absolute minimum) — in that case
    // we perform a single full-object GET just like before.
    let range = first_chunk::get_first_chunk_range(
        &*source,
        config,
        source_size,
        source_key,
        config.version_id.clone(),
    )
    .await?;

    debug!(
        key = source_key,
        size = source_size,
        range = range.as_deref(),
        "first chunk range for the object",
    );

    let get_object_output = source
        .get_object(
            source_key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            range.clone(),
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context(format!("failed to download source object: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    // Build object checksum. `build_object_checksum` branches on
    // content_range being populated to fetch the full part-size manifest when
    // needed for composite checksum verification. Pass `source_key` (the S3
    // key) so `get_object_parts_attributes` looks up parts on the source.
    let checksum_algorithms: Option<Vec<_>> = detected_algorithm.as_ref().map(|a| vec![a.clone()]);
    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        source_key,
        &get_object_output,
        checksum_algorithms.as_deref(),
        source_additional_checksum.clone(),
    )
    .await?;

    // Local storage dispatches on `content_range.is_some()`: when true, it
    // routes into `put_object_multipart`, which spawns parallel ranged GETs
    // against the source for the remaining chunks and writes each at the
    // correct offset in a temp file. `source_size` is the full object size
    // (not the first-chunk size) — required by the multipart write loop.
    let _put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
            get_object_output,
            None, // local storage ignores tagging
            object_checksum,
            None,
        )
        .await
        .context(format!("failed to write to target file: {target_key}"))?;

    debug!(
        source_key = source_key,
        target_key = target_key,
        size = source_size,
        "transfer completed."
    );

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
}
