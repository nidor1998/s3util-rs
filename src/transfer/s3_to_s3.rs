use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{debug, info, warn};

use crate::Config;
use crate::storage::{Storage, convert_head_to_get_object_output, parse_range_header_string};
use crate::transfer::{first_chunk, translate_source_head_object_error};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, get_additional_checksum};

/// Transfer an S3 object from one S3 location to another.
///
/// Supports two modes:
/// - Server-side copy: uses head_object on source + put_object with copy_source on target
/// - Download + upload: get_object from source + put_object to target
///
/// Uses first-chunk optimization: for objects above the multipart threshold,
/// only the first chunk is fetched initially via a range request. The upload
/// manager then fetches remaining chunks lazily during multipart upload.
/// Ported from s3sync/src/pipeline/syncer.rs.
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    source_key: &str,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // Get source size via head_object (without range) to calculate first chunk range.
    // For server-side copy, we also need the metadata from this head_object.
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
    let source_tag_count = head_object_output.tag_count();

    // Get the first chunk range if multipart upload is required.
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

    let get_object_output = if config.server_side_copy {
        // Server-side copy: the first HEAD above already returned every metadata
        // field we need. When a first-chunk range is active, downstream code
        // (validate_content_range, upload_manager first-part sizing) expects
        // content_length and content_range in the ranged-GET shape — synthesize
        // them from the range string and the full source size instead of paying
        // for a second ranged HEAD round-trip.
        let range_override = if let Some(range_str) = range.as_deref() {
            let (start, end) = parse_range_header_string(range_str)
                .context("failed to parse first-chunk range header")?;
            Some((start, end, source_size as u64))
        } else {
            None
        };
        convert_head_to_get_object_output(head_object_output, range_override)
    } else {
        // Download + upload: download from source with range
        source
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
            .context(format!("failed to download source object: {source_key}"))?
    };

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    // Validate content range if range was used
    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    // Raw source checksum from the (possibly ranged) GetObject response.
    // Used as a fallback when final_checksum isn't fetched (e.g., --enable-additional-checksum
    // not set but the SDK still surfaces a full-object checksum like CRC64NVME).
    let source_additional_checksum_raw = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Get tagging
    let tagging = if config.disable_tagging {
        None
    } else if config.tagging.is_some() {
        config.tagging.clone()
    } else if source_tag_count.is_none_or(|count| count == 0) {
        // Skip get_object_tagging when the source has no tags.
        // Notably, S3 Express One Zone buckets don't support tagging —
        // HeadObject returns no tag_count, and GetObjectTagging would fail.
        None
    } else {
        let tagging_output = source_clone
            .get_object_tagging(source_key, config.version_id.clone())
            .await
            .context(format!("failed to get source object tagging: {source_key}"))?;
        if tagging_output.tag_set().is_empty() {
            None
        } else {
            Some(
                tagging_output
                    .tag_set()
                    .iter()
                    .map(|tag| {
                        format!(
                            "{}={}",
                            urlencoding::encode(tag.key()),
                            urlencoding::encode(tag.value())
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("&"),
            )
        }
    };

    // Build checksum algorithm slice matching s3sync's object.checksum_algorithm() format
    let checksum_algorithms: Option<Vec<_>> = config
        .additional_checksum_algorithm
        .as_ref()
        .map(|a| vec![a.clone()]);
    let checksum_algorithm_slice = checksum_algorithms.as_deref();

    // Build object checksum using the s3sync-ported helpers
    let final_checksum = first_chunk::get_final_checksum(
        &*source,
        config,
        &get_object_output,
        range.as_deref(),
        source_key,
        config.version_id.clone(),
        checksum_algorithm_slice,
    )
    .await;

    // Pass source_key so that get_object_parts_attributes looks up parts on the source bucket.
    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        source_key,
        &get_object_output,
        checksum_algorithm_slice,
        final_checksum.clone(),
    )
    .await?;

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
    };

    // Prefer final_checksum (HEAD-fetched for ranged requests when --enable-additional-checksum
    // is set) over the raw ranged response. For composite multipart checksums (SHA256 etc.),
    // a ranged GET doesn't carry the composite value, but HEAD does. When mode is not set,
    // get_final_checksum returns None and we fall back to whatever the ranged GET exposed.
    let source_checksum_for_verify = final_checksum.clone().or(source_additional_checksum_raw);

    let put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_checksum_for_verify,
            get_object_output,
            tagging,
            object_checksum,
            if_none_match,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    if put_object_output.e_tag.is_some() {
        info!(
            source_key = source_key,
            target_key = target_key,
            size = source_size,
            "transfer completed."
        );
    } else {
        warn!(
            source_key = source_key,
            target_key = target_key,
            "transfer completed but no ETag returned."
        );
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}
