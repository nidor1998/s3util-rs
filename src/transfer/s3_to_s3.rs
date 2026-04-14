use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{info, warn};

use crate::Config;
use crate::storage::{Storage, convert_head_to_get_object_output};
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics, get_additional_checksum};

/// Transfer an S3 object from one S3 location to another.
///
/// Supports two modes:
/// - Server-side copy: uses head_object on source + put_object with copy_source on target
/// - Download + upload: get_object from source + put_object to target
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

    let get_object_output = if config.server_side_copy {
        // Server-side copy: use head_object to get metadata, no actual download
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
            .context("s3_to_s3: source.head_object() failed.")?;
        convert_head_to_get_object_output(head_object_output)
    } else {
        // Download + upload: download from source
        source
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
            .context("s3_to_s3: source.get_object() failed.")?
    };

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_size = get_object_output.content_length().unwrap_or(0) as u64;

    let source_additional_checksum = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Get tagging
    let tagging = if config.disable_tagging {
        None
    } else if config.tagging.is_some() {
        config.tagging.clone()
    } else {
        // Try to get tagging from source
        let tagging_output = source_clone
            .get_object_tagging(source_key, config.version_id.clone())
            .await;
        if let Ok(tagging_output) = tagging_output {
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
        } else {
            None
        }
    };

    // Build object checksum
    let object_checksum = ObjectChecksum {
        key: target_key.to_string(),
        version_id: config.version_id.clone(),
        checksum_algorithm: config.additional_checksum_algorithm.clone(),
        checksum_type: None,
        object_parts: None,
        final_checksum: source_additional_checksum.clone(),
    };

    // Build copy_source_if_match from source ETag if config requires it
    let copy_source_if_match = if config.copy_source_if_match {
        get_object_output.e_tag().map(|etag| etag.to_string())
    } else {
        None
    };

    let put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_size,
            source_additional_checksum,
            get_object_output,
            tagging,
            Some(object_checksum),
            None,
            None,
            copy_source_if_match,
        )
        .await
        .context("s3_to_s3: target.put_object() failed.")?;

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
