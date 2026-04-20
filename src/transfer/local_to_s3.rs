use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{debug, info, warn};

use crate::Config;
use crate::storage::Storage;
use crate::transfer::first_chunk;
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, get_additional_checksum};

/// Transfer a local file to S3.
///
/// Uses first-chunk optimization: for files above the multipart threshold,
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

    // Get source size before get_object to calculate first chunk range
    let head_object_output = source
        .head_object(
            source_key,
            None,
            None,
            None,
            None,
            crate::types::SseCustomerKey { key: None },
            None,
        )
        .await
        .context(format!(
            "source file not found or not accessible: {source_key}"
        ))?;

    let source_size = head_object_output.content_length().unwrap_or(0);

    // Get the first chunk range if multipart upload is required.
    // If not, the whole object will be downloaded.
    let range =
        first_chunk::get_first_chunk_range(&*source, config, source_size, source_key, None).await?;

    debug!(
        key = source_key,
        size = source_size,
        range = range.as_deref(),
        "first chunk range for the object",
    );

    // Get object from local storage (with range if multipart)
    let get_object_output = source
        .get_object(
            source_key,
            None,
            config.additional_checksum_mode.clone(),
            range.clone(),
            None,
            crate::types::SseCustomerKey { key: None },
            None,
        )
        .await
        .context(format!("failed to read source file: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    // Validate content range if range was used
    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    let source_additional_checksum = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Get tagging from config
    let tagging = if config.disable_tagging {
        None
    } else {
        config.tagging.clone()
    };

    // Build checksum algorithm slice matching s3sync's object.checksum_algorithm() format
    let checksum_algorithms: Option<Vec<_>> = config
        .additional_checksum_algorithm
        .as_ref()
        .map(|a| vec![a.clone()]);
    let checksum_algorithm_slice = checksum_algorithms.as_deref();

    // Build object checksum using the s3sync-ported helper
    let final_checksum = first_chunk::get_final_checksum(
        &*source,
        config,
        &get_object_output,
        range.as_deref(),
        source_key,
        None,
        checksum_algorithm_slice,
    )
    .await;

    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        target_key,
        &get_object_output,
        checksum_algorithm_slice,
        final_checksum,
    )
    .await?;

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
    };

    let put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
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
