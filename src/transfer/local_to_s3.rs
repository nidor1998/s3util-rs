use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{info, warn};

use crate::Config;
use crate::storage::Storage;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics, get_additional_checksum};

/// Transfer a local file to S3.
///
/// The key is derived from the local filename (basename of the source path).
/// The source Storage (local) provides get_object() which reads the file,
/// and the target Storage (S3) provides put_object() which handles upload
/// (including multipart if needed).
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // Get object from local storage
    let get_object_output = source
        .get_object(
            key,
            None,
            config.additional_checksum_mode.clone(),
            None,
            None,
            crate::types::SseCustomerKey { key: None },
            None,
        )
        .await
        .context("local_to_s3: source.get_object() failed.")?;

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_size = get_object_output.content_length().unwrap_or(0) as u64;

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

    // Build object checksum
    let object_checksum = ObjectChecksum {
        key: key.to_string(),
        version_id: None,
        checksum_algorithm: config.additional_checksum_algorithm.clone(),
        checksum_type: None,
        object_parts: None,
        final_checksum: source_additional_checksum.clone(),
    };

    let put_object_output = target
        .put_object(
            key,
            source_clone,
            source_size,
            source_additional_checksum,
            get_object_output,
            tagging,
            Some(object_checksum),
            None,
            None,
            None,
        )
        .await
        .context("local_to_s3: target.put_object() failed.")?;

    if put_object_output.e_tag.is_some() {
        info!(key = key, size = source_size, "transfer completed.");
    } else {
        warn!(key = key, "transfer completed but no ETag returned.");
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: key.to_string(),
        })
        .await;

    Ok(())
}
