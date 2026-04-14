use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{info, warn};

use crate::Config;
use crate::storage::Storage;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics, get_additional_checksum};

/// Transfer an S3 object to the local filesystem.
///
/// The source Storage (S3) provides get_object() which downloads the object,
/// and the target Storage (local) provides put_object() which writes the file
/// and performs verification (ETag / checksum).
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

    // Get object from S3 source
    let get_object_output = source
        .get_object(
            key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context("s3_to_local: source.get_object() failed.")?;

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_size = get_object_output.content_length().unwrap_or(0) as u64;

    let source_additional_checksum = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Build object checksum
    let object_checksum = ObjectChecksum {
        key: key.to_string(),
        version_id: config.version_id.clone(),
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
            None, // Local storage ignores tagging
            Some(object_checksum),
            None,
            None,
            None,
        )
        .await
        .context("s3_to_local: target.put_object() failed.")?;

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
