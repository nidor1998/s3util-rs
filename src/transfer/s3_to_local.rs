use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::info;

use crate::Config;
use crate::storage::Storage;
use crate::transfer::first_chunk;
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, detect_additional_checksum, get_additional_checksum};

/// Transfer an S3 object to the local filesystem.
///
/// The source Storage (S3) provides get_object() which downloads the object,
/// and the target Storage (local) provides put_object() which writes the file
/// and performs verification (ETag / checksum).
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

    // Detect checksum algorithm: use explicit config, or auto-detect from source response
    let (detected_algorithm, source_additional_checksum) =
        if let Some(algorithm) = config.additional_checksum_algorithm.clone() {
            let checksum = get_additional_checksum(&get_object_output, Some(algorithm.clone()));
            (Some(algorithm), checksum)
        } else if config.additional_checksum_mode.is_some() {
            if let Some((algorithm, checksum)) = detect_additional_checksum(&get_object_output) {
                (Some(algorithm), Some(checksum))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

    // Build object checksum. Uses build_object_checksum so that object_parts is populated
    // from the source's multipart manifest, which is required for composite multipart
    // checksum verification (SHA256/SHA1/CRC32/CRC32C). Ported from s3sync.
    // NOTE: pass `source_key` (the S3 key) not `target_key` (the local path) — the helper
    // fetches object parts from S3 via GetObjectAttributes on this key.
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

    let _put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size,
            source_additional_checksum,
            get_object_output,
            None, // Local storage ignores tagging
            object_checksum,
            None,
        )
        .await
        .context(format!("failed to write to target file: {target_key}"))?;

    info!(
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

    Ok(())
}
