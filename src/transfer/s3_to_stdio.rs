use anyhow::{Context, Result};
use async_channel::Sender;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::Config;
use crate::storage::Storage;
use crate::types::SyncStatistics;
use crate::types::token::PipelineCancellationToken;

/// Transfer an S3 object to stdout.
///
/// Downloads the object from S3 via source.get_object() and writes the body
/// chunks directly to stdout.
pub async fn transfer(
    config: &Config,
    source: Storage,
    source_key: &str,
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
        .context("s3_to_stdio: source.get_object() failed.")?;

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let content_length = get_object_output.content_length().unwrap_or(0) as u64;

    // Write body to stdout
    let mut stdout = tokio::io::stdout();
    let mut body = get_object_output.body.into_async_read();
    let mut total_bytes = 0u64;

    let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer
    loop {
        if cancellation_token.is_cancelled() {
            break;
        }

        let n = tokio::io::AsyncReadExt::read(&mut body, &mut buf)
            .await
            .context("s3_to_stdio: failed to read body")?;
        if n == 0 {
            break;
        }

        stdout
            .write_all(&buf[..n])
            .await
            .context("s3_to_stdio: failed to write to stdout")?;

        total_bytes += n as u64;
        let _ = stats_sender.send(SyncStatistics::SyncBytes(n as u64)).await;
    }

    stdout
        .flush()
        .await
        .context("s3_to_stdio: failed to flush stdout")?;

    info!(
        source_key = source_key,
        size = content_length,
        bytes_written = total_bytes,
        "stdout transfer completed."
    );

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: source_key.to_string(),
        })
        .await;

    Ok(())
}
