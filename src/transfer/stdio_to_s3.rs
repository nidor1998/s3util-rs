use anyhow::{Context, Result};
use async_channel::Sender;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_smithy_types::DateTime;
use tokio::io::AsyncReadExt;
use tracing::info;

use crate::Config;
use crate::storage::Storage;
use crate::storage::checksum::AdditionalChecksum;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics};

/// Transfer data from an async reader (typically stdin) to an S3 object.
///
/// The reader is drained to memory so the total size is known before dispatch
/// to the upload manager — which decides single-part vs multipart based on
/// `multipart_threshold`. Tests inject `Cursor<Vec<u8>>`; the CLI binary
/// injects `tokio::io::stdin()`.
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    mut reader: impl tokio::io::AsyncRead + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let target_clone = dyn_clone::clone_box(&*target);

    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .context("failed to read from stdin")?;

    let source_size = buffer.len() as u64;

    // Compute source-side additional checksum over the buffered data so
    // UploadManager::validate_checksum can verify it against what S3 stored.
    // Mirrors the chunksize-aware path used by LocalStorage::get_object.
    let source_additional_checksum =
        config
            .additional_checksum_algorithm
            .clone()
            .map(|algorithm| {
                compute_source_checksum(
                    &buffer,
                    algorithm,
                    config.transfer_config.multipart_chunksize as usize,
                    config.transfer_config.multipart_threshold as usize,
                    config.full_object_checksum,
                )
            });

    // Populate the synthetic GetObjectOutput's checksum field so
    // S3Storage::put_object can extract it via get_additional_checksum().
    let (checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32_c, checksum_crc64_nvme) =
        match config.additional_checksum_algorithm.as_ref() {
            Some(ChecksumAlgorithm::Sha256) => {
                (source_additional_checksum.clone(), None, None, None, None)
            }
            Some(ChecksumAlgorithm::Sha1) => {
                (None, source_additional_checksum.clone(), None, None, None)
            }
            Some(ChecksumAlgorithm::Crc32) => {
                (None, None, source_additional_checksum.clone(), None, None)
            }
            Some(ChecksumAlgorithm::Crc32C) => {
                (None, None, None, source_additional_checksum.clone(), None)
            }
            Some(ChecksumAlgorithm::Crc64Nvme) => {
                (None, None, None, None, source_additional_checksum.clone())
            }
            _ => (None, None, None, None, None),
        };

    let byte_stream = ByteStream::from(buffer);

    let get_object_output = GetObjectOutput::builder()
        .set_body(Some(byte_stream))
        .set_content_length(Some(source_size as i64))
        .set_content_type(config.content_type.clone())
        .set_last_modified(Some(DateTime::from_secs(chrono::Utc::now().timestamp())))
        .set_checksum_sha256(checksum_sha256)
        .set_checksum_sha1(checksum_sha1)
        .set_checksum_crc32(checksum_crc32)
        .set_checksum_crc32_c(checksum_crc32_c)
        .set_checksum_crc64_nvme(checksum_crc64_nvme)
        .build();

    let tagging = if config.disable_tagging {
        None
    } else {
        config.tagging.clone()
    };

    let object_checksum = ObjectChecksum {
        key: target_key.to_string(),
        version_id: None,
        checksum_algorithm: config.additional_checksum_algorithm.clone(),
        checksum_type: None,
        object_parts: None,
        final_checksum: None,
    };

    let _put_object_output = target
        .put_object(
            target_key,
            target_clone,
            target_key,
            source_size,
            source_additional_checksum,
            get_object_output,
            tagging,
            Some(object_checksum),
            None,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    info!(target_key = target_key, "stdin transfer completed.");

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}

/// Compute the additional checksum over an in-memory buffer using chunksize-aware
/// semantics identical to `generate_checksum_from_path_with_chunksize`:
/// if the total size is below `multipart_threshold`, return a single-part
/// checksum; otherwise return the multipart composite checksum.
fn compute_source_checksum(
    buffer: &[u8],
    algorithm: ChecksumAlgorithm,
    multipart_chunksize: usize,
    multipart_threshold: usize,
    full_object_checksum: bool,
) -> String {
    let mut checksum = AdditionalChecksum::new(algorithm, full_object_checksum);

    if buffer.len() < multipart_threshold {
        checksum.update(buffer);
        return checksum.finalize();
    }

    let mut offset = 0;
    while offset < buffer.len() {
        let end = std::cmp::min(offset + multipart_chunksize, buffer.len());
        checksum.update(&buffer[offset..end]);
        let _ = checksum.finalize();
        offset = end;
    }

    checksum.finalize_all()
}
