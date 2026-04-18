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

/// Read up to `limit` bytes from `reader` into a fresh `Vec<u8>`.
/// If the returned Vec's length is `< limit`, the reader reached EOF.
/// If `== limit`, the limit was reached and the reader may have more data.
/// `limit` must be > 0; with `limit = 0` the EOF vs limit-reached distinction collapses.
#[allow(dead_code)] // temporary: caller added in Task 10 (transfer dispatch)
async fn probe_up_to<R: tokio::io::AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    limit: usize,
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(limit);
    (&mut *reader)
        .take(limit as u64)
        .read_to_end(&mut buf)
        .await
        .context("probe_up_to: failed to read from reader")?;
    Ok(buf)
}

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

    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .context("failed to read from stdin")?;

    transfer_buffered(
        config,
        target,
        target_key,
        buffer,
        cancellation_token,
        stats_sender,
    )
    .await
}

async fn transfer_buffered(
    config: &Config,
    target: Storage,
    target_key: &str,
    buffer: Vec<u8>,
    _cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    let target_clone = dyn_clone::clone_box(&*target);

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

#[cfg(test)]
mod probe_tests {
    use super::probe_up_to;
    use std::io::Cursor;

    #[tokio::test]
    async fn returns_all_bytes_when_reader_smaller_than_limit() {
        let mut reader = Cursor::new(vec![1u8; 30]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 30);
        assert_eq!(buf, vec![1u8; 30]);
    }

    #[tokio::test]
    async fn returns_exactly_limit_bytes_when_reader_larger() {
        let mut reader = Cursor::new(vec![2u8; 200]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 100);
        assert_eq!(buf, vec![2u8; 100]);
    }

    #[tokio::test]
    async fn returns_limit_bytes_when_reader_exactly_limit() {
        let mut reader = Cursor::new(vec![3u8; 100]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 100);
        assert_eq!(buf, vec![3u8; 100]);
    }

    #[tokio::test]
    async fn returns_empty_for_empty_reader() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn leaves_remaining_bytes_in_reader() {
        let data = vec![5u8; 50];
        let mut reader = Cursor::new(data);
        let probed = probe_up_to(&mut reader, 20).await.unwrap();
        assert_eq!(probed, vec![5u8; 20]);
        // Read the rest — should be 30 bytes left
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut rest)
            .await
            .unwrap();
        assert_eq!(rest.len(), 30);
    }
}

#[cfg(test)]
mod checksum_invariant_tests {
    use super::compute_source_checksum;
    use crate::storage::checksum::AdditionalChecksum;
    use aws_sdk_s3::types::ChecksumAlgorithm;

    fn all_algorithms() -> Vec<ChecksumAlgorithm> {
        vec![
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
            ChecksumAlgorithm::Crc64Nvme,
        ]
    }

    // Simulates what upload_parts_stream will do: chunked update/finalize,
    // then one finalize_all at EOF.
    fn streaming_checksum(
        buffer: &[u8],
        algorithm: ChecksumAlgorithm,
        multipart_chunksize: usize,
        full_object_checksum: bool,
    ) -> String {
        let mut c = AdditionalChecksum::new(algorithm, full_object_checksum);
        let mut offset = 0;
        while offset < buffer.len() {
            let end = std::cmp::min(offset + multipart_chunksize, buffer.len());
            c.update(&buffer[offset..end]);
            let _ = c.finalize();
            offset = end;
        }
        c.finalize_all()
    }

    #[test]
    fn streaming_matches_buffered_for_multipart_sizes() {
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0xABu8; chunksize * 4 + 17]; // 4 full chunks + partial

        for algo in all_algorithms() {
            let batched =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let streamed = streaming_checksum(&buffer, algo.clone(), chunksize, false);
            assert_eq!(
                batched, streamed,
                "algorithm {:?}: batched vs streamed checksum mismatch",
                algo
            );
        }
    }

    #[test]
    fn streaming_matches_buffered_for_exact_chunksize_multiples() {
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0x5Au8; chunksize * 3];

        for algo in all_algorithms() {
            let batched =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let streamed = streaming_checksum(&buffer, algo.clone(), chunksize, false);
            assert_eq!(batched, streamed, "algorithm {:?}", algo);
        }
    }
}
