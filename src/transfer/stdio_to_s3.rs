use anyhow::{Context, Result};
use async_channel::Sender;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;
use aws_smithy_types::DateTime;
use tokio::io::AsyncReadExt;

use crate::Config;
use crate::storage::Storage;
use crate::storage::checksum::AdditionalChecksum;
use crate::transfer::TransferOutcome;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics};

/// Read up to `limit` bytes from `reader` into a fresh `Vec<u8>`.
/// If the returned Vec's length is `< limit`, the reader reached EOF.
/// If `== limit`, the limit was reached and the reader may have more data.
/// `limit` must be > 0; with `limit = 0` the EOF vs limit-reached distinction collapses.
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
/// Probes up to `multipart_threshold` bytes. If the reader hits EOF before the
/// threshold the data stays fully in memory (`transfer_buffered`). If the
/// threshold is reached the remaining stream is chained and uploaded via
/// `transfer_streaming` → `put_object_stream`.
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let threshold = config.transfer_config.multipart_threshold as usize;
    let initial = probe_up_to(&mut reader, threshold).await?;

    if initial.len() < threshold {
        // Reader hit EOF before the threshold — stays in the in-memory path.
        return transfer_buffered(
            config,
            target,
            target_key,
            initial,
            cancellation_token,
            stats_sender,
        )
        .await;
    }

    // Threshold reached — stream the rest.
    transfer_streaming(
        config,
        target,
        target_key,
        initial,
        reader,
        cancellation_token,
        stats_sender,
    )
    .await
}

async fn transfer_streaming(
    config: &Config,
    target: Storage,
    target_key: &str,
    initial: Vec<u8>,
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    _cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    // Chain the already-buffered bytes with the remaining reader.
    let chained: Box<dyn tokio::io::AsyncRead + Send + Unpin> =
        Box::new(std::io::Cursor::new(initial).chain(reader));

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

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
    };

    let _put_object_output = target
        .put_object_stream(
            target_key,
            chained,
            tagging,
            Some(object_checksum),
            if_none_match,
        )
        .await
        .context(format!("failed to stream to target: {target_key}"))?;

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
}

async fn transfer_buffered(
    config: &Config,
    target: Storage,
    target_key: &str,
    buffer: Vec<u8>,
    _cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
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

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
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
            if_none_match,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
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

    #[test]
    fn sub_threshold_matches_single_update_finalize() {
        // With buffer.len() < multipart_threshold, compute_source_checksum should
        // emit a single-part checksum equal to one update+finalize call.
        let chunksize = 1024usize;
        let threshold = 4096usize;
        let buffer = vec![0xC3u8; 1500];

        for algo in all_algorithms() {
            let actual =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let mut expected = AdditionalChecksum::new(algo.clone(), false);
            expected.update(&buffer);
            assert_eq!(
                actual,
                expected.finalize(),
                "algorithm {:?}: sub-threshold path should match single update+finalize",
                algo
            );
        }
    }

    #[test]
    fn empty_buffer_produces_well_defined_checksum() {
        // Empty buffer hits the sub-threshold branch (0 < threshold) and returns a
        // single-part checksum over zero bytes — must be stable and equal to a fresh
        // AdditionalChecksum that was never updated.
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer: Vec<u8> = Vec::new();

        for algo in all_algorithms() {
            let actual =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let mut expected = AdditionalChecksum::new(algo.clone(), false);
            assert_eq!(actual, expected.finalize(), "algorithm {:?}", algo);
        }
    }

    #[test]
    fn threshold_boundary_uses_multipart_path() {
        // buffer.len() == multipart_threshold takes the multipart branch (the < check
        // is strict). Pin that boundary so a future change to >= or > would be caught.
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0x11u8; threshold];

        for algo in all_algorithms() {
            let multipart =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let mut single = AdditionalChecksum::new(algo.clone(), false);
            single.update(&buffer);
            let single_part = single.finalize();
            // For algorithms where the composite differs from a single-part checksum
            // (SHA256/SHA1/CRC32/CRC32C with composite enabled), the boundary must
            // produce the multipart form, not the single-part one. Crc64Nvme is
            // always full-object and will match — that's expected.
            if !matches!(algo, ChecksumAlgorithm::Crc64Nvme) {
                assert_ne!(
                    multipart, single_part,
                    "algorithm {:?}: threshold boundary should take multipart path",
                    algo
                );
            }
        }
    }

    #[test]
    fn full_object_checksum_flag_is_threaded_through() {
        // For CRC algorithms, full_object_checksum=true should produce a checksum
        // distinct from full_object_checksum=false on the same multipart input.
        // (Crc64Nvme ignores the flag — only CRC32/CRC32C honor it.)
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0x77u8; chunksize * 2 + 5];

        for algo in [ChecksumAlgorithm::Crc32, ChecksumAlgorithm::Crc32C] {
            let composite =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, false);
            let full_object =
                compute_source_checksum(&buffer, algo.clone(), chunksize, threshold, true);
            assert_ne!(
                composite, full_object,
                "algorithm {:?}: full_object_checksum flag should change result",
                algo
            );
        }
    }
}

#[cfg(test)]
mod if_none_match_tests {
    use super::*;
    use crate::config::{CLITimeoutConfig, ClientConfig, RetryConfig, TransferConfig};
    use crate::storage::StorageTrait;
    use crate::types::token::create_pipeline_cancellation_token;
    use crate::types::{
        AnnotationMap, ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId,
        StoragePath,
    };
    use async_trait::async_trait;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::delete_object_annotation::DeleteObjectAnnotationOutput;
    use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::put_object_annotation::PutObjectAnnotationOutput;
    use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
    use aws_sdk_s3::types::{ChecksumMode, ObjectPart, Tagging};
    use aws_smithy_types::checksum_config::RequestChecksumCalculation;
    use leaky_bucket::RateLimiter;
    use std::path::PathBuf;
    use std::sync::{Arc, Mutex as StdMutex};
    use tokio::io::AsyncRead;
    use tokio::sync::Semaphore;

    /// What the target storage was asked to do, so the test can assert on the
    /// `if_none_match` value that actually reached it.
    #[derive(Clone, Debug, Default)]
    struct Recorded {
        put_object_if_none_match: Option<Option<String>>,
        put_object_stream_if_none_match: Option<Option<String>>,
    }

    #[derive(Clone, Debug)]
    struct RecordingTarget {
        recorded: Arc<StdMutex<Recorded>>,
    }

    impl RecordingTarget {
        fn new() -> Self {
            Self {
                recorded: Arc::new(StdMutex::new(Recorded::default())),
            }
        }
        fn recorded(&self) -> Recorded {
            self.recorded.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl StorageTrait for RecordingTarget {
        fn is_local_storage(&self) -> bool {
            false
        }
        fn is_express_onezone_storage(&self) -> bool {
            false
        }
        async fn get_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<GetObjectOutput> {
            unimplemented!()
        }
        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            unimplemented!()
        }
        async fn head_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            unimplemented!()
        }
        async fn head_object_first_part(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            unimplemented!()
        }
        async fn get_object_parts(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unimplemented!()
        }
        async fn get_object_parts_attributes(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _max_parts: i32,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unimplemented!()
        }
        async fn put_object(
            &self,
            _key: &str,
            _source: Storage,
            _source_key: &str,
            _source_size: u64,
            _source_additional_checksum: Option<String>,
            _get_object_output_first_chunk: GetObjectOutput,
            _tagging: Option<String>,
            _object_checksum: Option<ObjectChecksum>,
            if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            self.recorded.lock().unwrap().put_object_if_none_match = Some(if_none_match);
            Ok(PutObjectOutput::builder().build())
        }
        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
            unimplemented!()
        }
        async fn put_object_stream(
            &self,
            _key: &str,
            _reader: Box<dyn AsyncRead + Send + Unpin>,
            _tagging: Option<String>,
            _object_checksum: Option<ObjectChecksum>,
            if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            self.recorded
                .lock()
                .unwrap()
                .put_object_stream_if_none_match = Some(if_none_match);
            Ok(PutObjectOutput::builder().build())
        }
        async fn list_object_annotations(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _max_annotation_results: i32,
        ) -> Result<AnnotationMap> {
            unimplemented!()
        }
        async fn get_object_annotation(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _annotation_name: &str,
            _checksum_mode: Option<ChecksumMode>,
        ) -> Result<GetObjectAnnotationOutput> {
            unimplemented!()
        }
        async fn copy_object_annotation(
            &self,
            _key: &str,
            _target_version_id: Option<String>,
            _annotation_name: &str,
            _source_annotation: GetObjectAnnotationOutput,
        ) -> Result<PutObjectAnnotationOutput> {
            unimplemented!()
        }
        async fn delete_object_annotation(
            &self,
            _key: &str,
            _target_version_id: Option<String>,
            _annotation_name: &str,
        ) -> Result<DeleteObjectAnnotationOutput> {
            unimplemented!()
        }
        async fn delete_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<DeleteObjectOutput> {
            unimplemented!()
        }
        fn get_client(&self) -> Option<Arc<Client>> {
            None
        }
        fn get_stats_sender(&self) -> Sender<SyncStatistics> {
            unimplemented!()
        }
        async fn send_stats(&self, _stats: SyncStatistics) {}
        fn get_local_path(&self) -> PathBuf {
            PathBuf::new()
        }
        fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
            None
        }
        fn generate_copy_source_key(&self, _key: &str, _version_id: Option<String>) -> String {
            unimplemented!()
        }
        fn set_warning(&self) {}
    }

    fn config_with(if_none_match: bool, threshold: u64) -> Config {
        let client_config = ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: S3Credentials::FromEnvironment,
            region: None,
            endpoint_url: None,
            force_path_style: false,
            accelerate: false,
            request_payer: None,
            retry_config: RetryConfig {
                aws_max_attempts: 1,
                initial_backoff_milliseconds: 0,
            },
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        };

        Config {
            source: StoragePath::Stdio,
            target: StoragePath::S3 {
                bucket: "b".to_string(),
                prefix: "k".to_string(),
            },
            show_progress: false,
            source_client_config: None,
            target_client_config: Some(client_config),
            tracing_config: None,
            transfer_config: TransferConfig {
                multipart_threshold: threshold,
                multipart_chunksize: 5 * 1024 * 1024,
                auto_chunksize: false,
            },
            disable_tagging: false,
            server_side_copy: false,
            no_guess_mime_type: false,
            disable_multipart_verify: false,
            disable_etag_verify: false,
            disable_additional_checksum_verify: false,
            storage_class: None,
            sse: None,
            sse_kms_key_id: SseKmsKeyId { id: None },
            source_sse_c: None,
            source_sse_c_key: SseCustomerKey { key: None },
            source_sse_c_key_md5: None,
            target_sse_c: None,
            target_sse_c_key: SseCustomerKey { key: None },
            target_sse_c_key_md5: None,
            canned_acl: None,
            additional_checksum_mode: None,
            additional_checksum_algorithm: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_type: None,
            expires: None,
            metadata: None,
            no_sync_system_metadata: false,
            no_sync_user_defined_metadata: false,
            website_redirect: None,
            tagging: None,
            put_last_modified_metadata: false,
            disable_payload_signing: false,
            disable_content_md5_header: false,
            full_object_checksum: false,
            source_accelerate: false,
            target_accelerate: false,
            source_request_payer: false,
            target_request_payer: false,
            if_none_match,
            disable_stalled_stream_protection: false,
            disable_express_one_zone_additional_checksum: false,
            max_parallel_uploads: 1,
            rate_limit_bandwidth: None,
            version_id: None,
            is_stdio_source: true,
            is_stdio_target: false,
            no_fail_on_verify_error: false,
            skip_existing: false,
            dry_run: false,
            enable_sync_object_annotations: false,
            disable_check_annotation_etag: false,
        }
    }

    async fn run(config: Config, input: Vec<u8>) -> Recorded {
        let target = RecordingTarget::new();
        let handle = target.clone();
        let storage: Storage = Box::new(target);
        let (tx, _rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            storage,
            "k",
            std::io::Cursor::new(input),
            create_pipeline_cancellation_token(),
            tx,
        )
        .await
        .unwrap();
        handle.recorded()
    }

    /// `--if-none-match` must reach the buffered (sub-threshold) stdin upload.
    /// It was hard-coded to `None`, so the documented overwrite protection did
    /// not apply to stdin at all and an existing object was clobbered.
    #[tokio::test]
    async fn if_none_match_reaches_the_buffered_stdin_upload() {
        let recorded = run(config_with(true, 8 * 1024 * 1024), vec![0x41; 1024]).await;
        assert_eq!(
            recorded.put_object_if_none_match,
            Some(Some("*".to_string())),
            "buffered stdin upload must send If-None-Match: *"
        );
    }

    /// The same for the streaming (at-or-above-threshold) stdin upload.
    #[tokio::test]
    async fn if_none_match_reaches_the_streaming_stdin_upload() {
        let threshold = 1024u64;
        let recorded = run(config_with(true, threshold), vec![0x42; 4096]).await;
        assert_eq!(
            recorded.put_object_stream_if_none_match,
            Some(Some("*".to_string())),
            "streaming stdin upload must send If-None-Match: *"
        );
    }

    /// Without the flag neither path may send the header, or every stdin
    /// upload would start failing against an existing key.
    #[tokio::test]
    async fn if_none_match_is_absent_on_both_paths_when_flag_is_off() {
        let buffered = run(config_with(false, 8 * 1024 * 1024), vec![0x41; 1024]).await;
        assert_eq!(buffered.put_object_if_none_match, Some(None));

        let streaming = run(config_with(false, 1024), vec![0x42; 4096]).await;
        assert_eq!(streaming.put_object_stream_if_none_match, Some(None));
    }

    #[tokio::test]
    async fn cancelled_token_short_circuits_before_reading_stdin() {
        let config = config_with(false, 8 * 1024 * 1024);
        let target = RecordingTarget::new();
        let handle = target.clone();
        let storage: Storage = Box::new(target);
        let (tx, _rx) = async_channel::unbounded::<SyncStatistics>();
        let token = create_pipeline_cancellation_token();
        token.cancel();

        transfer(
            &config,
            storage,
            "k",
            std::io::Cursor::new(vec![0x41; 16]),
            token,
            tx,
        )
        .await
        .unwrap();

        let recorded = handle.recorded();
        assert_eq!(
            recorded.put_object_if_none_match, None,
            "no upload may start after cancellation"
        );
        assert_eq!(recorded.put_object_stream_if_none_match, None);
    }

    #[tokio::test]
    async fn disable_tagging_suppresses_tagging_on_the_streaming_path() {
        // --disable-tagging with a configured tagging string: the streaming
        // path must resolve tagging to None and still complete the upload.
        let mut config = config_with(false, 1024);
        config.tagging = Some("k=v".to_string());
        config.disable_tagging = true;

        let streaming = run(config, vec![0x42; 4096]).await;
        assert_eq!(streaming.put_object_stream_if_none_match, Some(None));
    }

    // ------------------------------------------------------------------
    // Direct mock-trait coverage. The transfer-level tests above only drive
    // `put_object` / `put_object_stream`; the assertions below pin the
    // remaining real-return methods and verify each `unimplemented!()` stub
    // still panics (so the regression guard remains intact).
    // ------------------------------------------------------------------

    async fn assert_future_panics<F, T>(future: F)
    where
        F: std::future::Future<Output = T>,
    {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;
        let result = AssertUnwindSafe(future).catch_unwind().await;
        assert!(result.is_err(), "expected the future to panic");
    }

    fn assert_call_panics<F, R>(f: F)
    where
        F: FnOnce() -> R,
    {
        use std::panic::AssertUnwindSafe;
        let result = std::panic::catch_unwind(AssertUnwindSafe(f));
        assert!(result.is_err(), "expected the call to panic");
    }

    fn dummy_tagging() -> Tagging {
        Tagging::builder()
            .set_tag_set(Some(vec![]))
            .build()
            .unwrap()
    }

    fn no_sse_c_key() -> SseCustomerKey {
        SseCustomerKey { key: None }
    }

    #[tokio::test]
    async fn recording_target_real_return_methods_behave_as_expected() {
        let target = RecordingTarget::new();

        assert!(!target.is_local_storage());
        assert!(!target.is_express_onezone_storage());

        // Nothing recorded until an upload actually runs.
        assert_eq!(target.recorded().put_object_if_none_match, None);
        assert_eq!(target.recorded().put_object_stream_if_none_match, None);

        // Both upload entry points record the value they were handed and
        // report success without an ETag.
        let put = target
            .put_object(
                "k",
                Box::new(RecordingTarget::new()),
                "src",
                0,
                None,
                GetObjectOutput::builder().build(),
                None,
                None,
                Some("*".to_string()),
            )
            .await
            .unwrap();
        assert_eq!(put.e_tag(), None);
        assert_eq!(
            target.recorded().put_object_if_none_match,
            Some(Some("*".to_string()))
        );

        let stream = target
            .put_object_stream("k", Box::new(tokio::io::empty()), None, None, None)
            .await
            .unwrap();
        assert_eq!(stream.e_tag(), None);
        assert_eq!(
            target.recorded().put_object_stream_if_none_match,
            Some(None)
        );

        assert!(target.get_client().is_none());
        assert!(target.get_rate_limit_bandwidth().is_none());
        assert_eq!(target.get_local_path(), PathBuf::new());
        target
            .send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        target.set_warning();
    }

    #[tokio::test]
    async fn recording_target_unimplemented_methods_panic() {
        let target = RecordingTarget::new();

        assert_future_panics(target.get_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(target.get_object_tagging("k", None)).await;
        assert_future_panics(target.head_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(target.head_object_first_part(
            "k",
            None,
            None,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(target.get_object_parts("k", None, None, no_sse_c_key(), None)).await;
        assert_future_panics(target.get_object_parts_attributes(
            "k",
            None,
            0,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(target.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(target.delete_object("k", None)).await;

        // The annotation overrides are stubs too: a stdin upload never syncs
        // annotations, so reaching any of them is a bug.
        assert_future_panics(target.list_object_annotations("k", None, 1000)).await;
        assert_future_panics(target.get_object_annotation("k", None, "name", None)).await;
        assert_future_panics(target.copy_object_annotation(
            "k",
            None,
            "name",
            GetObjectAnnotationOutput::builder().build(),
        ))
        .await;
        assert_future_panics(target.delete_object_annotation("k", None, "name")).await;

        assert_call_panics(|| target.get_stats_sender());
        assert_call_panics(|| target.generate_copy_source_key("k", None));
    }
}
