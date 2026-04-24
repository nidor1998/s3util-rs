use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::debug;

use crate::Config;
use crate::storage::Storage;
use crate::transfer::{TransferOutcome, first_chunk, translate_source_head_object_error};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, detect_additional_checksum_with_head_object};

/// Transfer an S3 object to the local filesystem.
///
/// Uses first-chunk optimization: for objects at or above `multipart_threshold`,
/// only the first chunk is fetched initially via a range request. Local storage's
/// `put_object_multipart` then fetches remaining chunks in parallel (bounded by
/// `max_parallel_uploads`) and writes them to the destination file at their
/// offsets. For smaller objects, a single non-ranged `get_object` is issued.
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    source_key: &str,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // HEAD the source to learn the full object size and the composite/final
    // checksum. A ranged GET against a composite-multipart object does not
    // return the root composite checksum; HEAD does.
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

    // Capture the source version-id observed at HEAD time. Threaded into the
    // returned TransferOutcome so `s3util mv` can delete exactly the version
    // that was copied, preserving any newer concurrent versions.
    let source_version_id = head_object_output.version_id().map(String::from);

    let source_size = head_object_output.content_length().unwrap_or(0);

    // Auto-detect checksum algorithm from HEAD. `additional_checksum_algorithm`
    // is rejected at CLI validation when the target is local, so only the mode
    // path applies.
    let (detected_algorithm, source_additional_checksum) =
        if config.additional_checksum_mode.is_some() {
            detect_additional_checksum_with_head_object(&head_object_output)
                .map(|(a, c)| (Some(a), Some(c)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    // Compute the first-chunk range. Returns None for objects below
    // `multipart_threshold` (or below 5 MiB absolute minimum) — in that case
    // we perform a single full-object GET just like before.
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

    let get_object_output = source
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
        .context(format!("failed to download source object: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    // Build object checksum. `build_object_checksum` branches on
    // content_range being populated to fetch the full part-size manifest when
    // needed for composite checksum verification. Pass `source_key` (the S3
    // key) so `get_object_parts_attributes` looks up parts on the source.
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

    // Local storage dispatches on `content_range.is_some()`: when true, it
    // routes into `put_object_multipart`, which spawns parallel ranged GETs
    // against the source for the remaining chunks and writes each at the
    // correct offset in a temp file. `source_size` is the full object size
    // (not the first-chunk size) — required by the multipart write loop.
    let _put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
            get_object_output,
            None, // local storage ignores tagging
            object_checksum,
            None,
        )
        .await
        .context(format!("failed to write to target file: {target_key}"))?;

    debug!(
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

    Ok(TransferOutcome { source_version_id })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TransferConfig;
    use crate::storage::StorageTrait;
    use crate::types::SseCustomerKey;
    use crate::types::token::create_pipeline_cancellation_token;
    use anyhow::anyhow;
    use async_channel::Sender;
    use async_trait::async_trait;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
    use aws_sdk_s3::primitives::{ByteStream, DateTime};
    use aws_sdk_s3::types::{ChecksumMode, ObjectPart, Tagging};
    use leaky_bucket::RateLimiter;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Minimal mock used to drive `transfer()` through to a successful return
    /// without touching real S3 or the local filesystem. The source mock owns
    /// a configurable `version_id` that `head_object` surfaces; downstream
    /// `get_object` and the target mock's `put_object` just return Ok shells
    /// so `transfer()` reaches the final return statement where the captured
    /// version-id is observable in the `TransferOutcome`.
    #[derive(Clone)]
    struct MockSource {
        version_id: Option<String>,
    }

    #[async_trait]
    impl StorageTrait for MockSource {
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
            // 4-byte body keeps the object well below MINIMUM_CHUNKSIZE so the
            // first-chunk path is skipped and a single-shot non-ranged GET is
            // simulated. last_modified is required by the LocalStorage put
            // path; the mock target ignores it but we set it for symmetry.
            Ok(GetObjectOutput::builder()
                .body(ByteStream::from(b"data".to_vec()))
                .content_length(4)
                .e_tag("\"abc\"")
                .last_modified(DateTime::from_secs(0))
                .set_version_id(self.version_id.clone())
                .build())
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
            Ok(HeadObjectOutput::builder()
                .content_length(4)
                .e_tag("\"abc\"")
                .last_modified(DateTime::from_secs(0))
                .set_version_id(self.version_id.clone())
                .build())
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            // The mock source is also handed to put_object as `source_clone`,
            // but in this test the target's put_object short-circuits without
            // calling back into the source. If anything ever does, we want a
            // loud failure to flag the regression rather than silent Ok.
            Err(anyhow!(
                "MockSource::put_object should not be invoked in this test"
            ))
        }
        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
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
            async_channel::unbounded().0
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

    /// Mock target that immediately returns a successful put without touching
    /// the filesystem. Lets us drive `transfer()` past the final put without
    /// having to satisfy LocalStorage's verify_local_file machinery.
    #[derive(Clone)]
    struct MockTarget;

    #[async_trait]
    impl StorageTrait for MockTarget {
        fn is_local_storage(&self) -> bool {
            // Return true so build_object_checksum's target.is_local_storage()
            // gate behaves like the real s3-to-local flow.
            true
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
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
            async_channel::unbounded().0
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

    /// Build a Config that's just enough to make `transfer()` exercise the
    /// non-multipart, non-checksum path. multipart_threshold > body size so
    /// the first-chunk branch is skipped.
    fn minimal_config() -> Config {
        Config {
            source: crate::types::StoragePath::S3 {
                bucket: "src".to_string(),
                prefix: String::new(),
            },
            target: crate::types::StoragePath::Local(PathBuf::from("/tmp")),
            show_progress: false,
            source_client_config: None,
            target_client_config: None,
            tracing_config: None,
            transfer_config: TransferConfig {
                multipart_threshold: 8 * 1024 * 1024,
                multipart_chunksize: 8 * 1024 * 1024,
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
            sse_kms_key_id: crate::types::SseKmsKeyId { id: None },
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
            if_none_match: false,
            disable_stalled_stream_protection: false,
            disable_express_one_zone_additional_checksum: false,
            max_parallel_uploads: 1,
            rate_limit_bandwidth: None,
            version_id: None,
            is_stdio_source: false,
            is_stdio_target: false,
            no_fail_on_verify_error: false,
        }
    }

    #[tokio::test]
    async fn transfer_captures_source_version_id_from_head_object() {
        let config = minimal_config();
        let source: Storage = Box::new(MockSource {
            version_id: Some("V123".to_string()),
        });
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(outcome.source_version_id.as_deref(), Some("V123"));
    }

    #[tokio::test]
    async fn transfer_captures_none_when_head_object_has_no_version_id() {
        let config = minimal_config();
        let source: Storage = Box::new(MockSource { version_id: None });
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(outcome.source_version_id, None);
    }
}
