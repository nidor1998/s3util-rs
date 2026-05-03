use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{debug, warn};

use crate::Config;
use crate::storage::{Storage, convert_head_to_get_object_output, parse_range_header_string};
use crate::transfer::{TransferOutcome, first_chunk, translate_source_head_object_error};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, get_additional_checksum};

/// Transfer an S3 object from one S3 location to another.
///
/// Supports two modes:
/// - Server-side copy: uses head_object on source + put_object with copy_source on target
/// - Download + upload: get_object from source + put_object to target
///
/// Uses first-chunk optimization: for objects above the multipart threshold,
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
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // Get source size via head_object (without range) to calculate first chunk range.
    // For server-side copy, we also need the metadata from this head_object.
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
    // that was copied, preserving any newer concurrent versions. Captured
    // identically for both the server-side-copy and download+upload paths —
    // head_object is read in both branches before the dispatch.
    let source_version_id = head_object_output.version_id().map(String::from);

    let source_size = head_object_output.content_length().unwrap_or(0);
    let source_tag_count = head_object_output.tag_count();

    // Get the first chunk range if multipart upload is required.
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

    let get_object_output = if config.server_side_copy {
        // Server-side copy: the first HEAD above already returned every metadata
        // field we need. When a first-chunk range is active, downstream code
        // (validate_content_range, upload_manager first-part sizing) expects
        // content_length and content_range in the ranged-GET shape — synthesize
        // them from the range string and the full source size instead of paying
        // for a second ranged HEAD round-trip.
        let range_override = if let Some(range_str) = range.as_deref() {
            let (start, end) = parse_range_header_string(range_str)
                .context("failed to parse first-chunk range header")?;
            Some((start, end, source_size as u64))
        } else {
            None
        };
        convert_head_to_get_object_output(head_object_output, range_override)
    } else {
        // Download + upload: download from source with range
        source
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
            .context(format!("failed to download source object: {source_key}"))?
    };

    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    // Validate content range if range was used
    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    // Raw source checksum from the (possibly ranged) GetObject response.
    // Used as a fallback when final_checksum isn't fetched (e.g., --enable-additional-checksum
    // not set but the SDK still surfaces a full-object checksum like CRC64NVME).
    let source_additional_checksum_raw = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Get tagging
    let tagging = if config.disable_tagging {
        None
    } else if config.tagging.is_some() {
        config.tagging.clone()
    } else if source_tag_count.is_none_or(|count| count == 0) {
        // Skip get_object_tagging when the source has no tags.
        // Notably, S3 Express One Zone buckets don't support tagging —
        // HeadObject returns no tag_count, and GetObjectTagging would fail.
        None
    } else {
        let tagging_output = source_clone
            .get_object_tagging(source_key, config.version_id.clone())
            .await
            .context(format!("failed to get source object tagging: {source_key}"))?;
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
    };

    // Build checksum algorithm slice matching s3sync's object.checksum_algorithm() format
    let checksum_algorithms: Option<Vec<_>> = config
        .additional_checksum_algorithm
        .as_ref()
        .map(|a| vec![a.clone()]);
    let checksum_algorithm_slice = checksum_algorithms.as_deref();

    // Build object checksum using the s3sync-ported helpers
    let final_checksum = first_chunk::get_final_checksum(
        &*source,
        config,
        &get_object_output,
        range.as_deref(),
        source_key,
        config.version_id.clone(),
        checksum_algorithm_slice,
    )
    .await;

    // Pass source_key so that get_object_parts_attributes looks up parts on the source bucket.
    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        source_key,
        &get_object_output,
        checksum_algorithm_slice,
        final_checksum.clone(),
    )
    .await?;

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
    };

    // Prefer final_checksum (HEAD-fetched for ranged requests when --enable-additional-checksum
    // is set) over the raw ranged response. For composite multipart checksums (SHA256 etc.),
    // a ranged GET doesn't carry the composite value, but HEAD does. When mode is not set,
    // get_final_checksum returns None and we fall back to whatever the ranged GET exposed.
    let source_checksum_for_verify = final_checksum.clone().or(source_additional_checksum_raw);

    let put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_checksum_for_verify,
            get_object_output,
            tagging,
            object_checksum,
            if_none_match,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    if put_object_output.e_tag.is_some() {
        debug!(
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
    /// without touching real S3. `version_id` is the value head_object surfaces
    /// — verified via `TransferOutcome.source_version_id` after the call.
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
            // tag_count is left None so tagging short-circuits without calling
            // get_object_tagging in the non-disabled, no-explicit-tagging path.
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

    /// Mock S3 target — returns a populated PutObjectOutput so the etag-present
    /// debug branch is taken (as opposed to the warn branch on no etag).
    #[derive(Clone)]
    struct MockTarget;

    #[async_trait]
    impl StorageTrait for MockTarget {
        fn is_local_storage(&self) -> bool {
            // S3 target. Drives build_object_checksum's target.is_local_storage()
            // gate the same way the real s3-to-s3 flow does.
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            Ok(PutObjectOutput::builder().e_tag("\"target-etag\"").build())
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

    /// Config tuned to skip first-chunk/multipart and disable all checksum/etag
    /// gating that would require richer fixtures than the head response.
    fn minimal_config(server_side_copy: bool) -> Config {
        Config {
            source: crate::types::StoragePath::S3 {
                bucket: "src".to_string(),
                prefix: String::new(),
            },
            target: crate::types::StoragePath::S3 {
                bucket: "dst".to_string(),
                prefix: String::new(),
            },
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
            server_side_copy,
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
            skip_existing: false,
            dry_run: false,
        }
    }

    #[tokio::test]
    async fn transfer_captures_source_version_id_from_head_object() {
        let config = minimal_config(false);
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
        let config = minimal_config(false);
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

    #[tokio::test]
    async fn transfer_returns_default_when_cancelled_before_start() {
        // Token cancelled before transfer() runs ⇒ transfer returns the default
        // outcome immediately, without making any source/target call.
        let config = minimal_config(false);
        let source: Storage = Box::new(MockSource { version_id: None });
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        // Default outcome ⇒ no version-id captured because no head_object ran.
        assert_eq!(outcome.source_version_id, None);
    }

    #[tokio::test]
    async fn transfer_captures_source_version_id_in_server_side_copy_mode() {
        // Server-side copy bypasses the source GET — version-id capture comes
        // from the head_object response identically to the download+upload path.
        let config = minimal_config(true);
        let source: Storage = Box::new(MockSource {
            version_id: Some("V456".to_string()),
        });
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(outcome.source_version_id.as_deref(), Some("V456"));
    }

    // ------------------------------------------------------------------
    // Direct mock-trait coverage. The transfer-level tests above only
    // exercise the methods used by the production `transfer()` path; the
    // assertions below pin the remaining real-return methods to their
    // expected values and verify each `unimplemented!()` stub still panics
    // (so the regression guard remains intact).
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

    fn dummy_get_object_output() -> GetObjectOutput {
        GetObjectOutput::builder().build()
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
    async fn mock_source_real_return_methods_behave_as_expected() {
        let source = MockSource {
            version_id: Some("v1".to_string()),
        };

        assert!(!source.is_local_storage());
        assert!(!source.is_express_onezone_storage());

        let head = source
            .head_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(head.version_id(), Some("v1"));
        assert_eq!(head.content_length(), Some(4));
        assert_eq!(head.e_tag(), Some("\"abc\""));

        let get = source
            .get_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(get.version_id(), Some("v1"));
        assert_eq!(get.content_length(), Some(4));
        assert_eq!(get.e_tag(), Some("\"abc\""));

        let put_err = source
            .put_object(
                "k",
                Box::new(MockSource { version_id: None }),
                "src",
                0,
                None,
                dummy_get_object_output(),
                None,
                None,
                None,
            )
            .await
            .unwrap_err();
        assert!(put_err.to_string().contains("should not be invoked"));

        assert!(source.get_client().is_none());
        assert!(source.get_rate_limit_bandwidth().is_none());
        assert_eq!(source.get_local_path(), PathBuf::new());
        let _tx = source.get_stats_sender();
        source
            .send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        source.set_warning();
    }

    #[tokio::test]
    async fn mock_source_unimplemented_methods_panic() {
        let source = MockSource { version_id: None };

        assert_future_panics(source.get_object_tagging("k", None)).await;
        assert_future_panics(source.head_object_first_part(
            "k",
            None,
            None,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(source.get_object_parts("k", None, None, no_sse_c_key(), None)).await;
        assert_future_panics(source.get_object_parts_attributes(
            "k",
            None,
            0,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(source.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(source.delete_object("k", None)).await;

        assert_call_panics(|| source.generate_copy_source_key("k", None));
    }

    #[tokio::test]
    async fn mock_target_real_return_methods_behave_as_expected() {
        let target = MockTarget;

        // S3 target — not a local storage.
        assert!(!target.is_local_storage());
        assert!(!target.is_express_onezone_storage());

        let put = target
            .put_object(
                "k",
                Box::new(MockSource { version_id: None }),
                "src",
                0,
                None,
                dummy_get_object_output(),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(put.e_tag(), Some("\"target-etag\""));

        assert!(target.get_client().is_none());
        assert!(target.get_rate_limit_bandwidth().is_none());
        assert_eq!(target.get_local_path(), PathBuf::new());
        let _tx = target.get_stats_sender();
        target
            .send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        target.set_warning();
    }

    #[tokio::test]
    async fn mock_target_unimplemented_methods_panic() {
        let target = MockTarget;

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

        assert_call_panics(|| target.generate_copy_source_key("k", None));
    }
}
