use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, ObjectPart};
use tracing::{error, warn};

use crate::config::Config;
use crate::storage::e_tag_verify;
use crate::storage::{StorageTrait, get_range_from_content_range, parse_range_header_string};
use crate::types::{
    ObjectChecksum, get_additional_checksum, get_additional_checksum_with_head_object,
    is_full_object_checksum,
};

/// Minimum S3 multipart part size (5 MiB). Objects smaller than this are always fetched in full.
/// Matches s3sync/src/types/mod.rs:49.
const MINIMUM_CHUNKSIZE: i64 = 5 * 1024 * 1024;

/// Default max_parts for get_object_parts_attributes. Matches s3sync default of 1000.
const MAX_PARTS_DEFAULT: i32 = 1000;

/// Calculate the range header for fetching only the first chunk.
/// Returns None if the full object should be fetched.
/// Ported from s3sync/src/pipeline/syncer.rs:1270-1352.
pub async fn get_first_chunk_range(
    source: &dyn StorageTrait,
    config: &Config,
    source_size: i64,
    source_key: &str,
    version_id: Option<String>,
) -> Result<Option<String>> {
    // If the object size is less than the minimum chunk size, no need to get the first chunk range.
    if source_size < MINIMUM_CHUNKSIZE {
        return Ok(None);
    }

    if source.is_local_storage() {
        if config
            .transfer_config
            .is_multipart_upload_required(source_size as u64)
        {
            let first_chunk_size =
                if source_size < config.transfer_config.multipart_chunksize as i64 {
                    source_size as u64
                } else {
                    config.transfer_config.multipart_chunksize
                };
            return Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)));
        }
        return Ok(None);
    }

    // If auto_chunksize is enabled, we need to get the first chunk size from the head object.
    // Without auto_chunksize, we do not need to get the first chunk range, even if the object has a additional checksum,
    if config.transfer_config.auto_chunksize {
        let head_object_result = source
            .head_object_first_part(
                source_key,
                version_id,
                Some(ChecksumMode::Enabled),
                config.source_sse_c.clone(),
                config.source_sse_c_key.clone(),
                config.source_sse_c_key_md5.clone(),
            )
            .await
            .context("get_first_chunk_range() failed.");

        if head_object_result.is_err() {
            error!(key = source_key, "get_first_chunk_range() failed.");

            return Err(anyhow!("get_first_chunk_range() failed."));
        }

        return Ok(Some(format!(
            "bytes=0-{}",
            head_object_result?.content_length.unwrap() - 1
        )));
    }

    if !config
        .transfer_config
        .is_multipart_upload_required(source_size as u64)
    {
        return Ok(None);
    }

    let first_chunk_size = if source_size < config.transfer_config.multipart_chunksize as i64 {
        source_size as u64
    } else {
        config.transfer_config.multipart_chunksize
    };
    Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)))
}

/// Validate that the response content-range matches the requested range.
/// Ported from s3sync/src/pipeline/syncer.rs:623-646.
pub fn validate_content_range(get_object_output: &GetObjectOutput, range: &str) -> Result<()> {
    if get_object_output.content_range().is_none() {
        return Err(anyhow!(
            "get_object() returned no content range. This is unexpected."
        ));
    }
    let (request_start, request_end) =
        parse_range_header_string(range).context("failed to parse request range header")?;
    let (response_start, response_end) = get_range_from_content_range(get_object_output)
        .context("get_object() returned no content range")?;
    if (request_start != response_start) || (request_end != response_end) {
        return Err(anyhow!(
            "get_object() returned unexpected content range. \
            expected: {}-{}, actual: {}-{}",
            request_start,
            request_end,
            response_start,
            response_end,
        ));
    }
    Ok(())
}

/// Get object parts from source if needed (for auto-chunksize or checksum verification).
/// Ported from s3sync/src/pipeline/syncer.rs:1355-1492.
#[allow(clippy::too_many_arguments)]
pub async fn get_object_parts_if_necessary(
    source: &dyn StorageTrait,
    config: &Config,
    key: &str,
    version_id: Option<&str>,
    e_tag: Option<&str>,
    first_chunk_content_length: i64,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    full_object_checksum: bool,
    range: Option<&str>,
) -> Result<Option<Vec<ObjectPart>>> {
    if !e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
        && range.is_none()
    {
        return Ok(None);
    }

    let key = key.to_string();

    // If auto_chunksize is disabled, we still need to get the object parts if the checksum algorithm is specified.
    if let Some(algorithm) = checksum_algorithm {
        // A full object checksum has no object parts.
        if !algorithm.is_empty() && !full_object_checksum {
            let object_parts = source
                .get_object_parts_attributes(
                    &key,
                    version_id.map(|version_id| version_id.to_string()),
                    MAX_PARTS_DEFAULT,
                    config.source_sse_c.clone(),
                    config.source_sse_c_key.clone(),
                    config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("get_object_parts_if_necessary() failed.")?;

            if object_parts.is_empty()
                && e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
            {
                if config.transfer_config.auto_chunksize {
                    error!(
                        key = key,
                        "failed to get object attributes information. \
                            Please remove --auto-chunksize option and retry."
                    );

                    return Err(anyhow!("failed to get object attributes information."));
                }

                // Source is multipart but has no per-part additional checksum metadata
                // (e.g., uploaded without --additional-checksum-algorithm, or only a
                // FULL_OBJECT CRC64NVME). GetObjectAttributes returns empty Parts in
                // that case; per-part additional-checksum verification is impossible
                // but the copy itself can proceed — downstream validate_checksum will
                // emit the algorithm-mismatch warning and skip verification.
                warn!(
                    key = key,
                    "source multipart object has no per-part additional checksum. \
                        skip additional checksum verification."
                );
                return Ok(None);
            }

            // Only assert that the first part matches first_chunk_content_length when a range
            // is active — for non-ranged fetches (e.g., s3-to-local full download), content_length
            // is the whole object size and won't match the first part size.
            if config.transfer_config.auto_chunksize
                && range.is_some()
                && object_parts[0].size.unwrap() != first_chunk_content_length
            {
                error!(
                    key = key,
                    "object parts(attribute) size does not match content length. \
                    This is unexpected. Please remove --auto-chunksize option and retry."
                );

                return Err(anyhow!(
                    "object parts(attribute) size does not match content length."
                ));
            }

            return Ok(Some(object_parts));
        }
    }

    if config.transfer_config.auto_chunksize && !source.is_local_storage() {
        let object_parts = source
            .get_object_parts(
                &key,
                version_id.map(|version_id| version_id.to_string()),
                config.source_sse_c.clone(),
                config.source_sse_c_key.clone(),
                config.source_sse_c_key_md5.clone(),
            )
            .await
            .context("get_object_parts_if_necessary() failed.")?;

        if e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string())) {
            // If the object is a multipart upload, and the object parts are empty, it should be a error.
            if object_parts.is_empty() {
                error!(
                    key = key,
                    "failed to get object parts information. \
                    this is most likely due to the lack of HeadObject support for partNumber parameter. \
                    Please remove --auto-chunksize option and retry."
                );

                return Err(anyhow!("failed to get object parts information."));
            }
        } else {
            // Even if the object is not a multipart upload, we need to return the object parts for auto-chunksize.
            let object_parts = vec![
                ObjectPartBuilder::default()
                    .size(first_chunk_content_length)
                    .build(),
            ];
            return Ok(Some(object_parts));
        }

        // Same reasoning as above: only check first-chunk/first-part size parity for ranged fetches.
        if range.is_some() && object_parts[0].size.unwrap() != first_chunk_content_length {
            error!(
                key = key,
                "object parts size does not match content length. \
                This is unexpected. Please remove --auto-chunksize option and retry."
            );

            return Err(anyhow!("object parts size does not match content length."));
        }

        Ok(Some(object_parts))
    } else {
        Ok(None)
    }
}

/// Retrieve the final checksum. When a range download was used, the first chunk doesn't
/// contain the final checksum, so a head_object call is needed.
/// Ported from s3sync/src/pipeline/syncer.rs:1176-1268.
#[allow(clippy::too_many_arguments)]
pub async fn get_final_checksum(
    source: &dyn StorageTrait,
    config: &Config,
    get_object_output: &GetObjectOutput,
    range: Option<&str>,
    source_key: &str,
    version_id: Option<String>,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
) -> Option<String> {
    let additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
        if algorithm.is_empty() {
            None
        } else {
            // Only one algorithm supported
            Some(algorithm[0].clone())
        }
    } else {
        None
    };

    // If the object is from local storage, we can get the additional checksum directly.
    if source.is_local_storage() {
        return get_additional_checksum(
            get_object_output,
            config.additional_checksum_algorithm.clone(),
        );
    }

    if config.additional_checksum_mode.is_none() && config.additional_checksum_algorithm.is_none() {
        return None;
    }

    // If range option is not specified, the final checksum is already calculated.
    if range.is_none() {
        return get_additional_checksum(
            get_object_output,
            config.additional_checksum_algorithm.clone(),
        );
    }

    // With a range, GetObject returns a range-scoped checksum (no -N suffix),
    // so fetch the whole-object composite via HeadObject. Force
    // ChecksumMode::Enabled regardless of config: if algorithm is set, we
    // need the server to return the checksum — callers that don't want
    // verification skip this function upstream.
    let head_object_result = source
        .head_object(
            source_key,
            version_id,
            Some(ChecksumMode::Enabled),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context("get_final_checksum() failed.");

    if head_object_result.is_err() {
        let message = "failed to get object parts information. \
            checksum verification may fail. This is most likely due to the lack of HeadObject support for partNumber parameter.";
        warn!(key = source_key, message);

        return None;
    }

    get_additional_checksum_with_head_object(
        &head_object_result.unwrap(),
        additional_checksum_algorithm,
    )
}

/// Build ObjectChecksum with correct object_parts populated.
/// Ported from s3sync/src/pipeline/syncer.rs:1494-1547.
#[allow(clippy::too_many_arguments)]
pub async fn build_object_checksum(
    source: &dyn StorageTrait,
    target: &dyn StorageTrait,
    config: &Config,
    key: &str,
    get_object_output: &GetObjectOutput,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    final_checksum: Option<String>,
) -> Result<Option<ObjectChecksum>> {
    let additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
        if algorithm.is_empty()
            || (config.additional_checksum_mode.is_none() && !target.is_local_storage())
        {
            None
        } else {
            // Only one algorithm supported
            Some(algorithm[0].clone())
        }
    } else {
        None
    };

    // If additional_checksum_mode is not set, do not build object checksum regardless of the existence of checksum.
    let checksum_algorithm = if config.additional_checksum_mode.is_none() {
        None
    } else {
        checksum_algorithm
    };

    let object_parts = get_object_parts_if_necessary(
        source,
        config,
        key,
        get_object_output.version_id(),
        get_object_output.e_tag(),
        get_object_output.content_length.unwrap(),
        checksum_algorithm,
        is_full_object_checksum(&final_checksum),
        get_object_output.content_range(),
    )
    .await?;

    Ok(Some(ObjectChecksum {
        key: key.to_string(),
        version_id: get_object_output
            .version_id()
            .map(|version_id| version_id.to_string()),
        checksum_algorithm: additional_checksum_algorithm.clone(),
        checksum_type: get_object_output.checksum_type().cloned(),
        object_parts,
        final_checksum: get_additional_checksum(get_object_output, additional_checksum_algorithm),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimum_chunksize_constant() {
        assert_eq!(MINIMUM_CHUNKSIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn test_max_parts_default_constant() {
        assert_eq!(MAX_PARTS_DEFAULT, 1000);
    }

    #[test]
    fn test_validate_content_range_matching() {
        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 0-8388607/30000000")
            .build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_content_range_missing() {
        let get_object_output = GetObjectOutput::builder().build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no content range"));
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 0-4194303/30000000")
            .build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected content range")
        );
    }

    #[test]
    fn test_validate_content_range_unparseable_response_returns_error() {
        // A malformed content_range header that get_range_from_content_range
        // can't parse must surface an error rather than silently succeed.
        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes garbage")
            .build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_err());
    }

    // ----------------------------------------------------------------------
    // Async function tests using a minimal in-process stub StorageTrait impl.
    // ----------------------------------------------------------------------

    use crate::config::{Config, TransferConfig};
    use crate::storage::Storage;
    use crate::storage::StorageTrait;
    use crate::types::token::{PipelineCancellationToken, create_pipeline_cancellation_token};
    use crate::types::{ObjectChecksum, SseCustomerKey, StoragePath, SyncStatistics};
    use anyhow::{Result, anyhow};
    use async_channel::Sender;
    use async_trait::async_trait;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::head_object::builders::HeadObjectOutputBuilder;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
    use aws_sdk_s3::types::Tagging;
    use leaky_bucket::RateLimiter;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicBool;

    /// Minimal stub for `StorageTrait`. The functions under test only exercise
    /// `is_local_storage()` and `head_object*()`/`get_object_parts*()` paths,
    /// so all other methods panic — keeps the surface tight.
    #[derive(Clone)]
    struct StubStorage {
        is_local: bool,
        head_object_first_part_response: Arc<Mutex<Option<Result<HeadObjectOutput, String>>>>,
        head_object_response: Arc<Mutex<Option<Result<HeadObjectOutput, String>>>>,
    }

    impl StubStorage {
        fn local() -> Self {
            Self {
                is_local: true,
                head_object_first_part_response: Arc::new(Mutex::new(None)),
                head_object_response: Arc::new(Mutex::new(None)),
            }
        }
        fn s3() -> Self {
            Self {
                is_local: false,
                head_object_first_part_response: Arc::new(Mutex::new(None)),
                head_object_response: Arc::new(Mutex::new(None)),
            }
        }
        fn with_head_object_first_part_response(self, r: Result<HeadObjectOutput, String>) -> Self {
            *self.head_object_first_part_response.lock().unwrap() = Some(r);
            self
        }
        fn with_head_object_response(self, r: Result<HeadObjectOutput, String>) -> Self {
            *self.head_object_response.lock().unwrap() = Some(r);
            self
        }
    }

    #[async_trait]
    impl StorageTrait for StubStorage {
        fn is_local_storage(&self) -> bool {
            self.is_local
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
            match self.head_object_response.lock().unwrap().clone() {
                Some(Ok(h)) => Ok(h),
                Some(Err(msg)) => Err(anyhow!(msg)),
                None => unimplemented!("head_object response not set"),
            }
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
            match self.head_object_first_part_response.lock().unwrap().clone() {
                Some(Ok(h)) => Ok(h),
                Some(Err(msg)) => Err(anyhow!(msg)),
                None => unimplemented!("head_object_first_part response not set"),
            }
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
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            unimplemented!()
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
            String::new()
        }
        fn set_warning(&self) {}
    }

    fn config_with_chunksize(threshold: u64, chunksize: u64, auto_chunksize: bool) -> Config {
        Config {
            source: StoragePath::Local("/".into()),
            target: StoragePath::Local("/".into()),
            show_progress: false,
            source_client_config: None,
            target_client_config: None,
            tracing_config: None,
            transfer_config: TransferConfig {
                multipart_threshold: threshold,
                multipart_chunksize: chunksize,
                auto_chunksize,
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
            skip_existing: false,
            dry_run: false,
        }
    }

    fn _suppress_unused_warnings(_: &PipelineCancellationToken, _: &AtomicBool) {}

    #[tokio::test]
    async fn get_first_chunk_range_returns_none_when_below_minimum_chunksize() {
        // Anything strictly smaller than 5 MiB should bypass ranged fetch entirely.
        let storage = StubStorage::s3();
        let config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 5 * 1024 * 1024 - 1, "k", None)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_first_chunk_range_local_below_threshold_returns_none() {
        // Local source with size < threshold ⇒ no multipart needed ⇒ no range.
        let storage = StubStorage::local();
        let config = config_with_chunksize(20 * 1024 * 1024, 8 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 10 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_first_chunk_range_local_smaller_than_chunksize_uses_full_size() {
        // Multipart kicks in (threshold≤size), but size < chunksize ⇒ first
        // chunk is the whole object.
        let storage = StubStorage::local();
        let config = config_with_chunksize(5 * 1024 * 1024, 16 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 6 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, Some(format!("bytes=0-{}", 6 * 1024 * 1024 - 1)));
    }

    #[tokio::test]
    async fn get_first_chunk_range_local_at_chunksize_uses_chunksize() {
        // Size > chunksize ⇒ first chunk is exactly chunksize bytes.
        let storage = StubStorage::local();
        let config = config_with_chunksize(5 * 1024 * 1024, 8 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 30 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, Some(format!("bytes=0-{}", 8 * 1024 * 1024 - 1)));
    }

    #[tokio::test]
    async fn get_first_chunk_range_s3_no_auto_below_threshold_returns_none() {
        let storage = StubStorage::s3();
        let config = config_with_chunksize(20 * 1024 * 1024, 8 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 10 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_first_chunk_range_s3_no_auto_above_threshold_returns_chunked_range() {
        let storage = StubStorage::s3();
        let config = config_with_chunksize(5 * 1024 * 1024, 8 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 30 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, Some(format!("bytes=0-{}", 8 * 1024 * 1024 - 1)));
    }

    #[tokio::test]
    async fn get_first_chunk_range_s3_no_auto_size_smaller_than_chunksize_uses_full_size() {
        // Forces line 88 (else { source_size }) where size < chunksize.
        let storage = StubStorage::s3();
        let config = config_with_chunksize(5 * 1024 * 1024, 16 * 1024 * 1024, false);
        let result = get_first_chunk_range(&storage, &config, 6 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, Some(format!("bytes=0-{}", 6 * 1024 * 1024 - 1)));
    }

    #[tokio::test]
    async fn get_first_chunk_range_s3_auto_chunksize_propagates_first_part_size() {
        // auto_chunksize=true ⇒ ask the source via head_object_first_part for
        // the first part's content_length and use it as the chunk boundary.
        let head = HeadObjectOutputBuilder::default()
            .content_length(7 * 1024 * 1024)
            .build();
        let storage = StubStorage::s3().with_head_object_first_part_response(Ok(head));
        let config = config_with_chunksize(5 * 1024 * 1024, 8 * 1024 * 1024, true);
        let result = get_first_chunk_range(&storage, &config, 30 * 1024 * 1024, "k", None)
            .await
            .unwrap();
        assert_eq!(result, Some(format!("bytes=0-{}", 7 * 1024 * 1024 - 1)));
    }

    #[tokio::test]
    async fn get_first_chunk_range_s3_auto_chunksize_returns_err_when_head_fails() {
        let storage =
            StubStorage::s3().with_head_object_first_part_response(Err("head failed".to_string()));
        let config = config_with_chunksize(5 * 1024 * 1024, 8 * 1024 * 1024, true);
        let result = get_first_chunk_range(&storage, &config, 30 * 1024 * 1024, "k", None).await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("get_first_chunk_range() failed"));
    }

    #[tokio::test]
    async fn get_final_checksum_returns_local_storage_checksum_directly() {
        let storage = StubStorage::local();
        let mut config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Sha256);
        let get = GetObjectOutput::builder()
            .checksum_sha256("local-sha")
            .build();
        let r = get_final_checksum(&storage, &config, &get, None, "k", None, None).await;
        assert_eq!(r, Some("local-sha".to_string()));
    }

    #[tokio::test]
    async fn get_final_checksum_returns_none_when_no_checksum_modes_configured() {
        // Neither checksum mode nor algorithm set ⇒ skip checksum entirely.
        let storage = StubStorage::s3();
        let config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        let get = GetObjectOutput::builder()
            .checksum_sha256("ignored")
            .build();
        let r =
            get_final_checksum(&storage, &config, &get, Some("bytes=0-99"), "k", None, None).await;
        assert_eq!(r, None);
    }

    #[tokio::test]
    async fn get_final_checksum_no_range_returns_get_object_checksum() {
        let storage = StubStorage::s3();
        let mut config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Sha256);
        let get = GetObjectOutput::builder()
            .checksum_sha256("dir-sha")
            .build();
        let r = get_final_checksum(&storage, &config, &get, None, "k", None, None).await;
        assert_eq!(r, Some("dir-sha".to_string()));
    }

    #[tokio::test]
    async fn get_final_checksum_with_range_uses_head_object_when_succeeds() {
        let head = HeadObjectOutputBuilder::default()
            .checksum_sha256("head-sha")
            .build();
        let storage = StubStorage::s3().with_head_object_response(Ok(head));
        let mut config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Sha256);
        let get = GetObjectOutput::builder()
            .checksum_sha256("ignored")
            .build();
        let r = get_final_checksum(
            &storage,
            &config,
            &get,
            Some("bytes=0-99"),
            "k",
            None,
            Some(&[ChecksumAlgorithm::Sha256]),
        )
        .await;
        assert_eq!(r, Some("head-sha".to_string()));
    }

    #[tokio::test]
    async fn get_final_checksum_with_range_returns_none_when_head_fails() {
        // head_object failure on the ranged path returns None (not an error)
        // — the caller treats that as "skip verification".
        let storage = StubStorage::s3().with_head_object_response(Err("head failed".to_string()));
        let mut config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Sha256);
        let get = GetObjectOutput::builder()
            .checksum_sha256("ignored")
            .build();
        let r = get_final_checksum(
            &storage,
            &config,
            &get,
            Some("bytes=0-99"),
            "k",
            None,
            Some(&[ChecksumAlgorithm::Sha256]),
        )
        .await;
        assert_eq!(r, None);
    }

    #[tokio::test]
    async fn get_final_checksum_empty_algorithm_slice_yields_none_via_head() {
        // Empty algorithm slice ⇒ no algorithm to extract ⇒ None even when
        // HeadObject would have returned a value.
        let head = HeadObjectOutputBuilder::default()
            .checksum_sha256("head-sha")
            .build();
        let storage = StubStorage::s3().with_head_object_response(Ok(head));
        let mut config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        config.additional_checksum_algorithm = Some(ChecksumAlgorithm::Sha256);
        let get = GetObjectOutput::builder()
            .checksum_sha256("ignored")
            .build();
        let r = get_final_checksum(
            &storage,
            &config,
            &get,
            Some("bytes=0-99"),
            "k",
            None,
            Some(&[]),
        )
        .await;
        assert_eq!(r, None);
    }

    #[tokio::test]
    async fn build_object_checksum_returns_oc_with_algorithm_when_target_local() {
        // target.is_local_storage() == true short-circuits the "no checksum mode"
        // gate so an algorithm survives even when checksum mode is not set.
        let source = StubStorage::s3();
        let target = StubStorage::local();
        let config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        let get = GetObjectOutput::builder()
            .content_length(100)
            .e_tag("\"abc\"")
            .checksum_sha256("v")
            .build();
        let oc = build_object_checksum(
            &source,
            &target,
            &config,
            "k",
            &get,
            Some(&[ChecksumAlgorithm::Sha256]),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(oc.key, "k");
        assert!(matches!(
            oc.checksum_algorithm,
            Some(ChecksumAlgorithm::Sha256)
        ));
        assert_eq!(oc.final_checksum, Some("v".to_string()));
    }

    #[tokio::test]
    async fn build_object_checksum_drops_algorithm_when_no_checksum_mode_and_target_not_local() {
        // The "additional_checksum_mode is None and target is not local" path
        // strips the algorithm from the returned ObjectChecksum.
        let source = StubStorage::s3();
        let target = StubStorage::s3();
        let config = config_with_chunksize(8 * 1024 * 1024, 8 * 1024 * 1024, false);
        let get = GetObjectOutput::builder()
            .content_length(100)
            .checksum_sha256("v")
            .build();
        let oc = build_object_checksum(
            &source,
            &target,
            &config,
            "k",
            &get,
            Some(&[ChecksumAlgorithm::Sha256]),
            None,
        )
        .await
        .unwrap()
        .unwrap();
        assert!(oc.checksum_algorithm.is_none());
    }

    #[tokio::test]
    async fn cancellation_token_smoke_for_test_helpers() {
        // Sanity: helpers used by transfer-module tests still link.
        let _ = create_pipeline_cancellation_token();
    }

    // ------------------------------------------------------------------
    // Direct StubStorage trait coverage. The function-under-test cases
    // above only invoke the head/get_parts paths used by the production
    // helpers; the assertions below pin the remaining real-return methods
    // to their expected values and verify each `unimplemented!()` stub
    // (and the `None` head-response branches) still panics.
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

    fn no_sse_c_key() -> SseCustomerKey {
        SseCustomerKey { key: None }
    }

    fn dummy_tagging() -> Tagging {
        Tagging::builder()
            .set_tag_set(Some(vec![]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn stub_storage_real_return_methods_behave_as_expected() {
        let s3 = StubStorage::s3();
        let local = StubStorage::local();

        assert!(!s3.is_local_storage());
        assert!(local.is_local_storage());
        assert!(!s3.is_express_onezone_storage());
        assert!(!local.is_express_onezone_storage());

        // head_object Ok branch.
        let stub = StubStorage::s3().with_head_object_response(Ok(HeadObjectOutput::builder()
            .e_tag("\"abc\"")
            .content_length(42)
            .build()));
        let head = stub
            .head_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(head.e_tag(), Some("\"abc\""));
        assert_eq!(head.content_length(), Some(42));

        // head_object Err branch.
        let stub = StubStorage::s3().with_head_object_response(Err("boom-head".to_string()));
        let err = stub
            .head_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom-head"));

        // head_object_first_part Ok branch.
        let stub =
            StubStorage::s3().with_head_object_first_part_response(Ok(HeadObjectOutput::builder()
                .e_tag("\"first\"")
                .build()));
        let head = stub
            .head_object_first_part("k", None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(head.e_tag(), Some("\"first\""));

        // head_object_first_part Err branch.
        let stub =
            StubStorage::s3().with_head_object_first_part_response(Err("boom-first".to_string()));
        let err = stub
            .head_object_first_part("k", None, None, None, no_sse_c_key(), None)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("boom-first"));

        // Trivial getters.
        assert!(s3.get_client().is_none());
        assert!(s3.get_rate_limit_bandwidth().is_none());
        assert_eq!(s3.get_local_path(), PathBuf::new());
        let _tx = s3.get_stats_sender();
        s3.send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        s3.set_warning();
        assert_eq!(s3.generate_copy_source_key("k", None), "");
    }

    #[tokio::test]
    async fn stub_storage_head_methods_panic_when_response_unset() {
        // The `None` branch in each head_* response slot is the regression
        // guard for tests that forget to wire a response — must still panic.
        let stub = StubStorage::s3();
        assert_future_panics(stub.head_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(stub.head_object_first_part(
            "k",
            None,
            None,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
    }

    #[tokio::test]
    async fn stub_storage_unimplemented_methods_panic() {
        let stub = StubStorage::s3();

        assert_future_panics(stub.get_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(stub.get_object_tagging("k", None)).await;
        assert_future_panics(stub.get_object_parts("k", None, None, no_sse_c_key(), None)).await;
        assert_future_panics(stub.get_object_parts_attributes(
            "k",
            None,
            0,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(stub.put_object(
            "k",
            Box::new(StubStorage::s3()),
            "src",
            0,
            None,
            GetObjectOutput::builder().build(),
            None,
            None,
            None,
        ))
        .await;
        assert_future_panics(stub.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(stub.delete_object("k", None)).await;
    }
}
