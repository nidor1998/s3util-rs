use anyhow::{Context, Result};
use async_channel::Sender;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::Config;
use crate::storage::e_tag_verify::is_multipart_upload_e_tag;
use crate::storage::{Storage, convert_head_to_get_object_output, parse_range_header_string};
use crate::transfer::{TransferOutcome, first_chunk, translate_source_head_object_error};
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, generate_annotation_differences, get_additional_checksum};

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

    // Pin every subsequent read of the source to the version this HEAD saw.
    //
    // Without this the first GET asked for "latest", so an overwrite landing
    // between the HEAD and that GET produced a copy of the NEW object truncated
    // to the OLD object's length: the size and part plan come from the HEAD,
    // parts 2+ are pinned to the first GET's version, and `validate_content_range`
    // compares only start/end — never the total — so nothing detected it. The
    // resulting ETag mismatch is a warning for remote sources and is skipped
    // outright under SSE-KMS/SSE-C, so `cp` could report success on a truncated
    // object. It also kept `TransferOutcome.source_version_id` consistent with
    // the bytes actually copied, which is what `mv` deletes.
    //
    // On an unversioned bucket HEAD returns no version-id, so this stays None
    // and behaviour is unchanged. An explicit --source-version-id still wins.
    let effective_version_id = config
        .version_id
        .clone()
        .or_else(|| source_version_id.clone());

    let source_size = head_object_output.content_length().unwrap_or(0);
    let source_tag_count = head_object_output.tag_count();

    // Get the first chunk range if multipart upload is required.
    let range = first_chunk::get_first_chunk_range(
        &*source,
        config,
        source_size,
        source_key,
        effective_version_id.clone(),
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
                effective_version_id.clone(),
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
            .get_object_tagging(source_key, effective_version_id.clone())
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
        effective_version_id.clone(),
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

    // Sync object annotations after the object itself has been written,
    // mirroring s3sync's syncer. A single-part server-side copy is skipped:
    // S3's CopyObject carries annotations to the target entirely within
    // Amazon S3. Multipart server-side copy (UploadPartCopy) does not, so it
    // still needs a manual sync. Source annotations are read at the HEAD-time
    // version pin (not config.version_id, which is None without
    // --source-version-id): the object bytes above were copied at that pin,
    // so reading "latest" here would attach a concurrent overwrite's
    // annotations to this copy.
    let target_etag = put_object_output.e_tag().map(|e| e.to_string());
    let need_sync_annotations = !config.server_side_copy || is_multipart_upload_e_tag(&target_etag);
    if config.enable_sync_object_annotations && need_sync_annotations {
        let target_version_id = put_object_output.version_id().map(|v| v.to_string());
        sync_object_annotations(
            config,
            &source,
            &target,
            source_key,
            target_key,
            source_version_id.clone(),
            target_version_id,
        )
        .await?;
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(TransferOutcome { source_version_id })
}

/// Annotation names fetched per ListObjectAnnotations page. s3sync passes its
/// `--max-keys` value here (default 1000); s3util has no such flag, so the
/// s3sync default is fixed. The pagination loop in `list_object_annotations`
/// handles objects with more annotations than one page.
const MAX_ANNOTATION_RESULTS: i32 = 1000;

/// [dry-run] List the source object's annotations and log each one that a
/// real run would copy, mirroring s3sync's per-annotation dry-run output.
/// cp/mv always write a fresh target object, so every source annotation is
/// copied (with single-part `--server-side-copy`, CopyObject carries them
/// inside Amazon S3 — either way they end up on the target). Only a
/// read-only ListObjectAnnotations call is made. Called from the CLI's
/// dry-run branch, which never reaches `transfer()`.
pub async fn log_dry_run_annotation_sync(
    config: &Config,
    source: &Storage,
    source_key: &str,
) -> Result<()> {
    if !config.enable_sync_object_annotations {
        return Ok(());
    }

    let source_annotation_map = source
        .list_object_annotations(
            source_key,
            config.version_id.clone(),
            MAX_ANNOTATION_RESULTS,
        )
        .await?;

    // Sorted for deterministic output (AnnotationMap is a HashMap).
    let mut annotation_names = source_annotation_map.keys().collect::<Vec<_>>();
    annotation_names.sort();

    let source_version_id_str = config.version_id.clone().unwrap_or_default();
    for annotation_name in annotation_names {
        let annotation = &source_annotation_map[annotation_name];
        info!(
            key = source_key,
            source_version_id = source_version_id_str.as_str(),
            annotation_name = annotation_name.as_str(),
            annotation_size = annotation.size,
            "[dry-run] would copy object annotation.",
        );
    }

    Ok(())
}

/// Bring the target object's annotations in line with the source object's.
/// Ported from s3sync/src/pipeline/syncer.rs `sync_object_annotations`, with
/// two adaptations: distinct source/target keys (cp/mv can rename), and no
/// dry-run 404 tolerance on the target listing — the target object provably
/// exists here (its put just succeeded) and s3util's dry-run never reaches
/// the transfer layer.
///
/// Returns whether any annotation was added, re-copied, or deleted. cp/mv
/// callers ignore the value; it is kept for parity with s3sync.
async fn sync_object_annotations(
    config: &Config,
    source: &Storage,
    target: &Storage,
    source_key: &str,
    target_key: &str,
    source_version_id: Option<String>,
    target_version_id: Option<String>,
) -> Result<bool> {
    let source_annotation_map = source
        .list_object_annotations(
            source_key,
            source_version_id.clone(),
            MAX_ANNOTATION_RESULTS,
        )
        .await?;
    let target_annotation_map = target
        .list_object_annotations(
            target_key,
            target_version_id.clone(),
            MAX_ANNOTATION_RESULTS,
        )
        .await?;

    let annotation_differences = generate_annotation_differences(
        target_key,
        &source_annotation_map,
        &target_annotation_map,
        config.disable_check_annotation_etag,
    );
    let need_modify = !(annotation_differences.added.is_empty()
        && annotation_differences.modified.is_empty()
        && annotation_differences.deleted.is_empty());
    let mut annotations_to_be_copied = annotation_differences.added.clone();
    annotations_to_be_copied.extend(annotation_differences.modified.clone());

    debug!(
        source_key = source_key,
        target_key = target_key,
        source_version_id = source_version_id.as_deref().unwrap_or("None"),
        target_version_id = target_version_id.as_deref().unwrap_or("None"),
        "Annotations to be copied: {:?}",
        annotations_to_be_copied
    );

    let mut annotation_copy_tasks = FuturesUnordered::new();
    let semaphore = config
        .target_client_config
        .as_ref()
        .unwrap()
        .parallel_upload_semaphore
        .clone();
    for added_annotation_name in annotations_to_be_copied {
        let source = dyn_clone::clone_box(&**source);
        let target = dyn_clone::clone_box(&**target);
        let source_version_id = source_version_id.clone();
        let target_version_id = target_version_id.clone();
        let source_key = source_key.to_string();
        let target_key = target_key.to_string();
        let checksum_mode = config.additional_checksum_mode.clone();
        let permit = semaphore.clone().acquire_owned().await;

        let task: JoinHandle<Result<()>> = task::spawn(async move {
            let _permit = permit; // Keep the semaphore permit in scope
            let annotation_data = source
                .get_object_annotation(
                    &source_key,
                    source_version_id.clone(),
                    &added_annotation_name,
                    checksum_mode,
                )
                .await?;

            target
                .copy_object_annotation(
                    &target_key,
                    target_version_id.clone(),
                    &added_annotation_name,
                    annotation_data,
                )
                .await?;

            Ok(())
        });
        annotation_copy_tasks.push(task);
    }

    while let Some(result) = annotation_copy_tasks.next().await {
        result??;
    }

    debug!(
        source_key = source_key,
        target_key = target_key,
        source_version_id = source_version_id.as_deref().unwrap_or("None"),
        target_version_id = target_version_id.as_deref().unwrap_or("None"),
        "Annotations to be deleted: {:?}",
        annotation_differences.deleted
    );

    let mut annotation_delete_tasks = FuturesUnordered::new();
    for annotation_name_to_be_deleted in annotation_differences.deleted {
        let target = dyn_clone::clone_box(&**target);
        let target_version_id = target_version_id.clone();
        let target_key = target_key.to_string();
        let permit = semaphore.clone().acquire_owned().await;

        let task: JoinHandle<Result<()>> = task::spawn(async move {
            let _permit = permit; // Keep the semaphore permit in scope
            target
                .delete_object_annotation(
                    &target_key,
                    target_version_id.clone(),
                    &annotation_name_to_be_deleted,
                )
                .await?;
            Ok(())
        });

        annotation_delete_tasks.push(task);
    }

    while let Some(result) = annotation_delete_tasks.next().await {
        result??;
    }

    Ok(need_modify)
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
        /// Records the version-id each `get_object` call was issued with, so a
        /// test can assert the first chunk is pinned to the HEAD-time version.
        get_object_version_ids: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    }

    impl MockSource {
        fn new(version_id: Option<String>) -> Self {
            Self {
                version_id,
                get_object_version_ids: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }
        fn recorded_get_object_version_ids(&self) -> Vec<Option<String>> {
            self.get_object_version_ids.lock().unwrap().clone()
        }
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
            version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<GetObjectOutput> {
            self.get_object_version_ids.lock().unwrap().push(version_id);
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
            enable_sync_object_annotations: false,
            disable_check_annotation_etag: false,
        }
    }

    #[tokio::test]
    async fn transfer_captures_source_version_id_from_head_object() {
        let config = minimal_config(false);
        let source: Storage = Box::new(MockSource::new(Some("V123".to_string())));
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

    /// The first GET must be pinned to the version the HEAD observed.
    ///
    /// Without the pin it asked for "latest", so an overwrite landing between
    /// the HEAD and that GET produced a copy of the NEW object truncated to the
    /// OLD object's length — the size and part plan come from the HEAD, and
    /// `validate_content_range` compares only start/end, never the total, so
    /// nothing detected it.
    #[tokio::test]
    async fn first_get_is_pinned_to_the_head_time_version() {
        let config = minimal_config(false);
        let mock = MockSource::new(Some("V123".to_string()));
        let recorder = mock.clone();
        let source: Storage = Box::new(mock);
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        let recorded = recorder.recorded_get_object_version_ids();
        assert!(!recorded.is_empty(), "get_object must have been called");
        for version_id in &recorded {
            assert_eq!(
                version_id.as_deref(),
                Some("V123"),
                "every source read must be pinned to the HEAD-time version, got: {recorded:?}"
            );
        }
    }

    /// On an unversioned bucket HEAD reports no version-id, so the pin stays
    /// None and the request shape is unchanged.
    #[tokio::test]
    async fn first_get_is_unpinned_when_the_bucket_is_unversioned() {
        let config = minimal_config(false);
        let mock = MockSource::new(None);
        let recorder = mock.clone();
        let source: Storage = Box::new(mock);
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        for version_id in recorder.recorded_get_object_version_ids() {
            assert_eq!(version_id, None);
        }
    }

    #[tokio::test]
    async fn transfer_captures_none_when_head_object_has_no_version_id() {
        let config = minimal_config(false);
        let source: Storage = Box::new(MockSource::new(None));
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
        let source: Storage = Box::new(MockSource::new(None));
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
        let source: Storage = Box::new(MockSource::new(Some("V456".to_string())));
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
        let source = MockSource::new(Some("v1".to_string()));

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
                Box::new(MockSource::new(None)),
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
        let source = MockSource::new(None);

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
                Box::new(MockSource::new(None)),
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

    #[tokio::test]
    async fn annotation_recording_source_unimplemented_methods_panic() {
        // The annotation source mock implements only the methods the annotation
        // `transfer()` path exercises; every other `StorageTrait` method is an
        // `unimplemented!()` stub that must panic if the production code ever
        // starts calling it unexpectedly.
        let source = recording_source(crate::types::AnnotationMap::new());

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
    async fn annotation_recording_target_unimplemented_methods_panic() {
        // As above for the target mock: only put/list/copy/delete-annotation are
        // implemented; the read-side and tagging/delete-object methods are
        // `unimplemented!()` stubs that must panic.
        let target = recording_target("\"etag\"", None, crate::types::AnnotationMap::new());

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

    use aws_sdk_s3::operation::delete_object_annotation::DeleteObjectAnnotationOutput;
    use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
    use aws_sdk_s3::operation::put_object_annotation::PutObjectAnnotationOutput;
    use std::sync::Mutex;

    fn test_client_config() -> crate::config::ClientConfig {
        crate::config::ClientConfig {
            client_config_location: crate::types::ClientConfigLocation {
                aws_config_file: None,
                aws_shared_credentials_file: None,
            },
            credential: crate::types::S3Credentials::FromEnvironment,
            region: None,
            endpoint_url: None,
            force_path_style: false,
            accelerate: false,
            request_payer: None,
            retry_config: crate::config::RetryConfig {
                aws_max_attempts: 1,
                initial_backoff_milliseconds: 1,
            },
            cli_timeout_config: crate::config::CLITimeoutConfig {
                operation_timeout_milliseconds: None,
                operation_attempt_timeout_milliseconds: None,
                connect_timeout_milliseconds: None,
                read_timeout_milliseconds: None,
            },
            disable_stalled_stream_protection: false,
            request_checksum_calculation:
                aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: Arc::new(tokio::sync::Semaphore::new(4)),
        }
    }

    fn annotation_entry(
        name: &str,
        e_tag: &str,
        last_modified_secs: i64,
    ) -> aws_sdk_s3::types::AnnotationEntry {
        aws_sdk_s3::types::AnnotationEntry::builder()
            .annotation_name(name)
            .e_tag(e_tag)
            .last_modified(DateTime::from_secs(last_modified_secs))
            .size(1)
            .build()
            .unwrap()
    }

    /// (key, version_id, annotation_name) triple recorded by the mocks.
    type AnnotationCall = (String, Option<String>, String);

    /// (key, version_id) pair recorded for annotation listings.
    type ListCall = (String, Option<String>);

    /// Source mock: same head/get behavior as MockSource, plus configurable
    /// annotation listing and recording of annotation reads.
    #[derive(Clone)]
    struct AnnotationRecordingSource {
        annotations: crate::types::AnnotationMap,
        /// Version id surfaced by head_object — the HEAD-time pin `transfer()`
        /// captures. None models an unversioned source bucket.
        head_version_id: Option<String>,
        list_calls: Arc<Mutex<Vec<ListCall>>>,
        get_calls: Arc<Mutex<Vec<AnnotationCall>>>,
    }

    #[async_trait]
    impl StorageTrait for AnnotationRecordingSource {
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
                .set_version_id(self.head_version_id.clone())
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
            Err(anyhow!("source put_object must not be invoked"))
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
        async fn list_object_annotations(
            &self,
            key: &str,
            version_id: Option<String>,
            _max_annotation_results: i32,
        ) -> Result<crate::types::AnnotationMap> {
            self.list_calls
                .lock()
                .unwrap()
                .push((key.to_string(), version_id));
            Ok(self.annotations.clone())
        }
        async fn get_object_annotation(
            &self,
            key: &str,
            version_id: Option<String>,
            annotation_name: &str,
            _checksum_mode: Option<ChecksumMode>,
        ) -> Result<GetObjectAnnotationOutput> {
            self.get_calls.lock().unwrap().push((
                key.to_string(),
                version_id,
                annotation_name.to_string(),
            ));
            Ok(GetObjectAnnotationOutput::builder()
                .annotation_payload(ByteStream::from(b"v".to_vec()))
                .content_length(1)
                .build())
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

    /// Target mock: put_object returns a configurable etag/version-id;
    /// annotation listing is configurable; copy/delete calls are recorded.
    #[derive(Clone)]
    struct AnnotationRecordingTarget {
        put_e_tag: String,
        put_version_id: Option<String>,
        annotations: crate::types::AnnotationMap,
        list_calls: Arc<Mutex<Vec<ListCall>>>,
        copy_calls: Arc<Mutex<Vec<AnnotationCall>>>,
        delete_calls: Arc<Mutex<Vec<AnnotationCall>>>,
    }

    #[async_trait]
    impl StorageTrait for AnnotationRecordingTarget {
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            Ok(PutObjectOutput::builder()
                .e_tag(self.put_e_tag.clone())
                .set_version_id(self.put_version_id.clone())
                .build())
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
        async fn list_object_annotations(
            &self,
            key: &str,
            version_id: Option<String>,
            _max_annotation_results: i32,
        ) -> Result<crate::types::AnnotationMap> {
            self.list_calls
                .lock()
                .unwrap()
                .push((key.to_string(), version_id));
            Ok(self.annotations.clone())
        }
        async fn copy_object_annotation(
            &self,
            key: &str,
            target_version_id: Option<String>,
            annotation_name: &str,
            _source_annotation: GetObjectAnnotationOutput,
        ) -> Result<PutObjectAnnotationOutput> {
            self.copy_calls.lock().unwrap().push((
                key.to_string(),
                target_version_id,
                annotation_name.to_string(),
            ));
            Ok(PutObjectAnnotationOutput::builder().build())
        }
        async fn delete_object_annotation(
            &self,
            key: &str,
            target_version_id: Option<String>,
            annotation_name: &str,
        ) -> Result<DeleteObjectAnnotationOutput> {
            self.delete_calls.lock().unwrap().push((
                key.to_string(),
                target_version_id,
                annotation_name.to_string(),
            ));
            Ok(DeleteObjectAnnotationOutput::builder().build())
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

    fn recording_source(annotations: crate::types::AnnotationMap) -> AnnotationRecordingSource {
        AnnotationRecordingSource {
            annotations,
            head_version_id: None,
            list_calls: Arc::new(Mutex::new(Vec::new())),
            get_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn recording_target(
        put_e_tag: &str,
        put_version_id: Option<&str>,
        annotations: crate::types::AnnotationMap,
    ) -> AnnotationRecordingTarget {
        AnnotationRecordingTarget {
            put_e_tag: put_e_tag.to_string(),
            put_version_id: put_version_id.map(|s| s.to_string()),
            annotations,
            list_calls: Arc::new(Mutex::new(Vec::new())),
            copy_calls: Arc::new(Mutex::new(Vec::new())),
            delete_calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    #[tokio::test]
    async fn dry_run_annotation_log_noop_when_flag_off() {
        let config = minimal_config(false);
        let source = recording_source(crate::types::AnnotationMap::new());
        let source_lists = source.list_calls.clone();

        let source: Storage = Box::new(source);
        log_dry_run_annotation_sync(&config, &source, "src/key")
            .await
            .unwrap();

        assert!(source_lists.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dry_run_annotation_log_lists_source_with_version() {
        let mut config = minimal_config(false);
        config.enable_sync_object_annotations = true;
        config.version_id = Some("V1".to_string());

        let source_map: crate::types::AnnotationMap = [
            ("a1".to_string(), annotation_entry("a1", "\"e1\"", 100)),
            ("a2".to_string(), annotation_entry("a2", "\"e2\"", 100)),
        ]
        .into_iter()
        .collect();
        let source = recording_source(source_map);
        let source_lists = source.list_calls.clone();

        let source: Storage = Box::new(source);
        log_dry_run_annotation_sync(&config, &source, "src/key")
            .await
            .unwrap();

        assert_eq!(
            source_lists.lock().unwrap().clone(),
            vec![("src/key".to_string(), Some("V1".to_string()))]
        );
    }

    #[tokio::test]
    async fn dry_run_annotation_log_propagates_list_error() {
        // MockSource does not override the annotation trait methods, so the
        // default "not supported" Err body stands in for a listing failure.
        let mut config = minimal_config(false);
        config.enable_sync_object_annotations = true;

        let source: Storage = Box::new(MockSource::new(None));
        let err = log_dry_run_annotation_sync(&config, &source, "src/key")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[tokio::test]
    async fn annotation_sync_not_invoked_when_flag_off() {
        let config = minimal_config(false);
        let source = recording_source(crate::types::AnnotationMap::new());
        let target = recording_target("\"target-etag\"", None, crate::types::AnnotationMap::new());
        let source_lists = source.list_calls.clone();
        let target_lists = target.list_calls.clone();

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        assert!(source_lists.lock().unwrap().is_empty());
        assert!(target_lists.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn annotation_sync_copies_added_and_deletes_removed() {
        let mut config = minimal_config(false);
        config.enable_sync_object_annotations = true;
        config.target_client_config = Some(test_client_config());

        // Source has a1 (new) and a2 (same etag/lm as target). Target has a3
        // that no longer exists on the source.
        let source_map: crate::types::AnnotationMap = [
            ("a1".to_string(), annotation_entry("a1", "\"e1\"", 100)),
            ("a2".to_string(), annotation_entry("a2", "\"e2\"", 100)),
        ]
        .into_iter()
        .collect();
        let target_map: crate::types::AnnotationMap = [
            ("a2".to_string(), annotation_entry("a2", "\"e2\"", 100)),
            ("a3".to_string(), annotation_entry("a3", "\"e3\"", 100)),
        ]
        .into_iter()
        .collect();

        let source = recording_source(source_map);
        let target = recording_target("\"target-etag\"", Some("TV1"), target_map);
        let source_lists = source.list_calls.clone();
        let source_gets = source.get_calls.clone();
        let target_lists = target.list_calls.clone();
        let copies = target.copy_calls.clone();
        let deletes = target.delete_calls.clone();

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        // Listings hit the right side with the right key/version.
        assert_eq!(
            source_lists.lock().unwrap().clone(),
            vec![("src/key".to_string(), None)]
        );
        assert_eq!(
            target_lists.lock().unwrap().clone(),
            vec![("dst/key".to_string(), Some("TV1".to_string()))]
        );
        // a1 was read from the source and copied to the target; a2 untouched.
        assert_eq!(
            source_gets.lock().unwrap().clone(),
            vec![("src/key".to_string(), None, "a1".to_string())]
        );
        assert_eq!(
            copies.lock().unwrap().clone(),
            vec![(
                "dst/key".to_string(),
                Some("TV1".to_string()),
                "a1".to_string()
            )]
        );
        // a3 was deleted from the target.
        assert_eq!(
            deletes.lock().unwrap().clone(),
            vec![(
                "dst/key".to_string(),
                Some("TV1".to_string()),
                "a3".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn annotation_sync_reads_source_at_head_pinned_version() {
        // The object bytes are copied at the HEAD-time version; the annotation
        // reads must use the same pin, or a concurrent overwrite between HEAD
        // and sync would attach the newer version's annotations to this copy.
        let mut config = minimal_config(false);
        config.enable_sync_object_annotations = true;
        config.target_client_config = Some(test_client_config());

        let source_map: crate::types::AnnotationMap =
            [("a1".to_string(), annotation_entry("a1", "\"e1\"", 100))]
                .into_iter()
                .collect();
        let mut source = recording_source(source_map);
        source.head_version_id = Some("V-HEAD".to_string());
        let source_lists = source.list_calls.clone();
        let source_gets = source.get_calls.clone();
        let target = recording_target("\"target-etag\"", None, crate::types::AnnotationMap::new());

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(
            source_lists.lock().unwrap().clone(),
            vec![("src/key".to_string(), Some("V-HEAD".to_string()))]
        );
        assert_eq!(
            source_gets.lock().unwrap().clone(),
            vec![(
                "src/key".to_string(),
                Some("V-HEAD".to_string()),
                "a1".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn annotation_sync_skipped_for_single_part_server_side_copy() {
        // CopyObject carries annotations inside S3 for single-part copies;
        // the target etag has no "-N" suffix so sync must be skipped.
        let mut config = minimal_config(true);
        config.enable_sync_object_annotations = true;
        config.target_client_config = Some(test_client_config());

        let source_map: crate::types::AnnotationMap =
            [("a1".to_string(), annotation_entry("a1", "\"e1\"", 100))]
                .into_iter()
                .collect();
        let source = recording_source(source_map);
        // A single-part CopyObject preserves the source's MD5-style etag —
        // crucially hyphen-free: is_multipart_upload_e_tag treats any '-' as
        // the multipart marker, so a fixture like "target-etag" would be
        // misclassified as multipart and defeat the skip under test.
        let target = recording_target("\"abc\"", None, crate::types::AnnotationMap::new());
        let source_lists = source.list_calls.clone();
        let target_lists = target.list_calls.clone();

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        assert!(source_lists.lock().unwrap().is_empty());
        assert!(target_lists.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn annotation_sync_runs_for_multipart_server_side_copy() {
        // UploadPartCopy does NOT carry annotations: a multipart-style target
        // etag ("...-2") with server_side_copy must still sync.
        let mut config = minimal_config(true);
        config.enable_sync_object_annotations = true;
        config.target_client_config = Some(test_client_config());

        let source_map: crate::types::AnnotationMap =
            [("a1".to_string(), annotation_entry("a1", "\"e1\"", 100))]
                .into_iter()
                .collect();
        let source = recording_source(source_map);
        let target = recording_target("\"abc-2\"", None, crate::types::AnnotationMap::new());
        let copies = target.copy_calls.clone();

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(
            copies.lock().unwrap().clone(),
            vec![("dst/key".to_string(), None, "a1".to_string())]
        );
    }

    #[tokio::test]
    async fn annotation_sync_deletes_only_when_source_has_no_annotations() {
        // Source carries no annotations while the target still has one. This
        // makes `added` and `modified` empty but `deleted` non-empty, which is
        // the only shape that evaluates the `modified`/`deleted` arms of the
        // `need_modify` short-circuit (a non-empty `added` would stop at the
        // first term).
        let mut config = minimal_config(false);
        config.enable_sync_object_annotations = true;
        config.target_client_config = Some(test_client_config());

        let target_map: crate::types::AnnotationMap =
            [("a3".to_string(), annotation_entry("a3", "\"e3\"", 100))]
                .into_iter()
                .collect();

        let source = recording_source(crate::types::AnnotationMap::new());
        let target = recording_target("\"target-etag\"", Some("TV1"), target_map);
        let copies = target.copy_calls.clone();
        let deletes = target.delete_calls.clone();

        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();
        transfer(
            &config,
            Box::new(source),
            Box::new(target),
            "src/key",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        // Nothing to copy, exactly a3 to delete.
        assert!(copies.lock().unwrap().is_empty());
        assert_eq!(
            deletes.lock().unwrap().clone(),
            vec![(
                "dst/key".to_string(),
                Some("TV1".to_string()),
                "a3".to_string()
            )]
        );
    }

    #[tokio::test]
    async fn annotation_recording_source_real_return_methods_behave_as_expected() {
        // The annotation source mock's non-panicking helper methods are not
        // touched by the annotation `transfer()` path (they only exercise
        // head/get/list/get-annotation). Pin their trivial contracts directly.
        let source = recording_source(crate::types::AnnotationMap::new());

        assert!(!source.is_local_storage());
        assert!(!source.is_express_onezone_storage());
        assert!(source.get_client().is_none());
        assert!(source.get_rate_limit_bandwidth().is_none());
        assert_eq!(source.get_local_path(), PathBuf::new());
        let _tx = source.get_stats_sender();
        source
            .send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        source.set_warning();

        // put_object is a guard that must reject being called on the source.
        let put_err = source
            .put_object(
                "k",
                Box::new(MockSource::new(None)),
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
        assert!(put_err.to_string().contains("must not be invoked"));
    }

    #[tokio::test]
    async fn annotation_recording_target_real_return_methods_behave_as_expected() {
        let target = recording_target("\"etag\"", None, crate::types::AnnotationMap::new());

        assert!(!target.is_local_storage());
        assert!(!target.is_express_onezone_storage());
        assert!(target.get_client().is_none());
        assert!(target.get_rate_limit_bandwidth().is_none());
        assert_eq!(target.get_local_path(), PathBuf::new());
        let _tx = target.get_stats_sender();
        target
            .send_stats(SyncStatistics::SyncComplete { key: "k".into() })
            .await;
        target.set_warning();
    }

    /// Source whose HEAD advertises a positive tag count but whose
    /// GetObjectTagging comes back empty — drives the "tags per HEAD but the
    /// tag set is actually empty ⇒ tagging is None" branch of `transfer()`.
    #[derive(Clone)]
    struct EmptyTaggingSource;

    #[async_trait]
    impl StorageTrait for EmptyTaggingSource {
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
                .build())
        }
        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            // Empty tag set ⇒ the caller resolves tagging to None.
            Ok(GetObjectTaggingOutput::builder()
                .set_tag_set(Some(Vec::new()))
                .build()
                .unwrap())
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
            // Positive tag_count so the tagging path does NOT short-circuit and
            // actually calls get_object_tagging.
            Ok(HeadObjectOutput::builder()
                .content_length(4)
                .e_tag("\"abc\"")
                .last_modified(DateTime::from_secs(0))
                .tag_count(2)
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
            unimplemented!()
        }
        fn set_warning(&self) {}
    }

    /// Target whose put_object returns a PutObjectOutput with no ETag — drives
    /// the "no ETag returned" warn branch of `transfer()`.
    #[derive(Clone)]
    struct NoEtagTarget;

    #[async_trait]
    impl StorageTrait for NoEtagTarget {
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            // No ETag ⇒ the warn branch fires.
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

    /// Source that cancels the pipeline token from inside head_object — lets a
    /// test drive `transfer()` past the entry-time cancellation check and into
    /// the post-download check, which is otherwise unreachable with a token
    /// that was already cancelled before the call.
    #[derive(Clone)]
    struct CancelOnHeadSource {
        token: PipelineCancellationToken,
    }

    #[async_trait]
    impl StorageTrait for CancelOnHeadSource {
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
            // Cancel AFTER the entry check has already passed, so the next
            // cancellation check (post-download) is the one that trips.
            self.token.cancel();
            Ok(HeadObjectOutput::builder()
                .content_length(4)
                .e_tag("\"abc\"")
                .last_modified(DateTime::from_secs(0))
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
            unimplemented!()
        }
        fn set_warning(&self) {}
    }

    #[tokio::test]
    async fn transfer_resolves_tagging_to_none_when_tag_set_is_empty() {
        // HEAD reports a positive tag_count, GetObjectTagging returns nothing ⇒
        // tagging is None and the transfer still completes.
        let config = minimal_config(false);
        let source: Storage = Box::new(EmptyTaggingSource);
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
    async fn transfer_skips_tagging_when_disabled() {
        // disable_tagging short-circuits tagging to None before any
        // GetObjectTagging call. MockSource's get_object_tagging is an
        // unimplemented!() stub, so reaching this branch cleanly proves the
        // call was skipped.
        let mut config = minimal_config(false);
        config.disable_tagging = true;
        let source: Storage = Box::new(MockSource::new(Some("V1".to_string())));
        let target: Storage = Box::new(MockTarget);
        let token = create_pipeline_cancellation_token();
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        assert_eq!(outcome.source_version_id.as_deref(), Some("V1"));
    }

    #[tokio::test]
    async fn transfer_warns_but_succeeds_when_target_returns_no_etag() {
        let config = minimal_config(false);
        let source: Storage = Box::new(MockSource::new(None));
        let target: Storage = Box::new(NoEtagTarget);
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
    async fn transfer_returns_default_when_cancelled_after_download() {
        // The token starts uncancelled (entry check passes) and is cancelled
        // from inside head_object, so the post-download cancellation check is
        // the one that returns the default outcome.
        let config = minimal_config(false);
        let token = create_pipeline_cancellation_token();
        let source: Storage = Box::new(CancelOnHeadSource {
            token: token.clone(),
        });
        let target: Storage = Box::new(MockTarget);
        let (stats_tx, _stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config, source, target, "src/key", "dst/key", token, stats_tx,
        )
        .await
        .unwrap();

        // Post-download cancellation ⇒ default outcome, no put_object ran
        // (MockTarget::put_object would have succeeded, but the version-id is
        // still None because the outcome is the default).
        assert_eq!(outcome.source_version_id, None);
    }

    // ------------------------------------------------------------------
    // Direct trait coverage for the branch-driving mocks above. Each one is
    // built for a single `transfer()` branch, so the transfer-level tests
    // touch only a handful of its methods; the pairs below pin the rest of
    // the real-return methods and verify every `unimplemented!()` stub still
    // panics (so the regression guard remains intact).
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn empty_tagging_source_real_return_methods_behave_as_expected() {
        let source = EmptyTaggingSource;

        assert!(!source.is_local_storage());
        assert!(!source.is_express_onezone_storage());

        // HEAD advertises tags so the tagging path is not short-circuited...
        let head = source
            .head_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(head.tag_count(), Some(2));
        assert_eq!(head.content_length(), Some(4));
        assert_eq!(head.e_tag(), Some("\"abc\""));

        // ...but the tag set itself is empty, which is the whole point of
        // this mock: the caller must resolve tagging to None.
        let tagging = source.get_object_tagging("k", None).await.unwrap();
        assert!(tagging.tag_set().is_empty());

        let get = source
            .get_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(get.content_length(), Some(4));
        assert_eq!(get.e_tag(), Some("\"abc\""));

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
    async fn empty_tagging_source_unimplemented_methods_panic() {
        let source = EmptyTaggingSource;

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
        assert_future_panics(source.put_object(
            "k",
            Box::new(MockTarget),
            "src",
            0,
            None,
            dummy_get_object_output(),
            None,
            None,
            None,
        ))
        .await;
        assert_future_panics(source.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(source.delete_object("k", None)).await;

        assert_call_panics(|| source.generate_copy_source_key("k", None));
    }

    #[tokio::test]
    async fn no_etag_target_real_return_methods_behave_as_expected() {
        let target = NoEtagTarget;

        assert!(!target.is_local_storage());
        assert!(!target.is_express_onezone_storage());

        // The defining behaviour: a successful put that reports no ETag.
        let put = target
            .put_object(
                "k",
                Box::new(MockSource::new(None)),
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
        assert_eq!(put.e_tag(), None);

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
    async fn no_etag_target_unimplemented_methods_panic() {
        let target = NoEtagTarget;

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

    #[tokio::test]
    async fn cancel_on_head_source_real_return_methods_behave_as_expected() {
        let token = create_pipeline_cancellation_token();
        let source = CancelOnHeadSource {
            token: token.clone(),
        };

        assert!(!source.is_local_storage());
        assert!(!source.is_express_onezone_storage());

        // GET does not touch the token — only HEAD cancels.
        let get = source
            .get_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(get.content_length(), Some(4));
        assert_eq!(get.e_tag(), Some("\"abc\""));
        assert!(!token.is_cancelled(), "get_object must not cancel");

        let head = source
            .head_object("k", None, None, None, None, no_sse_c_key(), None)
            .await
            .unwrap();
        assert_eq!(head.content_length(), Some(4));
        assert_eq!(head.e_tag(), Some("\"abc\""));
        assert!(
            token.is_cancelled(),
            "head_object must cancel the token so the post-download guard trips"
        );

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
    async fn cancel_on_head_source_unimplemented_methods_panic() {
        let source = CancelOnHeadSource {
            token: create_pipeline_cancellation_token(),
        };

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
        assert_future_panics(source.put_object(
            "k",
            Box::new(MockTarget),
            "src",
            0,
            None,
            dummy_get_object_output(),
            None,
            None,
            None,
        ))
        .await;
        assert_future_panics(source.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(source.delete_object("k", None)).await;

        assert_call_panics(|| source.generate_copy_source_key("k", None));
    }
}
