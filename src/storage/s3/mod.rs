use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_annotation::DeleteObjectAnnotationOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_annotation::PutObjectAnnotationOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectAttributes, ObjectPart, RequestPayer, Tagging,
};
use aws_smithy_types_convert::date_time::DateTimeExt;
use base64::Engine;
use base64::engine::general_purpose;
use leaky_bucket::RateLimiter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::io::AsyncReadExt;
use tracing::{debug, error, info, trace, warn};

use crate::Config;
use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::storage::s3::upload_manager::UploadManager;
use crate::storage::{
    Storage, StorageFactory, StorageTrait, convert_to_buf_byte_stream_with_callback,
};
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::{
    AnnotationMap, ObjectChecksum, SseCustomerKey, StoragePath, SyncStatistics,
    get_additional_checksum, is_full_object_checksum,
};

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

pub mod api;
mod client_builder;
mod upload_manager;

pub struct S3StorageFactory {}

#[async_trait]
impl StorageFactory for S3StorageFactory {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client_config: Option<ClientConfig>,
        request_payer: Option<RequestPayer>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
        _object_to_list: Option<String>,
    ) -> Storage {
        S3Storage::boxed_new(
            config,
            path,
            cancellation_token,
            stats_sender,
            Some(Arc::new(
                client_config.as_ref().unwrap().create_client().await,
            )),
            request_payer,
            rate_limit_bandwidth,
            has_warning,
        )
        .await
    }
}

#[derive(Clone)]
struct S3Storage {
    config: Config,
    bucket: String,
    cancellation_token: PipelineCancellationToken,
    client: Option<Arc<Client>>,
    request_payer: Option<RequestPayer>,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    has_warning: Arc<AtomicBool>,
}

impl S3Storage {
    #[allow(clippy::too_many_arguments)]
    async fn boxed_new(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client: Option<Arc<Client>>,
        request_payer: Option<RequestPayer>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
    ) -> Storage {
        let bucket = if let StoragePath::S3 { bucket, .. } = path {
            bucket
        } else {
            panic!("s3 path not found")
        };

        let storage = S3Storage {
            config,
            bucket,
            cancellation_token,
            client,
            request_payer,
            stats_sender,
            rate_limit_bandwidth,
            has_warning,
        };

        Box::new(storage)
    }
}

#[async_trait]
impl StorageTrait for S3Storage {
    fn is_local_storage(&self) -> bool {
        false
    }

    fn is_express_onezone_storage(&self) -> bool {
        is_express_onezone_storage(&self.bucket)
    }

    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(version_id)
            .set_checksum_mode(checksum_mode)
            .set_range(range)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::get_object() failed.")?;

        Ok(result)
    }

    async fn get_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_object_tagging()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(version_id)
            .send()
            .await
            .context("aws_sdk_s3::client::get_object_tagging() failed.")?;

        Ok(result)
    }

    async fn head_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(key)
            .set_range(range)
            .set_version_id(version_id)
            .set_checksum_mode(checksum_mode)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        Ok(result)
    }

    async fn head_object_first_part(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(version_id)
            .part_number(1)
            .set_checksum_mode(checksum_mode)
            .set_sse_customer_algorithm(sse_c)
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5)
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        Ok(result)
    }

    async fn get_object_parts(
        &self,
        key: &str,
        version_id: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        let object = self
            .client
            .as_ref()
            .unwrap()
            .head_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .part_number(1)
            .set_sse_customer_algorithm(sse_c.clone())
            .set_sse_customer_key(sse_c_key.key.clone())
            .set_sse_customer_key_md5(sse_c_key_md5.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::head_object() failed.")?;

        let mut object_parts = vec![];

        let parts_count = object.parts_count().unwrap_or_default();
        if parts_count == 0 {
            return Ok(vec![]);
        }

        object_parts.push(
            ObjectPartBuilder::default()
                .size(object.content_length().unwrap())
                .build(),
        );

        for part_number in 2..=parts_count {
            let object = self
                .client
                .as_ref()
                .unwrap()
                .head_object()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .key(key)
                .set_version_id(version_id.clone())
                .part_number(part_number)
                .set_sse_customer_algorithm(sse_c.clone())
                .set_sse_customer_key(sse_c_key.key.clone())
                .set_sse_customer_key_md5(sse_c_key_md5.clone())
                .send()
                .await
                .context("aws_sdk_s3::client::head_object() failed.")?;

            object_parts.push(
                ObjectPartBuilder::default()
                    .size(object.content_length().unwrap())
                    .build(),
            );
        }

        Ok(object_parts)
    }

    async fn get_object_parts_attributes(
        &self,
        key: &str,
        version_id: Option<String>,
        max_parts: i32,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        let mut object_parts = vec![];
        let mut part_number_marker = None;
        loop {
            let object = self
                .client
                .as_ref()
                .unwrap()
                .get_object_attributes()
                .set_request_payer(self.request_payer.clone())
                .bucket(&self.bucket)
                .key(key)
                .set_version_id(version_id.clone())
                .object_attributes(ObjectAttributes::ObjectParts)
                .set_part_number_marker(part_number_marker)
                .set_sse_customer_algorithm(sse_c.clone())
                .set_sse_customer_key(sse_c_key.key.clone())
                .set_sse_customer_key_md5(sse_c_key_md5.clone())
                .max_parts(max_parts)
                .send()
                .await
                .context("aws_sdk_s3::client::get_object_attributes() failed.")?;

            // A full object checksum has empty object parts.
            if object.object_parts().is_none() || object.object_parts().unwrap().parts().is_empty()
            {
                return Ok(vec![]);
            }

            for part in object.object_parts().unwrap().parts() {
                object_parts.push(part.clone());
            }

            if !object.object_parts().unwrap().is_truncated().unwrap() {
                break;
            }

            part_number_marker = object
                .object_parts()
                .unwrap()
                .next_part_number_marker()
                .map(|marker| marker.to_string());
        }

        Ok(object_parts)
    }

    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_key: &str,
        source_size: u64,
        source_additional_checksum: Option<String>,
        mut get_object_output_first_chunk: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        let mut version_id = "".to_string();
        if let Some(source_version_id) = get_object_output_first_chunk.version_id().as_ref() {
            version_id = source_version_id.to_string();
        }
        let target_key = key.to_string();
        let source_last_modified = aws_smithy_types::DateTime::from_millis(
            get_object_output_first_chunk
                .last_modified()
                .unwrap()
                .to_millis()?,
        )
        .to_chrono_utc()?
        .to_rfc3339();

        // In the case of full object checksum, we don't need to calculate checksum for each part and
        // don't need to pass it to the upload manager.
        let additional_checksum_value = get_additional_checksum(
            &get_object_output_first_chunk,
            object_checksum.as_ref().unwrap().checksum_algorithm.clone(),
        );
        let full_object_checksum = is_full_object_checksum(&additional_checksum_value);
        #[allow(clippy::unnecessary_unwrap)]
        let checksum = if object_checksum.is_some()
            && object_checksum
                .as_ref()
                .unwrap()
                .checksum_algorithm
                .is_some()
            && !self.config.full_object_checksum
            && !full_object_checksum
        {
            Some(Arc::new(AdditionalChecksum::new(
                object_checksum
                    .as_ref()
                    .unwrap()
                    .checksum_algorithm
                    .as_ref()
                    .unwrap()
                    .clone(),
                self.config.full_object_checksum,
            )))
        } else {
            None
        };

        // No SyncBytes emission at the source body read — SyncBytes are sent
        // after each upload_part/singlepart upload completes.
        get_object_output_first_chunk.body = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            None,
            self.rate_limit_bandwidth.clone(),
            checksum,
            object_checksum.clone(),
        );

        let mut upload_manager = UploadManager::new(
            self.client.clone().unwrap(),
            self.config.clone(),
            self.request_payer.clone(),
            self.cancellation_token.clone(),
            self.get_stats_sender(),
            tagging,
            object_checksum.unwrap_or_default().object_parts,
            self.is_express_onezone_storage(),
            source,
            source_key.to_string(),
            Some(source_size),
            source_additional_checksum,
            if_none_match,
            self.has_warning.clone(),
        );

        let put_object_output = upload_manager
            .upload(&self.bucket, &target_key, get_object_output_first_chunk)
            .await?;

        if put_object_output.e_tag.is_some() {
            debug!(
                key = key,
                source_version_id = version_id,
                source_last_modified = source_last_modified,
                target_key = target_key,
                size = source_size,
                "sync completed.",
            );
        }

        Ok(put_object_output)
    }

    async fn put_object_stream(
        &self,
        key: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        let mut upload_manager = UploadManager::new(
            self.client.clone().unwrap(),
            self.config.clone(),
            self.request_payer.clone(),
            self.cancellation_token.clone(),
            self.get_stats_sender(),
            tagging,
            object_checksum
                .as_ref()
                .and_then(|c| c.object_parts.clone()),
            self.is_express_onezone_storage(),
            Box::new(self.clone()),
            key.to_string(),
            None,
            None,
            if_none_match,
            self.has_warning.clone(),
        );

        upload_manager
            .upload_stream(&self.bucket, key, reader)
            .await
    }

    async fn put_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
        tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput> {
        let target_key = key.to_string();
        let version_id_str = version_id.clone().unwrap_or_default();

        let result = self
            .client
            .as_ref()
            .unwrap()
            .put_object_tagging()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(&target_key)
            .set_version_id(version_id.clone())
            .tagging(tagging)
            .send()
            .await
            .context("aws_sdk_s3::client::put_object_tagging() failed.")?;

        info!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            "sync(tagging only) completed.",
        );

        Ok(result)
    }

    async fn delete_object(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<DeleteObjectOutput> {
        let target_key = key.to_string();
        let version_id_str = version_id.clone().unwrap_or_default();

        let result = self
            .client
            .as_ref()
            .unwrap()
            .delete_object()
            .set_request_payer(self.request_payer.clone())
            .bucket(&self.bucket)
            .key(&target_key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::delete_object() failed.")?;

        trace!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            "Delete completed.",
        );

        Ok(result)
    }

    async fn list_object_annotations(
        &self,
        key: &str,
        version_id: Option<String>,
        max_annotation_results: i32,
    ) -> Result<AnnotationMap> {
        let mut continuation_token = None;
        let mut annotations_entries = vec![];

        // For the annotation's integrity, this method cannot be canceled.
        loop {
            let list_object_annotations_result = self
                .client
                .as_ref()
                .unwrap()
                .list_object_annotations()
                .bucket(&self.bucket)
                .key(key)
                .set_version_id(version_id.clone())
                .set_continuation_token(continuation_token)
                .set_request_payer(self.request_payer.clone())
                .max_annotation_results(max_annotation_results)
                .send()
                .await
                .context("aws_sdk_s3::client::list_object_annotations() failed.")?;

            if !list_object_annotations_result.annotations().is_empty() {
                annotations_entries.extend_from_slice(list_object_annotations_result.annotations());
            }

            if list_object_annotations_result
                .next_continuation_token
                .is_none()
            {
                break;
            }

            continuation_token = list_object_annotations_result
                .next_continuation_token()
                .map(|s| s.to_string());
        }

        let mut annotations_map = AnnotationMap::new();
        for annotation_entry in annotations_entries.iter() {
            annotations_map.insert(
                annotation_entry.annotation_name.clone(),
                annotation_entry.clone(),
            );
        }

        Ok(annotations_map)
    }

    async fn get_object_annotation(
        &self,
        key: &str,
        version_id: Option<String>,
        annotation_name: &str,
        checksum_mode: Option<ChecksumMode>,
    ) -> Result<GetObjectAnnotationOutput> {
        let result = self
            .client
            .as_ref()
            .unwrap()
            .get_object_annotation()
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(version_id)
            .annotation_name(annotation_name)
            .set_checksum_mode(checksum_mode)
            .set_request_payer(self.request_payer.clone())
            .send()
            .await?;

        Ok(result)
    }

    async fn copy_object_annotation(
        &self,
        key: &str,
        target_version_id: Option<String>,
        annotation_name: &str,
        source_annotation: GetObjectAnnotationOutput,
    ) -> Result<PutObjectAnnotationOutput> {
        let source_version_id_str = source_annotation
            .object_version_id
            .clone()
            .unwrap_or_default();
        let target_version_id_str = target_version_id.clone().unwrap_or_default();
        let source_annotation_size = source_annotation.content_length.unwrap() as usize;
        let source_server_side_encryption = source_annotation
            .server_side_encryption
            .as_ref()
            .map(|s| s.to_string());

        let checksum_algorithm = get_annotation_checksum_algorithm(&source_annotation);

        // As of June 2026, there are reportedly no annotations larger than 1 MiB.
        if source_annotation_size > 8 * 1024 * 1024 {
            // This is the safeguard against the case where the size is too large.
            return Err(anyhow!("invalid source annotation size"));
        }

        let mut buffer = Vec::<u8>::with_capacity(source_annotation_size);
        buffer.resize_with(source_annotation_size, Default::default);

        let mut body = convert_to_buf_byte_stream_with_callback(
            source_annotation.annotation_payload.into_async_read(),
            Some(self.get_stats_sender()),
            self.rate_limit_bandwidth.clone(),
            None,
            None,
        )
        .into_async_read();

        let read_result = body.read_exact(buffer.as_mut_slice()).await;
        if let Err(e) = read_result {
            warn!(key = key, "Failed to read annotation from the body: {e:?}");
            return Err(anyhow!(S3syncError::DownloadForceRetryableError));
        }

        let md5_digest = md5::compute(&buffer);
        let md5_digest_base64 = Some(general_purpose::STANDARD.encode(md5_digest.as_slice()));

        let buffer_stream = ByteStream::from(buffer);

        let result = self
            .client
            .as_ref()
            .unwrap()
            .put_object_annotation()
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(target_version_id.clone())
            .annotation_name(annotation_name)
            .annotation_payload(buffer_stream)
            .set_content_md5(md5_digest_base64)
            .set_checksum_algorithm(checksum_algorithm)
            .set_checksum_crc32(source_annotation.checksum_crc32)
            .set_checksum_crc32_c(source_annotation.checksum_crc32_c)
            .set_checksum_crc64_nvme(source_annotation.checksum_crc64_nvme)
            .set_checksum_sha1(source_annotation.checksum_sha1)
            .set_checksum_sha256(source_annotation.checksum_sha256)
            .set_checksum_sha512(source_annotation.checksum_sha512)
            .set_checksum_md5(source_annotation.checksum_md5)
            .set_checksum_xxhash64(source_annotation.checksum_xxhash64)
            .set_checksum_xxhash3(source_annotation.checksum_xxhash3)
            .set_checksum_xxhash128(source_annotation.checksum_xxhash128)
            .set_request_payer(self.request_payer.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::put_object_annotation() failed.")?;

        let target_server_side_encryption = result.server_side_encryption().map(|s| s.to_string());
        let mut skip_etag_verify = source_server_side_encryption.is_some()
            && source_server_side_encryption.unwrap() != "AES256";
        if !skip_etag_verify {
            skip_etag_verify = target_server_side_encryption.is_some()
                && target_server_side_encryption.unwrap() != "AES256";
        }

        if skip_etag_verify || result.e_tag == source_annotation.e_tag {
            info!(
                key = key,
                source_version_id = source_version_id_str,
                target_version_id = target_version_id_str,
                annotation_name = annotation_name,
                annotation_size = source_annotation_size,
                annotation_etag = result.e_tag().unwrap_or_default(),
                "sync object annotation completed."
            );
        } else {
            error!(
                key = key,
                source_version_id = source_version_id_str,
                target_version_id = target_version_id_str,
                annotation_name = annotation_name,
                annotation_size = source_annotation_size,
                source_etag = source_annotation.e_tag.unwrap_or_default(),
                target_etag = result.e_tag().unwrap_or_default(),
                "sync object annotation failed. etag mismatch."
            );
            return Err(anyhow!("sync object annotation failed. etag mismatch."));
        }

        Ok(result)
    }

    async fn delete_object_annotation(
        &self,
        key: &str,
        target_version_id: Option<String>,
        annotation_name: &str,
    ) -> Result<DeleteObjectAnnotationOutput> {
        let version_id_str = target_version_id.clone().unwrap_or_default();

        let result = self
            .client
            .as_ref()
            .unwrap()
            .delete_object_annotation()
            .bucket(&self.bucket)
            .key(key)
            .set_version_id(target_version_id.clone())
            .annotation_name(annotation_name)
            .set_request_payer(self.request_payer.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::delete_object_annotation() failed.")?;

        info!(
            key = key,
            target_version_id = version_id_str,
            annotation_name = annotation_name,
            "delete object annotation completed.",
        );

        Ok(result)
    }

    fn get_client(&self) -> Option<Arc<Client>> {
        self.client.clone()
    }

    fn get_stats_sender(&self) -> Sender<SyncStatistics> {
        self.stats_sender.clone()
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn get_local_path(&self) -> PathBuf {
        // S3 storage does not have a local path.
        unimplemented!();
    }

    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
        self.rate_limit_bandwidth.clone()
    }

    fn generate_copy_source_key(&self, key: &str, version_id: Option<String>) -> String {
        let encoded_key = urlencoding::encode(key);

        if version_id.is_some() {
            return format!(
                "{}/{}?versionId={}",
                &self.bucket,
                encoded_key,
                version_id.unwrap()
            );
        }
        format!("{}/{}", &self.bucket, encoded_key)
    }

    fn set_warning(&self) {
        self.has_warning
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
}

pub fn remove_s3_prefix(key: &str, prefix: &str) -> String {
    key.to_string().replacen(prefix, "", 1)
}

fn is_express_onezone_storage(bucket: &str) -> bool {
    bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
}

fn get_annotation_checksum_algorithm(
    source_annotation: &GetObjectAnnotationOutput,
) -> Option<ChecksumAlgorithm> {
    // Multiple checksum algorithms are not supported.
    if source_annotation.checksum_sha512.is_some() {
        Some(ChecksumAlgorithm::Sha512)
    } else if source_annotation.checksum_sha256.is_some() {
        Some(ChecksumAlgorithm::Sha256)
    } else if source_annotation.checksum_sha1.is_some() {
        Some(ChecksumAlgorithm::Sha1)
    } else if source_annotation.checksum_md5.is_some() {
        Some(ChecksumAlgorithm::Md5)
    } else if source_annotation.checksum_crc64_nvme.is_some() {
        Some(ChecksumAlgorithm::Crc64Nvme)
    } else if source_annotation.checksum_crc32.is_some() {
        Some(ChecksumAlgorithm::Crc32)
    } else if source_annotation.checksum_crc32_c.is_some() {
        Some(ChecksumAlgorithm::Crc32C)
    } else if source_annotation.checksum_xxhash64.is_some() {
        Some(ChecksumAlgorithm::Xxhash64)
    } else if source_annotation.checksum_xxhash3.is_some() {
        Some(ChecksumAlgorithm::Xxhash3)
    } else if source_annotation.checksum_xxhash128.is_some() {
        Some(ChecksumAlgorithm::Xxhash128)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn remove_s3_prefix_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_s3_prefix("dir1/data1", "dir1/data1"), "");

        assert_eq!(remove_s3_prefix("dir1/data1", "dir1"), "/data1");
        assert_eq!(remove_s3_prefix("dir1/data1", "dir1/"), "data1");
        assert_eq!(remove_s3_prefix("/dir1/data1", "/dir1"), "/data1");
        assert_eq!(remove_s3_prefix("/dir1/data1", "/dir1/"), "data1");
    }

    #[test]
    fn is_express_onezone_storage_test() {
        init_dummy_tracing_subscriber();

        assert!(is_express_onezone_storage("bucket--x-s3"));

        assert!(!is_express_onezone_storage("bucket-x-s3"));
        assert!(!is_express_onezone_storage("bucket--x-s3s"));
        assert!(!is_express_onezone_storage("bucket"));
    }

    #[test]
    fn get_annotation_checksum_algorithm_test() {
        init_dummy_tracing_subscriber();

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_sha512(Some("sha512".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Sha512)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_sha256(Some("sha256".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Sha256)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_sha1(Some("sha1".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Sha1)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_md5(Some("md5".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Md5)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_crc64_nvme(Some("crc64_nvme".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Crc64Nvme)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_crc32(Some("crc32".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Crc32)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_crc32_c(Some("crc32c".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Crc32C)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_xxhash64(Some("xxhash64".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Xxhash64)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_xxhash3(Some("xxhash3".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Xxhash3)
        );

        let source_annotation = GetObjectAnnotationOutput::builder()
            .set_checksum_xxhash128(Some("xxhash128".to_string()))
            .build();
        assert_eq!(
            get_annotation_checksum_algorithm(&source_annotation),
            Some(ChecksumAlgorithm::Xxhash128)
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }

    use crate::config::TransferConfig;
    use crate::types::SseKmsKeyId;
    use crate::types::token::create_pipeline_cancellation_token;
    use std::sync::atomic::Ordering;

    fn s3_storage_for_test(bucket: &str) -> S3Storage {
        let config = Config {
            source: StoragePath::S3 {
                bucket: bucket.to_string(),
                prefix: String::new(),
            },
            target: StoragePath::S3 {
                bucket: bucket.to_string(),
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
        };
        let (sender, _receiver) = async_channel::unbounded();
        S3Storage {
            config,
            bucket: bucket.to_string(),
            cancellation_token: create_pipeline_cancellation_token(),
            client: None,
            request_payer: None,
            stats_sender: sender,
            rate_limit_bandwidth: None,
            has_warning: Arc::new(AtomicBool::new(false)),
        }
    }

    #[test]
    fn s3_storage_generate_copy_source_key_no_version_id() {
        let storage = s3_storage_for_test("my-bucket");
        let key = storage.generate_copy_source_key("path/with spaces/key.txt", None);
        // Spaces and the slash in the key are URL-encoded by `urlencoding::encode`,
        // and the bucket is prefixed with a slash separator.
        assert!(key.starts_with("my-bucket/"));
        assert!(key.contains("path%2Fwith%20spaces%2Fkey.txt"));
        assert!(!key.contains("versionId="));
    }

    #[test]
    fn s3_storage_generate_copy_source_key_with_version_id_appends_query() {
        let storage = s3_storage_for_test("my-bucket");
        let key = storage.generate_copy_source_key("k.txt", Some("ABC123".to_string()));
        assert!(key.starts_with("my-bucket/"));
        assert!(key.ends_with("?versionId=ABC123"));
    }

    #[test]
    fn s3_storage_static_flags_for_regular_bucket() {
        let storage = s3_storage_for_test("regular-bucket");
        assert!(!storage.is_local_storage());
        assert!(!storage.is_express_onezone_storage());
        // No client set in test fixture.
        assert!(storage.get_client().is_none());
        assert!(storage.get_rate_limit_bandwidth().is_none());
    }

    #[test]
    fn s3_storage_is_express_onezone_when_bucket_suffix_matches() {
        let storage = s3_storage_for_test("zone-bucket--x-s3");
        assert!(storage.is_express_onezone_storage());
    }

    #[test]
    fn s3_storage_set_warning_flips_flag() {
        let storage = s3_storage_for_test("b");
        let flag = storage.has_warning.clone();
        assert!(!flag.load(Ordering::SeqCst));
        storage.set_warning();
        assert!(flag.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn s3_storage_send_stats_does_not_panic() {
        let storage = s3_storage_for_test("b");
        storage.send_stats(SyncStatistics::SyncBytes(123)).await;
    }

    #[test]
    #[should_panic(expected = "not implemented")]
    fn s3_storage_get_local_path_panics() {
        let storage = s3_storage_for_test("b");
        let _ = storage.get_local_path();
    }

    #[tokio::test]
    #[should_panic(expected = "s3 path not found")]
    async fn s3_storage_boxed_new_panics_for_local_path() {
        let (sender, _receiver) = async_channel::unbounded();
        let _ = S3Storage::boxed_new(
            s3_storage_for_test("b").config,
            StoragePath::Local("/tmp".into()),
            create_pipeline_cancellation_token(),
            sender,
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "s3 path not found")]
    async fn s3_storage_boxed_new_panics_for_stdio_path() {
        let (sender, _receiver) = async_channel::unbounded();
        let _ = S3Storage::boxed_new(
            s3_storage_for_test("b").config,
            StoragePath::Stdio,
            create_pipeline_cancellation_token(),
            sender,
            None,
            None,
            None,
            Arc::new(AtomicBool::new(false)),
        )
        .await;
    }
}
