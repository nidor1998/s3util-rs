use anyhow::{Context, Result};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{ChecksumMode, ObjectAttributes, ObjectPart, RequestPayer, Tagging};
use aws_smithy_types_convert::date_time::DateTimeExt;
use leaky_bucket::RateLimiter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tracing::info;

use crate::Config;
use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::storage::s3::upload_manager::UploadManager;
use crate::storage::{
    Storage, StorageFactory, StorageTrait, convert_to_buf_byte_stream_with_callback,
};
use crate::types::token::PipelineCancellationToken;
use crate::types::{
    ObjectChecksum, SseCustomerKey, StoragePath, SyncStatistics, get_additional_checksum,
    is_full_object_checksum,
};

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

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

        // Use a dummy stats sender for the body wrapper — we don't want SyncBytes
        // sent when reading from source (reads can be instant for local files).
        // Instead, SyncBytes are sent after each upload_part/singlepart upload completes.
        let (dummy_stats_sender, _dummy_stats_receiver) = async_channel::unbounded();
        get_object_output_first_chunk.body = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            Some(dummy_stats_sender),
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
            info!(
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

        info!(
            key = key,
            target_version_id = version_id_str,
            target_key = target_key,
            "delete completed.",
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

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
