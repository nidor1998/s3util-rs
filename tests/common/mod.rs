#![allow(dead_code)]
#![allow(clippy::assertions_on_constants)]

use anyhow::Result;
use aws_config::meta::region::{ProvideRegion, RegionProviderChain};
use aws_config::{BehaviorVersion, ConfigLoader};
use aws_sdk_s3::client::Client;
use aws_sdk_s3::config::Builder;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    BucketInfo, BucketLocationConstraint, BucketType, ChecksumMode, CreateBucketConfiguration,
    DataRedundancy, LocationInfo, LocationType, Object, Tag, Tagging,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired;
use aws_types::SdkConfig;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use aws_sdk_s3::types::RequestPayer;
use sha2::{Digest, Sha256};

use s3util_rs::config::Config;
use s3util_rs::config::args::{Commands, parse_from_args};
use s3util_rs::storage::StorageFactory;
use s3util_rs::storage::local::LocalStorageFactory;
use s3util_rs::storage::s3::S3StorageFactory;
use s3util_rs::transfer::{TransferDirection, detect_direction};
use s3util_rs::types::token::create_pipeline_cancellation_token;
use s3util_rs::types::{StoragePath, SyncStatistics};

pub const REGION: &str = "ap-northeast-1";
pub const EXPRESS_ONE_ZONE_AZ: &str = "apne1-az4";

pub const LARGE_FILE_SIZE: usize = 30 * 1024 * 1024;
pub const TEST_FILE_SIZE_8MIB: usize = 8 * 1024 * 1024;

pub const RANDOM_DATA_SEED_FILE: &str = "./test_data/random_data_seed";
pub const TEST_RANDOM_DATA_FILE_KEY: &str = "random_data";

pub const TEST_CONTENT_DISPOSITION: &str = "attachment; filename=\"filename.jpg\"";
pub const TEST_CONTENT_ENCODING: &str = "deflate";
pub const TEST_CONTENT_LANGUAGE: &str = "en-US,en-CA";
pub const TEST_CACHE_CONTROL: &str = "s-maxage=1604800";
pub const TEST_CONTENT_TYPE: &str = "application/vnd.ms-excel";
pub const TEST_TAGGING: &str = "tag1=tag_value1&tag2=tag_value2";
pub const TEST_METADATA_STRING: &str = "key1=value1,key2=value2";
pub const TEST_WEBSITE_REDIRECT: &str = "/redirect";

pub const TEST_EXPIRES: &str = "2055-05-20T00:00:00.000Z";

pub static TEST_METADATA: Lazy<HashMap<String, String>> = Lazy::new(|| {
    HashMap::from([
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
    ])
});

pub const TEST_SSE_C_KEY_1: &str = "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=";
pub const TEST_SSE_C_KEY_1_MD5: &str = "zZ5FnqcIqUjVwvWmyog4zw==";
pub const TEST_SSE_C_KEY_2: &str = "MTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTE=";
pub const TEST_SSE_C_KEY_2_MD5: &str = "GoDL8oWeAZVZNl1r5Hh5Tg==";

const PROFILE_NAME: &str = "s3sync-e2e-test";

#[derive(Debug, Default)]
pub struct StatsCount {
    pub sync_complete: u64,
    pub sync_error: u64,
    pub sync_warning: u64,
    pub e_tag_verified: u64,
    pub checksum_verified: u64,
}

#[cfg(e2e_test)]
pub struct TestHelper {
    client: Client,
}

#[cfg(e2e_test)]
impl TestHelper {
    pub async fn new() -> Self {
        Self {
            client: Self::create_client().await,
        }
    }

    pub fn generate_bucket_name() -> String {
        format!("s3util-e2e-{}", Uuid::new_v4())
    }

    pub fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }

    // ---------------------------------------------------------------
    // AWS Client helpers
    // ---------------------------------------------------------------

    pub async fn create_client() -> Client {
        Client::from_conf(
            Builder::from(&Self::load_sdk_config().await)
                .request_checksum_calculation(WhenRequired)
                .build(),
        )
    }

    async fn load_sdk_config() -> SdkConfig {
        let config_loader =
            Self::load_config_credential(aws_config::defaults(BehaviorVersion::latest()))
                .region(Self::build_provider_region());

        config_loader.load().await
    }

    fn load_config_credential(config_loader: ConfigLoader) -> ConfigLoader {
        let builder = aws_config::profile::ProfileFileCredentialsProvider::builder();
        config_loader.credentials_provider(builder.profile_name(PROFILE_NAME).build())
    }

    fn build_provider_region() -> Box<dyn ProvideRegion> {
        let mut builder = aws_config::profile::ProfileFileRegionProvider::builder();
        builder = builder.profile_name(PROFILE_NAME);
        let provider_region = RegionProviderChain::first_try(builder.build());
        Box::new(provider_region)
    }

    // ---------------------------------------------------------------
    // Bucket operations
    // ---------------------------------------------------------------

    pub async fn create_bucket(&self, bucket: &str, region: &str) {
        let constraint = BucketLocationConstraint::from(region);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();

        self.client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket)
            .send()
            .await
            .unwrap();
    }

    pub async fn create_directory_bucket(&self, bucket_name: &str, availability_zone: &str) {
        let location_info = LocationInfo::builder()
            .r#type(LocationType::AvailabilityZone)
            .name(availability_zone)
            .build();
        let bucket_info = BucketInfo::builder()
            .data_redundancy(DataRedundancy::SingleAvailabilityZone)
            .r#type(BucketType::Directory)
            .build();
        let configuration = CreateBucketConfiguration::builder()
            .location(location_info)
            .bucket(bucket_info)
            .build();

        self.client
            .create_bucket()
            .create_bucket_configuration(configuration)
            .bucket(bucket_name)
            .send()
            .await
            .unwrap();
    }

    pub async fn is_bucket_exist(&self, bucket: &str) -> bool {
        let head_bucket_result = self.client.head_bucket().bucket(bucket).send().await;
        if head_bucket_result.is_ok() {
            return true;
        }
        !head_bucket_result
            .err()
            .unwrap()
            .into_service_error()
            .is_not_found()
    }

    pub async fn delete_bucket_with_cascade(&self, bucket: &str) {
        if !self.is_bucket_exist(bucket).await {
            return;
        }

        self.delete_all_objects(bucket).await;

        let result = self.client.delete_bucket().bucket(bucket).send().await;
        if let Err(e) = result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                assert!(false, "S3 API error has occurred.")
            }
        }
    }

    pub async fn delete_directory_bucket_with_cascade(&self, bucket: &str) {
        if !self.is_bucket_exist(bucket).await {
            return;
        }

        self.delete_all_objects(bucket).await;

        let result = self.client.delete_bucket().bucket(bucket).send().await;
        if let Err(e) = result {
            let service_error = e.into_service_error();
            if let Some(code) = service_error.meta().code() {
                assert_eq!(code, "NoSuchBucket");
            } else {
                assert!(false, "S3 API error has occurred.")
            }
        }
    }

    // ---------------------------------------------------------------
    // Object operations
    // ---------------------------------------------------------------

    pub async fn list_objects(&self, bucket: &str, prefix: &str) -> Vec<Object> {
        let list_objects_output = self
            .client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(prefix)
            .send()
            .await
            .unwrap();

        list_objects_output.contents().to_vec()
    }

    pub async fn head_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> HeadObjectOutput {
        self.client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .checksum_mode(ChecksumMode::Enabled)
            .send()
            .await
            .unwrap()
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> GetObjectOutput {
        self.client
            .get_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap()
    }

    pub async fn get_object_tagging(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> GetObjectTaggingOutput {
        self.client
            .get_object_tagging()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id)
            .send()
            .await
            .unwrap()
    }

    pub async fn is_object_exist(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> bool {
        let head_object_result = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await;

        if head_object_result.is_ok() {
            return true;
        }

        !head_object_result
            .err()
            .unwrap()
            .into_service_error()
            .is_not_found()
    }

    pub async fn put_object(&self, bucket: &str, key: &str, body: Vec<u8>) {
        let stream = ByteStream::from(body);
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_object_from_file(&self, bucket: &str, key: &str, path: &str) {
        let stream = ByteStream::from_path(path).await.unwrap();
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_object_with_metadata(&self, bucket: &str, key: &str, path: &str) {
        let stream = ByteStream::from_path(path).await.unwrap();
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .cache_control(TEST_CACHE_CONTROL)
            .content_disposition(TEST_CONTENT_DISPOSITION)
            .content_encoding(TEST_CONTENT_ENCODING)
            .content_language(TEST_CONTENT_LANGUAGE)
            .content_type(TEST_CONTENT_TYPE)
            .set_metadata(Some(TEST_METADATA.clone()))
            .expires(DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime).unwrap())
            .website_redirect_location("/xxx")
            .tagging(TEST_TAGGING)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_sized_object(&self, bucket: &str, key: &str, size: usize) {
        let mut buffer = Vec::<u8>::with_capacity(size);
        buffer.resize_with(size, Default::default);
        let stream = ByteStream::from(buffer);

        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn put_empty_object(&self, bucket: &str, key: &str) {
        let stream = ByteStream::from_static(&[]);
        self.client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(stream)
            .send()
            .await
            .unwrap();
    }

    pub async fn delete_object(&self, bucket: &str, key: &str, version_id: Option<String>) {
        self.client
            .delete_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();
    }

    pub async fn delete_all_objects(&self, bucket: &str) {
        let list_objects_output_result = self.client.list_objects_v2().bucket(bucket).send().await;

        if list_objects_output_result.is_err() {
            return;
        }

        for object in list_objects_output_result.unwrap().contents() {
            self.delete_object(bucket, object.key().unwrap(), None)
                .await;
        }
    }

    // ---------------------------------------------------------------
    // Verification helpers
    // ---------------------------------------------------------------

    pub async fn verify_e_tag(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
        e_tag: &str,
    ) -> bool {
        let head_object_output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();

        head_object_output.e_tag().unwrap() == e_tag
    }

    pub async fn verify_test_object_metadata(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> bool {
        let head_object_output = self
            .client
            .head_object()
            .bucket(bucket)
            .key(key)
            .set_version_id(version_id.clone())
            .send()
            .await
            .unwrap();

        assert_eq!(
            head_object_output.cache_control().unwrap(),
            TEST_CACHE_CONTROL
        );
        assert_eq!(
            head_object_output.content_disposition().unwrap(),
            TEST_CONTENT_DISPOSITION
        );
        assert_eq!(
            head_object_output.content_encoding().unwrap(),
            TEST_CONTENT_ENCODING
        );
        assert_eq!(
            head_object_output.content_language().unwrap(),
            TEST_CONTENT_LANGUAGE
        );
        assert_eq!(
            head_object_output.content_type().unwrap(),
            TEST_CONTENT_TYPE
        );
        assert_eq!(
            head_object_output.metadata().unwrap(),
            &TEST_METADATA.clone()
        );
        assert_eq!(
            head_object_output.expires_string.unwrap(),
            DateTime::from_str(TEST_EXPIRES, DateTimeFormat::DateTime)
                .unwrap()
                .fmt(DateTimeFormat::HttpDate)
                .unwrap()
                .to_string()
        );

        let get_object_tagging_output = self
            .get_object_tagging(bucket, key, version_id.clone())
            .await;

        let tag_set = get_object_tagging_output.tag_set();
        let tag_map = Self::tag_set_to_map(tag_set);
        let expected_tag_map = HashMap::from([
            ("tag1".to_string(), "tag_value1".to_string()),
            ("tag2".to_string(), "tag_value2".to_string()),
        ]);

        assert_eq!(tag_map, expected_tag_map);

        true
    }

    // ---------------------------------------------------------------
    // File utilities
    // ---------------------------------------------------------------

    pub fn create_temp_dir() -> PathBuf {
        let dir = PathBuf::from(format!("./playground/tmp_{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub fn create_test_file(dir: &Path, name: &str, content: &[u8]) -> PathBuf {
        let file_path = dir.join(name);
        std::fs::write(&file_path, content).unwrap();
        file_path
    }

    pub fn create_sized_file(dir: &Path, name: &str, size: usize) -> PathBuf {
        let file_path = dir.join(name);
        let data = vec![0_u8; size];
        std::fs::write(&file_path, data.as_slice()).unwrap();
        file_path
    }

    pub fn create_random_data_file(
        dir: &Path,
        name: &str,
        size_mb: usize,
        extra: i32,
    ) -> Result<PathBuf> {
        let output_path = dir.join(name);

        // Use deterministic pseudo-random data for reproducible tests
        let mut data = Vec::with_capacity(size_mb * 1024 * 1024 + 1);
        let pattern: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();

        for _ in 0..size_mb * 1024 {
            data.extend_from_slice(&pattern);
        }

        if extra > 0 {
            data.push(b'Z');
        } else if extra < 0 {
            data.pop();
        }

        std::fs::write(&output_path, &data)?;

        Ok(output_path)
    }

    pub fn get_sha256_from_file(file_path: &str) -> String {
        let mut file = File::open(file_path).unwrap();
        let mut hasher = Sha256::new();
        let mut buffer = [0; 1024];

        loop {
            let bytes_read = file.read(&mut buffer).unwrap();
            if bytes_read == 0 {
                break;
            }
            hasher.update(&buffer[..bytes_read]);
        }

        let hash_result = hasher.finalize();
        hash_result
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    }

    pub fn md5_digest(path: &str) -> String {
        let mut file = File::open(path).unwrap();
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();
        format!("{:x}", md5::compute(&buffer))
    }

    pub fn tag_set_to_map(tag_set: &[Tag]) -> HashMap<String, String> {
        let mut map = HashMap::<_, _>::new();
        for tag in tag_set {
            map.insert(tag.key().to_string(), tag.value().to_string());
        }
        map
    }

    pub fn is_file_exist(path: &str) -> bool {
        PathBuf::from(path).try_exists().unwrap()
    }

    // ---------------------------------------------------------------
    // CP helpers — run the cp transfer programmatically
    // ---------------------------------------------------------------

    /// Run a cp operation and collect stats.
    pub async fn cp_test_data(&self, args: Vec<&str>) -> StatsCount {
        let cli = parse_from_args(args).unwrap();
        let cp_args = match cli.command {
            Commands::Cp(cp_args) => cp_args,
        };
        let config = Config::try_from(cp_args).unwrap();

        let cancellation_token = create_pipeline_cancellation_token();
        let (stats_sender, stats_receiver) = async_channel::unbounded();

        let source_str = match &config.source {
            StoragePath::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    format!("s3://{}", bucket)
                } else {
                    format!("s3://{}/{}", bucket, prefix)
                }
            }
            StoragePath::Local(path) => path.to_string_lossy().to_string(),
            StoragePath::Stdio => "-".to_string(),
        };
        let target_str = match &config.target {
            StoragePath::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    format!("s3://{}", bucket)
                } else {
                    format!("s3://{}/{}", bucket, prefix)
                }
            }
            StoragePath::Local(path) => path.to_string_lossy().to_string(),
            StoragePath::Stdio => "-".to_string(),
        };

        let direction = detect_direction(&source_str, &target_str).unwrap();

        let key = Self::extract_key_for_test(&config, &direction);
        let has_warning = Arc::new(AtomicBool::new(false));

        let result = match direction {
            TransferDirection::LocalToS3 => {
                let target_request_payer = if config.target_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };

                let source = LocalStorageFactory::create(
                    config.clone(),
                    config.source.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    None,
                    None,
                    None,
                    has_warning.clone(),
                )
                .await;

                let target = S3StorageFactory::create(
                    config.clone(),
                    config.target.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                s3util_rs::transfer::local_to_s3::transfer(
                    &config,
                    source,
                    target,
                    &key,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                )
                .await
            }
            TransferDirection::S3ToLocal => {
                let source_request_payer = if config.source_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };

                let source = S3StorageFactory::create(
                    config.clone(),
                    config.source.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                let target = LocalStorageFactory::create(
                    config.clone(),
                    config.target.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    None,
                    None,
                    None,
                    has_warning.clone(),
                )
                .await;

                s3util_rs::transfer::s3_to_local::transfer(
                    &config,
                    source,
                    target,
                    &key,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                )
                .await
            }
            TransferDirection::S3ToS3 => {
                let source_request_payer = if config.source_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };
                let target_request_payer = if config.target_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };

                let source = S3StorageFactory::create(
                    config.clone(),
                    config.source.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                let target = S3StorageFactory::create(
                    config.clone(),
                    config.target.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                s3util_rs::transfer::s3_to_s3::transfer(
                    &config,
                    source,
                    target,
                    &key,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                )
                .await
            }
            TransferDirection::StdioToS3 => {
                let target_request_payer = if config.target_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };

                let target = S3StorageFactory::create(
                    config.clone(),
                    config.target.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                s3util_rs::transfer::stdio_to_s3::transfer(
                    &config,
                    target,
                    &key,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                )
                .await
            }
            TransferDirection::S3ToStdio => {
                let source_request_payer = if config.source_request_payer {
                    Some(RequestPayer::Requester)
                } else {
                    None
                };

                let source = S3StorageFactory::create(
                    config.clone(),
                    config.source.clone(),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    has_warning.clone(),
                )
                .await;

                s3util_rs::transfer::s3_to_stdio::transfer(
                    &config,
                    source,
                    &key,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                )
                .await
            }
        };

        // Close the stats channel
        stats_sender.close();

        // Collect stats
        let mut stats = StatsCount::default();
        while let Ok(sync_stats) = stats_receiver.try_recv() {
            match sync_stats {
                SyncStatistics::SyncComplete { .. } => stats.sync_complete += 1,
                SyncStatistics::SyncError { .. } => stats.sync_error += 1,
                SyncStatistics::SyncWarning { .. } => stats.sync_warning += 1,
                SyncStatistics::ETagVerified { .. } => stats.e_tag_verified += 1,
                SyncStatistics::ChecksumVerified { .. } => stats.checksum_verified += 1,
                _ => {}
            }
        }

        if result.is_err() {
            stats.sync_error += 1;
        }

        stats
    }

    /// Run a cp operation and return (has_error, has_warning).
    pub async fn cp(&self, args: Vec<&str>) -> (bool, bool) {
        let stats = self.cp_test_data(args).await;
        (stats.sync_error > 0, stats.sync_warning > 0)
    }

    fn extract_key_for_test(config: &Config, direction: &TransferDirection) -> String {
        match direction {
            TransferDirection::LocalToS3 => {
                if let StoragePath::Local(path) = &config.source {
                    path.file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_default()
                } else {
                    String::new()
                }
            }
            TransferDirection::S3ToLocal
            | TransferDirection::S3ToS3
            | TransferDirection::S3ToStdio => {
                if let StoragePath::S3 { prefix, .. } = &config.source {
                    prefix.to_string()
                } else {
                    String::new()
                }
            }
            TransferDirection::StdioToS3 => {
                if let StoragePath::S3 { prefix, .. } = &config.target {
                    prefix.to_string()
                } else {
                    String::new()
                }
            }
        }
    }
}
