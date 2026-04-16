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
    DataRedundancy, LocationInfo, LocationType, Object, Tag,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation::WhenRequired;
use aws_types::SdkConfig;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
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

// Second set of metadata constants (same names as s3sync's TEST_*2).
// Used for re-sync/update tests to verify metadata changes.
pub const TEST_CONTENT_DISPOSITION2: &str = "attachment; filename=\"filename2.jpg\"";
pub const TEST_CONTENT_ENCODING2: &str = "gzip";
pub const TEST_CONTENT_LANGUAGE2: &str = "en-US,en-GB";
pub const TEST_CACHE_CONTROL2: &str = "s-maxage=1704800";
pub const TEST_CONTENT_TYPE2: &str = "application/excel";
pub const TEST_TAGGING2: &str = "tag1=tag_value1&tag2=tag_valueNew";
pub const TEST_METADATA_STRING2: &str = "key1=value1,key2=value2,key3=value3";
pub const TEST_WEBSITE_REDIRECT2: &str = "/redirect2";
pub const TEST_EXPIRES2: &str = "2055-04-20T00:00:00.000Z";

pub static TEST_METADATA2: Lazy<HashMap<String, String>> = Lazy::new(|| {
    HashMap::from([
        ("key1".to_string(), "value1".to_string()),
        ("key2".to_string(), "value2".to_string()),
        ("key3".to_string(), "value3".to_string()),
    ])
});

// ---------------------------------------------------------------
// Hard-coded ETag / SHA256 constants for deterministic test data.
// These are pre-computed from known test data and used to verify
// data integrity after upload and download operations, exactly
// replicating s3sync's verification mechanism.
// ---------------------------------------------------------------

// 9 MiB zero-filled file (create_sized_file with 9 * 1024 * 1024)
pub const ETAG_9M_ZEROS_NO_CHUNK: &str = "\"b82b4ab87e44976024abc14a1670dac0\"";
pub const ETAG_9M_ZEROS_8M_CHUNK: &str = "\"d126ef08817d0490e207e456cb0ae080-2\"";
pub const SHA256_9M_ZEROS: &str =
    "d2ee4703cd9698945ca7b9fe1689ea3095597eac1a0afd8dba00cac7894fdc43";

// 9 MiB zero-filled file with 5 MiB chunks
pub const ETAG_9M_ZEROS_5M_CHUNK: &str = "\"31decc4297aaa550907441d49af1735c-2\"";

// Empty file
pub const ETAG_EMPTY: &str = "\"d41d8cd98f00b204e9800998ecf8427e\"";
pub const SHA256_EMPTY: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

// Random data files use the same seed file as s3sync (test_data/random_data_seed).
// All constants below are pre-computed from that seed, matching s3sync exactly.
// Constant names follow s3sync's naming convention for human readability.

// 5M file with 5M chunk (same names as s3sync)
pub const ETAG_5M_FILE_5M_CHUNK: &str = "\"41c54a21b664d10684a24bb15b86b81b-1\"";
pub const ETAG_5M_PLUS_1_FILE_5M_CHUNK: &str = "\"c9f6c942564f9ebead5cb09e63b70dd7-2\"";
pub const ETAG_5M_MINUS_1_FILE_5M_CHUNK: &str = "\"74222dcf8ba716d84efe0dc716360087\"";
pub const SHA256_5M_FILE_WHOLE: &str =
    "27d49a61d9a504bf66761f4d3143702d97876ddf5864d4ba22467cd04cdc67f0";

// 8M file with 8M chunk (same names as s3sync)
pub const ETAG_8M_FILE_NO_CHUNK: &str = "\"e9d3e2caa0ac28fd50b183dac706ee29\"";
pub const ETAG_8M_FILE_5M_CHUNK: &str = "\"ebff86fc334a63cefaad7a0b621a0109-2\"";
pub const ETAG_8M_FILE_8M_CHUNK: &str = "\"13698b45ee34dbf0611fe527f76abfc7-1\"";
pub const ETAG_8M_PLUS_1_FILE_8M_CHUNK: &str = "\"61b0524a157f9391c45c09ae2b48dde4-2\"";
pub const ETAG_8M_MINUS_1_FILE_8M_CHUNK: &str = "\"c9c7b65a175f43ff8147d8027403e177\"";
pub const SHA256_8M_FILE_WHOLE: &str =
    "cd5f57c6ffe3f685104aba6ec7268baab8790603034bdec830228b572d84c5a4";

// 9M file (used in e2e_integrity_check and edge cases)
pub const ETAG_9M_FILE_NO_CHUNK: &str = "\"f4420c17234bf1af66cb4de063b28a87\"";
pub const ETAG_9M_FILE_5M_CHUNK: &str = "\"1ff4e384cc994ba509e38b8a58d9472b-2\"";
pub const ETAG_9M_FILE_7M_CHUNK: &str = "\"edaca60811bb44eceac3a6990ccad6c3-2\"";
pub const ETAG_9M_FILE_8M_CHUNK: &str = "\"62a3a89ec6809979873b115670cc4c32-2\"";
pub const ETAG_9M_FILE_9M_CHUNK: &str = "\"83789ae97e315329fe7642d5ac6c444b-1\"";
pub const SHA256_9M_FILE_WHOLE: &str =
    "497dfda0dfdb3b0ec8506a3b0afdc15c7612398eeb9ade7ef75fa386f2f70bc4";

// 10M file with 5M chunk (same names as s3sync)
pub const ETAG_10M_FILE_5M_CHUNK: &str = "\"fd863860e4b73868097377d43bd65a58-2\"";
pub const ETAG_10M_PLUS_1_FILE_5M_CHUNK: &str = "\"527cc728f7dd89f1ef3256e2ff5c808c-3\"";
pub const ETAG_10M_MINUS_1_FILE_5M_CHUNK: &str = "\"6e11660e4457458f925f9a92227be331-2\"";
pub const SHA256_10M_FILE_WHOLE: &str =
    "d5fc3f080e832d82161f9461291f87989b81a9e6281c33589d9563adefb46055";

// 16M file with 5M chunk (same names as s3sync)
pub const ETAG_16M_FILE_5M_CHUNK: &str = "\"db5daa6fb02e1c6b2063c5469b99e096-4\"";
pub const ETAG_16M_PLUS_1_FILE_5M_CHUNK: &str = "\"5e7e959b1416576b46fe9a7b3dea4c5e-4\"";
pub const ETAG_16M_MINUS_1_FILE_5M_CHUNK: &str = "\"cd769ef00f81a6d450848efda5e8870d-4\"";
pub const SHA256_16M_FILE_WHOLE: &str =
    "23bf32cdfd60784647663a160aee7c46ca7941173d48ad37db52713fda4562e1";

// 16M file with 8M chunk (same names as s3sync)
pub const ETAG_16M_FILE_8M_CHUNK: &str = "\"93724d91845349c1695f224995fa68ea-2\"";
pub const ETAG_16M_PLUS_1_FILE_8M_CHUNK: &str = "\"4c72ed54398639c8fdbf38f9455353c9-3\"";
pub const ETAG_16M_MINUS_1_FILE_8M_CHUNK: &str = "\"a34a18fab3e8715b3b1edc3cb9a52647-2\"";

// 30M file with 8M chunk (same names as s3sync)
pub const ETAG_30M_FILE_NO_CHUNK: &str = "\"94189ebb786dbc25aaf22d3d96e88aeb\"";
pub const ETAG_30M_FILE_8M_CHUNK: &str = "\"a81230a7666d413e511f9c2c2523947a-4\"";
pub const ETAG_30M_PLUS_1_FILE_8M_CHUNK: &str = "\"e10f60edd59877a2d1cd80b837460b80-4\"";
pub const ETAG_30M_MINUS_1_FILE_8M_CHUNK: &str = "\"4f36b633babe3a74e08884d6056ab6df-4\"";
pub const SHA256_30M_FILE_WHOLE_HEX: &str =
    "05c1c771d4886e4cefdf0a4c0b907913fe2f829dd767418c94ea278b0b8bc3f9";

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

    pub async fn create_bucket_with_sse_c_encryption(&self, bucket: &str, region: &str) {
        self.create_bucket(bucket, region).await;
        // Note: SSE-C is per-request encryption (key provided in each request header).
        // No PutBucketEncryption call needed — the --sse-c and --sse-c-key CLI args
        // handle this at the request level.
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

    pub async fn get_object_bytes(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<String>,
    ) -> Vec<u8> {
        let output = self.get_object(bucket, key, version_id).await;
        output.body.collect().await.unwrap().into_bytes().to_vec()
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

    pub async fn verify_test_object_metadata2(
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
            TEST_CACHE_CONTROL2
        );
        assert_eq!(
            head_object_output.content_disposition().unwrap(),
            TEST_CONTENT_DISPOSITION2
        );
        assert_eq!(
            head_object_output.content_encoding().unwrap(),
            TEST_CONTENT_ENCODING2
        );
        assert_eq!(
            head_object_output.content_language().unwrap(),
            TEST_CONTENT_LANGUAGE2
        );
        assert_eq!(
            head_object_output.content_type().unwrap(),
            TEST_CONTENT_TYPE2
        );
        assert_eq!(
            head_object_output.metadata().unwrap(),
            &TEST_METADATA2.clone()
        );
        assert_eq!(
            head_object_output.expires_string.unwrap(),
            DateTime::from_str(TEST_EXPIRES2, DateTimeFormat::DateTime)
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
            ("tag2".to_string(), "tag_valueNew".to_string()),
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

    /// Create a random data file using the same seed file as s3sync.
    /// This ensures the pre-computed ETag/SHA256 constants match exactly.
    pub fn create_random_data_file(
        dir: &Path,
        name: &str,
        size_mb: usize,
        extra: i32,
    ) -> Result<PathBuf> {
        let output_path = dir.join(name);

        let mut seed_file = File::open(RANDOM_DATA_SEED_FILE)?;
        let mut seed_data = vec![0u8; 1024];
        seed_file.read_exact(&mut seed_data)?;

        let mut data = Vec::with_capacity(size_mb * 1024 * 1024 + 1);
        for _ in 0..size_mb * 1024 {
            data.extend_from_slice(&seed_data);
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

    pub fn get_sha256_from_bytes(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
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

    /// Compute the MD5 hex digest of a byte slice.
    pub fn compute_md5_hex(data: &[u8]) -> String {
        format!("{:x}", md5::compute(data))
    }

    /// Compute the expected S3 ETag for a single-part upload.
    /// Returns a quoted string like `"\"<md5>\""`.
    pub fn expected_single_part_etag(data: &[u8]) -> String {
        format!("\"{}\"", Self::compute_md5_hex(data))
    }

    /// Compute the expected S3 ETag for a multipart upload with the given chunk size.
    /// Returns a quoted string like `"\"<md5>-<n>\""`.
    pub fn expected_multipart_etag(data: &[u8], chunk_size: usize) -> String {
        let mut part_md5s = Vec::new();
        for chunk in data.chunks(chunk_size) {
            part_md5s.extend_from_slice(&md5::compute(chunk).0);
        }
        let num_parts = (data.len() + chunk_size - 1) / chunk_size;
        format!("\"{}-{}\"", Self::compute_md5_hex(&part_md5s), num_parts)
    }

    /// Verify that an uploaded object's ETag matches the expected MD5
    /// of the given content (single-part upload).
    pub async fn verify_uploaded_object_etag(
        &self,
        bucket: &str,
        key: &str,
        expected_content: &[u8],
    ) {
        let head = self.head_object(bucket, key, None).await;
        let actual_etag = head.e_tag().unwrap();
        let expected_etag = Self::expected_single_part_etag(expected_content);
        assert_eq!(
            actual_etag, expected_etag,
            "ETag mismatch for s3://{}/{}: actual={}, expected={}",
            bucket, key, actual_etag, expected_etag
        );
    }

    /// Verify that an uploaded object's ETag matches a hard-coded expected value.
    pub async fn verify_uploaded_object_etag_value(
        &self,
        bucket: &str,
        key: &str,
        expected_etag: &str,
    ) {
        let head = self.head_object(bucket, key, None).await;
        let actual_etag = head.e_tag().unwrap();
        assert_eq!(
            actual_etag, expected_etag,
            "ETag mismatch for s3://{}/{}: actual={}, expected={}",
            bucket, key, actual_etag, expected_etag
        );
    }

    /// Verify that a downloaded file's MD5 matches the expected content's MD5.
    pub fn verify_downloaded_file_md5(path: &str, expected_content: &[u8]) {
        let actual_md5 = Self::md5_digest(path);
        let expected_md5 = Self::compute_md5_hex(expected_content);
        assert_eq!(
            actual_md5, expected_md5,
            "MD5 mismatch for {}: actual={}, expected={}",
            path, actual_md5, expected_md5
        );
    }

    /// Verify that a downloaded file's MD5 matches a hard-coded expected value.
    pub fn verify_downloaded_file_md5_value(path: &str, expected_md5: &str) {
        let actual_md5 = Self::md5_digest(path);
        assert_eq!(
            actual_md5, expected_md5,
            "MD5 mismatch for {}: actual={}, expected={}",
            path, actual_md5, expected_md5
        );
    }

    /// Verify that a downloaded file's SHA256 matches a hard-coded expected value.
    pub fn verify_downloaded_file_sha256(path: &str, expected_sha256: &str) {
        let actual_sha256 = Self::get_sha256_from_file(path);
        assert_eq!(
            actual_sha256, expected_sha256,
            "SHA256 mismatch for {}: actual={}, expected={}",
            path, actual_sha256, expected_sha256
        );
    }

    /// Verify that an S3 object's content matches the expected bytes
    /// by downloading and comparing MD5.
    pub async fn verify_object_content_md5(
        &self,
        bucket: &str,
        key: &str,
        expected_content: &[u8],
    ) {
        let bytes = self.get_object_bytes(bucket, key, None).await;
        let actual_md5 = Self::compute_md5_hex(&bytes);
        let expected_md5 = Self::compute_md5_hex(expected_content);
        assert_eq!(
            actual_md5, expected_md5,
            "Content MD5 mismatch for s3://{}/{}: actual={}, expected={}",
            bucket, key, actual_md5, expected_md5
        );
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

        let (source_key, target_key) = Self::extract_keys_for_test(&config, &direction);
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
                    StoragePath::Local(".".into()),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    None,
                    None,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                let empty_target = match &config.target {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let target = S3StorageFactory::create(
                    config.clone(),
                    empty_target,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                s3util_rs::transfer::local_to_s3::transfer(
                    &config,
                    source,
                    target,
                    &source_key,
                    &target_key,
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

                let empty_source = match &config.source {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let source = S3StorageFactory::create(
                    config.clone(),
                    empty_source,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                let target = LocalStorageFactory::create(
                    config.clone(),
                    StoragePath::Local(".".into()),
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    None,
                    None,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                s3util_rs::transfer::s3_to_local::transfer(
                    &config,
                    source,
                    target,
                    &source_key,
                    &target_key,
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

                let empty_source = match &config.source {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let source = S3StorageFactory::create(
                    config.clone(),
                    empty_source,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                let empty_target = match &config.target {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let target = S3StorageFactory::create(
                    config.clone(),
                    empty_target,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                s3util_rs::transfer::s3_to_s3::transfer(
                    &config,
                    source,
                    target,
                    &source_key,
                    &target_key,
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

                let empty_target = match &config.target {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let target = S3StorageFactory::create(
                    config.clone(),
                    empty_target,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.target_client_config.clone(),
                    target_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                s3util_rs::transfer::stdio_to_s3::transfer(
                    &config,
                    target,
                    &target_key,
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

                let empty_source = match &config.source {
                    StoragePath::S3 { bucket, .. } => StoragePath::S3 {
                        bucket: bucket.clone(),
                        prefix: String::new(),
                    },
                    other => other.clone(),
                };
                let source = S3StorageFactory::create(
                    config.clone(),
                    empty_source,
                    cancellation_token.clone(),
                    stats_sender.clone(),
                    config.source_client_config.clone(),
                    source_request_payer,
                    None,
                    None,
                    has_warning.clone(),
                    None,
                )
                .await;

                s3util_rs::transfer::s3_to_stdio::transfer(
                    &config,
                    source,
                    &source_key,
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

    /// Extract (source_key, target_key) matching the CLI's extract_keys logic.
    fn extract_keys_for_test(config: &Config, _direction: &TransferDirection) -> (String, String) {
        let source_key = match &config.source {
            StoragePath::S3 { prefix, .. } => prefix.clone(),
            StoragePath::Local(path) => path.to_string_lossy().to_string(),
            StoragePath::Stdio => String::new(),
        };
        let source_basename = std::path::Path::new(&source_key)
            .file_name()
            .map(|f| f.to_string_lossy().to_string())
            .unwrap_or(source_key.clone());

        let target_key = match &config.target {
            StoragePath::S3 { prefix, .. } => {
                if prefix.is_empty() || prefix.ends_with('/') {
                    format!("{prefix}{source_basename}")
                } else {
                    prefix.clone()
                }
            }
            StoragePath::Local(path) => {
                let p = path.clone();
                if p.is_dir() || p.to_string_lossy().ends_with(std::path::MAIN_SEPARATOR) {
                    p.join(&source_basename).to_string_lossy().to_string()
                } else {
                    p.to_string_lossy().to_string()
                }
            }
            StoragePath::Stdio => String::new(),
        };
        (source_key, target_key)
    }
}
