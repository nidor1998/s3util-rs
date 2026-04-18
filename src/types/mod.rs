use std::collections::HashMap;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::primitives::DateTime;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType, ObjectPart, Tag};
use sha1::{Digest, Sha1};
use zeroize_derive::{Zeroize, ZeroizeOnDrop};

pub mod async_callback;
pub mod error;

pub mod filter_message;
pub mod token;

pub const S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY: &str = "s3sync_origin_last_modified";
pub const SYNC_REPORT_SUMMERY_NAME: &str = "REPORT_SUMMARY";
pub const SYNC_REPORT_RECORD_NAME: &str = "SYNC_STATUS";
pub const SYNC_REPORT_EXISTENCE_TYPE: &str = "EXISTENCE";
pub const SYNC_REPORT_ETAG_TYPE: &str = "ETAG";
pub const SYNC_REPORT_CHECKSUM_TYPE: &str = "CHECKSUM";
pub const SYNC_REPORT_METADATA_TYPE: &str = "METADATA";
pub const SYNC_REPORT_TAGGING_TYPE: &str = "TAGGING";
pub const SYNC_REPORT_CONTENT_DISPOSITION_METADATA_KEY: &str = "Content-Disposition";
pub const SYNC_REPORT_CONTENT_ENCODING_METADATA_KEY: &str = "Content-Encoding";
pub const SYNC_REPORT_CONTENT_LANGUAGE_METADATA_KEY: &str = "Content-Language";
pub const SYNC_REPORT_CONTENT_TYPE_METADATA_KEY: &str = "Content-Type";
pub const SYNC_REPORT_CACHE_CONTROL_METADATA_KEY: &str = "Cache-Control";
pub const SYNC_REPORT_EXPIRES_METADATA_KEY: &str = "Expires";
pub const SYNC_REPORT_WEBSITE_REDIRECT_METADATA_KEY: &str = "x-amz-website-redirect-location";
pub const SYNC_REPORT_USER_DEFINED_METADATA_KEY: &str = "x-amz-meta-";

pub const METADATA_SYNC_REPORT_LOG_NAME: &str = "METADATA_SYNC_STATUS";
pub const TAGGING_SYNC_REPORT_LOG_NAME: &str = "TAGGING_SYNC_STATUS";
pub const SYNC_STATUS_MATCHES: &str = "MATCHES";
pub const SYNC_STATUS_MISMATCH: &str = "MISMATCH";
pub const SYNC_STATUS_NOT_FOUND: &str = "NOT_FOUND";
pub const SYNC_STATUS_UNKNOWN: &str = "UNKNOWN";

pub type Sha1Digest = [u8; 20];

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ObjectKey {
    KeyString(String),
    KeySHA1Digest(Sha1Digest),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectEntry {
    pub last_modified: DateTime,
    pub content_length: i64,
    pub e_tag: Option<String>,
}

pub type ObjectKeyMap = Arc<Mutex<HashMap<ObjectKey, ObjectEntry>>>;

#[derive(Debug, Clone, Default)]
pub struct SyncStatsReport {
    pub number_of_objects: usize,
    pub not_found: usize,
    pub etag_matches: usize,
    pub etag_mismatch: usize,
    pub etag_unknown: usize,
    pub checksum_matches: usize,
    pub checksum_mismatch: usize,
    pub checksum_unknown: usize,
    pub metadata_matches: usize,
    pub metadata_mismatch: usize,
    pub tagging_matches: usize,
    pub tagging_mismatch: usize,
}

impl SyncStatsReport {
    pub fn increment_number_of_objects(&mut self) {
        self.number_of_objects += 1;
    }
    pub fn increment_not_found(&mut self) {
        self.not_found += 1;
    }
    pub fn increment_etag_matches(&mut self) {
        self.etag_matches += 1;
    }
    pub fn increment_etag_mismatch(&mut self) {
        self.etag_mismatch += 1;
    }
    pub fn increment_etag_unknown(&mut self) {
        self.etag_unknown += 1;
    }
    pub fn increment_checksum_matches(&mut self) {
        self.checksum_matches += 1;
    }
    pub fn increment_checksum_mismatch(&mut self) {
        self.checksum_mismatch += 1;
    }
    pub fn increment_checksum_unknown(&mut self) {
        self.checksum_unknown += 1;
    }
    pub fn increment_metadata_matches(&mut self) {
        self.metadata_matches += 1;
    }
    pub fn increment_metadata_mismatch(&mut self) {
        self.metadata_mismatch += 1;
    }
    pub fn increment_tagging_matches(&mut self) {
        self.tagging_matches += 1;
    }
    pub fn increment_tagging_mismatch(&mut self) {
        self.tagging_mismatch += 1;
    }
}

#[derive(Clone, Default)]
pub struct ObjectChecksum {
    pub key: String,
    pub version_id: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
    pub object_parts: Option<Vec<ObjectPart>>,
    pub final_checksum: Option<String>,
}

pub fn format_metadata(metadata: &HashMap<String, String>) -> String {
    let mut sorted_keys: Vec<&String> = metadata.keys().collect();
    sorted_keys.sort();

    sorted_keys
        .iter()
        .map(|key| {
            let value = urlencoding::encode(&metadata[*key]).to_string();
            format!("{key}={value}")
        })
        .collect::<Vec<String>>()
        .join(",")
}

pub fn format_tags(tags: &[Tag]) -> String {
    let mut tags = tags
        .iter()
        .map(|tag| (tag.key(), tag.value()))
        .collect::<Vec<_>>();

    tags.sort_by(|a, b| a.0.cmp(b.0));

    tags.iter()
        .map(|(key, value)| {
            let escaped_key = urlencoding::encode(key).to_string();
            let encoded_value = urlencoding::encode(value).to_string();
            format!("{escaped_key}={encoded_value}")
        })
        .collect::<Vec<String>>()
        .join("&")
}

// sha1 uses generic-array v0.x internally, which is deprecated.
// Suppress warnings until the underlying library is updated.
#[allow(deprecated)]
pub fn sha1_digest_from_key(key: &str) -> Sha1Digest {
    let digest = Sha1::digest(key);
    TryInto::<Sha1Digest>::try_into(digest.as_slice()).unwrap()
}

/// Detect which checksum algorithm the source object uses by inspecting the GetObjectOutput fields.
/// Returns the algorithm and its value if found.
///
/// Priority favors explicitly user-chosen algorithms (SHA256/SHA1/CRC32/CRC32C) over CRC64NVME,
/// which S3 often auto-adds to objects uploaded with a different explicit algorithm. This ensures
/// a multipart object uploaded with `--additional-checksum-algorithm SHA256` is verified with
/// SHA256, not with the auto-added full-object CRC64NVME.
pub fn detect_additional_checksum(
    get_object_output: &GetObjectOutput,
) -> Option<(ChecksumAlgorithm, String)> {
    if let Some(v) = get_object_output.checksum_sha256() {
        return Some((ChecksumAlgorithm::Sha256, v.to_string()));
    }
    if let Some(v) = get_object_output.checksum_sha1() {
        return Some((ChecksumAlgorithm::Sha1, v.to_string()));
    }
    if let Some(v) = get_object_output.checksum_crc32_c() {
        return Some((ChecksumAlgorithm::Crc32C, v.to_string()));
    }
    if let Some(v) = get_object_output.checksum_crc32() {
        return Some((ChecksumAlgorithm::Crc32, v.to_string()));
    }
    if let Some(v) = get_object_output.checksum_crc64_nvme() {
        return Some((ChecksumAlgorithm::Crc64Nvme, v.to_string()));
    }
    None
}

pub fn get_additional_checksum(
    get_object_output: &GetObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => get_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => get_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => get_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => get_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => get_object_output
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn get_additional_checksum_with_head_object(
    head_object_output: &HeadObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => head_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => head_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => head_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => head_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => head_object_output
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn is_full_object_checksum(checksum: &Option<String>) -> bool {
    if checksum.is_none() {
        return false;
    }

    // As of February 2, 2025, Amazon S3 GetObject does not return ChecksumType::Composite.
    // So, we can't get the checksum type from GetObjectOutput and decide where checksum has '-' or not.
    let find_result = checksum.as_ref().unwrap().find('-');
    find_result.is_none()
}

#[derive(Debug, PartialEq)]
pub enum SyncStatistics {
    SyncBytes(u64),
    SyncComplete { key: String },
    SyncError { key: String },
    SyncWarning { key: String },
    ETagVerified { key: String },
    ChecksumVerified { key: String },
}

#[derive(Debug, Clone)]
pub enum StoragePath {
    S3 { bucket: String, prefix: String },
    Local(PathBuf),
    Stdio,
}

#[derive(Debug, Clone)]
pub struct ClientConfigLocation {
    pub aws_config_file: Option<PathBuf>,
    pub aws_shared_credentials_file: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct AccessKeys {
    pub access_key: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

impl Debug for AccessKeys {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("AccessKeys");
        let session_token = self
            .session_token
            .as_ref()
            .map_or("None", |_| "** redacted **");
        keys.field("access_key", &self.access_key)
            .field("secret_access_key", &"** redacted **")
            .field("session_token", &session_token);
        keys.finish()
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SseKmsKeyId {
    pub id: Option<String>,
}

impl Debug for SseKmsKeyId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("SseKmsKeyId");
        let sse_kms_key_id = self.id.as_ref().map_or("None", |_| "** redacted **");
        keys.field("sse_kms_key_id", &sse_kms_key_id);
        keys.finish()
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct SseCustomerKey {
    pub key: Option<String>,
}

impl Debug for SseCustomerKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut keys = f.debug_struct("SseCustomerKey");
        let sse_c_key = self.key.as_ref().map_or("None", |_| "** redacted **");
        keys.field("key", &sse_c_key);
        keys.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_print_access_keys() {
        init_dummy_tracing_subscriber();

        let access_keys = AccessKeys {
            access_key: "access_key".to_string(),
            secret_access_key: "secret_access_key".to_string(),
            session_token: Some("session_token".to_string()),
        };
        let debug_string = format!("{access_keys:?}");

        assert!(debug_string.contains("secret_access_key: \"** redacted **\""));
        assert!(debug_string.contains("session_token: \"** redacted **\""));
    }

    #[test]
    fn test_sse_kms_keyid_debug_string() {
        let secret = SseKmsKeyId {
            id: Some("secret".to_string()),
        };

        let debug_string = format!("{:?}", secret);
        assert!(debug_string.contains("redacted"))
    }

    #[test]
    fn test_sse_customer_key_debug_string() {
        let secret = SseCustomerKey {
            key: Some("secret".to_string()),
        };

        let debug_string = format!("{:?}", secret);
        assert!(debug_string.contains("redacted"))
    }

    #[test]
    fn test_format_metadata() {
        let metadata = HashMap::from([
            ("key3".to_string(), "value3".to_string()),
            ("key1".to_string(), "value1".to_string()),
            ("abc".to_string(), "\u{2603}".to_string()),
            ("xyz_abc".to_string(), "value_xyz".to_string()),
            ("key_comma".to_string(), "value,comma".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);

        let formatted = format_metadata(&metadata);
        assert_eq!(
            formatted,
            "abc=%E2%98%83,key1=value1,key2=value2,key3=value3,key_comma=value%2Ccomma,xyz_abc=value_xyz"
        );
    }

    #[test]
    fn test_format_tags() {
        let tags = vec![
            Tag::builder().key("key3").value("value3").build().unwrap(),
            Tag::builder().key("key1").value("value1").build().unwrap(),
            Tag::builder().key("abc").value("\u{2603}").build().unwrap(),
            Tag::builder()
                .key("\u{2603}")
                .value("value")
                .build()
                .unwrap(),
            Tag::builder()
                .key("xyz_abc")
                .value("value_xyz")
                .build()
                .unwrap(),
            Tag::builder()
                .key("key_comma")
                .value("value,comma")
                .build()
                .unwrap(),
            Tag::builder()
                .key("key_and")
                .value("value&and")
                .build()
                .unwrap(),
            Tag::builder().key("key2").value("value2").build().unwrap(),
        ];

        let formatted = format_tags(tags.as_slice());
        assert_eq!(
            formatted,
            "abc=%E2%98%83&key1=value1&key2=value2&key3=value3&key_and=value%26and&key_comma=value%2Ccomma&xyz_abc=value_xyz&%E2%98%83=value"
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }

    #[test]
    fn is_full_object_checksum_none() {
        assert!(!is_full_object_checksum(&None));
    }

    #[test]
    fn is_full_object_checksum_no_dash_means_full_object() {
        assert!(is_full_object_checksum(&Some("abc123==".to_string())));
    }

    #[test]
    fn is_full_object_checksum_dash_means_composite() {
        // Multipart composite checksums have the form "<base64>-<partcount>".
        assert!(!is_full_object_checksum(&Some("abc123==-4".to_string())));
    }

    #[test]
    fn detect_additional_checksum_returns_none_when_no_checksum_present() {
        let get = GetObjectOutput::builder().build();
        assert!(detect_additional_checksum(&get).is_none());
    }

    #[test]
    fn detect_additional_checksum_returns_sha256_when_present() {
        let get = GetObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .build();
        let (algo, value) = detect_additional_checksum(&get).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Sha256));
        assert_eq!(value, "sha256-value");
    }

    #[test]
    fn detect_additional_checksum_prefers_explicit_over_auto_added_crc64nvme() {
        // S3 may auto-add CRC64NVME alongside an explicitly chosen algorithm.
        // Per the function's documented contract, the explicit choice wins.
        let get = GetObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .checksum_crc64_nvme("crc64-value")
            .build();
        let (algo, value) = detect_additional_checksum(&get).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Sha256));
        assert_eq!(value, "sha256-value");
    }

    #[test]
    fn detect_additional_checksum_returns_crc64nvme_when_only_one_present() {
        let get = GetObjectOutput::builder()
            .checksum_crc64_nvme("crc64-value")
            .build();
        let (algo, value) = detect_additional_checksum(&get).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(value, "crc64-value");
    }

    #[test]
    fn get_additional_checksum_returns_none_when_algorithm_none() {
        let get = GetObjectOutput::builder()
            .checksum_sha256("ignored")
            .build();
        assert!(get_additional_checksum(&get, None).is_none());
    }

    #[test]
    fn get_additional_checksum_extracts_requested_algorithm() {
        let get = GetObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .checksum_sha1("sha1-value")
            .checksum_crc32("crc32-value")
            .checksum_crc32_c("crc32c-value")
            .checksum_crc64_nvme("crc64-value")
            .build();
        assert_eq!(
            get_additional_checksum(&get, Some(ChecksumAlgorithm::Sha256)).unwrap(),
            "sha256-value"
        );
        assert_eq!(
            get_additional_checksum(&get, Some(ChecksumAlgorithm::Sha1)).unwrap(),
            "sha1-value"
        );
        assert_eq!(
            get_additional_checksum(&get, Some(ChecksumAlgorithm::Crc32)).unwrap(),
            "crc32-value"
        );
        assert_eq!(
            get_additional_checksum(&get, Some(ChecksumAlgorithm::Crc32C)).unwrap(),
            "crc32c-value"
        );
        assert_eq!(
            get_additional_checksum(&get, Some(ChecksumAlgorithm::Crc64Nvme)).unwrap(),
            "crc64-value"
        );
        // Requested but absent on an empty output → None.
        let empty = GetObjectOutput::builder().build();
        assert!(get_additional_checksum(&empty, Some(ChecksumAlgorithm::Sha1)).is_none());
    }

    #[test]
    fn get_additional_checksum_with_head_object_extracts_correct_field() {
        use aws_sdk_s3::operation::head_object::HeadObjectOutput;
        let head = HeadObjectOutput::builder()
            .checksum_sha256("head-sha256")
            .checksum_sha1("head-sha1")
            .checksum_crc32("head-crc32")
            .checksum_crc32_c("head-crc32c")
            .checksum_crc64_nvme("head-crc64")
            .build();
        assert!(get_additional_checksum_with_head_object(&head, None).is_none());
        assert_eq!(
            get_additional_checksum_with_head_object(&head, Some(ChecksumAlgorithm::Sha256))
                .unwrap(),
            "head-sha256"
        );
        assert_eq!(
            get_additional_checksum_with_head_object(&head, Some(ChecksumAlgorithm::Sha1)).unwrap(),
            "head-sha1"
        );
        assert_eq!(
            get_additional_checksum_with_head_object(&head, Some(ChecksumAlgorithm::Crc32))
                .unwrap(),
            "head-crc32"
        );
        assert_eq!(
            get_additional_checksum_with_head_object(&head, Some(ChecksumAlgorithm::Crc32C))
                .unwrap(),
            "head-crc32c"
        );
        assert_eq!(
            get_additional_checksum_with_head_object(&head, Some(ChecksumAlgorithm::Crc64Nvme))
                .unwrap(),
            "head-crc64"
        );
        let empty = HeadObjectOutput::builder().build();
        assert!(
            get_additional_checksum_with_head_object(&empty, Some(ChecksumAlgorithm::Sha256))
                .is_none()
        );
    }

    #[test]
    fn sha1_digest_from_key_is_deterministic_and_correct() {
        let key = "some-object-key.dat";
        let a = sha1_digest_from_key(key);
        let b = sha1_digest_from_key(key);
        assert_eq!(a, b);
        // Length is enforced by the type (Sha1Digest = [u8; 20]); also verify
        // a different key yields a different digest.
        assert_ne!(sha1_digest_from_key("different-key"), a);
    }

    #[test]
    fn sync_stats_report_increments_advance_each_field() {
        let mut r = SyncStatsReport::default();
        r.increment_number_of_objects();
        r.increment_not_found();
        r.increment_etag_matches();
        r.increment_etag_mismatch();
        r.increment_etag_unknown();
        r.increment_checksum_matches();
        r.increment_checksum_mismatch();
        r.increment_checksum_unknown();
        r.increment_metadata_matches();
        r.increment_metadata_mismatch();
        r.increment_tagging_matches();
        r.increment_tagging_mismatch();

        assert_eq!(r.number_of_objects, 1);
        assert_eq!(r.not_found, 1);
        assert_eq!(r.etag_matches, 1);
        assert_eq!(r.etag_mismatch, 1);
        assert_eq!(r.etag_unknown, 1);
        assert_eq!(r.checksum_matches, 1);
        assert_eq!(r.checksum_mismatch, 1);
        assert_eq!(r.checksum_unknown, 1);
        assert_eq!(r.metadata_matches, 1);
        assert_eq!(r.metadata_mismatch, 1);
        assert_eq!(r.tagging_matches, 1);
        assert_eq!(r.tagging_mismatch, 1);
    }

    #[test]
    fn sync_stats_report_increments_accumulate() {
        let mut r = SyncStatsReport::default();
        for _ in 0..5 {
            r.increment_etag_matches();
        }
        assert_eq!(r.etag_matches, 5);
    }
}
