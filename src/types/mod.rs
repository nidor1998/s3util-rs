use std::fmt;
use std::fmt::{Debug, Formatter};
use std::path::PathBuf;

use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType, ObjectPart};
use zeroize_derive::{Zeroize, ZeroizeOnDrop};

pub mod async_callback;
pub mod error;
pub mod token;

pub(crate) const MINIMUM_CHUNKSIZE: usize = 5 * 1024 * 1024;

#[derive(Clone, Default)]
pub struct ObjectChecksum {
    pub key: String,
    pub version_id: Option<String>,
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: Option<ChecksumType>,
    pub object_parts: Option<Vec<ObjectPart>>,
    pub final_checksum: Option<String>,
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

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
