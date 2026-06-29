//! Pure, network-free helpers for `put-object-annotation`: payload size
//! validation, Content-MD5 / CRC64NVME computation, and CRC64NVME
//! response verification.

use anyhow::{Result, bail};
use aws_sdk_s3::types::ChecksumAlgorithm;
use base64::{Engine as _, engine::general_purpose};

use crate::storage::checksum::AdditionalChecksum;

/// Maximum annotation payload size accepted by S3 (1 MiB).
pub const MAX_ANNOTATION_PAYLOAD_LEN: usize = 1024 * 1024;

/// Validate the payload length against S3's 1-byte..=1-MiB constraint.
/// Checked locally so the user gets a clear error before any network call.
pub fn validate_payload_len(len: usize) -> Result<()> {
    if len == 0 {
        bail!("annotation payload must be at least 1 byte");
    }
    if len > MAX_ANNOTATION_PAYLOAD_LEN {
        bail!(
            "annotation payload must be at most 1 MiB ({MAX_ANNOTATION_PAYLOAD_LEN} bytes), got {len} bytes"
        );
    }
    Ok(())
}

/// Base64-encoded MD5 of the payload, for the `Content-MD5` request header.
pub fn content_md5_base64(payload: &[u8]) -> String {
    general_purpose::STANDARD.encode(md5::compute(payload).as_slice())
}

/// Base64-encoded full-object CRC64NVME of the payload, sent as the explicit
/// `x-amz-checksum-crc64nvme` value and compared against the response.
pub fn crc64nvme_base64(payload: &[u8]) -> String {
    let mut checksum = AdditionalChecksum::new(ChecksumAlgorithm::Crc64Nvme, true);
    checksum.update(payload);
    checksum.finalize()
}

/// Verify the CRC64NVME returned by S3 matches the locally computed value.
pub fn verify_crc64nvme(expected_base64: &str, returned: Option<&str>) -> Result<()> {
    match returned {
        Some(actual) if actual == expected_base64 => Ok(()),
        Some(actual) => {
            bail!("CRC64NVME verification failed: expected {expected_base64}, got {actual}")
        }
        None => {
            bail!("CRC64NVME verification failed: response did not include a CRC64NVME checksum")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_payload() {
        assert!(validate_payload_len(0).is_err());
    }

    #[test]
    fn accepts_one_byte_and_max() {
        assert!(validate_payload_len(1).is_ok());
        assert!(validate_payload_len(MAX_ANNOTATION_PAYLOAD_LEN).is_ok());
    }

    #[test]
    fn rejects_over_max() {
        assert!(validate_payload_len(MAX_ANNOTATION_PAYLOAD_LEN + 1).is_err());
    }

    #[test]
    fn content_md5_is_base64_of_raw_md5_digest() {
        let b64 = content_md5_base64(b"hello");
        // 16-byte digest -> 24 base64 chars (padded "==").
        assert_eq!(b64.len(), 24);
        let raw = general_purpose::STANDARD.decode(&b64).unwrap();
        assert_eq!(raw.as_slice(), md5::compute(b"hello").as_slice());
    }

    #[test]
    fn crc64nvme_is_deterministic_12_char_base64() {
        let a = crc64nvme_base64(b"hello world");
        let b = crc64nvme_base64(b"hello world");
        // 8-byte CRC64 digest -> 12 base64 chars (padded "=").
        assert_eq!(a.len(), 12);
        assert_eq!(a, b);
        assert_ne!(a, crc64nvme_base64(b"different"));
    }

    #[test]
    fn verify_crc64nvme_ok_on_match() {
        assert!(verify_crc64nvme("AAAAAAAAAAA=", Some("AAAAAAAAAAA=")).is_ok());
    }

    #[test]
    fn verify_crc64nvme_err_on_mismatch_or_missing() {
        assert!(verify_crc64nvme("AAAAAAAAAAA=", Some("BBBBBBBBBBB=")).is_err());
        assert!(verify_crc64nvme("AAAAAAAAAAA=", None).is_err());
    }
}
