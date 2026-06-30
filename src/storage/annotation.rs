//! Pure, network-free helpers for `put-object-annotation`: payload size
//! validation, Content-MD5 / CRC64NVME computation, and CRC64NVME
//! response verification.

use anyhow::{Result, bail};
use aws_sdk_s3::types::{ChecksumAlgorithm, ServerSideEncryption};
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
        bail!("annotation payload must be at most 1 MiB ({MAX_ANNOTATION_PAYLOAD_LEN} bytes)");
    }
    Ok(())
}

/// Maximum annotation-name length accepted by S3 (512 bytes).
pub const MAX_ANNOTATION_NAME_LEN: usize = 512;

/// Validate the annotation name against S3's 1..=512-byte constraint. Checked
/// locally so the user gets a clear error before any network call (the length is
/// measured in bytes, matching the documented "1-512 bytes" limit).
pub fn validate_annotation_name(name: &str) -> Result<()> {
    let len = name.len();
    if len == 0 {
        bail!("annotation name must be at least 1 byte");
    }
    if len > MAX_ANNOTATION_NAME_LEN {
        bail!("annotation name must be at most {MAX_ANNOTATION_NAME_LEN} bytes (got {len})");
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
    compute_checksum_base64(payload, ChecksumAlgorithm::Crc64Nvme)
}

/// Base64-encoded full-object checksum of `payload` for `algorithm`. Used by
/// both the put side (CRC64NVME) and the get side (verifying whichever
/// additional checksum S3 returns). The payload is single-part (≤1 MiB), so a
/// full-object checksum is always correct here.
pub fn compute_checksum_base64(payload: &[u8], algorithm: ChecksumAlgorithm) -> String {
    let mut checksum = AdditionalChecksum::new(algorithm, true);
    checksum.update(payload);
    checksum.finalize()
}

/// Verify the response ETag equals the payload's MD5. Returns `Some(true)` on
/// match, `Some(false)` on mismatch, and `None` when the check does not apply:
/// the object is not `AES256`-encrypted (strictly — an absent header is also
/// skipped), the ETag is missing, or the ETag is a multipart-style (`-N`) value
/// rather than a plain MD5.
pub fn verify_etag_md5(
    payload: &[u8],
    e_tag: Option<&str>,
    sse: Option<&ServerSideEncryption>,
) -> Option<bool> {
    if sse != Some(&ServerSideEncryption::Aes256) {
        return None;
    }
    let normalized = e_tag?.replace('"', "");
    if normalized.contains('-') {
        return None;
    }
    let computed = format!("{:x}", md5::compute(payload));
    Some(normalized.eq_ignore_ascii_case(&computed))
}

/// Verify `expected_base64` equals the locally computed full-object checksum of
/// `payload` for `algorithm`.
pub fn verify_additional_checksum(
    payload: &[u8],
    algorithm: ChecksumAlgorithm,
    expected_base64: &str,
) -> bool {
    compute_checksum_base64(payload, algorithm) == expected_base64
}

/// Verify the CRC64NVME returned by S3 matches the locally computed value.
pub fn verify_crc64nvme(expected_base64: &str, returned: Option<&str>) -> Result<()> {
    match returned {
        Some(actual) if actual == expected_base64 => Ok(()),
        Some(actual) => {
            bail!("CRC64NVME verification failed: expected {expected_base64}, got {actual}")
        }
        None => {
            bail!(
                "CRC64NVME verification failed: the annotation was written, but the response did \
                 not include a CRC64NVME checksum to verify it against"
            )
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
    fn annotation_name_rejects_empty() {
        assert!(validate_annotation_name("").is_err());
    }

    #[test]
    fn annotation_name_accepts_one_byte_and_max() {
        assert!(validate_annotation_name("x").is_ok());
        assert!(validate_annotation_name(&"x".repeat(MAX_ANNOTATION_NAME_LEN)).is_ok());
    }

    #[test]
    fn annotation_name_rejects_over_max() {
        assert!(validate_annotation_name(&"x".repeat(MAX_ANNOTATION_NAME_LEN + 1)).is_err());
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

    #[test]
    fn compute_checksum_base64_matches_put_side_crc64nvme() {
        // The generic helper must reproduce the put-side CRC64NVME digest.
        let p = b"hello world";
        assert_eq!(
            compute_checksum_base64(p, ChecksumAlgorithm::Crc64Nvme),
            crc64nvme_base64(p)
        );
    }

    #[test]
    fn verify_etag_md5_matches_on_aes256() {
        let payload = b"hello";
        let etag = format!("\"{:x}\"", md5::compute(payload));
        assert_eq!(
            verify_etag_md5(payload, Some(&etag), Some(&ServerSideEncryption::Aes256)),
            Some(true)
        );
    }

    #[test]
    fn verify_etag_md5_mismatch_on_aes256() {
        assert_eq!(
            verify_etag_md5(
                b"hello",
                Some("\"00000000000000000000000000000000\""),
                Some(&ServerSideEncryption::Aes256)
            ),
            Some(false)
        );
    }

    #[test]
    fn verify_etag_md5_skipped_without_aes256() {
        let payload = b"hello";
        let etag = format!("\"{:x}\"", md5::compute(payload));
        // No SSE header => skipped (strictly-AES256 rule).
        assert_eq!(verify_etag_md5(payload, Some(&etag), None), None);
        // KMS => skipped.
        assert_eq!(
            verify_etag_md5(payload, Some(&etag), Some(&ServerSideEncryption::AwsKms)),
            None
        );
        // Missing ETag => skipped.
        assert_eq!(
            verify_etag_md5(payload, None, Some(&ServerSideEncryption::Aes256)),
            None
        );
        // Multipart-style ETag => skipped (not a plain MD5).
        assert_eq!(
            verify_etag_md5(
                payload,
                Some("\"d41d8cd98f00b204e9800998ecf8427e-2\""),
                Some(&ServerSideEncryption::Aes256)
            ),
            None
        );
    }

    #[test]
    fn verify_additional_checksum_match_and_mismatch() {
        let payload = b"hello world";
        let expected = crc64nvme_base64(payload);
        assert!(verify_additional_checksum(
            payload,
            ChecksumAlgorithm::Crc64Nvme,
            &expected
        ));
        assert!(!verify_additional_checksum(
            payload,
            ChecksumAlgorithm::Crc64Nvme,
            "AAAAAAAAAAA="
        ));
    }
}
