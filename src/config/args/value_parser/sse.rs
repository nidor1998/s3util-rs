use std::str::FromStr;

use aws_sdk_s3::types::ServerSideEncryption;

const INVALID_SSE_VALUE: &str =
    "invalid sse value. valid choices: AES256 | aws:kms | aws:kms:dsse.";

pub fn parse_sse(sse: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(
        ServerSideEncryption::from_str(sse).unwrap(),
        ServerSideEncryption::Unknown(_)
    ) {
        return Err(INVALID_SSE_VALUE.to_string());
    }

    Ok(sse.to_string())
}

pub fn parse_sse_c(sse: &str) -> Result<String, String> {
    if !matches!(
        ServerSideEncryption::from_str(sse).unwrap(),
        ServerSideEncryption::Aes256
    ) {
        return Err(INVALID_SSE_VALUE.to_string());
    }

    Ok(sse.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sse_accepts_known_values() {
        for sse in ["AES256", "aws:kms", "aws:kms:dsse"] {
            assert_eq!(parse_sse(sse).unwrap(), sse);
        }
    }

    #[test]
    fn parse_sse_rejects_unknown() {
        let err = parse_sse("rot13").unwrap_err();
        assert!(err.contains("invalid sse value"));
    }

    #[test]
    fn parse_sse_c_accepts_only_aes256() {
        assert_eq!(parse_sse_c("AES256").unwrap(), "AES256");
    }

    #[test]
    fn parse_sse_c_rejects_kms_variants() {
        // SSE-C is AES256 with a customer-supplied key only — KMS variants are not allowed.
        assert!(parse_sse_c("aws:kms").is_err());
        assert!(parse_sse_c("aws:kms:dsse").is_err());
    }
}
