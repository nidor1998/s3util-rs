const INVALID_BUCKET_NAMESPACE: &str =
    "invalid bucket-namespace. valid choices: account-regional .";

/// Validate the `--bucket-namespace` value.
///
/// The only accepted value is `account-regional`, which opts the bucket into
/// the account-level regional namespace. Every other value — including the
/// SDK's `global` — is rejected, so the flag can only ever request the
/// account-regional behavior. When the flag is omitted entirely, nothing is
/// sent to the SDK and S3 uses its default (global) namespace.
pub fn parse_bucket_namespace(value: &str) -> Result<String, String> {
    match value {
        "account-regional" => Ok(value.to_string()),
        _ => Err(INVALID_BUCKET_NAMESPACE.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_account_regional() {
        assert_eq!(
            parse_bucket_namespace("account-regional").unwrap(),
            "account-regional"
        );
    }

    #[test]
    fn rejects_global() {
        // `global` is a valid SDK BucketNamespace variant but is deliberately
        // not accepted by this flag.
        let err = parse_bucket_namespace("global").unwrap_err();
        assert!(err.contains("invalid bucket-namespace"));
    }

    #[test]
    fn rejects_every_non_account_regional_value() {
        for value in [
            "",
            " ",
            "Account-Regional",   // wrong case
            "account_regional",   // underscore instead of hyphen
            "account-regional ",  // trailing whitespace
            " account-regional",  // leading whitespace
            "account-regionally", // superstring
            "regional",           // substring
            "ACCOUNT-REGIONAL",   // upper case
        ] {
            assert!(
                parse_bucket_namespace(value).is_err(),
                "expected {value:?} to be rejected"
            );
        }
    }
}
