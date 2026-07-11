const INVALID_CREATE_BUCKET_CONFIGURATION: &str = "invalid create-bucket-configuration. only LocationConstraint=<region> is supported \
     (e.g. LocationConstraint=ap-northeast-1).";

/// Validate the `--create-bucket-configuration` value and return the location
/// constraint (the region following `LocationConstraint=`).
///
/// Only the single-key `LocationConstraint=<region>` shorthand is accepted.
/// Any other key, an empty region, or additional comma-separated pairs are
/// rejected. The region string itself is passed through verbatim — it is not
/// checked against the SDK's known-region list — so newly launched regions
/// keep working, mirroring the region-derived path in `api::create_bucket`.
pub fn parse_create_bucket_configuration(value: &str) -> Result<String, String> {
    let location_constraint = value
        .strip_prefix("LocationConstraint=")
        .ok_or_else(|| INVALID_CREATE_BUCKET_CONFIGURATION.to_string())?;

    // Reject an empty region and any multi-pair shorthand: a second `=` or a
    // `,` separator means more than a lone LocationConstraint was supplied.
    if location_constraint.is_empty()
        || location_constraint.contains(',')
        || location_constraint.contains('=')
    {
        return Err(INVALID_CREATE_BUCKET_CONFIGURATION.to_string());
    }

    Ok(location_constraint.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_location_constraint() {
        assert_eq!(
            parse_create_bucket_configuration("LocationConstraint=ap-northeast-1").unwrap(),
            "ap-northeast-1"
        );
    }

    #[test]
    fn accepts_various_region_values_verbatim() {
        // The region is passed through unchecked (mirrors the region-derived
        // path in api::create_bucket), so any region code — including ones the
        // SDK enum may not know yet — is returned as-is.
        for region in [
            "us-east-1",
            "us-west-2",
            "eu-west-1",
            "ap-northeast-1",
            "me-central-1",
            "some-future-region-99",
        ] {
            let input = format!("LocationConstraint={region}");
            assert_eq!(parse_create_bucket_configuration(&input).unwrap(), region);
        }
    }

    #[test]
    fn rejects_missing_or_wrong_key() {
        // Case-sensitive, exact-key match: only `LocationConstraint=` is valid.
        for value in [
            "ap-northeast-1",                    // no key at all
            "Location=ap-northeast-1",           // wrong key
            "locationconstraint=ap-northeast-1", // wrong case
            "LocationConstraint",                // key with no `=`
            "LocationConstraint ap-northeast-1", // space instead of `=`
            "LocationType=AvailabilityZone",     // a different valid S3 config key
        ] {
            let err = parse_create_bucket_configuration(value).unwrap_err();
            assert!(
                err.contains("only LocationConstraint="),
                "expected {value:?} to be rejected with the LocationConstraint hint"
            );
        }
    }

    #[test]
    fn rejects_empty_region() {
        assert!(parse_create_bucket_configuration("LocationConstraint=").is_err());
    }

    #[test]
    fn rejects_multiple_pairs() {
        // A `,` separator or a second `=` means more than a lone
        // LocationConstraint was supplied.
        assert!(
            parse_create_bucket_configuration("LocationConstraint=ap-northeast-1,Foo=bar").is_err()
        );
        assert!(parse_create_bucket_configuration("LocationConstraint=a=b").is_err());
        assert!(
            parse_create_bucket_configuration("LocationConstraint=ap-northeast-1,LocationType=Foo")
                .is_err()
        );
    }
}
