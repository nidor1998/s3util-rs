//! AWS-CLI-shape JSON serialisation for S3 SDK response types.
//!
//! Hand-written because `aws-sdk-s3` response types do not implement
//! `Serialize`, and the SDK's internal field shapes don't match the
//! `aws s3api --output json` surface 1-to-1 (PascalCase, omission semantics,
//! double-encoded `Policy`, etc.).

use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use serde_json::{Map, Value};

/// Serialise a `HeadBucketOutput` to AWS CLI v2 `--output json` shape.
///
/// The keys present in the returned object match what `aws s3api head-bucket
/// --output json` produces. Optional fields are omitted when the SDK didn't
/// populate them; they are never emitted as `null`.
pub fn head_bucket_to_json(out: &HeadBucketOutput) -> Value {
    let mut map = Map::new();
    if let Some(region) = out.bucket_region() {
        map.insert(
            "BucketRegion".to_string(),
            Value::String(region.to_string()),
        );
    }
    if let Some(alias) = out.access_point_alias() {
        map.insert("AccessPointAlias".to_string(), Value::Bool(alias));
    }
    if let Some(location_type) = out.bucket_location_type() {
        map.insert(
            "BucketLocationType".to_string(),
            Value::String(location_type.as_str().to_string()),
        );
    }
    if let Some(name) = out.bucket_location_name() {
        map.insert(
            "BucketLocationName".to_string(),
            Value::String(name.to_string()),
        );
    }
    Value::Object(map)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn head_bucket_with_region_only() {
        let out = HeadBucketOutput::builder()
            .bucket_region("us-east-1")
            .build();
        let json = head_bucket_to_json(&out);
        assert_eq!(json["BucketRegion"], Value::String("us-east-1".into()));
        assert!(json.get("AccessPointAlias").is_none());
    }

    #[test]
    fn head_bucket_with_all_fields() {
        let out = HeadBucketOutput::builder()
            .bucket_region("ap-northeast-1")
            .access_point_alias(false)
            .build();
        let json = head_bucket_to_json(&out);
        assert_eq!(json["BucketRegion"], Value::String("ap-northeast-1".into()));
        assert_eq!(json["AccessPointAlias"], Value::Bool(false));
    }

    #[test]
    fn head_bucket_omits_unset_fields() {
        let out = HeadBucketOutput::builder().build();
        let json = head_bucket_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn head_bucket_pretty_prints_with_pascal_case_keys() {
        let out = HeadBucketOutput::builder()
            .bucket_region("us-west-2")
            .access_point_alias(true)
            .build();
        let json = head_bucket_to_json(&out);
        let pretty = serde_json::to_string_pretty(&json).unwrap();
        assert!(pretty.contains("\"BucketRegion\""));
        assert!(pretty.contains("\"AccessPointAlias\""));
    }
}
