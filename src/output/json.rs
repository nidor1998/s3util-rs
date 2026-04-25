//! AWS-CLI-shape JSON serialisation for S3 SDK response types.
//!
//! Hand-written because `aws-sdk-s3` response types do not implement
//! `Serialize`, and the SDK's internal field shapes don't match the
//! `aws s3api --output json` surface 1-to-1 (PascalCase, omission semantics,
//! double-encoded `Policy`, etc.).

use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
use aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_smithy_types_convert::date_time::DateTimeExt;
use serde_json::{Map, Value};

/// Serialise a `GetBucketPolicyOutput` to AWS CLI v2 `--output json` shape.
///
/// Mirrors `aws s3api get-bucket-policy --output json`: emits
/// `{"Policy": "<escaped-JSON-string>"}`. The `Policy` value is the raw
/// policy JSON string returned by S3, double-encoded as a JSON string.
/// When S3 does not return a policy, the output is `{}`.
pub fn get_bucket_policy_to_json(out: &GetBucketPolicyOutput) -> Value {
    let mut map = Map::new();
    if let Some(policy) = out.policy() {
        map.insert("Policy".to_string(), Value::String(policy.to_string()));
    }
    Value::Object(map)
}

/// Serialise a `GetBucketVersioningOutput` to AWS CLI v2 `--output json` shape.
///
/// When versioning has never been configured, S3 returns no `Status` element
/// and the SDK populates neither `status()` nor `mfa_delete()` → emits `{}`.
/// Otherwise emits `{"Status": "Enabled"|"Suspended"}` and optionally
/// `{"MFADelete": "Enabled"|"Disabled"}` when present.
pub fn get_bucket_versioning_to_json(out: &GetBucketVersioningOutput) -> Value {
    let mut map = Map::new();
    if let Some(status) = out.status() {
        map.insert(
            "Status".to_string(),
            Value::String(status.as_str().to_string()),
        );
    }
    if let Some(mfa) = out.mfa_delete() {
        map.insert(
            "MFADelete".to_string(),
            Value::String(mfa.as_str().to_string()),
        );
    }
    Value::Object(map)
}

/// Serialise a `GetBucketTaggingOutput` to AWS CLI v2 `--output json` shape.
///
/// `TagSet` is always emitted (as `[]` when the bucket has no tags).
pub fn get_bucket_tagging_to_json(out: &GetBucketTaggingOutput) -> Value {
    let mut map = Map::new();

    let tag_array: Vec<Value> = out
        .tag_set()
        .iter()
        .map(|tag| {
            let mut t = Map::new();
            t.insert("Key".to_string(), Value::String(tag.key().to_string()));
            t.insert("Value".to_string(), Value::String(tag.value().to_string()));
            Value::Object(t)
        })
        .collect();
    map.insert("TagSet".to_string(), Value::Array(tag_array));

    Value::Object(map)
}

/// Serialise a `GetObjectTaggingOutput` to AWS CLI v2 `--output json` shape.
///
/// `TagSet` is always emitted (as `[]` when the object has no tags).
/// `VersionId` is included only when S3 returned one (versioned objects).
pub fn get_object_tagging_to_json(out: &GetObjectTaggingOutput) -> Value {
    let mut map = Map::new();

    let tag_array: Vec<Value> = out
        .tag_set()
        .iter()
        .map(|tag| {
            let mut t = Map::new();
            t.insert("Key".to_string(), Value::String(tag.key().to_string()));
            t.insert("Value".to_string(), Value::String(tag.value().to_string()));
            Value::Object(t)
        })
        .collect();
    map.insert("TagSet".to_string(), Value::Array(tag_array));

    if let Some(v) = out.version_id() {
        map.insert("VersionId".to_string(), Value::String(v.to_string()));
    }

    Value::Object(map)
}

/// Serialise a `HeadObjectOutput` to AWS CLI v2 `--output json` shape.
///
/// Optional fields are omitted when absent; timestamps are RFC3339 strings.
pub fn head_object_to_json(out: &HeadObjectOutput) -> Value {
    let mut map = Map::new();

    if let Some(v) = out.e_tag() {
        map.insert("ETag".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.last_modified() {
        if let Ok(dt) = v.to_chrono_utc() {
            map.insert("LastModified".to_string(), Value::String(dt.to_rfc3339()));
        }
    }
    if let Some(v) = out.content_length() {
        map.insert(
            "ContentLength".to_string(),
            Value::Number(serde_json::Number::from(v)),
        );
    }
    if let Some(v) = out.content_type() {
        map.insert("ContentType".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.content_encoding() {
        map.insert("ContentEncoding".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.content_disposition() {
        map.insert(
            "ContentDisposition".to_string(),
            Value::String(v.to_string()),
        );
    }
    if let Some(v) = out.content_language() {
        map.insert("ContentLanguage".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.cache_control() {
        map.insert("CacheControl".to_string(), Value::String(v.to_string()));
    }
    // Use the string variant to avoid the deprecated DateTime getter.
    if let Some(v) = out.expires_string.as_deref() {
        map.insert("Expires".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.version_id() {
        map.insert("VersionId".to_string(), Value::String(v.to_string()));
    }
    if let Some(meta) = out.metadata() {
        if !meta.is_empty() {
            let obj: Map<String, Value> = meta
                .iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            map.insert("Metadata".to_string(), Value::Object(obj));
        }
    }
    if let Some(v) = out.server_side_encryption() {
        map.insert(
            "ServerSideEncryption".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.ssekms_key_id() {
        map.insert("SSEKMSKeyId".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.bucket_key_enabled() {
        map.insert("BucketKeyEnabled".to_string(), Value::Bool(v));
    }
    if let Some(v) = out.storage_class() {
        map.insert(
            "StorageClass".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.parts_count() {
        map.insert(
            "PartsCount".to_string(),
            Value::Number(serde_json::Number::from(v)),
        );
    }
    if let Some(v) = out.archive_status() {
        map.insert(
            "ArchiveStatus".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.restore() {
        map.insert("Restore".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.object_lock_mode() {
        map.insert(
            "ObjectLockMode".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.object_lock_retain_until_date() {
        if let Ok(dt) = v.to_chrono_utc() {
            map.insert(
                "ObjectLockRetainUntilDate".to_string(),
                Value::String(dt.to_rfc3339()),
            );
        }
    }
    if let Some(v) = out.object_lock_legal_hold_status() {
        map.insert(
            "ObjectLockLegalHoldStatus".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.checksum_sha256() {
        map.insert("ChecksumSHA256".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_sha1() {
        map.insert("ChecksumSHA1".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_crc32() {
        map.insert("ChecksumCRC32".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_crc32_c() {
        map.insert("ChecksumCRC32C".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_crc64_nvme() {
        map.insert(
            "ChecksumCRC64NVME".to_string(),
            Value::String(v.to_string()),
        );
    }
    if let Some(v) = out.checksum_type() {
        map.insert(
            "ChecksumType".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.accept_ranges() {
        map.insert("AcceptRanges".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.request_charged() {
        map.insert(
            "RequestCharged".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.replication_status() {
        map.insert(
            "ReplicationStatus".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.website_redirect_location() {
        map.insert(
            "WebsiteRedirectLocation".to_string(),
            Value::String(v.to_string()),
        );
    }
    if let Some(v) = out.expiration() {
        map.insert("Expiration".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.missing_meta() {
        map.insert(
            "MissingMeta".to_string(),
            Value::Number(serde_json::Number::from(v)),
        );
    }
    if let Some(v) = out.delete_marker() {
        map.insert("DeleteMarker".to_string(), Value::Bool(v));
    }

    Value::Object(map)
}

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
    use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
    use aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput;
    use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::types::{BucketVersioningStatus, MfaDeleteStatus, Tag};

    // ----- get_bucket_policy_to_json tests -----

    #[test]
    fn get_bucket_policy_with_policy_string() {
        let policy_str = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let out = GetBucketPolicyOutput::builder().policy(policy_str).build();
        let json = get_bucket_policy_to_json(&out);
        assert_eq!(
            json["Policy"],
            Value::String(policy_str.to_string()),
            "Policy must be the raw JSON string (double-encoded)"
        );
    }

    #[test]
    fn get_bucket_policy_absent_policy_yields_empty_object() {
        let out = GetBucketPolicyOutput::builder().build();
        let json = get_bucket_policy_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_policy_pretty_printed_uses_pascal_case_key() {
        let out = GetBucketPolicyOutput::builder()
            .policy(r#"{"Version":"2012-10-17"}"#)
            .build();
        let json = get_bucket_policy_to_json(&out);
        let pretty = serde_json::to_string_pretty(&json).unwrap();
        assert!(
            pretty.contains("\"Policy\""),
            "key must be PascalCase: {pretty}"
        );
    }

    // ----- get_bucket_versioning_to_json tests -----

    #[test]
    fn get_bucket_versioning_never_configured_yields_empty_object() {
        // S3 returns no Status element when versioning has never been configured.
        let out = GetBucketVersioningOutput::builder().build();
        let json = get_bucket_versioning_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_versioning_enabled_status() {
        let out = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Enabled)
            .build();
        let json = get_bucket_versioning_to_json(&out);
        assert_eq!(json["Status"], Value::String("Enabled".into()));
        assert!(json.get("MFADelete").is_none());
    }

    #[test]
    fn get_bucket_versioning_suspended_status() {
        let out = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Suspended)
            .build();
        let json = get_bucket_versioning_to_json(&out);
        assert_eq!(json["Status"], Value::String("Suspended".into()));
        assert!(json.get("MFADelete").is_none());
    }

    #[test]
    fn get_bucket_versioning_with_mfa_delete() {
        let out = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Enabled)
            .mfa_delete(MfaDeleteStatus::Enabled)
            .build();
        let json = get_bucket_versioning_to_json(&out);
        assert_eq!(json["Status"], Value::String("Enabled".into()));
        assert_eq!(json["MFADelete"], Value::String("Enabled".into()));
    }

    // ----- get_bucket_tagging_to_json tests -----

    #[test]
    fn get_bucket_tagging_empty_tag_set_yields_empty_array() {
        let out = GetBucketTaggingOutput::builder()
            .set_tag_set(Some(vec![]))
            .build()
            .unwrap();
        let json = get_bucket_tagging_to_json(&out);
        assert_eq!(json["TagSet"], Value::Array(vec![]));
    }

    #[test]
    fn get_bucket_tagging_with_tags() {
        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let out = GetBucketTaggingOutput::builder()
            .tag_set(tag)
            .build()
            .unwrap();
        let json = get_bucket_tagging_to_json(&out);
        let tags = json["TagSet"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], Value::String("env".into()));
        assert_eq!(tags[0]["Value"], Value::String("prod".into()));
    }

    #[test]
    fn get_bucket_tagging_multiple_tags_preserve_order() {
        let tag1 = Tag::builder().key("a").value("1").build().unwrap();
        let tag2 = Tag::builder().key("b").value("2").build().unwrap();
        let out = GetBucketTaggingOutput::builder()
            .tag_set(tag1)
            .tag_set(tag2)
            .build()
            .unwrap();
        let json = get_bucket_tagging_to_json(&out);
        let tags = json["TagSet"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], Value::String("a".into()));
        assert_eq!(tags[1]["Key"], Value::String("b".into()));
    }

    // ----- get_object_tagging_to_json tests -----

    #[test]
    fn get_object_tagging_empty_tag_set_yields_empty_array() {
        let out = GetObjectTaggingOutput::builder()
            .set_tag_set(Some(vec![]))
            .build()
            .unwrap();
        let json = get_object_tagging_to_json(&out);
        assert_eq!(json["TagSet"], Value::Array(vec![]));
        assert!(json.get("VersionId").is_none());
    }

    #[test]
    fn get_object_tagging_with_tags() {
        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let out = GetObjectTaggingOutput::builder()
            .tag_set(tag)
            .build()
            .unwrap();
        let json = get_object_tagging_to_json(&out);
        let tags = json["TagSet"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], Value::String("env".into()));
        assert_eq!(tags[0]["Value"], Value::String("prod".into()));
    }

    #[test]
    fn get_object_tagging_with_version_id() {
        let out = GetObjectTaggingOutput::builder()
            .set_tag_set(Some(vec![]))
            .version_id("vid-abc")
            .build()
            .unwrap();
        let json = get_object_tagging_to_json(&out);
        assert_eq!(json["VersionId"], Value::String("vid-abc".into()));
        assert_eq!(json["TagSet"], Value::Array(vec![]));
    }

    #[test]
    fn get_object_tagging_multiple_tags_preserve_order() {
        let tag1 = Tag::builder().key("a").value("1").build().unwrap();
        let tag2 = Tag::builder().key("b").value("2").build().unwrap();
        let out = GetObjectTaggingOutput::builder()
            .tag_set(tag1)
            .tag_set(tag2)
            .build()
            .unwrap();
        let json = get_object_tagging_to_json(&out);
        let tags = json["TagSet"].as_array().unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], Value::String("a".into()));
        assert_eq!(tags[1]["Key"], Value::String("b".into()));
    }

    // ----- head_object_to_json tests -----

    #[test]
    fn head_object_empty_output_yields_empty_object() {
        let out = HeadObjectOutput::builder().build();
        let json = head_object_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn head_object_with_etag_only() {
        let out = HeadObjectOutput::builder().e_tag("\"abc123\"").build();
        let json = head_object_to_json(&out);
        assert_eq!(json["ETag"], Value::String("\"abc123\"".into()));
        assert!(json.get("ContentLength").is_none());
        assert!(json.get("LastModified").is_none());
    }

    #[test]
    fn head_object_with_common_fields() {
        let out = HeadObjectOutput::builder()
            .e_tag("\"deadbeef\"")
            .content_length(1024)
            .content_type("application/octet-stream")
            .content_encoding("gzip")
            .content_language("en-US")
            .cache_control("no-cache")
            .version_id("v1")
            .accept_ranges("bytes")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["ETag"], Value::String("\"deadbeef\"".into()));
        assert_eq!(json["ContentLength"], Value::Number(1024i64.into()));
        assert_eq!(
            json["ContentType"],
            Value::String("application/octet-stream".into())
        );
        assert_eq!(json["ContentEncoding"], Value::String("gzip".into()));
        assert_eq!(json["ContentLanguage"], Value::String("en-US".into()));
        assert_eq!(json["CacheControl"], Value::String("no-cache".into()));
        assert_eq!(json["VersionId"], Value::String("v1".into()));
        assert_eq!(json["AcceptRanges"], Value::String("bytes".into()));
    }

    #[test]
    fn head_object_sse_kms_fields() {
        use aws_sdk_s3::types::ServerSideEncryption;
        let out = HeadObjectOutput::builder()
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id("arn:aws:kms:us-east-1:123456789012:key/abc")
            .bucket_key_enabled(true)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ServerSideEncryption"],
            Value::String("aws:kms".into())
        );
        assert_eq!(
            json["SSEKMSKeyId"],
            Value::String("arn:aws:kms:us-east-1:123456789012:key/abc".into())
        );
        assert_eq!(json["BucketKeyEnabled"], Value::Bool(true));
    }

    // ----- head_bucket_to_json tests -----

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

    #[test]
    fn head_bucket_with_location_type_and_name() {
        use aws_sdk_s3::types::LocationType;
        let out = HeadBucketOutput::builder()
            .bucket_region("us-east-1")
            .bucket_location_type(LocationType::AvailabilityZone)
            .bucket_location_name("use1-az4")
            .build();
        let json = head_bucket_to_json(&out);
        assert_eq!(
            json["BucketLocationType"],
            Value::String("AvailabilityZone".into())
        );
        assert_eq!(json["BucketLocationName"], Value::String("use1-az4".into()));
    }

    // ----- head_object_to_json tests for all conditional fields -----
    //
    // The existing tests above cover the common-fields and SSE-KMS paths.
    // The tests below exercise each remaining optional branch one at a time
    // so the JSON serialiser stays in lockstep with the AWS-CLI shape it
    // claims to mirror. New SDK fields added later should add a parallel test.

    #[test]
    fn head_object_with_content_disposition() {
        let out = HeadObjectOutput::builder()
            .content_disposition("attachment; filename=\"x.txt\"")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ContentDisposition"],
            Value::String("attachment; filename=\"x.txt\"".into())
        );
    }

    #[test]
    fn head_object_with_expires_string() {
        let out = HeadObjectOutput::builder()
            .expires_string("Wed, 21 Oct 2026 07:28:00 GMT")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["Expires"],
            Value::String("Wed, 21 Oct 2026 07:28:00 GMT".into())
        );
    }

    #[test]
    fn head_object_with_metadata_non_empty() {
        let out = HeadObjectOutput::builder()
            .metadata("k1", "v1")
            .metadata("k2", "v2")
            .build();
        let json = head_object_to_json(&out);
        let meta = json["Metadata"]
            .as_object()
            .expect("Metadata must be object");
        assert_eq!(meta["k1"], Value::String("v1".into()));
        assert_eq!(meta["k2"], Value::String("v2".into()));
    }

    #[test]
    fn head_object_with_empty_metadata_omits_key() {
        // The SDK can produce Some(empty map) when no user metadata is set;
        // the serialiser must omit the Metadata key in that case (matching
        // aws s3api which only emits Metadata when at least one entry exists).
        let out = HeadObjectOutput::builder().build();
        let json = head_object_to_json(&out);
        assert!(json.get("Metadata").is_none());
    }

    #[test]
    fn head_object_with_storage_class_and_parts_count() {
        use aws_sdk_s3::types::StorageClass;
        let out = HeadObjectOutput::builder()
            .storage_class(StorageClass::Standard)
            .parts_count(3)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["StorageClass"], Value::String("STANDARD".into()));
        assert_eq!(json["PartsCount"], Value::Number(3i32.into()));
    }

    #[test]
    fn head_object_with_archive_status_and_restore() {
        use aws_sdk_s3::types::ArchiveStatus;
        let out = HeadObjectOutput::builder()
            .archive_status(ArchiveStatus::ArchiveAccess)
            .restore("ongoing-request=\"false\"")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ArchiveStatus"],
            Value::String("ARCHIVE_ACCESS".into())
        );
        assert_eq!(
            json["Restore"],
            Value::String("ongoing-request=\"false\"".into())
        );
    }

    #[test]
    fn head_object_with_object_lock_fields() {
        use aws_sdk_s3::primitives::DateTime;
        use aws_sdk_s3::types::{ObjectLockLegalHoldStatus, ObjectLockMode};
        // Pick any fixed UTC instant; we assert the formatted shape, not the
        // specific calendar date, so we don't depend on epoch arithmetic.
        let until = DateTime::from_secs(1_700_000_000);
        let out = HeadObjectOutput::builder()
            .object_lock_mode(ObjectLockMode::Compliance)
            .object_lock_retain_until_date(until)
            .object_lock_legal_hold_status(ObjectLockLegalHoldStatus::On)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["ObjectLockMode"], Value::String("COMPLIANCE".into()));
        let retain = json["ObjectLockRetainUntilDate"]
            .as_str()
            .expect("retain date must be string");
        // RFC3339 shape: YYYY-MM-DDTHH:MM:SS[.fff]+00:00 (or Z)
        assert!(
            retain.len() >= 20 && retain.contains('T'),
            "expected RFC3339-shaped string, got {retain}"
        );
        assert!(
            retain.ends_with("+00:00") || retain.ends_with('Z'),
            "expected UTC suffix, got {retain}"
        );
        assert_eq!(
            json["ObjectLockLegalHoldStatus"],
            Value::String("ON".into())
        );
    }

    #[test]
    fn head_object_with_all_checksum_variants() {
        use aws_sdk_s3::types::ChecksumType;
        let out = HeadObjectOutput::builder()
            .checksum_sha256("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .checksum_sha1("AAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .checksum_crc32("AAAAAA==")
            .checksum_crc32_c("AAAAAA==")
            .checksum_crc64_nvme("AAAAAAAAAAA=")
            .checksum_type(ChecksumType::FullObject)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumSHA256"],
            Value::String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into())
        );
        assert_eq!(
            json["ChecksumSHA1"],
            Value::String("AAAAAAAAAAAAAAAAAAAAAAAAAAA=".into())
        );
        assert_eq!(json["ChecksumCRC32"], Value::String("AAAAAA==".into()));
        assert_eq!(json["ChecksumCRC32C"], Value::String("AAAAAA==".into()));
        assert_eq!(
            json["ChecksumCRC64NVME"],
            Value::String("AAAAAAAAAAA=".into())
        );
        assert_eq!(json["ChecksumType"], Value::String("FULL_OBJECT".into()));
    }

    #[test]
    fn head_object_with_request_charged_and_replication_status() {
        use aws_sdk_s3::types::{ReplicationStatus, RequestCharged};
        let out = HeadObjectOutput::builder()
            .request_charged(RequestCharged::Requester)
            .replication_status(ReplicationStatus::Completed)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["RequestCharged"], Value::String("requester".into()));
        assert_eq!(json["ReplicationStatus"], Value::String("COMPLETED".into()));
    }

    #[test]
    fn head_object_with_website_redirect_and_expiration() {
        let out = HeadObjectOutput::builder()
            .website_redirect_location("/new-location")
            .expiration("expiry-date=\"Fri, 23 Dec 2026 00:00:00 GMT\", rule-id=\"rule-1\"")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["WebsiteRedirectLocation"],
            Value::String("/new-location".into())
        );
        assert_eq!(
            json["Expiration"],
            Value::String(
                "expiry-date=\"Fri, 23 Dec 2026 00:00:00 GMT\", rule-id=\"rule-1\"".into()
            )
        );
    }

    #[test]
    fn head_object_with_missing_meta_and_delete_marker() {
        let out = HeadObjectOutput::builder()
            .missing_meta(2)
            .delete_marker(true)
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["MissingMeta"], Value::Number(2i32.into()));
        assert_eq!(json["DeleteMarker"], Value::Bool(true));
    }
}
