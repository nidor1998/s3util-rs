//! AWS-CLI-shape JSON serialisation for S3 SDK response types.
//!
//! Hand-written because `aws-sdk-s3` response types do not implement
//! `Serialize`, and the SDK's internal field shapes don't match the
//! `aws s3api --output json` surface 1-to-1 (PascalCase, omission semantics,
//! double-encoded `Policy`, etc.).

use aws_sdk_s3::operation::get_bucket_accelerate_configuration::GetBucketAccelerateConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
use aws_sdk_s3::operation::get_bucket_policy_status::GetBucketPolicyStatusOutput;
use aws_sdk_s3::operation::get_bucket_replication::GetBucketReplicationOutput;
use aws_sdk_s3::operation::get_bucket_request_payment::GetBucketRequestPaymentOutput;
use aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object_annotation::PutObjectAnnotationOutput;
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
/// Otherwise emits `{"Status": "Enabled"|"Suspended"}` and, when present,
/// `{"MFADelete": "Enabled"|"Disabled"}` (the casing AWS CLI v2 emits).
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

/// Serialise a `PutObjectAnnotation` response to `aws s3api`-shape JSON.
/// Absent fields are omitted. The ETag is echoed for information only; it is
/// not used for verification.
pub fn put_object_annotation_to_json(out: &PutObjectAnnotationOutput) -> Value {
    let mut map = Map::new();
    if let Some(v) = out.key() {
        map.insert("Key".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.annotation_name() {
        map.insert("AnnotationName".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.object_version_id() {
        map.insert("ObjectVersionId".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.e_tag() {
        map.insert("ETag".to_string(), Value::String(v.to_string()));
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
    if let Some(v) = out.server_side_encryption() {
        map.insert(
            "ServerSideEncryption".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.request_charged() {
        map.insert(
            "RequestCharged".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    Value::Object(map)
}

/// Serialise a `GetObjectAnnotation` response to `aws s3api`-shape JSON.
/// Absent fields are omitted. The annotation payload itself is never included
/// (it is written to the output file / stdout, not to the JSON).
pub fn get_object_annotation_to_json(out: &GetObjectAnnotationOutput) -> Value {
    let mut map = Map::new();
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
    if let Some(v) = out.e_tag() {
        map.insert("ETag".to_string(), Value::String(v.to_string()));
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
    if let Some(v) = out.checksum_sha1() {
        map.insert("ChecksumSHA1".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_sha256() {
        map.insert("ChecksumSHA256".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_type() {
        map.insert(
            "ChecksumType".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.server_side_encryption() {
        map.insert(
            "ServerSideEncryption".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.object_version_id() {
        map.insert("VersionId".to_string(), Value::String(v.to_string()));
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
    if let Some(v) = out.content_range() {
        map.insert("ContentRange".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.cache_control() {
        map.insert("CacheControl".to_string(), Value::String(v.to_string()));
    }
    // AWS CLI v2 emits both `Expires` (parsed ISO-8601 timestamp, deprecated
    // but still emitted) and `ExpiresString` (raw HTTP-date header value).
    // The SDK populates `expires` only when the header parsed successfully;
    // `expires_string` is set whenever the header was present.
    #[allow(deprecated)]
    if let Some(dt) = out.expires()
        && let Ok(c) = dt.to_chrono_utc()
    {
        map.insert("Expires".to_string(), Value::String(c.to_rfc3339()));
    }
    if let Some(v) = out.expires_string() {
        map.insert("ExpiresString".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.version_id() {
        map.insert("VersionId".to_string(), Value::String(v.to_string()));
    }
    // AWS CLI v2 always emits `Metadata` (as `{}` when no user metadata is set),
    // so we do too. The SDK returns `Some(empty map)` in that case.
    let meta_obj: Map<String, Value> = out
        .metadata()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect()
        })
        .unwrap_or_default();
    map.insert("Metadata".to_string(), Value::Object(meta_obj));
    if let Some(v) = out.server_side_encryption() {
        map.insert(
            "ServerSideEncryption".to_string(),
            Value::String(v.as_str().to_string()),
        );
    }
    if let Some(v) = out.ssekms_key_id() {
        map.insert("SSEKMSKeyId".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.sse_customer_algorithm() {
        map.insert(
            "SSECustomerAlgorithm".to_string(),
            Value::String(v.to_string()),
        );
    }
    if let Some(v) = out.sse_customer_key_md5() {
        map.insert(
            "SSECustomerKeyMD5".to_string(),
            Value::String(v.to_string()),
        );
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
    if let Some(v) = out.checksum_sha512() {
        map.insert("ChecksumSHA512".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_md5() {
        map.insert("ChecksumMD5".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_xxhash64() {
        map.insert("ChecksumXXHASH64".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_xxhash3() {
        map.insert("ChecksumXXHASH3".to_string(), Value::String(v.to_string()));
    }
    if let Some(v) = out.checksum_xxhash128() {
        map.insert(
            "ChecksumXXHASH128".to_string(),
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
    if let Some(v) = out.tag_count() {
        map.insert(
            "TagCount".to_string(),
            Value::Number(serde_json::Number::from(v)),
        );
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
    if let Some(arn) = out.bucket_arn() {
        map.insert("BucketArn".to_string(), Value::String(arn.to_string()));
    }
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

/// Serialise a `GetBucketLifecycleConfigurationOutput` to AWS CLI v2
/// `--output json` shape.
///
/// Top level: `{"Rules": [ … ], "TransitionDefaultMinimumObjectSize": "…"}`
/// (always emits `Rules`, as `[]` if empty). `TransitionDefaultMinimumObjectSize`
/// is the bucket-level default minimum object size for transitions, emitted
/// only when the SDK populates it. Each rule emits its present fields with
/// PascalCase keys; absent fields are omitted (never `null`).
pub fn get_bucket_lifecycle_configuration_to_json(
    out: &GetBucketLifecycleConfigurationOutput,
) -> Value {
    let mut map = Map::new();
    let rules: Vec<Value> = out.rules().iter().map(serialize_lifecycle_rule).collect();
    map.insert("Rules".to_string(), Value::Array(rules));
    if let Some(tdmos) = out.transition_default_minimum_object_size() {
        map.insert(
            "TransitionDefaultMinimumObjectSize".to_string(),
            Value::String(tdmos.as_str().to_string()),
        );
    }
    Value::Object(map)
}

fn serialize_lifecycle_rule(r: &aws_sdk_s3::types::LifecycleRule) -> Value {
    let mut m = Map::new();
    if let Some(id) = r.id() {
        m.insert("ID".to_string(), Value::String(id.to_string()));
    }
    m.insert(
        "Status".to_string(),
        Value::String(r.status().as_str().to_string()),
    );
    // `Prefix` on the rule itself is deprecated by S3 in favour of
    // `Filter.Prefix`, but kept for AWS-CLI output-shape parity.
    #[allow(deprecated)]
    if let Some(p) = r.prefix() {
        m.insert("Prefix".to_string(), Value::String(p.to_string()));
    }
    if let Some(f) = r.filter() {
        m.insert("Filter".to_string(), serialize_lifecycle_filter(f));
    }
    if let Some(e) = r.expiration() {
        m.insert("Expiration".to_string(), serialize_lifecycle_expiration(e));
    }
    if let Some(n) = r.noncurrent_version_expiration() {
        let mut nm = Map::new();
        if let Some(d) = n.noncurrent_days() {
            nm.insert(
                "NoncurrentDays".to_string(),
                Value::Number(serde_json::Number::from(d)),
            );
        }
        if let Some(d) = n.newer_noncurrent_versions() {
            nm.insert(
                "NewerNoncurrentVersions".to_string(),
                Value::Number(serde_json::Number::from(d)),
            );
        }
        m.insert("NoncurrentVersionExpiration".to_string(), Value::Object(nm));
    }
    if !r.transitions().is_empty() {
        let arr: Vec<Value> = r.transitions().iter().map(serialize_transition).collect();
        m.insert("Transitions".to_string(), Value::Array(arr));
    }
    if !r.noncurrent_version_transitions().is_empty() {
        let arr: Vec<Value> = r
            .noncurrent_version_transitions()
            .iter()
            .map(|n| {
                let mut nm = Map::new();
                if let Some(d) = n.noncurrent_days() {
                    nm.insert(
                        "NoncurrentDays".to_string(),
                        Value::Number(serde_json::Number::from(d)),
                    );
                }
                if let Some(sc) = n.storage_class() {
                    nm.insert(
                        "StorageClass".to_string(),
                        Value::String(sc.as_str().to_string()),
                    );
                }
                if let Some(d) = n.newer_noncurrent_versions() {
                    nm.insert(
                        "NewerNoncurrentVersions".to_string(),
                        Value::Number(serde_json::Number::from(d)),
                    );
                }
                Value::Object(nm)
            })
            .collect();
        m.insert(
            "NoncurrentVersionTransitions".to_string(),
            Value::Array(arr),
        );
    }
    if let Some(a) = r.abort_incomplete_multipart_upload() {
        let mut am = Map::new();
        if let Some(d) = a.days_after_initiation() {
            am.insert(
                "DaysAfterInitiation".to_string(),
                Value::Number(serde_json::Number::from(d)),
            );
        }
        m.insert(
            "AbortIncompleteMultipartUpload".to_string(),
            Value::Object(am),
        );
    }
    Value::Object(m)
}

fn serialize_lifecycle_filter(f: &aws_sdk_s3::types::LifecycleRuleFilter) -> Value {
    let mut m = Map::new();
    if let Some(p) = f.prefix() {
        m.insert("Prefix".to_string(), Value::String(p.to_string()));
    }
    if let Some(t) = f.tag() {
        let mut tm = Map::new();
        tm.insert("Key".to_string(), Value::String(t.key().to_string()));
        tm.insert("Value".to_string(), Value::String(t.value().to_string()));
        m.insert("Tag".to_string(), Value::Object(tm));
    }
    if let Some(n) = f.object_size_greater_than() {
        m.insert(
            "ObjectSizeGreaterThan".to_string(),
            Value::Number(serde_json::Number::from(n)),
        );
    }
    if let Some(n) = f.object_size_less_than() {
        m.insert(
            "ObjectSizeLessThan".to_string(),
            Value::Number(serde_json::Number::from(n)),
        );
    }
    if let Some(and) = f.and() {
        let mut am = Map::new();
        if let Some(p) = and.prefix() {
            am.insert("Prefix".to_string(), Value::String(p.to_string()));
        }
        if !and.tags().is_empty() {
            let arr: Vec<Value> = and
                .tags()
                .iter()
                .map(|t| {
                    let mut tm = Map::new();
                    tm.insert("Key".to_string(), Value::String(t.key().to_string()));
                    tm.insert("Value".to_string(), Value::String(t.value().to_string()));
                    Value::Object(tm)
                })
                .collect();
            am.insert("Tags".to_string(), Value::Array(arr));
        }
        if let Some(n) = and.object_size_greater_than() {
            am.insert(
                "ObjectSizeGreaterThan".to_string(),
                Value::Number(serde_json::Number::from(n)),
            );
        }
        if let Some(n) = and.object_size_less_than() {
            am.insert(
                "ObjectSizeLessThan".to_string(),
                Value::Number(serde_json::Number::from(n)),
            );
        }
        m.insert("And".to_string(), Value::Object(am));
    }
    Value::Object(m)
}

fn serialize_lifecycle_expiration(e: &aws_sdk_s3::types::LifecycleExpiration) -> Value {
    let mut m = Map::new();
    if let Some(d) = e.date()
        && let Ok(dt) = d.to_chrono_utc()
    {
        m.insert("Date".to_string(), Value::String(dt.to_rfc3339()));
    }
    if let Some(d) = e.days() {
        m.insert(
            "Days".to_string(),
            Value::Number(serde_json::Number::from(d)),
        );
    }
    if let Some(eodm) = e.expired_object_delete_marker() {
        m.insert("ExpiredObjectDeleteMarker".to_string(), Value::Bool(eodm));
    }
    Value::Object(m)
}

fn serialize_transition(t: &aws_sdk_s3::types::Transition) -> Value {
    let mut m = Map::new();
    if let Some(d) = t.date()
        && let Ok(dt) = d.to_chrono_utc()
    {
        m.insert("Date".to_string(), Value::String(dt.to_rfc3339()));
    }
    if let Some(d) = t.days() {
        m.insert(
            "Days".to_string(),
            Value::Number(serde_json::Number::from(d)),
        );
    }
    if let Some(sc) = t.storage_class() {
        m.insert(
            "StorageClass".to_string(),
            Value::String(sc.as_str().to_string()),
        );
    }
    Value::Object(m)
}

/// Serialise a `GetBucketEncryptionOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"ServerSideEncryptionConfiguration": {"Rules": [ … ]}}`.
/// Mirrors `aws s3api get-bucket-encryption --output json`.
pub fn get_bucket_encryption_to_json(out: &GetBucketEncryptionOutput) -> Value {
    let mut top = Map::new();
    if let Some(cfg) = out.server_side_encryption_configuration() {
        let mut inner = Map::new();
        let rules: Vec<Value> = cfg
            .rules()
            .iter()
            .map(|r| {
                let mut rm = Map::new();
                if let Some(d) = r.apply_server_side_encryption_by_default() {
                    let mut dm = Map::new();
                    dm.insert(
                        "SSEAlgorithm".to_string(),
                        Value::String(d.sse_algorithm().as_str().to_string()),
                    );
                    if let Some(k) = d.kms_master_key_id() {
                        dm.insert("KMSMasterKeyID".to_string(), Value::String(k.to_string()));
                    }
                    rm.insert(
                        "ApplyServerSideEncryptionByDefault".to_string(),
                        Value::Object(dm),
                    );
                }
                if let Some(b) = r.bucket_key_enabled() {
                    rm.insert("BucketKeyEnabled".to_string(), Value::Bool(b));
                }
                if let Some(bet) = r.blocked_encryption_types() {
                    let mut bm = Map::new();
                    if !bet.encryption_type().is_empty() {
                        let arr: Vec<Value> = bet
                            .encryption_type()
                            .iter()
                            .map(|e| Value::String(e.as_str().to_string()))
                            .collect();
                        bm.insert("EncryptionType".to_string(), Value::Array(arr));
                    }
                    rm.insert("BlockedEncryptionTypes".to_string(), Value::Object(bm));
                }
                Value::Object(rm)
            })
            .collect();
        inner.insert("Rules".to_string(), Value::Array(rules));
        top.insert(
            "ServerSideEncryptionConfiguration".to_string(),
            Value::Object(inner),
        );
    }
    Value::Object(top)
}

/// Serialise a `GetBucketCorsOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"CORSRules": [ … ]}` (always emits `CORSRules`, as `[]` if
/// empty). Each rule emits its present fields with PascalCase keys; absent
/// or empty optional fields are omitted (never `null`).
pub fn get_bucket_cors_to_json(out: &GetBucketCorsOutput) -> Value {
    let mut top = Map::new();
    let arr: Vec<Value> = out
        .cors_rules()
        .iter()
        .map(|r| {
            let mut m = Map::new();
            if let Some(id) = r.id() {
                m.insert("ID".to_string(), Value::String(id.to_string()));
            }
            if !r.allowed_headers().is_empty() {
                m.insert(
                    "AllowedHeaders".to_string(),
                    Value::Array(
                        r.allowed_headers()
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            m.insert(
                "AllowedMethods".to_string(),
                Value::Array(
                    r.allowed_methods()
                        .iter()
                        .map(|s| Value::String(s.clone()))
                        .collect(),
                ),
            );
            m.insert(
                "AllowedOrigins".to_string(),
                Value::Array(
                    r.allowed_origins()
                        .iter()
                        .map(|s| Value::String(s.clone()))
                        .collect(),
                ),
            );
            if !r.expose_headers().is_empty() {
                m.insert(
                    "ExposeHeaders".to_string(),
                    Value::Array(
                        r.expose_headers()
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            if let Some(m_age) = r.max_age_seconds() {
                m.insert(
                    "MaxAgeSeconds".to_string(),
                    Value::Number(serde_json::Number::from(m_age)),
                );
            }
            Value::Object(m)
        })
        .collect();
    top.insert("CORSRules".to_string(), Value::Array(arr));
    Value::Object(top)
}

/// Serialise a `GetBucketWebsiteOutput` to AWS CLI v2 `--output json` shape.
///
/// Top-level FLAT shape: `{"IndexDocument": …, "ErrorDocument": …,
/// "RedirectAllRequestsTo": …, "RoutingRules": [...]}`. Absent fields are
/// omitted (never `null`). Matches `aws s3api get-bucket-website --output json`.
pub fn get_bucket_website_to_json(out: &GetBucketWebsiteOutput) -> Value {
    let mut top = Map::new();
    if let Some(ix) = out.index_document() {
        let mut m = Map::new();
        m.insert("Suffix".to_string(), Value::String(ix.suffix().to_string()));
        top.insert("IndexDocument".to_string(), Value::Object(m));
    }
    if let Some(err) = out.error_document() {
        let mut m = Map::new();
        m.insert("Key".to_string(), Value::String(err.key().to_string()));
        top.insert("ErrorDocument".to_string(), Value::Object(m));
    }
    if let Some(r) = out.redirect_all_requests_to() {
        let mut m = Map::new();
        m.insert(
            "HostName".to_string(),
            Value::String(r.host_name().to_string()),
        );
        if let Some(p) = r.protocol() {
            m.insert(
                "Protocol".to_string(),
                Value::String(p.as_str().to_string()),
            );
        }
        top.insert("RedirectAllRequestsTo".to_string(), Value::Object(m));
    }
    if !out.routing_rules().is_empty() {
        let arr: Vec<Value> = out
            .routing_rules()
            .iter()
            .map(serialize_routing_rule)
            .collect();
        top.insert("RoutingRules".to_string(), Value::Array(arr));
    }
    Value::Object(top)
}

fn serialize_routing_rule(rr: &aws_sdk_s3::types::RoutingRule) -> Value {
    let mut m = Map::new();
    if let Some(c) = rr.condition() {
        let mut cm = Map::new();
        if let Some(v) = c.http_error_code_returned_equals() {
            cm.insert(
                "HttpErrorCodeReturnedEquals".to_string(),
                Value::String(v.to_string()),
            );
        }
        if let Some(v) = c.key_prefix_equals() {
            cm.insert("KeyPrefixEquals".to_string(), Value::String(v.to_string()));
        }
        m.insert("Condition".to_string(), Value::Object(cm));
    }
    if let Some(r) = rr.redirect() {
        let mut rm = Map::new();
        if let Some(v) = r.host_name() {
            rm.insert("HostName".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = r.http_redirect_code() {
            rm.insert("HttpRedirectCode".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = r.protocol() {
            rm.insert(
                "Protocol".to_string(),
                Value::String(v.as_str().to_string()),
            );
        }
        if let Some(v) = r.replace_key_prefix_with() {
            rm.insert(
                "ReplaceKeyPrefixWith".to_string(),
                Value::String(v.to_string()),
            );
        }
        if let Some(v) = r.replace_key_with() {
            rm.insert("ReplaceKeyWith".to_string(), Value::String(v.to_string()));
        }
        m.insert("Redirect".to_string(), Value::Object(rm));
    }
    Value::Object(m)
}

/// Serialise a `GetBucketLoggingOutput` to AWS CLI v2 `--output json` shape.
///
/// Mirrors `aws s3api get-bucket-logging --output json`: emits
/// `{"LoggingEnabled": { … }}` when logging is configured, or `{}` when no
/// logging is set on the bucket (S3 returns success with an empty body in
/// that case — there is no `NoSuchLoggingConfiguration` error code).
///
/// `TargetGrants` is rendered as an array when the SDK populates a non-empty
/// list. Each grant emits `Grantee` (with its `Type` plus the populated
/// identity fields) and `Permission`.
pub fn get_bucket_logging_to_json(out: &GetBucketLoggingOutput) -> Value {
    let mut top = Map::new();
    if let Some(le) = out.logging_enabled() {
        let mut inner = Map::new();
        inner.insert(
            "TargetBucket".to_string(),
            Value::String(le.target_bucket().to_string()),
        );
        inner.insert(
            "TargetPrefix".to_string(),
            Value::String(le.target_prefix().to_string()),
        );
        if let Some(fmt) = le.target_object_key_format() {
            let mut fmt_map = Map::new();
            if fmt.simple_prefix().is_some() {
                fmt_map.insert("SimplePrefix".to_string(), Value::Object(Map::new()));
            }
            if let Some(pp) = fmt.partitioned_prefix() {
                let mut pp_map = Map::new();
                if let Some(ds) = pp.partition_date_source() {
                    pp_map.insert(
                        "PartitionDateSource".to_string(),
                        Value::String(ds.as_str().to_string()),
                    );
                }
                fmt_map.insert("PartitionedPrefix".to_string(), Value::Object(pp_map));
            }
            inner.insert("TargetObjectKeyFormat".to_string(), Value::Object(fmt_map));
        }
        if !le.target_grants().is_empty() {
            let arr: Vec<Value> = le
                .target_grants()
                .iter()
                .map(serialize_target_grant)
                .collect();
            inner.insert("TargetGrants".to_string(), Value::Array(arr));
        }
        top.insert("LoggingEnabled".to_string(), Value::Object(inner));
    }
    Value::Object(top)
}

fn serialize_target_grant(g: &aws_sdk_s3::types::TargetGrant) -> Value {
    let mut m = Map::new();
    if let Some(grantee) = g.grantee() {
        let mut gm = Map::new();
        gm.insert(
            "Type".to_string(),
            Value::String(grantee.r#type().as_str().to_string()),
        );
        if let Some(v) = grantee.display_name() {
            gm.insert("DisplayName".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = grantee.email_address() {
            gm.insert("EmailAddress".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = grantee.id() {
            gm.insert("ID".to_string(), Value::String(v.to_string()));
        }
        if let Some(v) = grantee.uri() {
            gm.insert("URI".to_string(), Value::String(v.to_string()));
        }
        m.insert("Grantee".to_string(), Value::Object(gm));
    }
    if let Some(p) = g.permission() {
        m.insert(
            "Permission".to_string(),
            Value::String(p.as_str().to_string()),
        );
    }
    Value::Object(m)
}

/// Serialise a `GetBucketNotificationConfigurationOutput` to AWS CLI v2
/// `--output json` shape.
///
/// Top-level FLAT shape:
/// `{"TopicConfigurations": [...], "QueueConfigurations": [...],
///   "LambdaFunctionConfigurations": [...], "EventBridgeConfiguration": {}}`.
/// Each list is emitted only when present (the SDK distinguishes
/// `Some(vec![])` from `None`); the EventBridge marker is emitted only when
/// configured. When no notifications are configured S3 returns success
/// with an empty body and this function emits `{}`. Matches
/// `aws s3api get-bucket-notification-configuration --output json`.
pub fn get_bucket_notification_configuration_to_json(
    out: &GetBucketNotificationConfigurationOutput,
) -> Value {
    let mut top = Map::new();
    if let Some(topics) = out.topic_configurations.as_ref() {
        let arr: Vec<Value> = topics.iter().map(serialize_topic_configuration).collect();
        top.insert("TopicConfigurations".to_string(), Value::Array(arr));
    }
    if let Some(queues) = out.queue_configurations.as_ref() {
        let arr: Vec<Value> = queues.iter().map(serialize_queue_configuration).collect();
        top.insert("QueueConfigurations".to_string(), Value::Array(arr));
    }
    if let Some(lambdas) = out.lambda_function_configurations.as_ref() {
        let arr: Vec<Value> = lambdas
            .iter()
            .map(serialize_lambda_function_configuration)
            .collect();
        top.insert(
            "LambdaFunctionConfigurations".to_string(),
            Value::Array(arr),
        );
    }
    if out.event_bridge_configuration().is_some() {
        // EventBridgeConfiguration is a presence-only marker; AWS CLI emits `{}`.
        top.insert(
            "EventBridgeConfiguration".to_string(),
            Value::Object(Map::new()),
        );
    }
    Value::Object(top)
}

fn serialize_topic_configuration(t: &aws_sdk_s3::types::TopicConfiguration) -> Value {
    let mut m = Map::new();
    if let Some(id) = t.id() {
        m.insert("Id".to_string(), Value::String(id.to_string()));
    }
    m.insert(
        "TopicArn".to_string(),
        Value::String(t.topic_arn().to_string()),
    );
    m.insert("Events".to_string(), serialize_events(t.events()));
    if let Some(f) = t.filter() {
        m.insert("Filter".to_string(), serialize_notification_filter(f));
    }
    Value::Object(m)
}

fn serialize_queue_configuration(q: &aws_sdk_s3::types::QueueConfiguration) -> Value {
    let mut m = Map::new();
    if let Some(id) = q.id() {
        m.insert("Id".to_string(), Value::String(id.to_string()));
    }
    m.insert(
        "QueueArn".to_string(),
        Value::String(q.queue_arn().to_string()),
    );
    m.insert("Events".to_string(), serialize_events(q.events()));
    if let Some(f) = q.filter() {
        m.insert("Filter".to_string(), serialize_notification_filter(f));
    }
    Value::Object(m)
}

fn serialize_lambda_function_configuration(
    l: &aws_sdk_s3::types::LambdaFunctionConfiguration,
) -> Value {
    let mut m = Map::new();
    if let Some(id) = l.id() {
        m.insert("Id".to_string(), Value::String(id.to_string()));
    }
    m.insert(
        "LambdaFunctionArn".to_string(),
        Value::String(l.lambda_function_arn().to_string()),
    );
    m.insert("Events".to_string(), serialize_events(l.events()));
    if let Some(f) = l.filter() {
        m.insert("Filter".to_string(), serialize_notification_filter(f));
    }
    Value::Object(m)
}

fn serialize_events(events: &[aws_sdk_s3::types::Event]) -> Value {
    Value::Array(
        events
            .iter()
            .map(|e| Value::String(e.as_str().to_string()))
            .collect(),
    )
}

fn serialize_notification_filter(f: &aws_sdk_s3::types::NotificationConfigurationFilter) -> Value {
    let mut m = Map::new();
    if let Some(k) = f.key() {
        let mut km = Map::new();
        let rules: Vec<Value> = k
            .filter_rules()
            .iter()
            .map(|r| {
                let mut rm = Map::new();
                if let Some(name) = r.name() {
                    rm.insert("Name".to_string(), Value::String(name.as_str().to_string()));
                }
                if let Some(v) = r.value() {
                    rm.insert("Value".to_string(), Value::String(v.to_string()));
                }
                Value::Object(rm)
            })
            .collect();
        km.insert("FilterRules".to_string(), Value::Array(rules));
        m.insert("Key".to_string(), Value::Object(km));
    }
    Value::Object(m)
}

/// Serialise a `GetPublicAccessBlockOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"PublicAccessBlockConfiguration": { … }}`. Mirrors
/// `aws s3api get-public-access-block --output json`. Absent or empty
/// optional fields are omitted (never `null`).
pub fn get_public_access_block_to_json(out: &GetPublicAccessBlockOutput) -> Value {
    let mut top = Map::new();
    if let Some(c) = out.public_access_block_configuration() {
        let mut inner = Map::new();
        if let Some(v) = c.block_public_acls() {
            inner.insert("BlockPublicAcls".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.ignore_public_acls() {
            inner.insert("IgnorePublicAcls".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.block_public_policy() {
            inner.insert("BlockPublicPolicy".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.restrict_public_buckets() {
            inner.insert("RestrictPublicBuckets".to_string(), Value::Bool(v));
        }
        top.insert(
            "PublicAccessBlockConfiguration".to_string(),
            Value::Object(inner),
        );
    }
    Value::Object(top)
}

/// Serialise a `GetBucketReplicationOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"ReplicationConfiguration": { "Role": "...", "Rules": [...] }}`.
/// When the bucket has no replication configuration, the API returns an
/// `ReplicationConfigurationNotFoundError` (mapped to NotFound by the
/// caller) — so this function never emits an empty `{}`; it always wraps
/// a present configuration. Absent or empty optional fields are omitted.
pub fn get_bucket_replication_to_json(out: &GetBucketReplicationOutput) -> Value {
    let mut top = Map::new();
    if let Some(cfg) = out.replication_configuration() {
        let mut inner = Map::new();
        inner.insert("Role".to_string(), Value::String(cfg.role().to_string()));
        let rules: Vec<Value> = cfg.rules().iter().map(serialize_replication_rule).collect();
        inner.insert("Rules".to_string(), Value::Array(rules));
        top.insert("ReplicationConfiguration".to_string(), Value::Object(inner));
    }
    Value::Object(top)
}

fn serialize_replication_rule(r: &aws_sdk_s3::types::ReplicationRule) -> Value {
    let mut m = Map::new();
    if let Some(id) = r.id() {
        m.insert("ID".to_string(), Value::String(id.to_string()));
    }
    if let Some(p) = r.priority() {
        m.insert(
            "Priority".to_string(),
            Value::Number(serde_json::Number::from(p)),
        );
    }
    #[allow(deprecated)]
    if let Some(prefix) = r.prefix() {
        m.insert("Prefix".to_string(), Value::String(prefix.to_string()));
    }
    if let Some(f) = r.filter() {
        m.insert("Filter".to_string(), serialize_replication_filter(f));
    }
    m.insert(
        "Status".to_string(),
        Value::String(r.status().as_str().to_string()),
    );
    if let Some(ssc) = r.source_selection_criteria() {
        m.insert(
            "SourceSelectionCriteria".to_string(),
            serialize_source_selection_criteria(ssc),
        );
    }
    if let Some(eor) = r.existing_object_replication() {
        let mut em = Map::new();
        em.insert(
            "Status".to_string(),
            Value::String(eor.status().as_str().to_string()),
        );
        m.insert("ExistingObjectReplication".to_string(), Value::Object(em));
    }
    if let Some(d) = r.destination() {
        m.insert("Destination".to_string(), serialize_destination(d));
    }
    if let Some(dmr) = r.delete_marker_replication() {
        let mut dm = Map::new();
        if let Some(s) = dmr.status() {
            dm.insert("Status".to_string(), Value::String(s.as_str().to_string()));
        }
        m.insert("DeleteMarkerReplication".to_string(), Value::Object(dm));
    }
    Value::Object(m)
}

fn serialize_replication_filter(f: &aws_sdk_s3::types::ReplicationRuleFilter) -> Value {
    let mut m = Map::new();
    if let Some(prefix) = f.prefix() {
        m.insert("Prefix".to_string(), Value::String(prefix.to_string()));
    }
    if let Some(t) = f.tag() {
        let mut tm = Map::new();
        tm.insert("Key".to_string(), Value::String(t.key().to_string()));
        tm.insert("Value".to_string(), Value::String(t.value().to_string()));
        m.insert("Tag".to_string(), Value::Object(tm));
    }
    if let Some(a) = f.and() {
        let mut am = Map::new();
        if let Some(prefix) = a.prefix() {
            am.insert("Prefix".to_string(), Value::String(prefix.to_string()));
        }
        if !a.tags().is_empty() {
            let tags: Vec<Value> = a
                .tags()
                .iter()
                .map(|t| {
                    let mut tm = Map::new();
                    tm.insert("Key".to_string(), Value::String(t.key().to_string()));
                    tm.insert("Value".to_string(), Value::String(t.value().to_string()));
                    Value::Object(tm)
                })
                .collect();
            am.insert("Tags".to_string(), Value::Array(tags));
        }
        m.insert("And".to_string(), Value::Object(am));
    }
    Value::Object(m)
}

fn serialize_source_selection_criteria(s: &aws_sdk_s3::types::SourceSelectionCriteria) -> Value {
    let mut m = Map::new();
    if let Some(sse) = s.sse_kms_encrypted_objects() {
        let mut em = Map::new();
        em.insert(
            "Status".to_string(),
            Value::String(sse.status().as_str().to_string()),
        );
        m.insert("SseKmsEncryptedObjects".to_string(), Value::Object(em));
    }
    if let Some(rm) = s.replica_modifications() {
        let mut em = Map::new();
        em.insert(
            "Status".to_string(),
            Value::String(rm.status().as_str().to_string()),
        );
        m.insert("ReplicaModifications".to_string(), Value::Object(em));
    }
    Value::Object(m)
}

fn serialize_destination(d: &aws_sdk_s3::types::Destination) -> Value {
    let mut m = Map::new();
    m.insert("Bucket".to_string(), Value::String(d.bucket().to_string()));
    if let Some(a) = d.account() {
        m.insert("Account".to_string(), Value::String(a.to_string()));
    }
    if let Some(sc) = d.storage_class() {
        m.insert(
            "StorageClass".to_string(),
            Value::String(sc.as_str().to_string()),
        );
    }
    if let Some(act) = d.access_control_translation() {
        let mut am = Map::new();
        am.insert(
            "Owner".to_string(),
            Value::String(act.owner().as_str().to_string()),
        );
        m.insert("AccessControlTranslation".to_string(), Value::Object(am));
    }
    if let Some(ec) = d.encryption_configuration() {
        let mut em = Map::new();
        if let Some(k) = ec.replica_kms_key_id() {
            em.insert("ReplicaKmsKeyID".to_string(), Value::String(k.to_string()));
        }
        m.insert("EncryptionConfiguration".to_string(), Value::Object(em));
    }
    if let Some(rt) = d.replication_time() {
        let mut rm = Map::new();
        rm.insert(
            "Status".to_string(),
            Value::String(rt.status().as_str().to_string()),
        );
        if let Some(t) = rt.time() {
            let mut tm = Map::new();
            if let Some(min) = t.minutes() {
                tm.insert(
                    "Minutes".to_string(),
                    Value::Number(serde_json::Number::from(min)),
                );
            }
            rm.insert("Time".to_string(), Value::Object(tm));
        }
        m.insert("ReplicationTime".to_string(), Value::Object(rm));
    }
    if let Some(metrics) = d.metrics() {
        let mut mm = Map::new();
        mm.insert(
            "Status".to_string(),
            Value::String(metrics.status().as_str().to_string()),
        );
        if let Some(et) = metrics.event_threshold() {
            let mut tm = Map::new();
            if let Some(min) = et.minutes() {
                tm.insert(
                    "Minutes".to_string(),
                    Value::Number(serde_json::Number::from(min)),
                );
            }
            mm.insert("EventThreshold".to_string(), Value::Object(tm));
        }
        m.insert("Metrics".to_string(), Value::Object(mm));
    }
    Value::Object(m)
}

/// Serialise a `GetBucketAccelerateConfigurationOutput` to AWS CLI v2
/// `--output json` shape.
///
/// Mirrors `aws s3api get-bucket-accelerate-configuration --output json`:
/// emits `{"Status": "Enabled"|"Suspended"}` when configured, or `{}` when
/// acceleration has never been set on the bucket (S3 returns success with
/// an empty body in that case — there is no per-resource NotFound code).
/// `RequestCharged` is included when the SDK surfaces it (Requester Pays
/// buckets where the caller paid for the request).
pub fn get_bucket_accelerate_configuration_to_json(
    out: &GetBucketAccelerateConfigurationOutput,
) -> Value {
    let mut map = Map::new();
    if let Some(status) = out.status() {
        map.insert(
            "Status".to_string(),
            Value::String(status.as_str().to_string()),
        );
    }
    if let Some(rc) = out.request_charged() {
        map.insert(
            "RequestCharged".to_string(),
            Value::String(rc.as_str().to_string()),
        );
    }
    Value::Object(map)
}

/// Serialise a `GetBucketRequestPaymentOutput` to AWS CLI v2 `--output json`
/// shape.
///
/// Emits `{"Payer": "Requester"|"BucketOwner"}`. The API always returns a
/// payer value (defaults to `BucketOwner` for new buckets).
pub fn get_bucket_request_payment_to_json(out: &GetBucketRequestPaymentOutput) -> Value {
    let mut map = Map::new();
    if let Some(p) = out.payer() {
        map.insert("Payer".to_string(), Value::String(p.as_str().to_string()));
    }
    Value::Object(map)
}

/// Serialise a `GetBucketPolicyStatusOutput` to AWS CLI v2 `--output json`
/// shape.
///
/// Top level: `{"PolicyStatus": {"IsPublic": true|false}}`. Mirrors
/// `aws s3api get-bucket-policy-status --output json`.
pub fn get_bucket_policy_status_to_json(out: &GetBucketPolicyStatusOutput) -> Value {
    let mut top = Map::new();
    if let Some(ps) = out.policy_status() {
        let mut inner = Map::new();
        if let Some(b) = ps.is_public() {
            inner.insert("IsPublic".to_string(), Value::Bool(b));
        }
        top.insert("PolicyStatus".to_string(), Value::Object(inner));
    }
    Value::Object(top)
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
    fn head_object_empty_output_yields_only_metadata_key() {
        // AWS CLI v2 always emits `Metadata` (as `{}` when no user metadata is
        // set), so an otherwise-empty HeadObjectOutput maps to `{"Metadata": {}}`.
        let out = HeadObjectOutput::builder().build();
        let json = head_object_to_json(&out);
        let mut expected = Map::new();
        expected.insert("Metadata".to_string(), Value::Object(Map::new()));
        assert_eq!(json, Value::Object(expected));
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
    fn head_bucket_with_bucket_arn() {
        let out = HeadBucketOutput::builder()
            .bucket_arn("arn:aws:s3:::data.cpp17.org")
            .bucket_region("ap-northeast-1")
            .access_point_alias(false)
            .build();
        let json = head_bucket_to_json(&out);
        assert_eq!(
            json["BucketArn"],
            Value::String("arn:aws:s3:::data.cpp17.org".into())
        );
        assert_eq!(json["BucketRegion"], Value::String("ap-northeast-1".into()));
        assert_eq!(json["AccessPointAlias"], Value::Bool(false));
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
    fn head_object_with_expires_string_only() {
        // Header was unparseable: SDK populates `expires_string` only; we
        // emit `ExpiresString` and omit `Expires`.
        let out = HeadObjectOutput::builder()
            .expires_string("Wed, 21 Oct 2026 07:28:00 GMT")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ExpiresString"],
            Value::String("Wed, 21 Oct 2026 07:28:00 GMT".into())
        );
        assert!(json.get("Expires").is_none());
    }

    #[test]
    fn head_object_with_expires_and_expires_string() {
        // Header parsed cleanly: AWS CLI v2 emits both `Expires` (ISO-8601)
        // and `ExpiresString` (raw HTTP-date).
        #[allow(deprecated)]
        let out = HeadObjectOutput::builder()
            .expires(
                aws_smithy_types::DateTime::from_str(
                    "2026-10-21T07:28:00Z",
                    aws_smithy_types::date_time::Format::DateTime,
                )
                .unwrap(),
            )
            .expires_string("Wed, 21 Oct 2026 07:28:00 GMT")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["Expires"],
            Value::String("2026-10-21T07:28:00+00:00".into())
        );
        assert_eq!(
            json["ExpiresString"],
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
    fn head_object_with_empty_metadata_emits_empty_object() {
        // AWS CLI v2 always emits `Metadata`, even as `{}` when no user
        // metadata is set. The serialiser must do the same.
        let out = HeadObjectOutput::builder().build();
        let json = head_object_to_json(&out);
        assert_eq!(json["Metadata"], Value::Object(Map::new()));
    }

    #[test]
    fn head_object_with_tag_count() {
        let out = HeadObjectOutput::builder().tag_count(2).build();
        let json = head_object_to_json(&out);
        assert_eq!(json["TagCount"], Value::Number(serde_json::Number::from(2)));
    }

    #[test]
    fn head_object_with_sse_customer_fields() {
        let out = HeadObjectOutput::builder()
            .sse_customer_algorithm("AES256")
            .sse_customer_key_md5("md5digest==")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(json["SSECustomerAlgorithm"], Value::String("AES256".into()));
        assert_eq!(
            json["SSECustomerKeyMD5"],
            Value::String("md5digest==".into())
        );
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

    /// Pin the exact byte format of timestamp output to match
    /// `aws s3api ... --output json` (AWS CLI v2): UTC, second precision,
    /// `+00:00` offset. If chrono's RFC3339 default ever changes (e.g. to
    /// `Z`), this test catches it before the format drift reaches users.
    #[test]
    fn head_object_last_modified_matches_aws_cli_v2_format() {
        use aws_sdk_s3::primitives::DateTime;
        // 1_700_000_000 epoch seconds == 2023-11-14T22:13:20Z
        let dt = DateTime::from_secs(1_700_000_000);
        let out = HeadObjectOutput::builder().last_modified(dt).build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["LastModified"],
            Value::String("2023-11-14T22:13:20+00:00".into()),
            "LastModified must match the AWS CLI v2 byte format"
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

    // ----- get_bucket_lifecycle_configuration_to_json -----

    #[test]
    fn get_bucket_lifecycle_configuration_empty_rules_yields_empty_array() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .set_rules(Some(vec![]))
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(json["Rules"], Value::Array(vec![]));
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_minimal_rule() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule};
        let rule = LifecycleRule::builder()
            .id("r1")
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let rules = json["Rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["ID"], Value::String("r1".into()));
        assert_eq!(rules[0]["Status"], Value::String("Enabled".into()));
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_filter_prefix() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
        let filter = LifecycleRuleFilter::builder().prefix("logs/").build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Filter"]["Prefix"],
            Value::String("logs/".into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_expiration_days() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleExpiration, LifecycleRule};
        let exp = LifecycleExpiration::builder().days(365).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .expiration(exp)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Expiration"]["Days"],
            Value::Number(365i32.into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_transitions() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, Transition, TransitionStorageClass,
        };
        let t = Transition::builder()
            .days(30)
            .storage_class(TransitionStorageClass::Glacier)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .transitions(t)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Transitions"][0]["StorageClass"],
            Value::String("GLACIER".into())
        );
    }

    // ----- get_bucket_encryption_to_json -----

    #[test]
    fn get_bucket_encryption_empty_output_yields_empty_object() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        let out = GetBucketEncryptionOutput::builder().build();
        let json = get_bucket_encryption_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_encryption_with_aes256_rule() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let inner = &json["ServerSideEncryptionConfiguration"];
        assert_eq!(
            inner["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"],
            Value::String("AES256".into())
        );
    }

    #[test]
    fn get_bucket_encryption_with_kms_rule_includes_key_id_and_bucket_key() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::AwsKms)
            .kms_master_key_id("arn:aws:kms:us-east-1:111111111111:key/abc")
            .build()
            .unwrap();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .bucket_key_enabled(true)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let inner = &json["ServerSideEncryptionConfiguration"];
        assert_eq!(
            inner["Rules"][0]["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"],
            Value::String("arn:aws:kms:us-east-1:111111111111:key/abc".into())
        );
        assert_eq!(inner["Rules"][0]["BucketKeyEnabled"], Value::Bool(true));
    }

    // ----- get_bucket_cors_to_json -----

    #[test]
    fn get_bucket_cors_empty_yields_empty_array() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        let out = GetBucketCorsOutput::builder()
            .set_cors_rules(Some(vec![]))
            .build();
        let json = get_bucket_cors_to_json(&out);
        assert_eq!(json["CORSRules"], Value::Array(vec![]));
    }

    #[test]
    fn get_bucket_cors_with_typical_rule() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        use aws_sdk_s3::types::CorsRule;
        let r = CorsRule::builder()
            .id("r1")
            .allowed_methods("GET")
            .allowed_methods("HEAD")
            .allowed_origins("*")
            .max_age_seconds(3000)
            .build()
            .unwrap();
        let out = GetBucketCorsOutput::builder().cors_rules(r).build();
        let json = get_bucket_cors_to_json(&out);
        let r0 = &json["CORSRules"][0];
        assert_eq!(r0["ID"], Value::String("r1".into()));
        assert_eq!(
            r0["AllowedMethods"],
            Value::Array(vec!["GET".into(), "HEAD".into()])
        );
        assert_eq!(r0["AllowedOrigins"], Value::Array(vec!["*".into()]));
        assert_eq!(r0["MaxAgeSeconds"], Value::Number(3000i32.into()));
    }

    #[test]
    fn get_bucket_cors_with_allowed_and_expose_headers() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        use aws_sdk_s3::types::CorsRule;
        let r = CorsRule::builder()
            .allowed_methods("GET")
            .allowed_origins("*")
            .allowed_headers("*")
            .expose_headers("x-amz-id-2")
            .build()
            .unwrap();
        let out = GetBucketCorsOutput::builder().cors_rules(r).build();
        let json = get_bucket_cors_to_json(&out);
        assert_eq!(
            json["CORSRules"][0]["AllowedHeaders"],
            Value::Array(vec!["*".into()])
        );
        assert_eq!(
            json["CORSRules"][0]["ExposeHeaders"],
            Value::Array(vec!["x-amz-id-2".into()])
        );
    }

    // ----- get_public_access_block_to_json -----

    #[test]
    fn get_pab_empty_output_yields_empty_object() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        let out = GetPublicAccessBlockOutput::builder().build();
        let json = get_public_access_block_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_pab_all_true() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        use aws_sdk_s3::types::PublicAccessBlockConfiguration;
        let cfg = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(true)
            .build();
        let out = GetPublicAccessBlockOutput::builder()
            .public_access_block_configuration(cfg)
            .build();
        let json = get_public_access_block_to_json(&out);
        let inner = &json["PublicAccessBlockConfiguration"];
        assert_eq!(inner["BlockPublicAcls"], Value::Bool(true));
        assert_eq!(inner["IgnorePublicAcls"], Value::Bool(true));
        assert_eq!(inner["BlockPublicPolicy"], Value::Bool(true));
        assert_eq!(inner["RestrictPublicBuckets"], Value::Bool(true));
    }

    #[test]
    fn get_pab_partial_fields() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        use aws_sdk_s3::types::PublicAccessBlockConfiguration;
        let cfg = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        let out = GetPublicAccessBlockOutput::builder()
            .public_access_block_configuration(cfg)
            .build();
        let json = get_public_access_block_to_json(&out);
        let inner = &json["PublicAccessBlockConfiguration"];
        assert_eq!(inner["BlockPublicAcls"], Value::Bool(true));
        assert!(inner.get("IgnorePublicAcls").is_none());
    }

    // ----- get_bucket_website_to_json -----

    #[test]
    fn get_bucket_website_empty_yields_empty_object() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        let out = GetBucketWebsiteOutput::builder().build();
        let json = get_bucket_website_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_website_with_index_document_only() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::IndexDocument;
        let ix = IndexDocument::builder()
            .suffix("index.html")
            .build()
            .unwrap();
        let out = GetBucketWebsiteOutput::builder().index_document(ix).build();
        let json = get_bucket_website_to_json(&out);
        assert_eq!(
            json["IndexDocument"]["Suffix"],
            Value::String("index.html".into())
        );
        assert!(json.get("ErrorDocument").is_none());
        assert!(json.get("RedirectAllRequestsTo").is_none());
        assert!(json.get("RoutingRules").is_none());
    }

    #[test]
    fn get_bucket_website_with_index_and_error_documents() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::{ErrorDocument, IndexDocument};
        let ix = IndexDocument::builder()
            .suffix("index.html")
            .build()
            .unwrap();
        let er = ErrorDocument::builder().key("error.html").build().unwrap();
        let out = GetBucketWebsiteOutput::builder()
            .index_document(ix)
            .error_document(er)
            .build();
        let json = get_bucket_website_to_json(&out);
        assert_eq!(
            json["IndexDocument"]["Suffix"],
            Value::String("index.html".into())
        );
        assert_eq!(
            json["ErrorDocument"]["Key"],
            Value::String("error.html".into())
        );
    }

    #[test]
    fn get_bucket_website_with_redirect_all_requests_to() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::{Protocol, RedirectAllRequestsTo};
        let r = RedirectAllRequestsTo::builder()
            .host_name("example.com")
            .protocol(Protocol::Https)
            .build()
            .unwrap();
        let out = GetBucketWebsiteOutput::builder()
            .redirect_all_requests_to(r)
            .build();
        let json = get_bucket_website_to_json(&out);
        assert_eq!(
            json["RedirectAllRequestsTo"]["HostName"],
            Value::String("example.com".into())
        );
        assert_eq!(
            json["RedirectAllRequestsTo"]["Protocol"],
            Value::String("https".into())
        );
    }

    #[test]
    fn get_bucket_website_with_redirect_all_requests_to_omits_absent_protocol() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::RedirectAllRequestsTo;
        let r = RedirectAllRequestsTo::builder()
            .host_name("example.com")
            .build()
            .unwrap();
        let out = GetBucketWebsiteOutput::builder()
            .redirect_all_requests_to(r)
            .build();
        let json = get_bucket_website_to_json(&out);
        assert_eq!(
            json["RedirectAllRequestsTo"]["HostName"],
            Value::String("example.com".into())
        );
        assert!(json["RedirectAllRequestsTo"].get("Protocol").is_none());
    }

    #[test]
    fn get_bucket_website_with_routing_rule_condition_and_redirect() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::{Condition, Protocol, Redirect, RoutingRule};
        let cond = Condition::builder()
            .http_error_code_returned_equals("404")
            .key_prefix_equals("docs/")
            .build();
        let red = Redirect::builder()
            .host_name("new.example.com")
            .http_redirect_code("301")
            .protocol(Protocol::Https)
            .replace_key_prefix_with("documents/")
            .build();
        let rule = RoutingRule::builder().condition(cond).redirect(red).build();
        let out = GetBucketWebsiteOutput::builder()
            .routing_rules(rule)
            .build();
        let json = get_bucket_website_to_json(&out);
        let r0 = &json["RoutingRules"][0];
        assert_eq!(
            r0["Condition"]["HttpErrorCodeReturnedEquals"],
            Value::String("404".into())
        );
        assert_eq!(
            r0["Condition"]["KeyPrefixEquals"],
            Value::String("docs/".into())
        );
        assert_eq!(
            r0["Redirect"]["HostName"],
            Value::String("new.example.com".into())
        );
        assert_eq!(
            r0["Redirect"]["HttpRedirectCode"],
            Value::String("301".into())
        );
        assert_eq!(r0["Redirect"]["Protocol"], Value::String("https".into()));
        assert_eq!(
            r0["Redirect"]["ReplaceKeyPrefixWith"],
            Value::String("documents/".into())
        );
        assert!(r0["Redirect"].get("ReplaceKeyWith").is_none());
    }

    #[test]
    fn get_bucket_website_with_routing_rule_redirect_only_replace_key_with() {
        use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
        use aws_sdk_s3::types::{Redirect, RoutingRule};
        let red = Redirect::builder().replace_key_with("error.html").build();
        let rule = RoutingRule::builder().redirect(red).build();
        let out = GetBucketWebsiteOutput::builder()
            .routing_rules(rule)
            .build();
        let json = get_bucket_website_to_json(&out);
        let r0 = &json["RoutingRules"][0];
        assert!(r0.get("Condition").is_none());
        assert_eq!(
            r0["Redirect"]["ReplaceKeyWith"],
            Value::String("error.html".into())
        );
    }

    // ----- get_bucket_logging_to_json -----

    #[test]
    fn get_bucket_logging_empty_yields_empty_object() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        // S3 returns success with no LoggingEnabled when no logging is set.
        let out = GetBucketLoggingOutput::builder().build();
        let json = get_bucket_logging_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_logging_with_target_bucket_and_prefix() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::LoggingEnabled;
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        assert_eq!(
            json["LoggingEnabled"]["TargetBucket"],
            Value::String("log-bucket".into())
        );
        assert_eq!(
            json["LoggingEnabled"]["TargetPrefix"],
            Value::String("logs/".into())
        );
        assert!(
            json["LoggingEnabled"]
                .get("TargetObjectKeyFormat")
                .is_none()
        );
    }

    #[test]
    fn get_bucket_logging_with_simple_prefix_marker() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::{LoggingEnabled, SimplePrefix, TargetObjectKeyFormat};
        let fmt = TargetObjectKeyFormat::builder()
            .simple_prefix(SimplePrefix::builder().build())
            .build();
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .target_object_key_format(fmt)
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        assert_eq!(
            json["LoggingEnabled"]["TargetObjectKeyFormat"]["SimplePrefix"],
            Value::Object(Map::new())
        );
        assert!(
            json["LoggingEnabled"]["TargetObjectKeyFormat"]
                .get("PartitionedPrefix")
                .is_none()
        );
    }

    #[test]
    fn get_bucket_logging_with_partitioned_prefix_event_time() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::{
            LoggingEnabled, PartitionDateSource, PartitionedPrefix, TargetObjectKeyFormat,
        };
        let pp = PartitionedPrefix::builder()
            .partition_date_source(PartitionDateSource::EventTime)
            .build();
        let fmt = TargetObjectKeyFormat::builder()
            .partitioned_prefix(pp)
            .build();
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .target_object_key_format(fmt)
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        assert_eq!(
            json["LoggingEnabled"]["TargetObjectKeyFormat"]["PartitionedPrefix"]["PartitionDateSource"],
            Value::String("EventTime".into())
        );
    }

    #[test]
    fn get_bucket_logging_with_partitioned_prefix_without_date_source() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::{LoggingEnabled, PartitionedPrefix, TargetObjectKeyFormat};
        let pp = PartitionedPrefix::builder().build();
        let fmt = TargetObjectKeyFormat::builder()
            .partitioned_prefix(pp)
            .build();
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .target_object_key_format(fmt)
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        let pp_obj = &json["LoggingEnabled"]["TargetObjectKeyFormat"]["PartitionedPrefix"];
        assert!(pp_obj.is_object());
        assert!(pp_obj.get("PartitionDateSource").is_none());
    }

    // ----- get_bucket_notification_configuration_to_json -----

    #[test]
    fn get_bucket_notification_configuration_empty_yields_empty_object() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        // S3 returns success with no configurations when none are set.
        let out = GetBucketNotificationConfigurationOutput::builder().build();
        let json = get_bucket_notification_configuration_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_notification_configuration_with_topic() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{Event, TopicConfiguration};
        let topic = TopicConfiguration::builder()
            .id("t1")
            .topic_arn("arn:aws:sns:us-east-1:111111111111:topic-x")
            .events(Event::S3ObjectCreated)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .topic_configurations(topic)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let arr = &json["TopicConfigurations"];
        assert!(arr.is_array());
        assert_eq!(arr[0]["Id"], Value::String("t1".into()));
        assert_eq!(
            arr[0]["TopicArn"],
            Value::String("arn:aws:sns:us-east-1:111111111111:topic-x".into())
        );
        assert_eq!(
            arr[0]["Events"],
            Value::Array(vec![Value::String("s3:ObjectCreated:*".into())])
        );
        assert!(arr[0].get("Filter").is_none());
    }

    #[test]
    fn get_bucket_notification_configuration_with_queue() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{Event, QueueConfiguration};
        let queue = QueueConfiguration::builder()
            .queue_arn("arn:aws:sqs:us-east-1:111111111111:queue-x")
            .events(Event::S3ObjectRemovedDelete)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .queue_configurations(queue)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let arr = &json["QueueConfigurations"];
        assert_eq!(
            arr[0]["QueueArn"],
            Value::String("arn:aws:sqs:us-east-1:111111111111:queue-x".into())
        );
        assert_eq!(
            arr[0]["Events"],
            Value::Array(vec![Value::String("s3:ObjectRemoved:Delete".into())])
        );
    }

    #[test]
    fn get_bucket_notification_configuration_with_lambda() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{Event, LambdaFunctionConfiguration};
        let lambda = LambdaFunctionConfiguration::builder()
            .lambda_function_arn("arn:aws:lambda:us-east-1:111111111111:function:fn-x")
            .events(Event::S3ObjectCreatedPut)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .lambda_function_configurations(lambda)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let arr = &json["LambdaFunctionConfigurations"];
        assert_eq!(
            arr[0]["LambdaFunctionArn"],
            Value::String("arn:aws:lambda:us-east-1:111111111111:function:fn-x".into())
        );
    }

    #[test]
    fn get_bucket_notification_configuration_with_event_bridge_marker() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::EventBridgeConfiguration;
        let out = GetBucketNotificationConfigurationOutput::builder()
            .event_bridge_configuration(EventBridgeConfiguration::builder().build())
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        assert_eq!(json["EventBridgeConfiguration"], Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_notification_configuration_with_filter_prefix_and_suffix() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{
            Event, FilterRule, FilterRuleName, NotificationConfigurationFilter, S3KeyFilter,
            TopicConfiguration,
        };
        let prefix_rule = FilterRule::builder()
            .name(FilterRuleName::Prefix)
            .value("images/")
            .build();
        let suffix_rule = FilterRule::builder()
            .name(FilterRuleName::Suffix)
            .value(".jpg")
            .build();
        let key = S3KeyFilter::builder()
            .filter_rules(prefix_rule)
            .filter_rules(suffix_rule)
            .build();
        let filter = NotificationConfigurationFilter::builder().key(key).build();
        let topic = TopicConfiguration::builder()
            .topic_arn("arn:aws:sns:us-east-1:111111111111:topic-x")
            .events(Event::S3ObjectCreated)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .topic_configurations(topic)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let rules = &json["TopicConfigurations"][0]["Filter"]["Key"]["FilterRules"];
        assert!(rules.is_array());
        assert_eq!(rules[0]["Name"], Value::String("prefix".into()));
        assert_eq!(rules[0]["Value"], Value::String("images/".into()));
        assert_eq!(rules[1]["Name"], Value::String("suffix".into()));
        assert_eq!(rules[1]["Value"], Value::String(".jpg".into()));
    }

    #[test]
    fn get_bucket_notification_configuration_omits_empty_optional_fields() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        // SDK distinguishes Some(vec![]) from None. With every field None,
        // every key must be absent (matches AWS-CLI emitting `{}`).
        let out = GetBucketNotificationConfigurationOutput::builder().build();
        let json = get_bucket_notification_configuration_to_json(&out);
        assert!(json.get("TopicConfigurations").is_none());
        assert!(json.get("QueueConfigurations").is_none());
        assert!(json.get("LambdaFunctionConfigurations").is_none());
        assert!(json.get("EventBridgeConfiguration").is_none());
    }

    // ----- get_bucket_lifecycle_configuration_to_json additional branches -----

    #[test]
    fn get_bucket_lifecycle_with_deprecated_prefix_field_emits_it() {
        // The `Prefix` field directly on a rule is the deprecated S3 form
        // we still emit for AWS-CLI output-shape parity.
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule};
        #[allow(deprecated)]
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .prefix("legacy/")
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(json["Rules"][0]["Prefix"], Value::String("legacy/".into()));
    }

    #[test]
    fn get_bucket_lifecycle_with_noncurrent_version_expiration_emits_it() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, NoncurrentVersionExpiration};
        let nve = NoncurrentVersionExpiration::builder()
            .noncurrent_days(7)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_expiration(nve)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let nve_json = &json["Rules"][0]["NoncurrentVersionExpiration"];
        assert_eq!(nve_json["NoncurrentDays"], Value::Number(7i32.into()));
    }

    #[test]
    fn get_bucket_lifecycle_with_noncurrent_version_transitions_emits_them() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, NoncurrentVersionTransition, TransitionStorageClass,
        };
        let nvt = NoncurrentVersionTransition::builder()
            .noncurrent_days(15)
            .storage_class(TransitionStorageClass::Glacier)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_transitions(nvt)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let arr = json["Rules"][0]["NoncurrentVersionTransitions"]
            .as_array()
            .expect("NoncurrentVersionTransitions must be array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["NoncurrentDays"], Value::Number(15i32.into()));
        assert_eq!(arr[0]["StorageClass"], Value::String("GLACIER".into()));
    }

    #[test]
    fn get_bucket_lifecycle_with_abort_incomplete_multipart_upload_emits_it() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{AbortIncompleteMultipartUpload, ExpirationStatus, LifecycleRule};
        let aimu = AbortIncompleteMultipartUpload::builder()
            .days_after_initiation(3)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .abort_incomplete_multipart_upload(aimu)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"],
            Value::Number(3i32.into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_filter_with_tag_emits_key_value() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter, Tag};
        let tag = Tag::builder()
            .key("team")
            .value("platform")
            .build()
            .unwrap();
        let filter = LifecycleRuleFilter::builder().tag(tag).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let tag_obj = &json["Rules"][0]["Filter"]["Tag"];
        assert_eq!(tag_obj["Key"], Value::String("team".into()));
        assert_eq!(tag_obj["Value"], Value::String("platform".into()));
    }

    #[test]
    fn get_bucket_lifecycle_filter_with_and_emits_prefix_and_tags() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter, Tag,
        };
        let t1 = Tag::builder().key("a").value("1").build().unwrap();
        let t2 = Tag::builder().key("b").value("2").build().unwrap();
        let and = LifecycleRuleAndOperator::builder()
            .prefix("docs/")
            .tags(t1)
            .tags(t2)
            .build();
        let filter = LifecycleRuleFilter::builder().and(and).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let and_obj = &json["Rules"][0]["Filter"]["And"];
        assert_eq!(and_obj["Prefix"], Value::String("docs/".into()));
        let tags = and_obj["Tags"].as_array().expect("Tags must be array");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0]["Key"], Value::String("a".into()));
        assert_eq!(tags[1]["Key"], Value::String("b".into()));
    }

    #[test]
    fn get_bucket_lifecycle_filter_with_and_omits_tags_when_empty() {
        // The serializer should omit "Tags" entirely when the And operator
        // has no tags (just a prefix). Exercises the empty-tags branch.
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
        };
        let and = LifecycleRuleAndOperator::builder().prefix("docs/").build();
        let filter = LifecycleRuleFilter::builder().and(and).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let and_obj = &json["Rules"][0]["Filter"]["And"];
        assert_eq!(and_obj["Prefix"], Value::String("docs/".into()));
        assert!(and_obj.get("Tags").is_none());
    }

    #[test]
    fn get_bucket_lifecycle_expiration_with_date_emits_rfc3339() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::primitives::DateTime;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleExpiration, LifecycleRule};
        // 1_700_000_000 epoch seconds == 2023-11-14T22:13:20Z
        let dt = DateTime::from_secs(1_700_000_000);
        let exp = LifecycleExpiration::builder().date(dt).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .expiration(exp)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Expiration"]["Date"],
            Value::String("2023-11-14T22:13:20+00:00".into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_expiration_with_expired_object_delete_marker_emits_it() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleExpiration, LifecycleRule};
        let exp = LifecycleExpiration::builder()
            .expired_object_delete_marker(true)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .expiration(exp)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Expiration"]["ExpiredObjectDeleteMarker"],
            Value::Bool(true)
        );
    }

    #[test]
    fn get_bucket_lifecycle_transition_with_date_emits_rfc3339() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::primitives::DateTime;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, Transition, TransitionStorageClass,
        };
        let dt = DateTime::from_secs(1_700_000_000);
        let t = Transition::builder()
            .date(dt)
            .storage_class(TransitionStorageClass::Glacier)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .transitions(t)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let t0 = &json["Rules"][0]["Transitions"][0];
        assert_eq!(
            t0["Date"],
            Value::String("2023-11-14T22:13:20+00:00".into())
        );
        assert_eq!(t0["StorageClass"], Value::String("GLACIER".into()));
    }

    // ----- serialize_queue_configuration / serialize_lambda_function_configuration -----

    #[test]
    fn get_bucket_notification_queue_with_id_and_filter_emits_them() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{
            Event, FilterRule, FilterRuleName, NotificationConfigurationFilter, QueueConfiguration,
            S3KeyFilter,
        };
        let prefix_rule = FilterRule::builder()
            .name(FilterRuleName::Prefix)
            .value("a/")
            .build();
        let key = S3KeyFilter::builder().filter_rules(prefix_rule).build();
        let filter = NotificationConfigurationFilter::builder().key(key).build();
        let queue = QueueConfiguration::builder()
            .id("q1")
            .queue_arn("arn:aws:sqs:us-east-1:111111111111:queue-x")
            .events(Event::S3ObjectCreated)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .queue_configurations(queue)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let q0 = &json["QueueConfigurations"][0];
        assert_eq!(q0["Id"], Value::String("q1".into()));
        let rules = &q0["Filter"]["Key"]["FilterRules"];
        assert!(rules.is_array());
        assert_eq!(rules[0]["Name"], Value::String("prefix".into()));
    }

    #[test]
    fn get_bucket_notification_lambda_with_id_and_filter_emits_them() {
        use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
        use aws_sdk_s3::types::{
            Event, FilterRule, FilterRuleName, LambdaFunctionConfiguration,
            NotificationConfigurationFilter, S3KeyFilter,
        };
        let suffix_rule = FilterRule::builder()
            .name(FilterRuleName::Suffix)
            .value(".jpg")
            .build();
        let key = S3KeyFilter::builder().filter_rules(suffix_rule).build();
        let filter = NotificationConfigurationFilter::builder().key(key).build();
        let lambda = LambdaFunctionConfiguration::builder()
            .id("l1")
            .lambda_function_arn("arn:aws:lambda:us-east-1:111111111111:function:fn-x")
            .events(Event::S3ObjectCreatedPut)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketNotificationConfigurationOutput::builder()
            .lambda_function_configurations(lambda)
            .build();
        let json = get_bucket_notification_configuration_to_json(&out);
        let l0 = &json["LambdaFunctionConfigurations"][0];
        assert_eq!(l0["Id"], Value::String("l1".into()));
        let rules = &l0["Filter"]["Key"]["FilterRules"];
        assert!(rules.is_array());
        assert_eq!(rules[0]["Name"], Value::String("suffix".into()));
    }

    // ----- get_bucket_replication_to_json tests -----

    #[test]
    fn get_bucket_replication_absent_yields_empty_object() {
        let out = GetBucketReplicationOutput::builder().build();
        let json = get_bucket_replication_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_replication_minimal_emits_role_and_rules() {
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::dest-bucket")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/replication")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let inner = &json["ReplicationConfiguration"];
        assert_eq!(
            inner["Role"],
            Value::String("arn:aws:iam::111111111111:role/replication".into())
        );
        let rules = inner["Rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["Status"], Value::String("Enabled".into()));
        assert_eq!(
            rules[0]["Destination"]["Bucket"],
            Value::String("arn:aws:s3:::dest-bucket".into())
        );
    }

    #[test]
    fn get_bucket_replication_rule_with_id_priority_filter_prefix() {
        let filter = aws_sdk_s3::types::ReplicationRuleFilter::builder()
            .prefix("logs/")
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::dest-bucket")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .id("rule-1")
            .priority(2)
            .filter(filter)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Disabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let r = &json["ReplicationConfiguration"]["Rules"][0];
        assert_eq!(r["ID"], Value::String("rule-1".into()));
        assert_eq!(r["Priority"], Value::Number(serde_json::Number::from(2)));
        assert_eq!(r["Filter"]["Prefix"], Value::String("logs/".into()));
        assert_eq!(r["Status"], Value::String("Disabled".into()));
    }

    #[test]
    fn get_bucket_replication_filter_with_tag_emits_key_value() {
        let tag = Tag::builder().key("env").value("prod").build().unwrap();
        let filter = aws_sdk_s3::types::ReplicationRuleFilter::builder()
            .tag(tag)
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .filter(filter)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let f = &json["ReplicationConfiguration"]["Rules"][0]["Filter"]["Tag"];
        assert_eq!(f["Key"], Value::String("env".into()));
        assert_eq!(f["Value"], Value::String("prod".into()));
    }

    #[test]
    fn get_bucket_replication_filter_with_and_emits_prefix_and_tags() {
        let tag = Tag::builder().key("a").value("1").build().unwrap();
        let and = aws_sdk_s3::types::ReplicationRuleAndOperator::builder()
            .prefix("p/")
            .tags(tag)
            .build();
        let filter = aws_sdk_s3::types::ReplicationRuleFilter::builder()
            .and(and)
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .filter(filter)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let and_ = &json["ReplicationConfiguration"]["Rules"][0]["Filter"]["And"];
        assert_eq!(and_["Prefix"], Value::String("p/".into()));
        let tags = and_["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], Value::String("a".into()));
    }

    #[test]
    fn get_bucket_replication_filter_with_and_omits_tags_when_empty() {
        let and = aws_sdk_s3::types::ReplicationRuleAndOperator::builder()
            .prefix("p/")
            .build();
        let filter = aws_sdk_s3::types::ReplicationRuleFilter::builder()
            .and(and)
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .filter(filter)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let and_ = &json["ReplicationConfiguration"]["Rules"][0]["Filter"]["And"];
        assert!(
            and_.get("Tags").is_none(),
            "empty Tags should be omitted: {and_:?}"
        );
    }

    #[test]
    fn get_bucket_replication_with_destination_options() {
        let enc = aws_sdk_s3::types::EncryptionConfiguration::builder()
            .replica_kms_key_id("arn:aws:kms:us-east-1:1:key/abc")
            .build();
        let act = aws_sdk_s3::types::AccessControlTranslation::builder()
            .owner(aws_sdk_s3::types::OwnerOverride::Destination)
            .build()
            .unwrap();
        let rt = aws_sdk_s3::types::ReplicationTime::builder()
            .status(aws_sdk_s3::types::ReplicationTimeStatus::Enabled)
            .time(
                aws_sdk_s3::types::ReplicationTimeValue::builder()
                    .minutes(15)
                    .build(),
            )
            .build()
            .unwrap();
        let metrics = aws_sdk_s3::types::Metrics::builder()
            .status(aws_sdk_s3::types::MetricsStatus::Enabled)
            .event_threshold(
                aws_sdk_s3::types::ReplicationTimeValue::builder()
                    .minutes(15)
                    .build(),
            )
            .build()
            .unwrap();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .account("222222222222")
            .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
            .access_control_translation(act)
            .encryption_configuration(enc)
            .replication_time(rt)
            .metrics(metrics)
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let d = &json["ReplicationConfiguration"]["Rules"][0]["Destination"];
        assert_eq!(d["Account"], Value::String("222222222222".into()));
        assert_eq!(d["StorageClass"], Value::String("STANDARD_IA".into()));
        assert_eq!(
            d["AccessControlTranslation"]["Owner"],
            Value::String("Destination".into())
        );
        assert_eq!(
            d["EncryptionConfiguration"]["ReplicaKmsKeyID"],
            Value::String("arn:aws:kms:us-east-1:1:key/abc".into())
        );
        assert_eq!(
            d["ReplicationTime"]["Status"],
            Value::String("Enabled".into())
        );
        assert_eq!(
            d["ReplicationTime"]["Time"]["Minutes"],
            Value::Number(serde_json::Number::from(15))
        );
        assert_eq!(d["Metrics"]["Status"], Value::String("Enabled".into()));
        assert_eq!(
            d["Metrics"]["EventThreshold"]["Minutes"],
            Value::Number(serde_json::Number::from(15))
        );
    }

    #[test]
    fn get_bucket_replication_emits_time_wrapper_when_minutes_absent() {
        // ReplicationTime.Time and Metrics.EventThreshold are containers and
        // must be emitted whenever the SDK populated them, even if the inner
        // Minutes field is unexpectedly absent.
        let rt = aws_sdk_s3::types::ReplicationTime::builder()
            .status(aws_sdk_s3::types::ReplicationTimeStatus::Enabled)
            .time(aws_sdk_s3::types::ReplicationTimeValue::builder().build())
            .build()
            .unwrap();
        let metrics = aws_sdk_s3::types::Metrics::builder()
            .status(aws_sdk_s3::types::MetricsStatus::Enabled)
            .event_threshold(aws_sdk_s3::types::ReplicationTimeValue::builder().build())
            .build()
            .unwrap();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .replication_time(rt)
            .metrics(metrics)
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let d = &json["ReplicationConfiguration"]["Rules"][0]["Destination"];
        assert!(
            d["ReplicationTime"]["Time"].is_object(),
            "Time wrapper must be emitted even when Minutes is None"
        );
        assert!(d["ReplicationTime"]["Time"].get("Minutes").is_none());
        assert!(
            d["Metrics"]["EventThreshold"].is_object(),
            "EventThreshold wrapper must be emitted even when Minutes is None"
        );
        assert!(d["Metrics"]["EventThreshold"].get("Minutes").is_none());
    }

    #[test]
    fn get_bucket_replication_with_source_selection_criteria() {
        let sse = aws_sdk_s3::types::SseKmsEncryptedObjects::builder()
            .status(aws_sdk_s3::types::SseKmsEncryptedObjectsStatus::Enabled)
            .build()
            .unwrap();
        let rmod = aws_sdk_s3::types::ReplicaModifications::builder()
            .status(aws_sdk_s3::types::ReplicaModificationsStatus::Enabled)
            .build()
            .unwrap();
        let ssc = aws_sdk_s3::types::SourceSelectionCriteria::builder()
            .sse_kms_encrypted_objects(sse)
            .replica_modifications(rmod)
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .source_selection_criteria(ssc)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let s = &json["ReplicationConfiguration"]["Rules"][0]["SourceSelectionCriteria"];
        assert_eq!(
            s["SseKmsEncryptedObjects"]["Status"],
            Value::String("Enabled".into())
        );
        assert_eq!(
            s["ReplicaModifications"]["Status"],
            Value::String("Enabled".into())
        );
    }

    #[test]
    fn get_bucket_replication_with_existing_object_replication_and_delete_marker() {
        let eor = aws_sdk_s3::types::ExistingObjectReplication::builder()
            .status(aws_sdk_s3::types::ExistingObjectReplicationStatus::Enabled)
            .build()
            .unwrap();
        let dmr = aws_sdk_s3::types::DeleteMarkerReplication::builder()
            .status(aws_sdk_s3::types::DeleteMarkerReplicationStatus::Disabled)
            .build();
        let dest = aws_sdk_s3::types::Destination::builder()
            .bucket("arn:aws:s3:::d")
            .build()
            .unwrap();
        let rule = aws_sdk_s3::types::ReplicationRule::builder()
            .existing_object_replication(eor)
            .delete_marker_replication(dmr)
            .status(aws_sdk_s3::types::ReplicationRuleStatus::Enabled)
            .destination(dest)
            .build()
            .unwrap();
        let cfg = aws_sdk_s3::types::ReplicationConfiguration::builder()
            .role("arn:aws:iam::111111111111:role/r")
            .rules(rule)
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(cfg)
            .build();
        let json = get_bucket_replication_to_json(&out);
        let r = &json["ReplicationConfiguration"]["Rules"][0];
        assert_eq!(
            r["ExistingObjectReplication"]["Status"],
            Value::String("Enabled".into())
        );
        assert_eq!(
            r["DeleteMarkerReplication"]["Status"],
            Value::String("Disabled".into())
        );
    }

    // ----- get_bucket_accelerate_configuration_to_json tests -----

    #[test]
    fn get_bucket_accelerate_configuration_never_configured_yields_empty_object() {
        let out = GetBucketAccelerateConfigurationOutput::builder().build();
        let json = get_bucket_accelerate_configuration_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_accelerate_configuration_enabled_status() {
        let out = GetBucketAccelerateConfigurationOutput::builder()
            .status(aws_sdk_s3::types::BucketAccelerateStatus::Enabled)
            .build();
        let json = get_bucket_accelerate_configuration_to_json(&out);
        assert_eq!(json["Status"], Value::String("Enabled".into()));
    }

    #[test]
    fn get_bucket_accelerate_configuration_suspended_status() {
        let out = GetBucketAccelerateConfigurationOutput::builder()
            .status(aws_sdk_s3::types::BucketAccelerateStatus::Suspended)
            .build();
        let json = get_bucket_accelerate_configuration_to_json(&out);
        assert_eq!(json["Status"], Value::String("Suspended".into()));
    }

    // ----- get_bucket_request_payment_to_json tests -----

    #[test]
    fn get_bucket_request_payment_absent_payer_yields_empty_object() {
        let out = GetBucketRequestPaymentOutput::builder().build();
        let json = get_bucket_request_payment_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_request_payment_requester() {
        let out = GetBucketRequestPaymentOutput::builder()
            .payer(aws_sdk_s3::types::Payer::Requester)
            .build();
        let json = get_bucket_request_payment_to_json(&out);
        assert_eq!(json["Payer"], Value::String("Requester".into()));
    }

    #[test]
    fn get_bucket_request_payment_bucket_owner() {
        let out = GetBucketRequestPaymentOutput::builder()
            .payer(aws_sdk_s3::types::Payer::BucketOwner)
            .build();
        let json = get_bucket_request_payment_to_json(&out);
        assert_eq!(json["Payer"], Value::String("BucketOwner".into()));
    }

    // ----- get_bucket_policy_status_to_json tests -----

    #[test]
    fn get_bucket_policy_status_absent_yields_empty_object() {
        let out = GetBucketPolicyStatusOutput::builder().build();
        let json = get_bucket_policy_status_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_policy_status_public_true() {
        let ps = aws_sdk_s3::types::PolicyStatus::builder()
            .is_public(true)
            .build();
        let out = GetBucketPolicyStatusOutput::builder()
            .policy_status(ps)
            .build();
        let json = get_bucket_policy_status_to_json(&out);
        assert_eq!(json["PolicyStatus"]["IsPublic"], Value::Bool(true));
    }

    #[test]
    fn get_bucket_policy_status_public_false() {
        let ps = aws_sdk_s3::types::PolicyStatus::builder()
            .is_public(false)
            .build();
        let out = GetBucketPolicyStatusOutput::builder()
            .policy_status(ps)
            .build();
        let json = get_bucket_policy_status_to_json(&out);
        assert_eq!(json["PolicyStatus"]["IsPublic"], Value::Bool(false));
    }

    #[test]
    fn get_bucket_policy_status_present_but_unset_is_public_yields_empty_inner_object() {
        let ps = aws_sdk_s3::types::PolicyStatus::builder().build();
        let out = GetBucketPolicyStatusOutput::builder()
            .policy_status(ps)
            .build();
        let json = get_bucket_policy_status_to_json(&out);
        assert_eq!(
            json["PolicyStatus"],
            Value::Object(Map::new()),
            "missing IsPublic must be omitted from inner object"
        );
    }

    // ----- Lifecycle Filter ObjectSize predicates (output) -----

    #[test]
    fn get_bucket_lifecycle_filter_with_object_size_predicates_emits_them() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
        let filter = LifecycleRuleFilter::builder()
            .object_size_greater_than(1024)
            .object_size_less_than(1_048_576)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let f = &json["Rules"][0]["Filter"];
        assert_eq!(f["ObjectSizeGreaterThan"], Value::Number(1024i64.into()));
        assert_eq!(f["ObjectSizeLessThan"], Value::Number(1_048_576i64.into()));
    }

    #[test]
    fn get_bucket_lifecycle_filter_and_with_object_size_predicates_emits_them() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
        };
        let and = LifecycleRuleAndOperator::builder()
            .prefix("data/")
            .object_size_greater_than(1024)
            .object_size_less_than(1_048_576)
            .build();
        let filter = LifecycleRuleFilter::builder().and(and).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let and_obj = &json["Rules"][0]["Filter"]["And"];
        assert_eq!(and_obj["Prefix"], Value::String("data/".into()));
        assert_eq!(
            and_obj["ObjectSizeGreaterThan"],
            Value::Number(1024i64.into())
        );
        assert_eq!(
            and_obj["ObjectSizeLessThan"],
            Value::Number(1_048_576i64.into())
        );
    }

    // ----- NewerNoncurrentVersions (output) -----

    #[test]
    fn get_bucket_lifecycle_noncurrent_version_expiration_emits_newer_noncurrent_versions() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, NoncurrentVersionExpiration};
        let nve = NoncurrentVersionExpiration::builder()
            .noncurrent_days(30)
            .newer_noncurrent_versions(3)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_expiration(nve)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let nve_json = &json["Rules"][0]["NoncurrentVersionExpiration"];
        assert_eq!(nve_json["NoncurrentDays"], Value::Number(30i32.into()));
        assert_eq!(
            nve_json["NewerNoncurrentVersions"],
            Value::Number(3i32.into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_noncurrent_version_transition_emits_newer_noncurrent_versions() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, NoncurrentVersionTransition, TransitionStorageClass,
        };
        let nvt = NoncurrentVersionTransition::builder()
            .noncurrent_days(7)
            .storage_class(TransitionStorageClass::Glacier)
            .newer_noncurrent_versions(2)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_transitions(nvt)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let arr = json["Rules"][0]["NoncurrentVersionTransitions"]
            .as_array()
            .expect("NoncurrentVersionTransitions must be array");
        assert_eq!(arr[0]["NoncurrentDays"], Value::Number(7i32.into()));
        assert_eq!(
            arr[0]["NewerNoncurrentVersions"],
            Value::Number(2i32.into())
        );
    }

    // ----- TransitionDefaultMinimumObjectSize -----

    #[test]
    fn get_bucket_lifecycle_emits_transition_default_minimum_object_size() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::TransitionDefaultMinimumObjectSize;
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .set_rules(Some(vec![]))
            .transition_default_minimum_object_size(
                TransitionDefaultMinimumObjectSize::AllStorageClasses128K,
            )
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["TransitionDefaultMinimumObjectSize"],
            Value::String("all_storage_classes_128K".into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_omits_transition_default_minimum_object_size_when_absent() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .set_rules(Some(vec![]))
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert!(
            json.get("TransitionDefaultMinimumObjectSize").is_none(),
            "absent field must be omitted"
        );
    }

    // ----- get_bucket_accelerate_configuration RequestCharged -----

    #[test]
    fn get_bucket_accelerate_emits_request_charged() {
        use aws_sdk_s3::types::{BucketAccelerateStatus, RequestCharged};
        let out = GetBucketAccelerateConfigurationOutput::builder()
            .status(BucketAccelerateStatus::Enabled)
            .request_charged(RequestCharged::Requester)
            .build();
        let json = get_bucket_accelerate_configuration_to_json(&out);
        assert_eq!(json["Status"], Value::String("Enabled".into()));
        assert_eq!(json["RequestCharged"], Value::String("requester".into()));
    }

    #[test]
    fn get_bucket_accelerate_omits_request_charged_when_absent() {
        use aws_sdk_s3::types::BucketAccelerateStatus;
        let out = GetBucketAccelerateConfigurationOutput::builder()
            .status(BucketAccelerateStatus::Enabled)
            .build();
        let json = get_bucket_accelerate_configuration_to_json(&out);
        assert!(json.get("RequestCharged").is_none());
    }

    // ----- BlockedEncryptionTypes output -----

    #[test]
    fn get_bucket_encryption_emits_blocked_encryption_types() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            BlockedEncryptionTypes, EncryptionType, ServerSideEncryption,
            ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let bet = BlockedEncryptionTypes::builder()
            .encryption_type(EncryptionType::SseC)
            .build();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .blocked_encryption_types(bet)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let inner = &json["ServerSideEncryptionConfiguration"];
        assert_eq!(
            inner["Rules"][0]["BlockedEncryptionTypes"]["EncryptionType"],
            Value::Array(vec![Value::String("SSE-C".into())])
        );
    }

    // ----- HeadObject ContentRange -----

    #[test]
    fn head_object_with_content_range() {
        let out = HeadObjectOutput::builder()
            .content_range("bytes 0-1023/2048")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ContentRange"],
            Value::String("bytes 0-1023/2048".into())
        );
    }

    #[test]
    fn head_object_omits_content_range_when_absent() {
        let out = HeadObjectOutput::builder().e_tag("\"x\"").build();
        let json = head_object_to_json(&out);
        assert!(json.get("ContentRange").is_none());
    }

    // ----- ObjectSize predicates: omitted when absent (output) -----

    #[test]
    fn get_bucket_lifecycle_filter_omits_object_size_predicates_when_absent() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
        let filter = LifecycleRuleFilter::builder().prefix("data/").build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let f = &json["Rules"][0]["Filter"];
        assert!(f.get("ObjectSizeGreaterThan").is_none());
        assert!(f.get("ObjectSizeLessThan").is_none());
    }

    #[test]
    fn get_bucket_lifecycle_filter_and_omits_object_size_predicates_when_absent() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
        };
        let and = LifecycleRuleAndOperator::builder().prefix("data/").build();
        let filter = LifecycleRuleFilter::builder().and(and).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let and_obj = &json["Rules"][0]["Filter"]["And"];
        assert!(and_obj.get("ObjectSizeGreaterThan").is_none());
        assert!(and_obj.get("ObjectSizeLessThan").is_none());
    }

    #[test]
    fn get_bucket_lifecycle_filter_emits_only_greater_than_when_only_greater_set() {
        // Lower bound only: render must include `ObjectSizeGreaterThan` and
        // omit `ObjectSizeLessThan`.
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
        let filter = LifecycleRuleFilter::builder()
            .object_size_greater_than(2048)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let f = &json["Rules"][0]["Filter"];
        assert_eq!(f["ObjectSizeGreaterThan"], Value::Number(2048i64.into()));
        assert!(f.get("ObjectSizeLessThan").is_none());
    }

    // ----- NewerNoncurrentVersions: omitted when absent (output) -----

    #[test]
    fn get_bucket_lifecycle_noncurrent_version_expiration_omits_newer_versions_when_absent() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, NoncurrentVersionExpiration};
        let nve = NoncurrentVersionExpiration::builder()
            .noncurrent_days(30)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_expiration(nve)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let nve_json = &json["Rules"][0]["NoncurrentVersionExpiration"];
        assert!(nve_json.get("NewerNoncurrentVersions").is_none());
    }

    #[test]
    fn get_bucket_lifecycle_noncurrent_version_transition_omits_newer_versions_when_absent() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, NoncurrentVersionTransition, TransitionStorageClass,
        };
        let nvt = NoncurrentVersionTransition::builder()
            .noncurrent_days(7)
            .storage_class(TransitionStorageClass::Glacier)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .noncurrent_version_transitions(nvt)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let nvt_json = &json["Rules"][0]["NoncurrentVersionTransitions"][0];
        assert!(nvt_json.get("NewerNoncurrentVersions").is_none());
    }

    // ----- TransitionDefaultMinimumObjectSize: VariesByStorageClass variant -----

    #[test]
    fn get_bucket_lifecycle_transition_default_minimum_varies_by_storage_class() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::TransitionDefaultMinimumObjectSize;
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .set_rules(Some(vec![]))
            .transition_default_minimum_object_size(
                TransitionDefaultMinimumObjectSize::VariesByStorageClass,
            )
            .build();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["TransitionDefaultMinimumObjectSize"],
            Value::String("varies_by_storage_class".into())
        );
    }

    // ----- BlockedEncryptionTypes: more output coverage -----

    #[test]
    fn get_bucket_encryption_blocked_encryption_types_multiple_values() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            BlockedEncryptionTypes, EncryptionType, ServerSideEncryption,
            ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let bet = BlockedEncryptionTypes::builder()
            .encryption_type(EncryptionType::SseC)
            .encryption_type(EncryptionType::None)
            .build();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .blocked_encryption_types(bet)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let arr = &json["ServerSideEncryptionConfiguration"]["Rules"][0]["BlockedEncryptionTypes"]
            ["EncryptionType"];
        assert_eq!(
            arr,
            &Value::Array(vec![
                Value::String("SSE-C".into()),
                Value::String("NONE".into()),
            ])
        );
    }

    #[test]
    fn get_bucket_encryption_blocked_encryption_types_empty_inner_emits_empty_object() {
        // SDK populates BlockedEncryptionTypes with no encryption_type entries
        // ⇒ render `"BlockedEncryptionTypes": {}` (mirrors the omit-empty-array
        // pattern used elsewhere in this file).
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            BlockedEncryptionTypes, ServerSideEncryption, ServerSideEncryptionByDefault,
            ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let bet = BlockedEncryptionTypes::builder().build();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .blocked_encryption_types(bet)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let bet_obj =
            &json["ServerSideEncryptionConfiguration"]["Rules"][0]["BlockedEncryptionTypes"];
        assert_eq!(bet_obj, &Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_encryption_omits_blocked_encryption_types_when_absent() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
            ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        assert!(
            json["ServerSideEncryptionConfiguration"]["Rules"][0]
                .get("BlockedEncryptionTypes")
                .is_none()
        );
    }

    // ----- HeadObject: extra checksum fields (SHA512, MD5, XXHASH64/3/128) -----

    #[test]
    fn head_object_with_checksum_sha512() {
        let out = HeadObjectOutput::builder()
            .checksum_sha512("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumSHA512"],
            Value::String("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=".into())
        );
    }

    #[test]
    fn head_object_with_checksum_md5() {
        let out = HeadObjectOutput::builder()
            .checksum_md5("MD5BASE64ENCODED==")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumMD5"],
            Value::String("MD5BASE64ENCODED==".into())
        );
    }

    #[test]
    fn head_object_with_checksum_xxhash64() {
        let out = HeadObjectOutput::builder()
            .checksum_xxhash64("XXHASH64BASE64==")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumXXHASH64"],
            Value::String("XXHASH64BASE64==".into())
        );
    }

    #[test]
    fn head_object_with_checksum_xxhash3() {
        let out = HeadObjectOutput::builder()
            .checksum_xxhash3("XXHASH3BASE64==")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumXXHASH3"],
            Value::String("XXHASH3BASE64==".into())
        );
    }

    #[test]
    fn head_object_with_checksum_xxhash128() {
        let out = HeadObjectOutput::builder()
            .checksum_xxhash128("XXHASH128BASE64==")
            .build();
        let json = head_object_to_json(&out);
        assert_eq!(
            json["ChecksumXXHASH128"],
            Value::String("XXHASH128BASE64==".into())
        );
    }

    #[test]
    fn head_object_omits_extra_checksums_when_absent() {
        // Empty output ⇒ none of the new checksum keys appear.
        let out = HeadObjectOutput::builder().build();
        let json = head_object_to_json(&out);
        for key in [
            "ChecksumSHA512",
            "ChecksumMD5",
            "ChecksumXXHASH64",
            "ChecksumXXHASH3",
            "ChecksumXXHASH128",
        ] {
            assert!(json.get(key).is_none(), "{key} must be omitted when absent");
        }
    }

    // ----- get_bucket_versioning_to_json: only MFADelete is emitted -----

    #[test]
    fn get_bucket_versioning_emits_only_mfadelete_not_mfadelete_xml_form() {
        // AWS CLI v2 emits only `MFADelete`; the literal-XML `MfaDelete`
        // duplicate previously rendered here is no longer produced.
        let out = GetBucketVersioningOutput::builder()
            .status(BucketVersioningStatus::Enabled)
            .mfa_delete(MfaDeleteStatus::Enabled)
            .build();
        let json = get_bucket_versioning_to_json(&out);
        assert_eq!(json["MFADelete"], Value::String("Enabled".into()));
        assert!(
            json.get("MfaDelete").is_none(),
            "MfaDelete duplicate must not be emitted"
        );
    }

    // ----- get_bucket_logging_to_json: TargetGrants -----

    #[test]
    fn get_bucket_logging_with_target_grants_canonical_user() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::{
            BucketLogsPermission, Grantee, LoggingEnabled, TargetGrant, Type as GranteeType,
        };
        let grantee = Grantee::builder()
            .r#type(GranteeType::CanonicalUser)
            .id("abcd1234")
            .display_name("alice")
            .build()
            .unwrap();
        let grant = TargetGrant::builder()
            .grantee(grantee)
            .permission(BucketLogsPermission::FullControl)
            .build();
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .target_grants(grant)
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        let grants = json["LoggingEnabled"]["TargetGrants"]
            .as_array()
            .expect("TargetGrants must be array");
        assert_eq!(grants.len(), 1);
        assert_eq!(
            grants[0]["Permission"],
            Value::String("FULL_CONTROL".into())
        );
        let grantee_json = &grants[0]["Grantee"];
        assert_eq!(grantee_json["Type"], Value::String("CanonicalUser".into()));
        assert_eq!(grantee_json["ID"], Value::String("abcd1234".into()));
        assert_eq!(grantee_json["DisplayName"], Value::String("alice".into()));
        assert!(grantee_json.get("EmailAddress").is_none());
        assert!(grantee_json.get("URI").is_none());
    }

    #[test]
    fn get_bucket_logging_with_target_grants_group_uri() {
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::{
            BucketLogsPermission, Grantee, LoggingEnabled, TargetGrant, Type as GranteeType,
        };
        let grantee = Grantee::builder()
            .r#type(GranteeType::Group)
            .uri("http://acs.amazonaws.com/groups/s3/LogDelivery")
            .build()
            .unwrap();
        let grant = TargetGrant::builder()
            .grantee(grantee)
            .permission(BucketLogsPermission::Write)
            .build();
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .target_grants(grant)
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        let grants = &json["LoggingEnabled"]["TargetGrants"];
        assert_eq!(grants[0]["Grantee"]["Type"], Value::String("Group".into()));
        assert_eq!(
            grants[0]["Grantee"]["URI"],
            Value::String("http://acs.amazonaws.com/groups/s3/LogDelivery".into())
        );
        assert_eq!(grants[0]["Permission"], Value::String("WRITE".into()));
    }

    #[test]
    fn get_bucket_logging_omits_target_grants_when_empty() {
        // The SDK's accessor returns an empty slice when target_grants is None
        // OR Some(vec![]); both cases must omit the key from the JSON output.
        use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
        use aws_sdk_s3::types::LoggingEnabled;
        let le = LoggingEnabled::builder()
            .target_bucket("log-bucket")
            .target_prefix("logs/")
            .build()
            .unwrap();
        let out = GetBucketLoggingOutput::builder()
            .logging_enabled(le)
            .build();
        let json = get_bucket_logging_to_json(&out);
        assert!(json["LoggingEnabled"].get("TargetGrants").is_none());
    }

    #[test]
    fn put_object_annotation_to_json_shape() {
        use aws_sdk_s3::operation::put_object_annotation::PutObjectAnnotationOutput;

        let out = PutObjectAnnotationOutput::builder()
            .key("dir/obj.txt")
            .annotation_name("note")
            .object_version_id("v1")
            .e_tag("\"abc123\"")
            .checksum_crc64_nvme("AAAAAAAAAAA=")
            .build();

        let json = put_object_annotation_to_json(&out);
        assert_eq!(json["Key"], "dir/obj.txt");
        assert_eq!(json["AnnotationName"], "note");
        assert_eq!(json["ObjectVersionId"], "v1");
        assert_eq!(json["ETag"], "\"abc123\"");
        assert_eq!(json["ChecksumCRC64NVME"], "AAAAAAAAAAA=");
        // Absent fields are omitted, per the file's convention.
        assert!(json.get("ServerSideEncryption").is_none());
        assert!(json.get("RequestCharged").is_none());
    }

    #[test]
    fn get_object_annotation_to_json_shape() {
        use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
        use aws_sdk_s3::primitives::{ByteStream, DateTime};
        use aws_sdk_s3::types::{ChecksumType, ServerSideEncryption};

        let out = GetObjectAnnotationOutput::builder()
            .annotation_payload(ByteStream::from_static(b"payload"))
            .last_modified(DateTime::from_secs(1_750_000_000))
            .content_length(678260)
            .e_tag("\"b373009fdd7e9a9a2b266c2044fb7948\"")
            .checksum_crc64_nvme("hdWdywUmntQ=")
            .checksum_type(ChecksumType::FullObject)
            .server_side_encryption(ServerSideEncryption::Aes256)
            .object_version_id("v1")
            .build();

        let json = get_object_annotation_to_json(&out);
        assert_eq!(json["ContentLength"], 678260);
        assert_eq!(json["ETag"], "\"b373009fdd7e9a9a2b266c2044fb7948\"");
        assert_eq!(json["ChecksumCRC64NVME"], "hdWdywUmntQ=");
        assert_eq!(json["ChecksumType"], "FULL_OBJECT");
        assert_eq!(json["ServerSideEncryption"], "AES256");
        assert_eq!(json["VersionId"], "v1");
        assert!(json.get("LastModified").is_some());
        // Payload bytes are never serialized.
        assert!(json.get("AnnotationPayload").is_none());
    }
}
