//! AWS-CLI-shape JSON serialisation for S3 SDK response types.
//!
//! Hand-written because `aws-sdk-s3` response types do not implement
//! `Serialize`, and the SDK's internal field shapes don't match the
//! `aws s3api --output json` surface 1-to-1 (PascalCase, omission semantics,
//! double-encoded `Policy`, etc.).

use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_logging::GetBucketLoggingOutput;
use aws_sdk_s3::operation::get_bucket_notification_configuration::GetBucketNotificationConfigurationOutput;
use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
use aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::operation::get_bucket_website::GetBucketWebsiteOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
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
/// Otherwise emits `{"Status": "Enabled"|"Suspended"}` and, when present,
/// both `{"MFADelete": "Enabled"|"Disabled"}` (the AWS-CLI/SDK casing) and
/// `{"MfaDelete": ...}` (the literal S3 XML tag name).
pub fn get_bucket_versioning_to_json(out: &GetBucketVersioningOutput) -> Value {
    let mut map = Map::new();
    if let Some(status) = out.status() {
        map.insert(
            "Status".to_string(),
            Value::String(status.as_str().to_string()),
        );
    }
    if let Some(mfa) = out.mfa_delete() {
        let v = Value::String(mfa.as_str().to_string());
        map.insert("MFADelete".to_string(), v.clone());
        map.insert("MfaDelete".to_string(), v);
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
/// Top level: `{"Rules": [ … ]}` (always emits `Rules`, as `[]` if empty).
/// Each rule emits its present fields with PascalCase keys; absent fields
/// are omitted (never `null`).
pub fn get_bucket_lifecycle_configuration_to_json(
    out: &GetBucketLifecycleConfigurationOutput,
) -> Value {
    let mut map = Map::new();
    let rules: Vec<Value> = out.rules().iter().map(serialize_lifecycle_rule).collect();
    map.insert("Rules".to_string(), Value::Array(rules));
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
/// `TargetGrants` is intentionally omitted from the output — buckets with
/// the modern bucket-owner-enforced Object Ownership setting reject grants,
/// and the field is rarely populated in practice.
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
        top.insert("LoggingEnabled".to_string(), Value::Object(inner));
    }
    Value::Object(top)
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
        // `MfaDelete` is the literal S3 XML tag name; emitted alongside
        // the AWS-CLI/SDK-cased `MFADelete`.
        assert_eq!(json["MfaDelete"], Value::String("Enabled".into()));
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
}
