//! AWS-CLI-shape JSON deserialisation for S3 SDK input types used by the
//! `put-*` subcommands.
//!
//! The `aws-sdk-s3` input types are smithy-generated and do not derive
//! `serde::Deserialize`, so this module defines mirror structs shaped
//! exactly like `aws s3api put-* --generate-cli-skeleton input` (PascalCase
//! fields). Each top-level mirror exposes `into_sdk(self) -> Result<…>`
//! that builds the SDK type and returns the SDK builder error verbatim
//! (so S3-side error messages match what the user would get from the AWS
//! CLI).
//!
//! Per-resource structs are added by their respective family tasks.

use anyhow::Result;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, BucketLoggingStatus, Condition,
    CorsConfiguration, CorsRule, ErrorDocument, Event, EventBridgeConfiguration, ExpirationStatus,
    FilterRule, FilterRuleName, IndexDocument, LambdaFunctionConfiguration, LifecycleExpiration,
    LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter, LoggingEnabled,
    NoncurrentVersionExpiration, NoncurrentVersionTransition, NotificationConfiguration,
    NotificationConfigurationFilter, PartitionDateSource, PartitionedPrefix, Protocol,
    PublicAccessBlockConfiguration, QueueConfiguration, Redirect, RedirectAllRequestsTo,
    RoutingRule, S3KeyFilter, ServerSideEncryption, ServerSideEncryptionByDefault,
    ServerSideEncryptionConfiguration, ServerSideEncryptionRule, SimplePrefix, Tag as SdkTag,
    TargetObjectKeyFormat, TopicConfiguration, Transition, TransitionStorageClass,
    WebsiteConfiguration,
};
use aws_smithy_types::DateTime;
use serde::Deserialize;

/// Mirror of `BucketLifecycleConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-lifecycle-configuration` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleConfigurationJson {
    pub Rules: Vec<LifecycleRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleJson {
    pub ID: Option<String>,
    /// `Enabled` or `Disabled`. SDK rejects other values at builder time.
    pub Status: String,
    /// Deprecated S3 field, kept for AWS-CLI shape parity.
    pub Prefix: Option<String>,
    pub Filter: Option<LifecycleRuleFilterJson>,
    pub Expiration: Option<LifecycleExpirationJson>,
    pub NoncurrentVersionExpiration: Option<NoncurrentVersionExpirationJson>,
    pub Transitions: Option<Vec<TransitionJson>>,
    pub NoncurrentVersionTransitions: Option<Vec<NoncurrentVersionTransitionJson>>,
    pub AbortIncompleteMultipartUpload: Option<AbortIncompleteMultipartUploadJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleFilterJson {
    pub Prefix: Option<String>,
    pub Tag: Option<TagJson>,
    pub And: Option<LifecycleRuleAndOperatorJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleAndOperatorJson {
    pub Prefix: Option<String>,
    pub Tags: Option<Vec<TagJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TagJson {
    pub Key: String,
    pub Value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleExpirationJson {
    /// RFC3339 timestamp.
    pub Date: Option<String>,
    pub Days: Option<i32>,
    pub ExpiredObjectDeleteMarker: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionExpirationJson {
    pub NoncurrentDays: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TransitionJson {
    pub Date: Option<String>,
    pub Days: Option<i32>,
    pub StorageClass: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionTransitionJson {
    pub NoncurrentDays: Option<i32>,
    pub StorageClass: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct AbortIncompleteMultipartUploadJson {
    pub DaysAfterInitiation: Option<i32>,
}

impl LifecycleConfigurationJson {
    /// Build the SDK `BucketLifecycleConfiguration`. Returns the SDK builder
    /// error verbatim so error messages match what the AWS CLI would emit.
    pub fn into_sdk(self) -> Result<BucketLifecycleConfiguration> {
        let rules: Result<Vec<LifecycleRule>> = self
            .Rules
            .into_iter()
            .map(LifecycleRuleJson::into_sdk)
            .collect();
        Ok(BucketLifecycleConfiguration::builder()
            .set_rules(Some(rules?))
            .build()?)
    }
}

impl LifecycleRuleJson {
    fn into_sdk(self) -> Result<LifecycleRule> {
        let mut b = LifecycleRule::builder().status(ExpirationStatus::from(self.Status.as_str()));
        if let Some(id) = self.ID {
            b = b.id(id);
        }
        if let Some(p) = self.Prefix {
            // `Prefix` on the rule itself is deprecated by S3 in favour of
            // `Filter.Prefix`, but kept for AWS-CLI input-shape parity.
            #[allow(deprecated)]
            {
                b = b.prefix(p);
            }
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk()?);
        }
        if let Some(e) = self.Expiration {
            b = b.expiration(e.into_sdk()?);
        }
        if let Some(n) = self.NoncurrentVersionExpiration {
            b = b.noncurrent_version_expiration(n.into_sdk());
        }
        if let Some(ts) = self.Transitions {
            for t in ts {
                b = b.transitions(t.into_sdk()?);
            }
        }
        if let Some(nts) = self.NoncurrentVersionTransitions {
            for n in nts {
                b = b.noncurrent_version_transitions(n.into_sdk());
            }
        }
        if let Some(a) = self.AbortIncompleteMultipartUpload {
            b = b.abort_incomplete_multipart_upload(a.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl LifecycleRuleFilterJson {
    fn into_sdk(self) -> Result<LifecycleRuleFilter> {
        // S3 lifecycle Filter is a one-of (Prefix XOR Tag XOR And); when
        // multiple are supplied, the SDK builder accepts the last setter.
        // We honour AWS-CLI semantics by passing values as-is and letting
        // S3 reject bad combinations.
        let mut b = LifecycleRuleFilter::builder();
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(t) = self.Tag {
            b = b.tag(SdkTag::builder().key(t.Key).value(t.Value).build()?);
        }
        if let Some(and) = self.And {
            b = b.and(and.into_sdk()?);
        }
        Ok(b.build())
    }
}

impl LifecycleRuleAndOperatorJson {
    fn into_sdk(self) -> Result<LifecycleRuleAndOperator> {
        let mut b = LifecycleRuleAndOperator::builder();
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(tags) = self.Tags {
            for t in tags {
                b = b.tags(SdkTag::builder().key(t.Key).value(t.Value).build()?);
            }
        }
        Ok(b.build())
    }
}

impl LifecycleExpirationJson {
    fn into_sdk(self) -> Result<LifecycleExpiration> {
        let mut b = LifecycleExpiration::builder();
        if let Some(d) = self.Date {
            b = b.date(parse_rfc3339(&d)?);
        }
        if let Some(days) = self.Days {
            b = b.days(days);
        }
        if let Some(eodm) = self.ExpiredObjectDeleteMarker {
            b = b.expired_object_delete_marker(eodm);
        }
        Ok(b.build())
    }
}

impl NoncurrentVersionExpirationJson {
    fn into_sdk(self) -> NoncurrentVersionExpiration {
        let mut b = NoncurrentVersionExpiration::builder();
        if let Some(n) = self.NoncurrentDays {
            b = b.noncurrent_days(n);
        }
        b.build()
    }
}

impl TransitionJson {
    fn into_sdk(self) -> Result<Transition> {
        let mut b = Transition::builder();
        if let Some(d) = self.Date {
            b = b.date(parse_rfc3339(&d)?);
        }
        if let Some(days) = self.Days {
            b = b.days(days);
        }
        if let Some(sc) = self.StorageClass {
            b = b.storage_class(TransitionStorageClass::from(sc.as_str()));
        }
        Ok(b.build())
    }
}

impl NoncurrentVersionTransitionJson {
    fn into_sdk(self) -> NoncurrentVersionTransition {
        let mut b = NoncurrentVersionTransition::builder();
        if let Some(n) = self.NoncurrentDays {
            b = b.noncurrent_days(n);
        }
        if let Some(sc) = self.StorageClass {
            b = b.storage_class(TransitionStorageClass::from(sc.as_str()));
        }
        b.build()
    }
}

impl AbortIncompleteMultipartUploadJson {
    fn into_sdk(self) -> AbortIncompleteMultipartUpload {
        let mut b = AbortIncompleteMultipartUpload::builder();
        if let Some(d) = self.DaysAfterInitiation {
            b = b.days_after_initiation(d);
        }
        b.build()
    }
}

fn parse_rfc3339(s: &str) -> Result<DateTime> {
    DateTime::from_str(s, aws_smithy_types::date_time::Format::DateTime)
        .map_err(|e| anyhow::anyhow!("invalid RFC3339 timestamp {s:?}: {e}"))
}

/// Mirror of `ServerSideEncryptionConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-encryption` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionConfigurationJson {
    pub Rules: Vec<ServerSideEncryptionRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionRuleJson {
    pub ApplyServerSideEncryptionByDefault: Option<ApplyServerSideEncryptionByDefaultJson>,
    pub BucketKeyEnabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ApplyServerSideEncryptionByDefaultJson {
    /// `AES256` or `aws:kms` or `aws:kms:dsse`.
    pub SSEAlgorithm: String,
    pub KMSMasterKeyID: Option<String>,
}

impl ServerSideEncryptionConfigurationJson {
    /// Build the SDK `ServerSideEncryptionConfiguration`. Returns the SDK
    /// builder error verbatim so error messages match what the AWS CLI
    /// would emit.
    pub fn into_sdk(self) -> Result<ServerSideEncryptionConfiguration> {
        let rules: Result<Vec<ServerSideEncryptionRule>> = self
            .Rules
            .into_iter()
            .map(ServerSideEncryptionRuleJson::into_sdk)
            .collect();
        Ok(ServerSideEncryptionConfiguration::builder()
            .set_rules(Some(rules?))
            .build()?)
    }
}

impl ServerSideEncryptionRuleJson {
    fn into_sdk(self) -> Result<ServerSideEncryptionRule> {
        let mut b = ServerSideEncryptionRule::builder();
        if let Some(d) = self.ApplyServerSideEncryptionByDefault {
            let mut bb = ServerSideEncryptionByDefault::builder()
                .sse_algorithm(ServerSideEncryption::from(d.SSEAlgorithm.as_str()));
            if let Some(k) = d.KMSMasterKeyID {
                bb = bb.kms_master_key_id(k);
            }
            b = b.apply_server_side_encryption_by_default(bb.build()?);
        }
        if let Some(bke) = self.BucketKeyEnabled {
            b = b.bucket_key_enabled(bke);
        }
        Ok(b.build())
    }
}

/// Mirror of `CorsConfiguration` for the AWS-CLI input shape.
/// Top-level field name `CORSRules` matches the AWS CLI exactly
/// (note the all-uppercase `CORS`).
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct CorsConfigurationJson {
    pub CORSRules: Vec<CorsRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct CorsRuleJson {
    pub ID: Option<String>,
    pub AllowedHeaders: Option<Vec<String>>,
    pub AllowedMethods: Vec<String>,
    pub AllowedOrigins: Vec<String>,
    pub ExposeHeaders: Option<Vec<String>>,
    pub MaxAgeSeconds: Option<i32>,
}

impl CorsConfigurationJson {
    /// Build the SDK `CorsConfiguration`. Returns the SDK builder error
    /// verbatim so error messages match what the AWS CLI would emit.
    pub fn into_sdk(self) -> Result<CorsConfiguration> {
        let rules: Result<Vec<CorsRule>> = self
            .CORSRules
            .into_iter()
            .map(CorsRuleJson::into_sdk)
            .collect();
        Ok(CorsConfiguration::builder()
            .set_cors_rules(Some(rules?))
            .build()?)
    }
}

impl CorsRuleJson {
    fn into_sdk(self) -> Result<CorsRule> {
        let mut b = CorsRule::builder()
            .set_allowed_methods(Some(self.AllowedMethods))
            .set_allowed_origins(Some(self.AllowedOrigins));
        if let Some(id) = self.ID {
            b = b.id(id);
        }
        if let Some(h) = self.AllowedHeaders {
            b = b.set_allowed_headers(Some(h));
        }
        if let Some(eh) = self.ExposeHeaders {
            b = b.set_expose_headers(Some(eh));
        }
        if let Some(m) = self.MaxAgeSeconds {
            b = b.max_age_seconds(m);
        }
        Ok(b.build()?)
    }
}

/// Mirror of `PublicAccessBlockConfiguration` for the AWS-CLI input shape.
/// All four fields are optional in the input JSON; absent fields are
/// passed to the SDK as `Some(false)` (matching AWS CLI v2 behaviour).
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct PublicAccessBlockConfigurationJson {
    pub BlockPublicAcls: Option<bool>,
    pub IgnorePublicAcls: Option<bool>,
    pub BlockPublicPolicy: Option<bool>,
    pub RestrictPublicBuckets: Option<bool>,
}

impl PublicAccessBlockConfigurationJson {
    /// Build the SDK `PublicAccessBlockConfiguration`. Absent fields are
    /// sent as `false`, matching AWS CLI v2 behaviour.
    pub fn into_sdk(self) -> Result<PublicAccessBlockConfiguration> {
        Ok(PublicAccessBlockConfiguration::builder()
            .block_public_acls(self.BlockPublicAcls.unwrap_or(false))
            .ignore_public_acls(self.IgnorePublicAcls.unwrap_or(false))
            .block_public_policy(self.BlockPublicPolicy.unwrap_or(false))
            .restrict_public_buckets(self.RestrictPublicBuckets.unwrap_or(false))
            .build())
    }
}

/// Mirror of `WebsiteConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-website` input JSON.
///
/// The two top-level shapes (a "site config" with `IndexDocument` and
/// optional `ErrorDocument`/`RoutingRules`, OR a redirect-only config with
/// `RedirectAllRequestsTo`) are mutually exclusive per AWS, but accepted
/// at parse time. S3 rejects invalid combinations server-side — same
/// approach as lifecycle's `Filter` one-of.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct WebsiteConfigurationJson {
    pub IndexDocument: Option<IndexDocumentJson>,
    pub ErrorDocument: Option<ErrorDocumentJson>,
    pub RedirectAllRequestsTo: Option<RedirectAllRequestsToJson>,
    pub RoutingRules: Option<Vec<RoutingRuleJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct IndexDocumentJson {
    pub Suffix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ErrorDocumentJson {
    pub Key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct RedirectAllRequestsToJson {
    pub HostName: String,
    pub Protocol: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct RoutingRuleJson {
    pub Condition: Option<ConditionJson>,
    pub Redirect: RedirectJson,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ConditionJson {
    pub HttpErrorCodeReturnedEquals: Option<String>,
    pub KeyPrefixEquals: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct RedirectJson {
    pub HostName: Option<String>,
    pub HttpRedirectCode: Option<String>,
    pub Protocol: Option<String>,
    pub ReplaceKeyPrefixWith: Option<String>,
    pub ReplaceKeyWith: Option<String>,
}

impl WebsiteConfigurationJson {
    /// Build the SDK `WebsiteConfiguration`. Returns the SDK builder error
    /// verbatim so error messages match what the AWS CLI would emit.
    pub fn into_sdk(self) -> Result<WebsiteConfiguration> {
        let mut b = WebsiteConfiguration::builder();
        if let Some(ix) = self.IndexDocument {
            b = b.index_document(IndexDocument::builder().suffix(ix.Suffix).build()?);
        }
        if let Some(err) = self.ErrorDocument {
            b = b.error_document(ErrorDocument::builder().key(err.Key).build()?);
        }
        if let Some(r) = self.RedirectAllRequestsTo {
            let mut rb = RedirectAllRequestsTo::builder().host_name(r.HostName);
            if let Some(p) = r.Protocol {
                rb = rb.protocol(Protocol::from(p.as_str()));
            }
            b = b.redirect_all_requests_to(rb.build()?);
        }
        if let Some(rules) = self.RoutingRules {
            for rr in rules {
                b = b.routing_rules(rr.into_sdk()?);
            }
        }
        Ok(b.build())
    }
}

impl RoutingRuleJson {
    fn into_sdk(self) -> Result<RoutingRule> {
        let mut b = RoutingRule::builder().redirect(self.Redirect.into_sdk());
        if let Some(c) = self.Condition {
            let mut cb = Condition::builder();
            if let Some(code) = c.HttpErrorCodeReturnedEquals {
                cb = cb.http_error_code_returned_equals(code);
            }
            if let Some(prefix) = c.KeyPrefixEquals {
                cb = cb.key_prefix_equals(prefix);
            }
            b = b.condition(cb.build());
        }
        Ok(b.build())
    }
}

impl RedirectJson {
    fn into_sdk(self) -> Redirect {
        let mut b = Redirect::builder();
        if let Some(h) = self.HostName {
            b = b.host_name(h);
        }
        if let Some(c) = self.HttpRedirectCode {
            b = b.http_redirect_code(c);
        }
        if let Some(p) = self.Protocol {
            b = b.protocol(Protocol::from(p.as_str()));
        }
        if let Some(p) = self.ReplaceKeyPrefixWith {
            b = b.replace_key_prefix_with(p);
        }
        if let Some(k) = self.ReplaceKeyWith {
            b = b.replace_key_with(k);
        }
        b.build()
    }
}

/// Mirror of `BucketLoggingStatus` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-logging` input JSON.
///
/// An empty object (`{}` — no `LoggingEnabled` field) builds an empty
/// `BucketLoggingStatus`, which disables logging on the bucket. AWS does
/// not expose a `DeleteBucketLogging` API; this is the documented way
/// to remove a logging configuration.
///
/// `TargetGrants` is intentionally not modelled — buckets that use the
/// bucket-owner-enforced Object Ownership setting (the modern default)
/// reject target grants, and the partitioned/simple-prefix configuration
/// covers the common use case.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct BucketLoggingStatusJson {
    pub LoggingEnabled: Option<LoggingEnabledJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LoggingEnabledJson {
    pub TargetBucket: String,
    pub TargetPrefix: String,
    pub TargetObjectKeyFormat: Option<TargetObjectKeyFormatJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TargetObjectKeyFormatJson {
    pub SimplePrefix: Option<SimplePrefixJson>,
    pub PartitionedPrefix: Option<PartitionedPrefixJson>,
}

/// Marker shape for `SimplePrefix` — set to `{}` in the input JSON to
/// select the simple key format. The SDK uses a unit-style struct.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct SimplePrefixJson {}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct PartitionedPrefixJson {
    /// `EventTime` or `DeliveryTime`. Unknown values are passed through to
    /// the SDK enum; S3 will reject them server-side.
    pub PartitionDateSource: Option<String>,
}

impl BucketLoggingStatusJson {
    /// Build the SDK `BucketLoggingStatus`. Returns the SDK builder error
    /// verbatim so error messages match what the AWS CLI would emit.
    ///
    /// An empty input object yields an empty `BucketLoggingStatus`
    /// (no `LoggingEnabled`), which disables logging on the bucket.
    pub fn into_sdk(self) -> Result<BucketLoggingStatus> {
        let mut b = BucketLoggingStatus::builder();
        if let Some(le) = self.LoggingEnabled {
            b = b.logging_enabled(le.into_sdk()?);
        }
        Ok(b.build())
    }
}

impl LoggingEnabledJson {
    fn into_sdk(self) -> Result<LoggingEnabled> {
        let mut b = LoggingEnabled::builder()
            .target_bucket(self.TargetBucket)
            .target_prefix(self.TargetPrefix);
        if let Some(fmt) = self.TargetObjectKeyFormat {
            b = b.target_object_key_format(fmt.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl TargetObjectKeyFormatJson {
    fn into_sdk(self) -> TargetObjectKeyFormat {
        // AWS docs say only one of `SimplePrefix` / `PartitionedPrefix`
        // may be set. We pass through whatever the user supplied; S3
        // validates server-side.
        let mut b = TargetObjectKeyFormat::builder();
        if self.SimplePrefix.is_some() {
            b = b.simple_prefix(SimplePrefix::builder().build());
        }
        if let Some(pp) = self.PartitionedPrefix {
            b = b.partitioned_prefix(pp.into_sdk());
        }
        b.build()
    }
}

impl PartitionedPrefixJson {
    fn into_sdk(self) -> PartitionedPrefix {
        let mut b = PartitionedPrefix::builder();
        if let Some(s) = self.PartitionDateSource {
            b = b.partition_date_source(PartitionDateSource::from(s.as_str()));
        }
        b.build()
    }
}

/// Mirror of `NotificationConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-notification-configuration` input JSON.
///
/// An empty object (`{}` — every field absent) builds an empty
/// `NotificationConfiguration`, which disables every notification on the
/// bucket. AWS does not expose a `DeleteBucketNotificationConfiguration`
/// API; this is the documented way to remove notifications.
///
/// The four optional fields mirror `aws s3api put-bucket-notification-configuration
/// --generate-cli-skeleton input` exactly: `TopicConfigurations`,
/// `QueueConfigurations`, `LambdaFunctionConfigurations`, and the
/// presence-only marker `EventBridgeConfiguration`.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NotificationConfigurationJson {
    pub TopicConfigurations: Option<Vec<TopicConfigurationJson>>,
    pub QueueConfigurations: Option<Vec<QueueConfigurationJson>>,
    pub LambdaFunctionConfigurations: Option<Vec<LambdaFunctionConfigurationJson>>,
    pub EventBridgeConfiguration: Option<EventBridgeConfigurationJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TopicConfigurationJson {
    pub Id: Option<String>,
    pub TopicArn: String,
    /// Event names like `"s3:ObjectCreated:*"`, `"s3:ObjectRemoved:Delete"`.
    /// Unknown values are passed through to the SDK enum; S3 will reject
    /// them server-side.
    pub Events: Vec<String>,
    pub Filter: Option<NotificationConfigurationFilterJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct QueueConfigurationJson {
    pub Id: Option<String>,
    pub QueueArn: String,
    pub Events: Vec<String>,
    pub Filter: Option<NotificationConfigurationFilterJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LambdaFunctionConfigurationJson {
    pub Id: Option<String>,
    pub LambdaFunctionArn: String,
    pub Events: Vec<String>,
    pub Filter: Option<NotificationConfigurationFilterJson>,
}

/// Marker shape for `EventBridgeConfiguration` — set to `{}` in the input
/// JSON to enable EventBridge delivery. The SDK uses a unit-style struct.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct EventBridgeConfigurationJson {}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NotificationConfigurationFilterJson {
    pub Key: Option<S3KeyFilterJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct S3KeyFilterJson {
    pub FilterRules: Option<Vec<FilterRuleJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct FilterRuleJson {
    /// `prefix` or `suffix`. Unknown values are passed through to the SDK
    /// enum; S3 will reject them server-side.
    pub Name: String,
    pub Value: String,
}

impl NotificationConfigurationJson {
    /// Build the SDK `NotificationConfiguration`. Returns the SDK builder
    /// error verbatim so error messages match what the AWS CLI would emit.
    ///
    /// An empty input object yields an empty `NotificationConfiguration`
    /// (every field absent), which disables every notification on the bucket.
    pub fn into_sdk(self) -> Result<NotificationConfiguration> {
        let mut b = NotificationConfiguration::builder();
        if let Some(topics) = self.TopicConfigurations {
            let v: Result<Vec<TopicConfiguration>> = topics
                .into_iter()
                .map(TopicConfigurationJson::into_sdk)
                .collect();
            b = b.set_topic_configurations(Some(v?));
        }
        if let Some(queues) = self.QueueConfigurations {
            let v: Result<Vec<QueueConfiguration>> = queues
                .into_iter()
                .map(QueueConfigurationJson::into_sdk)
                .collect();
            b = b.set_queue_configurations(Some(v?));
        }
        if let Some(lambdas) = self.LambdaFunctionConfigurations {
            let v: Result<Vec<LambdaFunctionConfiguration>> = lambdas
                .into_iter()
                .map(LambdaFunctionConfigurationJson::into_sdk)
                .collect();
            b = b.set_lambda_function_configurations(Some(v?));
        }
        if self.EventBridgeConfiguration.is_some() {
            b = b.event_bridge_configuration(EventBridgeConfiguration::builder().build());
        }
        Ok(b.build())
    }
}

impl TopicConfigurationJson {
    fn into_sdk(self) -> Result<TopicConfiguration> {
        let events: Vec<Event> = self
            .Events
            .iter()
            .map(|s| Event::from(s.as_str()))
            .collect();
        let mut b = TopicConfiguration::builder()
            .topic_arn(self.TopicArn)
            .set_events(Some(events));
        if let Some(id) = self.Id {
            b = b.id(id);
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl QueueConfigurationJson {
    fn into_sdk(self) -> Result<QueueConfiguration> {
        let events: Vec<Event> = self
            .Events
            .iter()
            .map(|s| Event::from(s.as_str()))
            .collect();
        let mut b = QueueConfiguration::builder()
            .queue_arn(self.QueueArn)
            .set_events(Some(events));
        if let Some(id) = self.Id {
            b = b.id(id);
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl LambdaFunctionConfigurationJson {
    fn into_sdk(self) -> Result<LambdaFunctionConfiguration> {
        let events: Vec<Event> = self
            .Events
            .iter()
            .map(|s| Event::from(s.as_str()))
            .collect();
        let mut b = LambdaFunctionConfiguration::builder()
            .lambda_function_arn(self.LambdaFunctionArn)
            .set_events(Some(events));
        if let Some(id) = self.Id {
            b = b.id(id);
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl NotificationConfigurationFilterJson {
    fn into_sdk(self) -> NotificationConfigurationFilter {
        let mut b = NotificationConfigurationFilter::builder();
        if let Some(k) = self.Key {
            b = b.key(k.into_sdk());
        }
        b.build()
    }
}

impl S3KeyFilterJson {
    fn into_sdk(self) -> S3KeyFilter {
        let mut b = S3KeyFilter::builder();
        if let Some(rules) = self.FilterRules {
            for r in rules {
                b = b.filter_rules(r.into_sdk());
            }
        }
        b.build()
    }
}

impl FilterRuleJson {
    fn into_sdk(self) -> FilterRule {
        FilterRule::builder()
            .name(FilterRuleName::from(self.Name.as_str()))
            .value(self.Value)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- LifecycleConfigurationJson -----

    #[test]
    fn lifecycle_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "Rules": [
            {
              "ID": "ExpireOldLogs",
              "Status": "Enabled",
              "Filter": { "Prefix": "logs/" },
              "Expiration": { "Days": 365 }
            }
          ]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).expect("parses");
        assert_eq!(parsed.Rules.len(), 1);
        assert_eq!(parsed.Rules[0].ID.as_deref(), Some("ExpireOldLogs"));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_id_and_status() {
        let json = r#"{"Rules":[{"ID":"r1","Status":"Enabled","Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rules = cfg.rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id(), Some("r1"));
        assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn lifecycle_into_sdk_preserves_filter_prefix() {
        let json = r#"{"Rules":[{"Status":"Enabled","Filter":{"Prefix":"logs/"},"Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().expect("filter");
        assert_eq!(f.prefix(), Some("logs/"));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_filter_and_with_tags() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"And":{"Prefix":"x/","Tags":[{"Key":"a","Value":"1"}]}},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().expect("and");
        assert_eq!(and.prefix(), Some("x/"));
        assert_eq!(and.tags().len(), 1);
        assert_eq!(and.tags()[0].key(), "a");
        assert_eq!(and.tags()[0].value(), "1");
    }

    #[test]
    fn lifecycle_into_sdk_preserves_transitions() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Transitions":[{"Days":30,"StorageClass":"GLACIER"}],
            "Expiration":{"Days":365}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let t = &cfg.rules()[0].transitions()[0];
        assert_eq!(t.days(), Some(30));
        assert_eq!(t.storage_class(), Some(&TransitionStorageClass::Glacier));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_noncurrent_version_expiration() {
        let json = r#"{"Rules":[{"Status":"Enabled","NoncurrentVersionExpiration":{"NoncurrentDays":7}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let n = cfg.rules()[0]
            .noncurrent_version_expiration()
            .expect("noncurrent version expiration");
        assert_eq!(n.noncurrent_days(), Some(7));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_abort_incomplete_multipart_upload() {
        let json = r#"{"Rules":[{"Status":"Enabled","AbortIncompleteMultipartUpload":{"DaysAfterInitiation":3}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let a = cfg.rules()[0]
            .abort_incomplete_multipart_upload()
            .expect("abort");
        assert_eq!(a.days_after_initiation(), Some(3));
    }

    #[test]
    fn lifecycle_invalid_json_errors() {
        let res: Result<LifecycleConfigurationJson, _> = serde_json::from_str("{not json");
        assert!(res.is_err());
    }

    #[test]
    fn lifecycle_missing_rules_errors() {
        let res: Result<LifecycleConfigurationJson, _> = serde_json::from_str("{}");
        assert!(res.is_err(), "missing required `Rules` must error");
    }

    #[test]
    fn lifecycle_invalid_date_errors_at_into_sdk() {
        let json = r#"{"Rules":[{"Status":"Enabled","Expiration":{"Date":"not-a-date"}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let res = parsed.into_sdk();
        assert!(res.is_err(), "invalid date must error at into_sdk()");
    }

    // ----- ServerSideEncryptionConfigurationJson -----

    #[test]
    fn encryption_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "Rules": [
            { "ApplyServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" } }
          ]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.Rules.len(), 1);
    }

    #[test]
    fn encryption_into_sdk_preserves_aes256() {
        let json =
            r#"{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        assert_eq!(
            r.apply_server_side_encryption_by_default()
                .unwrap()
                .sse_algorithm(),
            &ServerSideEncryption::Aes256
        );
    }

    #[test]
    fn encryption_into_sdk_preserves_kms_with_key_id() {
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{
              "SSEAlgorithm":"aws:kms",
              "KMSMasterKeyID":"arn:aws:kms:us-east-1:111111111111:key/abc"
            },
            "BucketKeyEnabled": true
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        let d = r.apply_server_side_encryption_by_default().unwrap();
        assert_eq!(d.sse_algorithm(), &ServerSideEncryption::AwsKms);
        assert_eq!(
            d.kms_master_key_id(),
            Some("arn:aws:kms:us-east-1:111111111111:key/abc")
        );
        assert_eq!(r.bucket_key_enabled(), Some(true));
    }

    #[test]
    fn encryption_invalid_json_errors() {
        assert!(
            serde_json::from_str::<ServerSideEncryptionConfigurationJson>("{not json").is_err()
        );
    }

    #[test]
    fn encryption_missing_rules_errors() {
        assert!(serde_json::from_str::<ServerSideEncryptionConfigurationJson>("{}").is_err());
    }

    // ----- CorsConfigurationJson -----

    #[test]
    fn cors_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "CORSRules": [
            {
              "AllowedMethods": ["GET", "HEAD"],
              "AllowedOrigins": ["*"],
              "AllowedHeaders": ["*"],
              "MaxAgeSeconds": 3000
            }
          ]
        }"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.CORSRules.len(), 1);
    }

    #[test]
    fn cors_into_sdk_preserves_methods_and_origins() {
        let json = r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["https://example.com"]}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.cors_rules()[0];
        assert_eq!(r.allowed_methods(), &["GET".to_string()]);
        assert_eq!(r.allowed_origins(), &["https://example.com".to_string()]);
    }

    #[test]
    fn cors_into_sdk_preserves_max_age_and_id() {
        let json = r#"{"CORSRules":[{"ID":"r1","AllowedMethods":["GET"],"AllowedOrigins":["*"],"MaxAgeSeconds":600}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.cors_rules()[0];
        assert_eq!(r.id(), Some("r1"));
        assert_eq!(r.max_age_seconds(), Some(600));
    }

    #[test]
    fn cors_into_sdk_preserves_expose_headers() {
        let json = r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"],"ExposeHeaders":["x-amz-id-2"]}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(
            cfg.cors_rules()[0].expose_headers(),
            &["x-amz-id-2".to_string()]
        );
    }

    #[test]
    fn cors_invalid_json_errors() {
        assert!(serde_json::from_str::<CorsConfigurationJson>("{not json").is_err());
    }

    #[test]
    fn cors_missing_cors_rules_errors() {
        assert!(serde_json::from_str::<CorsConfigurationJson>("{}").is_err());
    }

    // ----- PublicAccessBlockConfigurationJson -----

    #[test]
    fn pab_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "BlockPublicAcls": true,
          "IgnorePublicAcls": true,
          "BlockPublicPolicy": true,
          "RestrictPublicBuckets": true
        }"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.BlockPublicAcls, Some(true));
        assert_eq!(parsed.RestrictPublicBuckets, Some(true));
    }

    #[test]
    fn pab_into_sdk_all_true() {
        let json = r#"{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":true,"RestrictPublicBuckets":true}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(true));
        assert_eq!(cfg.ignore_public_acls(), Some(true));
        assert_eq!(cfg.block_public_policy(), Some(true));
        assert_eq!(cfg.restrict_public_buckets(), Some(true));
    }

    #[test]
    fn pab_into_sdk_absent_fields_default_to_false() {
        let json = r#"{}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(false));
        assert_eq!(cfg.ignore_public_acls(), Some(false));
        assert_eq!(cfg.block_public_policy(), Some(false));
        assert_eq!(cfg.restrict_public_buckets(), Some(false));
    }

    #[test]
    fn pab_into_sdk_partial_input() {
        let json = r#"{"BlockPublicAcls":true,"BlockPublicPolicy":true}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(true));
        assert_eq!(cfg.ignore_public_acls(), Some(false));
        assert_eq!(cfg.block_public_policy(), Some(true));
        assert_eq!(cfg.restrict_public_buckets(), Some(false));
    }

    #[test]
    fn pab_invalid_json_errors() {
        assert!(serde_json::from_str::<PublicAccessBlockConfigurationJson>("{not json").is_err());
    }

    // ----- WebsiteConfigurationJson -----

    #[test]
    fn website_parses_minimal_index_document_shape() {
        let json = r#"{
          "IndexDocument": { "Suffix": "index.html" }
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(
            parsed.IndexDocument.as_ref().map(|d| d.Suffix.as_str()),
            Some("index.html")
        );
        assert!(parsed.ErrorDocument.is_none());
        assert!(parsed.RedirectAllRequestsTo.is_none());
        assert!(parsed.RoutingRules.is_none());
    }

    #[test]
    fn website_into_sdk_preserves_index_document() {
        let json = r#"{"IndexDocument":{"Suffix":"index.html"}}"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.index_document().map(|d| d.suffix()), Some("index.html"));
        assert!(cfg.error_document().is_none());
        assert!(cfg.redirect_all_requests_to().is_none());
        assert!(cfg.routing_rules().is_empty());
    }

    #[test]
    fn website_into_sdk_preserves_index_and_error_documents() {
        let json = r#"{
          "IndexDocument": { "Suffix": "index.html" },
          "ErrorDocument": { "Key": "error.html" }
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.index_document().map(|d| d.suffix()), Some("index.html"));
        assert_eq!(cfg.error_document().map(|d| d.key()), Some("error.html"));
    }

    #[test]
    fn website_into_sdk_preserves_redirect_all_requests_to_with_protocol() {
        let json = r#"{
          "RedirectAllRequestsTo": { "HostName": "example.com", "Protocol": "https" }
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = cfg.redirect_all_requests_to().expect("redirect");
        assert_eq!(r.host_name(), "example.com");
        assert_eq!(
            r.protocol().map(aws_sdk_s3::types::Protocol::as_str),
            Some("https")
        );
    }

    #[test]
    fn website_into_sdk_preserves_redirect_all_requests_to_without_protocol() {
        let json = r#"{
          "RedirectAllRequestsTo": { "HostName": "example.com" }
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = cfg.redirect_all_requests_to().expect("redirect");
        assert_eq!(r.host_name(), "example.com");
        assert!(r.protocol().is_none());
    }

    #[test]
    fn website_into_sdk_preserves_routing_rules_with_condition_and_redirect() {
        let json = r#"{
          "IndexDocument": { "Suffix": "index.html" },
          "RoutingRules": [
            {
              "Condition": {
                "HttpErrorCodeReturnedEquals": "404",
                "KeyPrefixEquals": "docs/"
              },
              "Redirect": {
                "HostName": "new.example.com",
                "HttpRedirectCode": "301",
                "Protocol": "https",
                "ReplaceKeyPrefixWith": "documents/"
              }
            }
          ]
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rules = cfg.routing_rules();
        assert_eq!(rules.len(), 1);
        let cond = rules[0].condition().expect("condition");
        assert_eq!(cond.http_error_code_returned_equals(), Some("404"));
        assert_eq!(cond.key_prefix_equals(), Some("docs/"));
        let red = rules[0].redirect().expect("redirect");
        assert_eq!(red.host_name(), Some("new.example.com"));
        assert_eq!(red.http_redirect_code(), Some("301"));
        assert_eq!(
            red.protocol().map(aws_sdk_s3::types::Protocol::as_str),
            Some("https")
        );
        assert_eq!(red.replace_key_prefix_with(), Some("documents/"));
        assert!(red.replace_key_with().is_none());
    }

    #[test]
    fn website_into_sdk_routing_rule_redirect_only_with_replace_key_with() {
        let json = r#"{
          "IndexDocument": { "Suffix": "index.html" },
          "RoutingRules": [
            { "Redirect": { "ReplaceKeyWith": "error.html" } }
          ]
        }"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rules = cfg.routing_rules();
        assert_eq!(rules.len(), 1);
        assert!(rules[0].condition().is_none());
        let red = rules[0].redirect().expect("redirect");
        assert_eq!(red.replace_key_with(), Some("error.html"));
    }

    #[test]
    fn website_empty_object_parses_and_sdk_is_empty() {
        // Both top-level shapes are optional; an empty object parses
        // (S3 will reject it server-side as a malformed config).
        let json = r#"{}"#;
        let parsed: WebsiteConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.index_document().is_none());
        assert!(cfg.error_document().is_none());
        assert!(cfg.redirect_all_requests_to().is_none());
        assert!(cfg.routing_rules().is_empty());
    }

    #[test]
    fn website_invalid_json_errors() {
        assert!(serde_json::from_str::<WebsiteConfigurationJson>("{not json").is_err());
    }

    // ----- BucketLoggingStatusJson -----

    #[test]
    fn logging_empty_object_parses_and_sdk_is_empty() {
        // Empty input disables logging — top-level `LoggingEnabled` is omitted.
        let json = r#"{}"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        assert!(parsed.LoggingEnabled.is_none());
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.logging_enabled().is_none());
    }

    #[test]
    fn logging_parses_minimal_enabled_shape() {
        let json = r#"{
          "LoggingEnabled": {
            "TargetBucket": "log-bucket",
            "TargetPrefix": "logs/"
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let le = parsed.LoggingEnabled.as_ref().expect("LoggingEnabled");
        assert_eq!(le.TargetBucket, "log-bucket");
        assert_eq!(le.TargetPrefix, "logs/");
        assert!(le.TargetObjectKeyFormat.is_none());
    }

    #[test]
    fn logging_into_sdk_preserves_target_bucket_and_prefix() {
        let json = r#"{"LoggingEnabled":{"TargetBucket":"log-bucket","TargetPrefix":"logs/"}}"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let le = cfg.logging_enabled().expect("logging_enabled");
        assert_eq!(le.target_bucket(), "log-bucket");
        assert_eq!(le.target_prefix(), "logs/");
        assert!(le.target_object_key_format().is_none());
    }

    #[test]
    fn logging_into_sdk_preserves_simple_prefix_marker() {
        let json = r#"{
          "LoggingEnabled": {
            "TargetBucket": "log-bucket",
            "TargetPrefix": "logs/",
            "TargetObjectKeyFormat": { "SimplePrefix": {} }
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let fmt = cfg
            .logging_enabled()
            .unwrap()
            .target_object_key_format()
            .expect("target_object_key_format");
        assert!(fmt.simple_prefix().is_some());
        assert!(fmt.partitioned_prefix().is_none());
    }

    #[test]
    fn logging_into_sdk_preserves_partitioned_prefix_with_event_time() {
        let json = r#"{
          "LoggingEnabled": {
            "TargetBucket": "log-bucket",
            "TargetPrefix": "logs/",
            "TargetObjectKeyFormat": {
              "PartitionedPrefix": { "PartitionDateSource": "EventTime" }
            }
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let pp = cfg
            .logging_enabled()
            .unwrap()
            .target_object_key_format()
            .unwrap()
            .partitioned_prefix()
            .expect("partitioned_prefix");
        assert_eq!(
            pp.partition_date_source().map(PartitionDateSource::as_str),
            Some("EventTime")
        );
    }

    #[test]
    fn logging_into_sdk_preserves_partitioned_prefix_with_delivery_time() {
        let json = r#"{
          "LoggingEnabled": {
            "TargetBucket": "log-bucket",
            "TargetPrefix": "logs/",
            "TargetObjectKeyFormat": {
              "PartitionedPrefix": { "PartitionDateSource": "DeliveryTime" }
            }
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let pp = cfg
            .logging_enabled()
            .unwrap()
            .target_object_key_format()
            .unwrap()
            .partitioned_prefix()
            .expect("partitioned_prefix");
        assert_eq!(
            pp.partition_date_source().map(PartitionDateSource::as_str),
            Some("DeliveryTime")
        );
    }

    #[test]
    fn logging_into_sdk_partitioned_prefix_without_date_source() {
        let json = r#"{
          "LoggingEnabled": {
            "TargetBucket": "log-bucket",
            "TargetPrefix": "logs/",
            "TargetObjectKeyFormat": { "PartitionedPrefix": {} }
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let pp = cfg
            .logging_enabled()
            .unwrap()
            .target_object_key_format()
            .unwrap()
            .partitioned_prefix()
            .expect("partitioned_prefix");
        assert!(pp.partition_date_source().is_none());
    }

    #[test]
    fn logging_missing_target_bucket_errors_at_parse() {
        // serde reports the missing required field at deserialise time.
        let json = r#"{"LoggingEnabled":{"TargetPrefix":"logs/"}}"#;
        assert!(serde_json::from_str::<BucketLoggingStatusJson>(json).is_err());
    }

    #[test]
    fn logging_missing_target_prefix_errors_at_parse() {
        let json = r#"{"LoggingEnabled":{"TargetBucket":"log-bucket"}}"#;
        assert!(serde_json::from_str::<BucketLoggingStatusJson>(json).is_err());
    }

    #[test]
    fn logging_invalid_json_errors() {
        assert!(serde_json::from_str::<BucketLoggingStatusJson>("{not json").is_err());
    }

    // ----- NotificationConfigurationJson -----

    #[test]
    fn notification_empty_object_parses_and_sdk_is_empty() {
        // Empty input disables every notification — every field omitted.
        let json = r#"{}"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        assert!(parsed.TopicConfigurations.is_none());
        assert!(parsed.QueueConfigurations.is_none());
        assert!(parsed.LambdaFunctionConfigurations.is_none());
        assert!(parsed.EventBridgeConfiguration.is_none());
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.topic_configurations.is_none());
        assert!(cfg.queue_configurations.is_none());
        assert!(cfg.lambda_function_configurations.is_none());
        assert!(cfg.event_bridge_configuration().is_none());
    }

    #[test]
    fn notification_parses_topic_configuration() {
        let json = r#"{
          "TopicConfigurations": [
            {
              "Id": "t1",
              "TopicArn": "arn:aws:sns:us-east-1:111111111111:topic-x",
              "Events": ["s3:ObjectCreated:*"]
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let topics = cfg
            .topic_configurations
            .as_ref()
            .expect("topic_configurations");
        assert_eq!(topics.len(), 1);
        assert_eq!(topics[0].id(), Some("t1"));
        assert_eq!(
            topics[0].topic_arn(),
            "arn:aws:sns:us-east-1:111111111111:topic-x"
        );
        assert_eq!(topics[0].events().len(), 1);
        assert_eq!(topics[0].events()[0].as_str(), "s3:ObjectCreated:*");
    }

    #[test]
    fn notification_parses_queue_configuration() {
        let json = r#"{
          "QueueConfigurations": [
            {
              "QueueArn": "arn:aws:sqs:us-east-1:111111111111:queue-x",
              "Events": ["s3:ObjectRemoved:Delete"]
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let queues = cfg
            .queue_configurations
            .as_ref()
            .expect("queue_configurations");
        assert_eq!(queues.len(), 1);
        assert_eq!(
            queues[0].queue_arn(),
            "arn:aws:sqs:us-east-1:111111111111:queue-x"
        );
        assert_eq!(queues[0].events()[0].as_str(), "s3:ObjectRemoved:Delete");
    }

    #[test]
    fn notification_parses_lambda_configuration() {
        let json = r#"{
          "LambdaFunctionConfigurations": [
            {
              "LambdaFunctionArn": "arn:aws:lambda:us-east-1:111111111111:function:fn-x",
              "Events": ["s3:ObjectCreated:Put"]
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let lambdas = cfg
            .lambda_function_configurations
            .as_ref()
            .expect("lambda_function_configurations");
        assert_eq!(lambdas.len(), 1);
        assert_eq!(
            lambdas[0].lambda_function_arn(),
            "arn:aws:lambda:us-east-1:111111111111:function:fn-x"
        );
    }

    #[test]
    fn notification_parses_event_bridge_marker() {
        let json = r#"{ "EventBridgeConfiguration": {} }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        assert!(parsed.EventBridgeConfiguration.is_some());
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.event_bridge_configuration().is_some());
    }

    #[test]
    fn notification_parses_filter_with_prefix_and_suffix() {
        let json = r#"{
          "TopicConfigurations": [
            {
              "TopicArn": "arn:aws:sns:us-east-1:111111111111:topic-x",
              "Events": ["s3:ObjectCreated:*"],
              "Filter": {
                "Key": {
                  "FilterRules": [
                    { "Name": "prefix", "Value": "images/" },
                    { "Name": "suffix", "Value": ".jpg" }
                  ]
                }
              }
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let topic = &cfg.topic_configurations.as_ref().unwrap()[0];
        let key = topic.filter().unwrap().key().expect("key filter");
        let rules = key.filter_rules();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0].name().map(|n| n.as_str()), Some("prefix"));
        assert_eq!(rules[0].value(), Some("images/"));
        assert_eq!(rules[1].name().map(|n| n.as_str()), Some("suffix"));
        assert_eq!(rules[1].value(), Some(".jpg"));
    }

    #[test]
    fn notification_topic_missing_arn_errors_at_parse() {
        let json = r#"{
          "TopicConfigurations": [
            { "Events": ["s3:ObjectCreated:*"] }
          ]
        }"#;
        assert!(serde_json::from_str::<NotificationConfigurationJson>(json).is_err());
    }

    #[test]
    fn notification_queue_missing_arn_errors_at_parse() {
        let json = r#"{
          "QueueConfigurations": [
            { "Events": ["s3:ObjectCreated:*"] }
          ]
        }"#;
        assert!(serde_json::from_str::<NotificationConfigurationJson>(json).is_err());
    }

    #[test]
    fn notification_lambda_missing_arn_errors_at_parse() {
        let json = r#"{
          "LambdaFunctionConfigurations": [
            { "Events": ["s3:ObjectCreated:*"] }
          ]
        }"#;
        assert!(serde_json::from_str::<NotificationConfigurationJson>(json).is_err());
    }

    #[test]
    fn notification_invalid_json_errors() {
        assert!(serde_json::from_str::<NotificationConfigurationJson>("{not json").is_err());
    }

    // ----- LifecycleRuleJson::into_sdk additional branches -----

    #[test]
    fn lifecycle_rule_with_deprecated_prefix_into_sdk_preserves_it() {
        // The `Prefix` field directly on a rule (not inside Filter) is the
        // pre-2018 deprecated S3 form. We keep it for AWS-CLI input shape parity.
        let json =
            r#"{"Rules":[{"Status":"Enabled","Prefix":"old-style/","Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        #[allow(deprecated)]
        let p = cfg.rules()[0].prefix();
        assert_eq!(p, Some("old-style/"));
    }

    #[test]
    fn lifecycle_rule_filter_with_tag_into_sdk_preserves_key_value() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"Tag":{"Key":"team","Value":"platform"}},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let tag = cfg.rules()[0].filter().unwrap().tag().expect("tag filter");
        assert_eq!(tag.key(), "team");
        assert_eq!(tag.value(), "platform");
    }

    #[test]
    fn lifecycle_expiration_with_date_into_sdk_parses_rfc3339() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Expiration":{"Date":"2030-01-02T03:04:05Z"}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let exp = cfg.rules()[0].expiration().expect("expiration");
        let date = exp.date().expect("date set");
        // The SDK DateTime parses 2030-01-02T03:04:05Z as 1_893_553_445 epoch seconds.
        assert_eq!(date.secs(), 1_893_553_445);
    }

    #[test]
    fn lifecycle_expiration_with_expired_object_delete_marker_into_sdk_preserves_it() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Expiration":{"ExpiredObjectDeleteMarker":true}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let exp = cfg.rules()[0].expiration().expect("expiration");
        assert_eq!(exp.expired_object_delete_marker(), Some(true));
    }

    #[test]
    fn lifecycle_transition_with_date_into_sdk_parses_rfc3339() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Transitions":[{"Date":"2030-01-02T03:04:05Z","StorageClass":"GLACIER"}]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let t = &cfg.rules()[0].transitions()[0];
        let date = t.date().expect("date set");
        assert_eq!(date.secs(), 1_893_553_445);
        assert_eq!(t.storage_class(), Some(&TransitionStorageClass::Glacier));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_noncurrent_version_transitions() {
        // Drives `NoncurrentVersionTransitionJson::into_sdk` via the top-level
        // builder. Covers both the `NoncurrentDays` and `StorageClass` branches.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionTransitions":[
              {"NoncurrentDays":7,"StorageClass":"GLACIER"}
            ]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let nvts = cfg.rules()[0].noncurrent_version_transitions();
        assert_eq!(nvts.len(), 1);
        assert_eq!(nvts[0].noncurrent_days(), Some(7));
        assert_eq!(
            nvts[0].storage_class(),
            Some(&TransitionStorageClass::Glacier)
        );
    }

    #[test]
    fn lifecycle_into_sdk_noncurrent_version_transition_omits_absent_fields() {
        // Empty `NoncurrentVersionTransition` body — both inner Option arms are
        // `None`. Exercises the no-setter branch of the impl.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionTransitions":[ {} ]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let nvt = &cfg.rules()[0].noncurrent_version_transitions()[0];
        assert!(nvt.noncurrent_days().is_none());
        assert!(nvt.storage_class().is_none());
    }

    // ----- NotificationConfigurationJson::into_sdk additional branches -----

    #[test]
    fn notification_queue_into_sdk_preserves_id_and_filter() {
        // Covers QueueConfigurationJson::into_sdk Id (line 782) and Filter
        // (line 785) Option branches.
        let json = r#"{
          "QueueConfigurations":[
            {
              "Id":"q1",
              "QueueArn":"arn:aws:sqs:us-east-1:111111111111:queue-x",
              "Events":["s3:ObjectCreated:*"],
              "Filter":{"Key":{"FilterRules":[{"Name":"prefix","Value":"a/"}]}}
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let q = &cfg.queue_configurations.as_ref().unwrap()[0];
        assert_eq!(q.id(), Some("q1"));
        let key = q.filter().unwrap().key().expect("key filter");
        assert_eq!(key.filter_rules().len(), 1);
    }

    #[test]
    fn notification_lambda_into_sdk_preserves_id_and_filter() {
        // Covers LambdaFunctionConfigurationJson::into_sdk Id (line 802) and
        // Filter (line 805) Option branches.
        let json = r#"{
          "LambdaFunctionConfigurations":[
            {
              "Id":"l1",
              "LambdaFunctionArn":"arn:aws:lambda:us-east-1:111111111111:function:fn-x",
              "Events":["s3:ObjectCreated:Put"],
              "Filter":{"Key":{"FilterRules":[{"Name":"suffix","Value":".jpg"}]}}
            }
          ]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let l = &cfg.lambda_function_configurations.as_ref().unwrap()[0];
        assert_eq!(l.id(), Some("l1"));
        let key = l.filter().unwrap().key().expect("key filter");
        assert_eq!(key.filter_rules().len(), 1);
    }
}
