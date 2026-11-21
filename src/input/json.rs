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
//! Every mirror carries `#[serde(deny_unknown_fields)]`, matching the AWS
//! CLI's "Unknown parameter" rejection. Without it a misspelled or wrongly
//! nested key is dropped silently and the `put-*` command then replaces the
//! whole bucket configuration with the truncated result — e.g. feeding the
//! wrapped `{"PublicAccessBlockConfiguration": {…}}` shape emitted by
//! `get-public-access-block` straight back into `put-public-access-block`
//! would parse as "all four flags absent" and disable every protection.
//!
//! Per-resource structs are added by their respective family tasks.

use crate::types::datetime::has_rfc3339_separator;
use anyhow::Result;
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, AccessControlTranslation, BlockedEncryptionTypes,
    BucketLifecycleConfiguration, BucketLoggingStatus, BucketLogsPermission, Condition,
    CorsConfiguration, CorsRule, DeleteMarkerReplication, DeleteMarkerReplicationStatus,
    Destination, EncryptionConfiguration, EncryptionType, ErrorDocument, Event,
    EventBridgeConfiguration, ExistingObjectReplication, ExistingObjectReplicationStatus,
    ExpirationStatus, FilterRule, FilterRuleName, Grantee, IndexDocument,
    LambdaFunctionConfiguration, LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator,
    LifecycleRuleFilter, LoggingEnabled, Metrics, MetricsStatus, NoncurrentVersionExpiration,
    NoncurrentVersionTransition, NotificationConfiguration, NotificationConfigurationFilter,
    OwnerOverride, PartitionDateSource, PartitionedPrefix, Protocol,
    PublicAccessBlockConfiguration, QueueConfiguration, Redirect, RedirectAllRequestsTo,
    ReplicaModifications, ReplicaModificationsStatus, ReplicationConfiguration, ReplicationRule,
    ReplicationRuleAndOperator, ReplicationRuleFilter, ReplicationRuleStatus, ReplicationTime,
    ReplicationTimeStatus, ReplicationTimeValue, RoutingRule, S3KeyFilter, ServerSideEncryption,
    ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
    SimplePrefix, SourceSelectionCriteria, SseKmsEncryptedObjects, SseKmsEncryptedObjectsStatus,
    StorageClass, Tag as SdkTag, TargetGrant, TargetObjectKeyFormat, TopicConfiguration,
    Transition, TransitionStorageClass, Type as GranteeType, WebsiteConfiguration,
};
use aws_smithy_types::DateTime;
use aws_smithy_types_convert::date_time::DateTimeExt;
use serde::Deserialize;
use std::borrow::Cow;
use time::OffsetDateTime;
use time::format_description::well_known::{Rfc2822, Rfc3339};

/// Mirror of `BucketLifecycleConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-lifecycle-configuration` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct LifecycleConfigurationJson {
    pub Rules: Vec<LifecycleRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct LifecycleRuleFilterJson {
    pub Prefix: Option<String>,
    pub Tag: Option<TagJson>,
    pub ObjectSizeGreaterThan: Option<i64>,
    pub ObjectSizeLessThan: Option<i64>,
    pub And: Option<LifecycleRuleAndOperatorJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct LifecycleRuleAndOperatorJson {
    pub Prefix: Option<String>,
    pub Tags: Option<Vec<TagJson>>,
    pub ObjectSizeGreaterThan: Option<i64>,
    pub ObjectSizeLessThan: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct TagJson {
    pub Key: String,
    pub Value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct LifecycleExpirationJson {
    /// Timestamp: an ISO 8601 / RFC 2822 string or epoch seconds.
    pub Date: Option<TimestampJson>,
    pub Days: Option<i32>,
    pub ExpiredObjectDeleteMarker: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionExpirationJson {
    pub NoncurrentDays: Option<i32>,
    pub NewerNoncurrentVersions: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct TransitionJson {
    pub Date: Option<TimestampJson>,
    pub Days: Option<i32>,
    pub StorageClass: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionTransitionJson {
    pub NoncurrentDays: Option<i32>,
    pub StorageClass: Option<String>,
    pub NewerNoncurrentVersions: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
        // S3 lifecycle Filter is a one-of (Prefix XOR Tag XOR ObjectSize* XOR
        // And); when multiple are supplied, the SDK builder accepts the last
        // setter. We honour AWS-CLI semantics by passing values as-is and
        // letting S3 reject bad combinations.
        let mut b = LifecycleRuleFilter::builder();
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(t) = self.Tag {
            b = b.tag(SdkTag::builder().key(t.Key).value(t.Value).build()?);
        }
        if let Some(n) = self.ObjectSizeGreaterThan {
            b = b.object_size_greater_than(n);
        }
        if let Some(n) = self.ObjectSizeLessThan {
            b = b.object_size_less_than(n);
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
        if let Some(n) = self.ObjectSizeGreaterThan {
            b = b.object_size_greater_than(n);
        }
        if let Some(n) = self.ObjectSizeLessThan {
            b = b.object_size_less_than(n);
        }
        Ok(b.build())
    }
}

impl LifecycleExpirationJson {
    fn into_sdk(self) -> Result<LifecycleExpiration> {
        let mut b = LifecycleExpiration::builder();
        if let Some(d) = self.Date {
            b = b.date(d.into_sdk()?);
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
        if let Some(n) = self.NewerNoncurrentVersions {
            b = b.newer_noncurrent_versions(n);
        }
        b.build()
    }
}

impl TransitionJson {
    fn into_sdk(self) -> Result<Transition> {
        let mut b = Transition::builder();
        if let Some(d) = self.Date {
            b = b.date(d.into_sdk()?);
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
        if let Some(n) = self.NewerNoncurrentVersions {
            b = b.newer_noncurrent_versions(n);
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

/// Parse a `put-*` JSON document into one of this module's mirror structs.
///
/// Identical to `serde_json::from_str`, except that a repeated object key
/// takes its last value instead of failing with `duplicate field` — what the
/// AWS CLI (Python's `json`) does, and what RFC 8259 leaves to the parser.
///
/// The document is first parsed directly, which keeps the line/column in
/// every ordinary error. Only when that fails on the data (not the syntax) is
/// it re-read through `serde_json::Value`, whose maps keep the last value of a
/// repeated key. If that succeeds, the failure was caused by a duplicate; if
/// not, the document is wrong either way and the more useful of the two
/// errors is returned.
pub fn from_json_str<T: serde::de::DeserializeOwned>(body: &str) -> serde_json::Result<T> {
    let first = match serde_json::from_str::<T>(body) {
        Ok(v) => return Ok(v),
        Err(e) if !e.is_data() => return Err(e),
        Err(e) => e,
    };
    serde_json::from_str::<serde_json::Value>(body)
        .and_then(serde_json::from_value::<T>)
        .map_err(|second| {
            if first.to_string().starts_with("duplicate field") {
                second
            } else {
                first
            }
        })
}

/// A timestamp member (`Expiration.Date`, `Transitions[].Date`) as the AWS CLI
/// accepts it: either a string or a JSON number of seconds since the Unix
/// epoch.
#[derive(Debug, Clone, PartialEq)]
pub enum TimestampJson {
    Text(String),
    EpochSeconds(f64),
}

impl From<String> for TimestampJson {
    fn from(s: String) -> Self {
        TimestampJson::Text(s)
    }
}

impl From<&str> for TimestampJson {
    fn from(s: &str) -> Self {
        TimestampJson::Text(s.to_string())
    }
}

impl<'de> Deserialize<'de> for TimestampJson {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct V;
        impl serde::de::Visitor<'_> for V {
            type Value = TimestampJson;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a timestamp string or a number of seconds since the Unix epoch")
            }
            fn visit_str<E>(self, v: &str) -> std::result::Result<TimestampJson, E> {
                Ok(TimestampJson::Text(v.to_string()))
            }
            fn visit_i64<E>(self, v: i64) -> std::result::Result<TimestampJson, E> {
                Ok(integer_timestamp(v.unsigned_abs(), v.to_string(), v as f64))
            }
            fn visit_u64<E>(self, v: u64) -> std::result::Result<TimestampJson, E> {
                Ok(integer_timestamp(v, v.to_string(), v as f64))
            }
            fn visit_f64<E>(self, v: f64) -> std::result::Result<TimestampJson, E> {
                Ok(TimestampJson::EpochSeconds(v))
            }
        }
        d.deserialize_any(V)
    }
}

/// An eight-digit JSON integer is handed on as text, so that it gets the same
/// reading as the eight-digit string: the ISO 8601 basic date (`20240101`),
/// never an epoch, and an error when it is not a valid date. See
/// [`parse_epoch_string`].
fn integer_timestamp(magnitude: u64, text: String, epoch: f64) -> TimestampJson {
    if (10_000_000..=99_999_999).contains(&magnitude) {
        TimestampJson::Text(text)
    } else {
        TimestampJson::EpochSeconds(epoch)
    }
}

impl TimestampJson {
    fn into_sdk(self) -> Result<DateTime> {
        match self {
            TimestampJson::Text(s) => parse_timestamp(&s),
            TimestampJson::EpochSeconds(v) => epoch_seconds_to_sdk(v)
                .ok_or_else(|| anyhow::anyhow!("timestamp {v} is out of range (years 0000-9999)")),
        }
    }
}

/// Seconds since the Unix epoch -> SDK `DateTime`, limited to the years
/// 0000-9999 that the wire format can spell.
fn epoch_seconds_to_sdk(v: f64) -> Option<DateTime> {
    const MIN: f64 = -62_167_219_200.0; // 0000-01-01T00:00:00Z
    const MAX: f64 = 253_402_300_800.0; // 10000-01-01T00:00:00Z (exclusive)
    if !(MIN..MAX).contains(&v) {
        return None; // also rejects NaN and the infinities
    }
    let secs = v.floor();
    // `from_secs_and_nanos` panics on nanos >= 1e9; float rounding must not
    // be able to reach it.
    let nanos = (((v - secs) * 1e9) as u32).min(999_999_999);
    Some(DateTime::from_secs_and_nanos(secs as i64, nanos))
}

fn parse_timestamp(s: &str) -> Result<DateTime> {
    let invalid = || {
        anyhow::anyhow!(
            "invalid timestamp {s:?}: expected an ISO 8601 date (\"2024-01-01\") or date-time \
             (\"2024-01-01T00:00:00Z\", \"2024-01-01T00:00:00+09:00\"), an RFC 2822 date \
             (\"Mon, 01 Jan 2024 00:00:00 GMT\"), or seconds since the Unix epoch"
        )
    };
    // S3 has no use for a leap second (`23:59:60`), so reject it like the
    // AWS CLI does. `time` would otherwise accept one at the end of a month
    // and silently read it as `23:59:59.999999999`.
    let leap_second =
        || anyhow::anyhow!("invalid timestamp {s:?}: leap seconds (\":60\") are not supported");
    match parse_iso8601(s) {
        Some(Iso8601::Leap) => return Err(leap_second()),
        Some(Iso8601::Parsed(dt)) => return Ok(DateTime::from_time(dt)),
        None => {}
    }
    let rfc2822 = expand_rfc2822_three_digit_year(s);
    if let Ok(dt) = OffsetDateTime::parse(&rfc2822, &Rfc2822) {
        if !rfc2822_weekday_matches(&rfc2822, &dt) {
            return Err(anyhow::anyhow!(
                "invalid timestamp {s:?}: the day of the week does not match the date"
            ));
        }
        // RFC 2822 has no fractional seconds, so the only way to get this
        // value is the leap-second stand-in described above.
        if dt.nanosecond() == 999_999_999 {
            return Err(leap_second());
        }
        return Ok(DateTime::from_time(dt));
    }
    if is_rfc2822_leap_second(&rfc2822) {
        return Err(leap_second());
    }
    parse_epoch_string(s)
        .and_then(epoch_seconds_to_sdk)
        .ok_or_else(invalid)
}

/// Rewrite a three-digit RFC 2822 year as four digits by adding 1900
/// (`Mon, 01 Jan 124 ...` -> `Mon, 01 Jan 2024 ...`), as RFC 2822 section 4.3
/// prescribes for the obsolete year syntax and as `chrono` did. `time` accepts
/// only two- and four-digit years.
///
/// Only the token right after the month name is considered; anything else is
/// returned unchanged and left for the RFC 2822 parser to judge.
fn expand_rfc2822_three_digit_year(s: &str) -> Cow<'_, str> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let mut tokens = s.split_ascii_whitespace();
    // `time` matches month names case-insensitively, so do the same here.
    let Some(year) = tokens
        .by_ref()
        .find(|t| MONTHS.iter().any(|m| m.eq_ignore_ascii_case(t)))
        .and(tokens.next())
    else {
        return Cow::Borrowed(s);
    };
    if year.len() != 3 || !year.bytes().all(|b| b.is_ascii_digit()) {
        return Cow::Borrowed(s);
    }
    // `year` is a subslice of `s`, so this is its byte offset within `s`.
    let start = year.as_ptr() as usize - s.as_ptr() as usize;
    let expanded = 1900 + year.parse::<u16>().expect("three ASCII digits");
    Cow::Owned(format!("{}{expanded}{}", &s[..start], &s[start + 3..]))
}

/// Whether the optional day of the week of an RFC 2822 date-time (`Mon, ...`)
/// names the day `dt` falls on. `time` parses the weekday but never checks it,
/// whereas `chrono` rejected a mismatch, and so does the AWS CLI; a mismatch
/// means the date was mistyped. Input without a weekday always matches.
fn rfc2822_weekday_matches(s: &str, dt: &OffsetDateTime) -> bool {
    let Some((name, _)) = s.trim_start().split_once(',') else {
        return true;
    };
    // `dt` keeps the offset as written, so this is the weekday of the date as
    // written, which is what the weekday name refers to.
    let expected = dt.weekday().to_string();
    name.eq_ignore_ascii_case(&expected[..3])
}

/// Whether `s` is an RFC 2822 date-time that would be valid if its seconds
/// (`HH:MM:60`) were `59`. `time` accepts a leap second only at the end of a
/// month and rejects any other as malformed, which would otherwise surface as
/// the generic "invalid timestamp" error rather than the leap-second one.
fn is_rfc2822_leap_second(s: &str) -> bool {
    s.match_indices(":60").any(|(i, _)| {
        // Only the seconds field: `:60` must follow `HH:MM`, so that minute 60
        // (`12:60:00`) is not mistaken for a leap second.
        i >= 3
            && s.as_bytes()[i - 3] == b':'
            && OffsetDateTime::parse(&format!("{}:59{}", &s[..i], &s[i + 3..]), &Rfc2822).is_ok()
    })
}

/// A string holding a plain decimal number is read as seconds since the Unix
/// epoch, as the AWS CLI does.
///
/// One deliberate difference: exactly eight digits is the ISO 8601 basic date
/// (`20240101`), never an epoch. The AWS CLI reads that string as 1970-08-23,
/// which is not what anyone writing it means; and a mistyped basic date
/// (`20241301`) must stay an error rather than quietly become a 1970 date.
fn parse_epoch_string(s: &str) -> Option<f64> {
    let digits = s.strip_prefix(['-', '+']).unwrap_or(s);
    let plain_decimal = !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit() || b == b'.')
        && digits.bytes().any(|b| b.is_ascii_digit());
    if !plain_decimal || (digits.len() == 8 && !digits.contains('.')) {
        return None;
    }
    s.parse::<f64>().ok()
}

/// Parse the ISO 8601 profiles S3 documents for Lifecycle/Transition `Date`,
/// normalising to UTC.
///
/// Parsing is done with `time`'s RFC 3339 parser rather than the smithy
/// `Format::DateTime` parser: the latter accepts only a trailing `Z`, but
/// ISO 8601 also admits numeric offsets — and
/// `get-bucket-lifecycle-configuration` itself emits `+00:00`, so the smithy
/// parser broke the GET -> PUT round-trip.
///
/// RFC 3339 alone is not enough either: it is a strict subset of ISO 8601 and
/// rejects the basic format (`20240101T000000Z`), colon-less offsets
/// (`+0000`), reduced-precision times (`T00:00Z`), a comma as the decimal
/// sign, and a date-time with no offset at all — all of which the AWS CLI
/// accepts, so rejecting them would turn valid input into an error.
fn parse_iso8601(s: &str) -> Option<Iso8601> {
    // The common case, and what our own `get-bucket-lifecycle-configuration`
    // emits. `time` accepts any byte between date and time, so check the
    // separator first; anything else goes through the normaliser.
    if has_rfc3339_separator(s)
        && let Some(parsed) = parse_rfc3339(s)
    {
        return Some(parsed);
    }
    parse_rfc3339(&normalise_iso8601(s)?)
}

/// The outcome of [`parse_iso8601`] for an input in ISO 8601 shape.
enum Iso8601 {
    Parsed(OffsetDateTime),
    /// A leap second (`:60`), which is rejected rather than parsed.
    Leap,
}

/// Parse a string already in RFC 3339 form. A leap second is reported as
/// [`Iso8601::Leap`] wherever it falls; `time` itself accepts one only at the
/// end of a month, and reads it as `23:59:59.999999999`.
fn parse_rfc3339(s: &str) -> Option<Iso8601> {
    let dt = OffsetDateTime::parse(s, &Rfc3339).ok();
    // RFC 3339 spells the seconds at a fixed position: `YYYY-MM-DDTHH:MM:SS`.
    // Checked only on otherwise-valid shapes, so that e.g. `T00:60:00` (minute
    // 60) is not mistaken for a leap second.
    let leap = s.get(17..19) == Some("60")
        && OffsetDateTime::parse(&format!("{}59{}", &s[..17], &s[19..]), &Rfc3339).is_ok();
    if leap {
        return Some(Iso8601::Leap);
    }
    dt.map(Iso8601::Parsed)
}

/// Rewrite the ISO 8601 spellings RFC 3339 rejects into RFC 3339 form, so that
/// a single strict parser still validates every accepted input. Handles:
///
/// - the basic format (`20240101`, `20240101T000000Z`);
/// - a time without seconds or minutes (`T00:00`, `T0000`, `T00`);
/// - a comma as the decimal sign (`T00:00:00,500Z`);
/// - a space (or lowercase `t`) between date and time, and a space before the
///   offset (`2024-01-01 00:00:00 +09:00`);
/// - offsets written `+09` or `+0900`;
/// - a date-time with no offset at all (read as UTC — what the AWS CLI does,
///   and the only zone S3 renders the field in).
///
/// Returns `None` when the input is not one of those forms. Rewriting only
/// moves separators around: the result is still handed to the RFC 3339 parser,
/// so a well-shaped but nonsensical value such as `20241301` is rejected there
/// rather than smuggled through, and loose spellings like `2030-1-2` — which
/// ISO 8601 does not admit — keep failing as before.
fn normalise_iso8601(s: &str) -> Option<String> {
    fn all_digits(b: &[u8]) -> bool {
        b.iter().all(u8::is_ascii_digit)
    }
    /// Split a time-of-day from its trailing zone designator, if any.
    fn split_offset(tail: &str) -> (&str, &str) {
        if tail.ends_with(['Z', 'z']) {
            return (&tail[..tail.len() - 1], "Z");
        }
        // A time-of-day never contains `+`/`-`, so either introduces the offset.
        match tail.rfind(['+', '-']) {
            Some(i) => (&tail[..i], &tail[i..]),
            None => (tail, ""),
        }
    }
    /// `HH`, `HH:MM`, `HH:MM:SS[.frac]` and their colon-less basic forms ->
    /// `HH:MM:SS[.frac]`. A fraction is taken only on the seconds: ISO 8601
    /// also allows fractional hours and minutes, but nobody writes those.
    fn expand_time(time: &str) -> Option<String> {
        let (hms, frac) = match time.find(['.', ',']) {
            Some(i) => (&time[..i], Some(&time[i + 1..])),
            None => (time, None),
        };
        let parts: Vec<&str> = if hms.contains(':') {
            hms.split(':').collect()
        } else {
            match hms.len() {
                2 => vec![hms],
                4 => vec![&hms[..2], &hms[2..]],
                6 => vec![&hms[..2], &hms[2..4], &hms[4..]],
                _ => return None,
            }
        };
        if parts.len() > 3
            || parts
                .iter()
                .any(|p| p.len() != 2 || !all_digits(p.as_bytes()))
        {
            return None;
        }
        let field = |i: usize| parts.get(i).copied().unwrap_or("00");
        let hms = format!("{}:{}:{}", field(0), field(1), field(2));
        match frac {
            None => Some(hms),
            Some(f) if parts.len() == 3 && !f.is_empty() && all_digits(f.as_bytes()) => {
                Some(format!("{hms}.{f}"))
            }
            Some(_) => None,
        }
    }

    // The input is arbitrary user text and everything below slices by byte
    // offset; restricting to ASCII makes every offset a char boundary. (No
    // ISO 8601 form needs more.)
    if !s.is_ascii() {
        return None;
    }

    let expand_date = |d: &str| -> String {
        if d.len() == 8 && all_digits(d.as_bytes()) {
            format!("{}-{}-{}", &d[0..4], &d[4..6], &d[6..8])
        } else {
            d.to_string()
        }
    };

    // A bare calendar date, basic or extended: promote to midnight UTC.
    let b = s.as_bytes();
    if (b.len() == 8 && all_digits(b)) || (b.len() == 10 && b[4] == b'-' && b[7] == b'-') {
        return Some(format!("{}T00:00:00Z", expand_date(s)));
    }

    let sep = s.find(['T', 't', ' '])?;
    let (date, tail) = (&s[..sep], s[sep + 1..].trim());
    let (time, offset) = split_offset(tail);
    let time = expand_time(time.trim_end())?;

    // Anything that fails the digit guards is passed through unchanged for
    // the RFC 3339 parser to reject.
    let ob = offset.as_bytes();
    let offset = match ob.len() {
        0 => "Z".to_string(),                                 // no zone given
        3 if all_digits(&ob[1..3]) => format!("{offset}:00"), // `+09`
        5 if all_digits(&ob[1..5]) => format!("{}:{}", &offset[..3], &offset[3..]), // `+0900`
        _ => offset.to_string(),                              // `Z`, `+09:00`, …
    };

    Some(format!("{}T{}{}", expand_date(date), time, offset))
}

/// Mirror of `ServerSideEncryptionConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-encryption` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionConfigurationJson {
    pub Rules: Vec<ServerSideEncryptionRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionRuleJson {
    pub ApplyServerSideEncryptionByDefault: Option<ApplyServerSideEncryptionByDefaultJson>,
    pub BucketKeyEnabled: Option<bool>,
    pub BlockedEncryptionTypes: Option<BlockedEncryptionTypesJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ApplyServerSideEncryptionByDefaultJson {
    /// `AES256` or `aws:kms` or `aws:kms:dsse`.
    pub SSEAlgorithm: String,
    pub KMSMasterKeyID: Option<String>,
}

/// Mirror of `BlockedEncryptionTypes`. `EncryptionType` values are
/// `NONE` and `SSE-C` (currently only SSE-C is supported by S3).
/// Unknown values are passed through to the SDK enum; S3 will reject
/// them server-side.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct BlockedEncryptionTypesJson {
    pub EncryptionType: Option<Vec<String>>,
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
        if let Some(bet) = self.BlockedEncryptionTypes {
            let mut bb = BlockedEncryptionTypes::builder();
            if let Some(types) = bet.EncryptionType {
                for t in types {
                    bb = bb.encryption_type(EncryptionType::from(t.as_str()));
                }
            }
            b = b.blocked_encryption_types(bb.build());
        }
        Ok(b.build())
    }
}

/// Mirror of `CorsConfiguration` for the AWS-CLI input shape.
/// Top-level field name `CORSRules` matches the AWS CLI exactly
/// (note the all-uppercase `CORS`).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct CorsConfigurationJson {
    pub CORSRules: Vec<CorsRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct WebsiteConfigurationJson {
    pub IndexDocument: Option<IndexDocumentJson>,
    pub ErrorDocument: Option<ErrorDocumentJson>,
    pub RedirectAllRequestsTo: Option<RedirectAllRequestsToJson>,
    pub RoutingRules: Option<Vec<RoutingRuleJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct IndexDocumentJson {
    pub Suffix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ErrorDocumentJson {
    pub Key: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct RedirectAllRequestsToJson {
    pub HostName: String,
    pub Protocol: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct RoutingRuleJson {
    pub Condition: Option<ConditionJson>,
    pub Redirect: RedirectJson,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ConditionJson {
    pub HttpErrorCodeReturnedEquals: Option<String>,
    pub KeyPrefixEquals: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
/// `TargetGrants` is supported for AWS-CLI shape parity. Buckets using the
/// bucket-owner-enforced Object Ownership setting reject target grants
/// server-side, but the field is accepted at parse time so users can lift
/// existing AWS-CLI inputs unchanged.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct BucketLoggingStatusJson {
    pub LoggingEnabled: Option<LoggingEnabledJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct LoggingEnabledJson {
    pub TargetBucket: String,
    pub TargetPrefix: String,
    pub TargetObjectKeyFormat: Option<TargetObjectKeyFormatJson>,
    pub TargetGrants: Option<Vec<TargetGrantJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct TargetGrantJson {
    pub Grantee: Option<GranteeJson>,
    /// `FULL_CONTROL`, `READ`, or `WRITE`. Unknown values are passed
    /// through to the SDK enum; S3 will reject them server-side.
    pub Permission: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct GranteeJson {
    pub DisplayName: Option<String>,
    pub EmailAddress: Option<String>,
    pub ID: Option<String>,
    pub URI: Option<String>,
    /// `CanonicalUser`, `AmazonCustomerByEmail`, or `Group`. Unknown
    /// values are passed through to the SDK enum; S3 will reject them
    /// server-side.
    pub Type: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct TargetObjectKeyFormatJson {
    pub SimplePrefix: Option<SimplePrefixJson>,
    pub PartitionedPrefix: Option<PartitionedPrefixJson>,
}

/// Marker shape for `SimplePrefix` — set to `{}` in the input JSON to
/// select the simple key format. The SDK uses a unit-style struct.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct SimplePrefixJson {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
        if let Some(grants) = self.TargetGrants {
            for g in grants {
                b = b.target_grants(g.into_sdk()?);
            }
        }
        Ok(b.build()?)
    }
}

impl TargetGrantJson {
    fn into_sdk(self) -> Result<TargetGrant> {
        let mut b = TargetGrant::builder();
        if let Some(g) = self.Grantee {
            b = b.grantee(g.into_sdk()?);
        }
        if let Some(p) = self.Permission {
            b = b.permission(BucketLogsPermission::from(p.as_str()));
        }
        Ok(b.build())
    }
}

impl GranteeJson {
    fn into_sdk(self) -> Result<Grantee> {
        let mut b = Grantee::builder().r#type(GranteeType::from(self.Type.as_str()));
        if let Some(v) = self.DisplayName {
            b = b.display_name(v);
        }
        if let Some(v) = self.EmailAddress {
            b = b.email_address(v);
        }
        if let Some(v) = self.ID {
            b = b.id(v);
        }
        if let Some(v) = self.URI {
            b = b.uri(v);
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
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct NotificationConfigurationJson {
    pub TopicConfigurations: Option<Vec<TopicConfigurationJson>>,
    pub QueueConfigurations: Option<Vec<QueueConfigurationJson>>,
    pub LambdaFunctionConfigurations: Option<Vec<LambdaFunctionConfigurationJson>>,
    pub EventBridgeConfiguration: Option<EventBridgeConfigurationJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct QueueConfigurationJson {
    pub Id: Option<String>,
    pub QueueArn: String,
    pub Events: Vec<String>,
    pub Filter: Option<NotificationConfigurationFilterJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct EventBridgeConfigurationJson {}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct NotificationConfigurationFilterJson {
    pub Key: Option<S3KeyFilterJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct S3KeyFilterJson {
    pub FilterRules: Option<Vec<FilterRuleJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct FilterRuleJson {
    /// `prefix` or `suffix`. Unknown values are passed through to the SDK
    /// enum; S3 will reject them server-side. Both members are optional in
    /// the S3 model.
    pub Name: Option<String>,
    pub Value: Option<String>,
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
            .set_name(self.Name.as_deref().map(FilterRuleName::from))
            .set_value(self.Value)
            .build()
    }
}

/// Mirror of `ReplicationConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-replication` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationConfigurationJson {
    pub Role: String,
    pub Rules: Vec<ReplicationRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationRuleJson {
    pub ID: Option<String>,
    pub Priority: Option<i32>,
    /// Deprecated S3 field, kept for AWS-CLI shape parity.
    pub Prefix: Option<String>,
    pub Filter: Option<ReplicationRuleFilterJson>,
    /// `Enabled` or `Disabled`.
    pub Status: String,
    pub SourceSelectionCriteria: Option<SourceSelectionCriteriaJson>,
    pub ExistingObjectReplication: Option<ExistingObjectReplicationJson>,
    pub Destination: DestinationJson,
    pub DeleteMarkerReplication: Option<DeleteMarkerReplicationJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationRuleFilterJson {
    pub Prefix: Option<String>,
    pub Tag: Option<TagJson>,
    pub And: Option<ReplicationRuleAndOperatorJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationRuleAndOperatorJson {
    pub Prefix: Option<String>,
    pub Tags: Option<Vec<TagJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct SourceSelectionCriteriaJson {
    pub SseKmsEncryptedObjects: Option<SseKmsEncryptedObjectsJson>,
    pub ReplicaModifications: Option<ReplicaModificationsJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct SseKmsEncryptedObjectsJson {
    pub Status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicaModificationsJson {
    pub Status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ExistingObjectReplicationJson {
    pub Status: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct DestinationJson {
    pub Bucket: String,
    pub Account: Option<String>,
    pub StorageClass: Option<String>,
    pub AccessControlTranslation: Option<AccessControlTranslationJson>,
    pub EncryptionConfiguration: Option<EncryptionConfigurationJson>,
    pub ReplicationTime: Option<ReplicationTimeJson>,
    pub Metrics: Option<MetricsJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct AccessControlTranslationJson {
    pub Owner: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct EncryptionConfigurationJson {
    pub ReplicaKmsKeyID: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationTimeJson {
    pub Status: String,
    pub Time: ReplicationTimeValueJson,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct ReplicationTimeValueJson {
    /// Optional in the S3 model; S3 currently accepts only `15`.
    pub Minutes: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct MetricsJson {
    pub Status: String,
    pub EventThreshold: Option<ReplicationTimeValueJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(non_snake_case)]
pub struct DeleteMarkerReplicationJson {
    /// Optional in the S3 model.
    pub Status: Option<String>,
}

impl ReplicationConfigurationJson {
    /// Build the SDK `ReplicationConfiguration`. Returns the SDK builder
    /// error verbatim so error messages match what the AWS CLI would emit.
    pub fn into_sdk(self) -> Result<ReplicationConfiguration> {
        let rules: Result<Vec<ReplicationRule>> = self
            .Rules
            .into_iter()
            .map(ReplicationRuleJson::into_sdk)
            .collect();
        Ok(ReplicationConfiguration::builder()
            .role(self.Role)
            .set_rules(Some(rules?))
            .build()?)
    }
}

impl ReplicationRuleJson {
    #[allow(deprecated)]
    fn into_sdk(self) -> Result<ReplicationRule> {
        let mut b = ReplicationRule::builder()
            .status(ReplicationRuleStatus::from(self.Status.as_str()))
            .destination(self.Destination.into_sdk()?);
        if let Some(id) = self.ID {
            b = b.id(id);
        }
        if let Some(p) = self.Priority {
            b = b.priority(p);
        }
        if let Some(prefix) = self.Prefix {
            b = b.prefix(prefix);
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk()?);
        }
        if let Some(ssc) = self.SourceSelectionCriteria {
            b = b.source_selection_criteria(ssc.into_sdk()?);
        }
        if let Some(eor) = self.ExistingObjectReplication {
            b = b.existing_object_replication(
                ExistingObjectReplication::builder()
                    .status(ExistingObjectReplicationStatus::from(eor.Status.as_str()))
                    .build()?,
            );
        }
        if let Some(dmr) = self.DeleteMarkerReplication {
            b = b.delete_marker_replication(
                DeleteMarkerReplication::builder()
                    .set_status(
                        dmr.Status
                            .as_deref()
                            .map(DeleteMarkerReplicationStatus::from),
                    )
                    .build(),
            );
        }
        Ok(b.build()?)
    }
}

impl ReplicationRuleFilterJson {
    fn into_sdk(self) -> Result<ReplicationRuleFilter> {
        let mut b = ReplicationRuleFilter::builder();
        if let Some(prefix) = self.Prefix {
            b = b.prefix(prefix);
        }
        if let Some(t) = self.Tag {
            b = b.tag(SdkTag::builder().key(t.Key).value(t.Value).build()?);
        }
        if let Some(a) = self.And {
            let mut ab = ReplicationRuleAndOperator::builder();
            if let Some(prefix) = a.Prefix {
                ab = ab.prefix(prefix);
            }
            if let Some(tags) = a.Tags {
                let sdk_tags: Result<Vec<SdkTag>> = tags
                    .into_iter()
                    .map(|t| Ok(SdkTag::builder().key(t.Key).value(t.Value).build()?))
                    .collect();
                ab = ab.set_tags(Some(sdk_tags?));
            }
            b = b.and(ab.build());
        }
        Ok(b.build())
    }
}

impl SourceSelectionCriteriaJson {
    fn into_sdk(self) -> Result<SourceSelectionCriteria> {
        let mut b = SourceSelectionCriteria::builder();
        if let Some(sse) = self.SseKmsEncryptedObjects {
            b = b.sse_kms_encrypted_objects(
                SseKmsEncryptedObjects::builder()
                    .status(SseKmsEncryptedObjectsStatus::from(sse.Status.as_str()))
                    .build()?,
            );
        }
        if let Some(rm) = self.ReplicaModifications {
            b = b.replica_modifications(
                ReplicaModifications::builder()
                    .status(ReplicaModificationsStatus::from(rm.Status.as_str()))
                    .build()?,
            );
        }
        Ok(b.build())
    }
}

impl DestinationJson {
    fn into_sdk(self) -> Result<Destination> {
        let mut b = Destination::builder().bucket(self.Bucket);
        if let Some(a) = self.Account {
            b = b.account(a);
        }
        if let Some(sc) = self.StorageClass {
            b = b.storage_class(StorageClass::from(sc.as_str()));
        }
        if let Some(act) = self.AccessControlTranslation {
            b = b.access_control_translation(
                AccessControlTranslation::builder()
                    .owner(OwnerOverride::from(act.Owner.as_str()))
                    .build()?,
            );
        }
        if let Some(ec) = self.EncryptionConfiguration {
            let mut eb = EncryptionConfiguration::builder();
            if let Some(k) = ec.ReplicaKmsKeyID {
                eb = eb.replica_kms_key_id(k);
            }
            b = b.encryption_configuration(eb.build());
        }
        if let Some(rt) = self.ReplicationTime {
            b = b.replication_time(
                ReplicationTime::builder()
                    .status(ReplicationTimeStatus::from(rt.Status.as_str()))
                    .time(
                        ReplicationTimeValue::builder()
                            .set_minutes(rt.Time.Minutes)
                            .build(),
                    )
                    .build()?,
            );
        }
        if let Some(metrics) = self.Metrics {
            let mut mb = Metrics::builder().status(MetricsStatus::from(metrics.Status.as_str()));
            if let Some(et) = metrics.EventThreshold {
                mb = mb.event_threshold(
                    ReplicationTimeValue::builder()
                        .set_minutes(et.Minutes)
                        .build(),
                );
            }
            b = b.metrics(mb.build()?);
        }
        Ok(b.build()?)
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

    /// Feeding the wrapped shape emitted by `get-public-access-block` back into
    /// `put-public-access-block` must be rejected, not silently read as "all four
    /// flags absent" — which `into_sdk` would turn into `false` for every field,
    /// disabling all public-access protection on the bucket.
    #[test]
    fn pab_wrapped_get_output_shape_is_rejected_not_silently_emptied() {
        let get_output_shape = r#"{
          "PublicAccessBlockConfiguration": {
            "BlockPublicAcls": true,
            "IgnorePublicAcls": true,
            "BlockPublicPolicy": true,
            "RestrictPublicBuckets": true
          }
        }"#;

        let err = serde_json::from_str::<PublicAccessBlockConfigurationJson>(get_output_shape)
            .expect_err("wrapped get-output shape must not parse as a put input");
        assert!(
            err.to_string().contains("PublicAccessBlockConfiguration"),
            "error must name the offending key, got: {err}"
        );
    }

    /// A misspelled key must fail loudly rather than being dropped and replacing
    /// the bucket's configuration with the truncated remainder.
    ///
    /// Covers every top-level type consumed by a `put-*` subcommand, plus a
    /// nested mirror (the lifecycle rule filter) to prove the rejection is not
    /// limited to the outermost struct.
    #[test]
    fn unknown_fields_are_rejected_across_mirrors() {
        assert!(
            serde_json::from_str::<PublicAccessBlockConfigurationJson>(
                r#"{"BlockPublicAcls":true,"BlockPublicAcl":true}"#
            )
            .is_err(),
            "misspelled PAB field must be rejected"
        );
        assert!(
            serde_json::from_str::<LifecycleConfigurationJson>(
                r#"{"Rules":[{"Status":"Enabled","Filter":{"prefix":"logs/"}}]}"#
            )
            .is_err(),
            "lower-cased lifecycle Filter key must be rejected, not read as an empty filter \
             matching every object in the bucket"
        );
        assert!(
            serde_json::from_str::<BucketLoggingStatusJson>(r#"{"LoggingEnable":{}}"#).is_err(),
            "misspelled logging wrapper must be rejected, not read as 'disable logging'"
        );
        assert!(
            serde_json::from_str::<NotificationConfigurationJson>(r#"{"TopicConfiguration":[]}"#)
                .is_err(),
            "misspelled notification key must be rejected, not read as 'remove all notifications'"
        );
        assert!(
            serde_json::from_str::<CorsConfigurationJson>(
                r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"],"MaxAge":100}]}"#
            )
            .is_err(),
            "misspelled CORS rule field (MaxAge vs MaxAgeSeconds) must be rejected"
        );
        assert!(
            serde_json::from_str::<ServerSideEncryptionConfigurationJson>(
                r#"{"Rules":[{"BucketKeyEnabled":true}],"Rule":[]}"#
            )
            .is_err(),
            "misspelled encryption key must be rejected"
        );
        assert!(
            serde_json::from_str::<ReplicationConfigurationJson>(
                r#"{"Role":"arn:aws:iam::1:role/r","Rules":[],"Roles":[]}"#
            )
            .is_err(),
            "misspelled replication key must be rejected"
        );
        assert!(
            serde_json::from_str::<WebsiteConfigurationJson>(
                r#"{"IndexDocument":{"Suffix":"index.html"},"ErrorDocuments":{}}"#
            )
            .is_err(),
            "misspelled website key must be rejected, not silently dropped"
        );
    }

    /// The corresponding positive case: the exact AWS-CLI input shape for every
    /// top-level `put-*` type must still parse after `deny_unknown_fields`.
    /// Guards against the fix over-rejecting legitimate configuration.
    #[test]
    fn deny_unknown_fields_does_not_reject_valid_aws_cli_shapes() {
        serde_json::from_str::<PublicAccessBlockConfigurationJson>(
            r#"{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":true,"RestrictPublicBuckets":true}"#,
        )
        .expect("valid PAB shape must parse");
        serde_json::from_str::<LifecycleConfigurationJson>(
            r#"{"Rules":[{"ID":"r","Status":"Enabled","Filter":{"Prefix":"logs/"},"Expiration":{"Days":30}}]}"#,
        )
        .expect("valid lifecycle shape must parse");
        serde_json::from_str::<CorsConfigurationJson>(
            r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"],"MaxAgeSeconds":100}]}"#,
        )
        .expect("valid CORS shape must parse");
        serde_json::from_str::<ServerSideEncryptionConfigurationJson>(
            r#"{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},"BucketKeyEnabled":true}]}"#,
        )
        .expect("valid encryption shape must parse");
        serde_json::from_str::<ReplicationConfigurationJson>(
            r#"{"Role":"arn:aws:iam::1:role/r","Rules":[{"Status":"Enabled","Destination":{"Bucket":"arn:aws:s3:::b"}}]}"#,
        )
        .expect("valid replication shape must parse");
        serde_json::from_str::<WebsiteConfigurationJson>(
            r#"{"IndexDocument":{"Suffix":"index.html"},"ErrorDocument":{"Key":"error.html"}}"#,
        )
        .expect("valid website shape must parse");
        serde_json::from_str::<BucketLoggingStatusJson>(
            r#"{"LoggingEnabled":{"TargetBucket":"b","TargetPrefix":"logs/"}}"#,
        )
        .expect("valid logging shape must parse");
        serde_json::from_str::<NotificationConfigurationJson>(
            r#"{"TopicConfigurations":[{"TopicArn":"arn:aws:sns:us-east-1:1:t","Events":["s3:ObjectCreated:*"]}]}"#,
        )
        .expect("valid notification shape must parse");
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

    // ----- ReplicationConfigurationJson -----

    #[test]
    fn replication_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/replication",
          "Rules": [
            {
              "Status": "Enabled",
              "Destination": { "Bucket": "arn:aws:s3:::dest-bucket" }
            }
          ]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.Role, "arn:aws:iam::111111111111:role/replication");
        assert_eq!(parsed.Rules.len(), 1);
    }

    #[test]
    fn replication_into_sdk_minimal_preserves_role_and_rule() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.role(), "arn:aws:iam::111111111111:role/r");
        let rules = cfg.rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].status(), &ReplicationRuleStatus::Enabled);
        assert_eq!(rules[0].destination().unwrap().bucket(), "arn:aws:s3:::d");
    }

    #[test]
    fn replication_into_sdk_preserves_id_priority_prefix() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "ID": "my-rule",
            "Priority": 5,
            "Prefix": "logs/",
            "Status": "Disabled",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        assert_eq!(r.id(), Some("my-rule"));
        assert_eq!(r.priority(), Some(5));
        #[allow(deprecated)]
        let prefix_value = r.prefix();
        assert_eq!(prefix_value, Some("logs/"));
        assert_eq!(r.status(), &ReplicationRuleStatus::Disabled);
    }

    #[test]
    fn replication_into_sdk_filter_prefix() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Filter": { "Prefix": "logs/" },
            "Status": "Enabled",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().unwrap();
        assert_eq!(f.prefix(), Some("logs/"));
    }

    #[test]
    fn replication_into_sdk_filter_with_tag() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Filter": { "Tag": { "Key": "env", "Value": "prod" } },
            "Status": "Enabled",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let t = cfg.rules()[0].filter().unwrap().tag().unwrap();
        assert_eq!(t.key(), "env");
        assert_eq!(t.value(), "prod");
    }

    #[test]
    fn replication_into_sdk_filter_with_and_prefix_and_tags() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Filter": { "And": { "Prefix": "p/", "Tags": [{"Key":"a","Value":"1"}] } },
            "Status": "Enabled",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().unwrap();
        assert_eq!(and.prefix(), Some("p/"));
        assert_eq!(and.tags().len(), 1);
        assert_eq!(and.tags()[0].key(), "a");
    }

    #[test]
    fn replication_into_sdk_source_selection_criteria() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "SourceSelectionCriteria": {
              "SseKmsEncryptedObjects": { "Status": "Enabled" },
              "ReplicaModifications": { "Status": "Enabled" }
            },
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let ssc = cfg.rules()[0].source_selection_criteria().unwrap();
        assert_eq!(
            ssc.sse_kms_encrypted_objects().unwrap().status(),
            &SseKmsEncryptedObjectsStatus::Enabled
        );
        assert_eq!(
            ssc.replica_modifications().unwrap().status(),
            &ReplicaModificationsStatus::Enabled
        );
    }

    #[test]
    fn replication_into_sdk_existing_object_replication() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "ExistingObjectReplication": { "Status": "Enabled" },
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let eor = cfg.rules()[0].existing_object_replication().unwrap();
        assert_eq!(eor.status(), &ExistingObjectReplicationStatus::Enabled);
    }

    #[test]
    fn replication_into_sdk_delete_marker_replication() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "DeleteMarkerReplication": { "Status": "Disabled" },
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let dmr = cfg.rules()[0].delete_marker_replication().unwrap();
        assert_eq!(dmr.status(), Some(&DeleteMarkerReplicationStatus::Disabled));
    }

    #[test]
    fn replication_into_sdk_destination_with_account_and_storage_class() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "Account": "222222222222",
              "StorageClass": "STANDARD_IA"
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let d = cfg.rules()[0].destination().unwrap();
        assert_eq!(d.account(), Some("222222222222"));
        assert_eq!(d.storage_class(), Some(&StorageClass::StandardIa));
    }

    #[test]
    fn replication_into_sdk_destination_with_access_control_translation() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "AccessControlTranslation": { "Owner": "Destination" }
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let act = cfg.rules()[0]
            .destination()
            .unwrap()
            .access_control_translation()
            .unwrap();
        assert_eq!(act.owner().as_str(), "Destination");
    }

    #[test]
    fn replication_into_sdk_destination_with_encryption_configuration() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "EncryptionConfiguration": { "ReplicaKmsKeyID": "arn:aws:kms:us-east-1:1:key/abc" }
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let ec = cfg.rules()[0]
            .destination()
            .unwrap()
            .encryption_configuration()
            .unwrap();
        assert_eq!(
            ec.replica_kms_key_id(),
            Some("arn:aws:kms:us-east-1:1:key/abc")
        );
    }

    #[test]
    fn replication_into_sdk_destination_with_replication_time_and_metrics() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "ReplicationTime": { "Status": "Enabled", "Time": { "Minutes": 15 } },
              "Metrics": { "Status": "Enabled", "EventThreshold": { "Minutes": 15 } }
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let d = cfg.rules()[0].destination().unwrap();
        let rt = d.replication_time().unwrap();
        assert_eq!(rt.status(), &ReplicationTimeStatus::Enabled);
        assert_eq!(rt.time().unwrap().minutes(), Some(15));
        let m = d.metrics().unwrap();
        assert_eq!(m.status(), &MetricsStatus::Enabled);
        assert_eq!(m.event_threshold().unwrap().minutes(), Some(15));
    }

    #[test]
    fn replication_invalid_json_errors() {
        let res: Result<ReplicationConfigurationJson, _> = serde_json::from_str("{not json");
        assert!(res.is_err());
    }

    #[test]
    fn replication_missing_role_errors() {
        let res: Result<ReplicationConfigurationJson, _> = serde_json::from_str(r#"{"Rules":[]}"#);
        assert!(res.is_err(), "missing required `Role` must error");
    }

    #[test]
    fn replication_missing_rules_errors() {
        let res: Result<ReplicationConfigurationJson, _> = serde_json::from_str(r#"{"Role":"r"}"#);
        assert!(res.is_err(), "missing required `Rules` must error");
    }

    #[test]
    fn replication_missing_destination_in_rule_errors() {
        let res: Result<ReplicationConfigurationJson, _> =
            serde_json::from_str(r#"{"Role":"r","Rules":[{"Status":"Enabled"}]}"#);
        assert!(res.is_err(), "missing required `Destination` must error");
    }

    #[test]
    fn replication_missing_status_in_rule_errors() {
        let res: Result<ReplicationConfigurationJson, _> =
            serde_json::from_str(r#"{"Role":"r","Rules":[{"Destination":{"Bucket":"d"}}]}"#);
        assert!(res.is_err(), "missing required `Status` must error");
    }

    #[test]
    fn replication_into_sdk_unknown_status_passes_through() {
        // Unknown enum values are passed through to the SDK; S3 rejects
        // server-side. Mirrors how lifecycle/notification handle unknowns.
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "UnknownState",
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        // ReplicationRuleStatus::Unknown(...) — exact variant is opaque, but
        // as_str round-trips back to the original value.
        assert_eq!(cfg.rules()[0].status().as_str(), "UnknownState");
    }

    // ----- Lifecycle Filter ObjectSize predicates -----

    #[test]
    fn lifecycle_filter_with_object_size_greater_than_into_sdk() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"ObjectSizeGreaterThan": 1048576},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().expect("filter");
        assert_eq!(f.object_size_greater_than(), Some(1_048_576));
        assert!(f.object_size_less_than().is_none());
    }

    #[test]
    fn lifecycle_filter_with_object_size_less_than_into_sdk() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"ObjectSizeLessThan": 5368709120},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().expect("filter");
        assert_eq!(f.object_size_less_than(), Some(5_368_709_120));
    }

    #[test]
    fn lifecycle_filter_and_with_object_size_predicates_into_sdk() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"And":{
              "Prefix":"data/",
              "Tags":[{"Key":"a","Value":"1"}],
              "ObjectSizeGreaterThan": 1024,
              "ObjectSizeLessThan": 1073741824
            }},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().expect("and");
        assert_eq!(and.prefix(), Some("data/"));
        assert_eq!(and.tags().len(), 1);
        assert_eq!(and.object_size_greater_than(), Some(1024));
        assert_eq!(and.object_size_less_than(), Some(1_073_741_824));
    }

    // ----- NewerNoncurrentVersions -----

    #[test]
    fn lifecycle_noncurrent_version_expiration_with_newer_versions_into_sdk() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionExpiration":{"NoncurrentDays":30,"NewerNoncurrentVersions":3}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let n = cfg.rules()[0]
            .noncurrent_version_expiration()
            .expect("noncurrent version expiration");
        assert_eq!(n.noncurrent_days(), Some(30));
        assert_eq!(n.newer_noncurrent_versions(), Some(3));
    }

    #[test]
    fn lifecycle_noncurrent_version_transition_with_newer_versions_into_sdk() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionTransitions":[
              {"NoncurrentDays":7,"StorageClass":"GLACIER","NewerNoncurrentVersions":2}
            ]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let nvt = &cfg.rules()[0].noncurrent_version_transitions()[0];
        assert_eq!(nvt.noncurrent_days(), Some(7));
        assert_eq!(nvt.newer_noncurrent_versions(), Some(2));
    }

    // ----- BlockedEncryptionTypes round-trip -----

    #[test]
    fn encryption_with_blocked_encryption_types_into_sdk() {
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},
            "BlockedEncryptionTypes":{"EncryptionType":["SSE-C"]}
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        let bet = r.blocked_encryption_types().expect("blocked types");
        assert_eq!(bet.encryption_type().len(), 1);
        assert_eq!(bet.encryption_type()[0].as_str(), "SSE-C");
    }

    #[test]
    fn encryption_with_blocked_encryption_types_empty_list_into_sdk() {
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},
            "BlockedEncryptionTypes":{}
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        let bet = r.blocked_encryption_types().expect("blocked types");
        assert!(bet.encryption_type().is_empty());
    }

    #[test]
    fn encryption_with_blocked_encryption_types_multiple_into_sdk() {
        // Multiple values, including the unknown-pass-through case.
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"},
            "BlockedEncryptionTypes":{"EncryptionType":["SSE-C","NONE","FUTURE"]}
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let bet = cfg.rules()[0]
            .blocked_encryption_types()
            .expect("blocked types");
        let strs: Vec<&str> = bet.encryption_type().iter().map(|e| e.as_str()).collect();
        assert_eq!(strs, vec!["SSE-C", "NONE", "FUTURE"]);
    }

    #[test]
    fn encryption_without_blocked_encryption_types_into_sdk_is_none() {
        // Absent in JSON ⇒ SDK rule has no blocked_encryption_types.
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.rules()[0].blocked_encryption_types().is_none());
    }

    // ----- Lifecycle Filter ObjectSize predicates: absence ⇒ SDK None -----

    #[test]
    fn lifecycle_filter_without_object_size_predicates_into_sdk_is_none() {
        // Absent in JSON ⇒ SDK filter has neither object-size predicate set.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"Prefix":"data/"},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().expect("filter");
        assert!(f.object_size_greater_than().is_none());
        assert!(f.object_size_less_than().is_none());
    }

    #[test]
    fn lifecycle_filter_and_without_object_size_predicates_into_sdk_is_none() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"And":{"Prefix":"data/","Tags":[{"Key":"a","Value":"1"}]}},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().expect("and");
        assert!(and.object_size_greater_than().is_none());
        assert!(and.object_size_less_than().is_none());
    }

    // ----- NewerNoncurrentVersions: absence ⇒ SDK None -----

    #[test]
    fn lifecycle_noncurrent_version_expiration_without_newer_versions_into_sdk_is_none() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionExpiration":{"NoncurrentDays":30}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let n = cfg.rules()[0]
            .noncurrent_version_expiration()
            .expect("noncurrent version expiration");
        assert_eq!(n.noncurrent_days(), Some(30));
        assert!(n.newer_noncurrent_versions().is_none());
    }

    #[test]
    fn lifecycle_noncurrent_version_transition_without_newer_versions_into_sdk_is_none() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "NoncurrentVersionTransitions":[{"NoncurrentDays":7,"StorageClass":"GLACIER"}]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let nvt = &cfg.rules()[0].noncurrent_version_transitions()[0];
        assert!(nvt.newer_noncurrent_versions().is_none());
    }

    // ----- parse_rfc3339: ISO 8601 date-only support -----

    #[test]
    fn lifecycle_expiration_with_date_only_into_sdk_treats_as_midnight_utc() {
        // S3 documents Lifecycle `Date` as ISO 8601, which permits a bare
        // `YYYY-MM-DD`; the parser must lift it to midnight UTC.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Expiration":{"Date":"2030-01-02"}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let date = cfg.rules()[0]
            .expiration()
            .expect("expiration")
            .date()
            .expect("date set");
        // 2030-01-02T00:00:00Z == 1_893_542_400 epoch seconds.
        assert_eq!(date.secs(), 1_893_542_400);
    }

    #[test]
    fn lifecycle_transition_with_date_only_into_sdk_treats_as_midnight_utc() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Transitions":[{"Date":"2030-01-02","StorageClass":"GLACIER"}]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let date = cfg.rules()[0].transitions()[0].date().expect("date set");
        assert_eq!(date.secs(), 1_893_542_400);
    }

    #[test]
    fn lifecycle_expiration_full_rfc3339_still_parses() {
        // Regression: the date-only branch must not reject full timestamps.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Expiration":{"Date":"2030-01-02T03:04:05Z"}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let date = cfg.rules()[0]
            .expiration()
            .expect("expiration")
            .date()
            .expect("date set");
        assert_eq!(date.secs(), 1_893_553_445);
    }

    #[test]
    fn lifecycle_expiration_rfc3339_with_utc_offset_parses() {
        // `get-bucket-lifecycle-configuration` emits `+00:00` rather than
        // `Z`; feeding its output back in must round-trip.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Expiration":{"Date":"2030-01-02T03:04:05+00:00"}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let date = cfg.rules()[0]
            .expiration()
            .expect("expiration")
            .date()
            .expect("date set");
        assert_eq!(date.secs(), 1_893_553_445);
    }

    #[test]
    fn lifecycle_transition_rfc3339_with_non_utc_offset_normalises_to_utc() {
        // Any ISO 8601 numeric offset is accepted and converted to UTC:
        // 2030-01-02T12:04:05+09:00 == 2030-01-02T03:04:05Z.
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Transitions":[{"Date":"2030-01-02T12:04:05+09:00","StorageClass":"GLACIER"}]
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let date = cfg.rules()[0].transitions()[0].date().expect("date set");
        assert_eq!(date.secs(), 1_893_553_445);
    }

    // ----- ISO 8601 spellings RFC 3339 alone rejects -----

    /// Every one of these is valid ISO 8601 and accepted by `aws s3api`, so
    /// rejecting them would turn valid input into an error. All denote the
    /// same instant as `2030-01-02T03:04:05Z` (epoch 1_893_553_445).
    #[test]
    fn lifecycle_expiration_accepts_iso8601_beyond_rfc3339() {
        for date in [
            "2030-01-02T03:04:05Z",     // RFC 3339 (baseline)
            "2030-01-02T03:04:05",      // no offset -> read as UTC
            "2030-01-02T03:04:05+0000", // offset without a colon
            "2030-01-02T03:04:05+00",   // hour-only offset
            "20300102T030405Z",         // basic format
            "20300102T030405",          // basic format, no offset
            "20300102T030405+0000",     // basic format, colon-less offset
            "2030-01-02T12:04:05+0900", // colon-less non-UTC offset
            "20300102T120405+0900",     // basic format, non-UTC offset
        ] {
            let json =
                format!(r#"{{"Rules":[{{"Status":"Enabled","Expiration":{{"Date":"{date}"}}}}]}}"#);
            let parsed: LifecycleConfigurationJson = serde_json::from_str(&json).unwrap();
            let cfg = parsed
                .into_sdk()
                .unwrap_or_else(|e| panic!("{date} must be accepted: {e}"));
            assert_eq!(
                cfg.rules()[0].expiration().unwrap().date().unwrap().secs(),
                1_893_553_445,
                "{date} must normalise to 2030-01-02T03:04:05Z"
            );
        }
    }

    /// A bare calendar date in the basic format is midnight UTC, matching the
    /// extended-format `2030-01-02` behaviour.
    #[test]
    fn lifecycle_expiration_basic_format_date_only_is_midnight_utc() {
        let json = r#"{"Rules":[{"Status":"Enabled","Expiration":{"Date":"20300102"}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(
            cfg.rules()[0].expiration().unwrap().date().unwrap().secs(),
            1_893_542_400
        );
    }

    /// Normalising must not become a way to smuggle nonsense past validation:
    /// the rewritten string is still parsed strictly.
    #[test]
    fn lifecycle_well_shaped_but_invalid_dates_still_error() {
        for date in [
            "20301301",              // month 13, basic format
            "20300230T000000Z",      // 30 February
            "2030-01-02T25:04:05Z",  // hour 25
            "20300102T030405+9999",  // impossible offset
            "2030-01-02T03:04:05+0", // truncated offset
            "T030405Z",              // no date
            "2030-01-02T",           // no time
            "20300102030405",        // no date/time separator
        ] {
            let json =
                format!(r#"{{"Rules":[{{"Status":"Enabled","Expiration":{{"Date":"{date}"}}}}]}}"#);
            let parsed: LifecycleConfigurationJson = serde_json::from_str(&json).unwrap();
            assert!(
                parsed.into_sdk().is_err(),
                "{date} must be rejected, not silently normalised"
            );
        }
    }

    /// `Date` is arbitrary user input, so the normaliser must never panic —
    /// notably it must not byte-slice through a multi-byte char.
    #[test]
    fn lifecycle_hostile_dates_error_without_panicking() {
        for date in [
            // 5-byte offsets whose byte 3 falls *inside* a char: the exact
            // shape that panics if the slicing is done unguarded.
            "2030-01-02T00:00:00+a\u{20ac}", // `+` + `a` + 3-byte `\u{20ac}`
            "2030-01-02T00:00:00+\u{1f600}", // `+` + 4-byte emoji
            "2030-01-02T00:00:00-a\u{20ac}", // same, negative offset
            "2030-01-02T00:00:00+\u{e9}\u{e9}", // 5-byte offset, boundary-aligned
            "2030-01-02T00:00:00\u{e9}",     // multi-byte where a zone would go
            "2030-01-02T\u{e9}\u{e9}\u{e9}Z", // multi-byte time-of-day
            "\u{e9}\u{e9}\u{e9}\u{e9}T000000Z", // 8-byte non-ASCII "date"
            "\u{1f600}\u{1f600}",            // emoji only
            "T",
            "",
            "Z",
            "----------",
            "++++++++T++++++",
        ] {
            let parsed = LifecycleExpirationJson {
                Date: Some(date.into()),
                Days: None,
                ExpiredObjectDeleteMarker: None,
            };
            assert!(
                parsed.into_sdk().is_err(),
                "{date:?} must error, not panic or parse"
            );
        }
    }

    #[test]
    fn lifecycle_invalid_date_only_format_still_errors() {
        // `2030-1-2` is neither RFC 3339 nor 10 chars with the right shape;
        // must surface as an error rather than silently constructing a date.
        let json = r#"{"Rules":[{"Status":"Enabled","Expiration":{"Date":"2030-1-2"}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let res = parsed.into_sdk();
        assert!(res.is_err(), "malformed date must error");
    }

    // ----- TargetGrants in put-bucket-logging -----

    #[test]
    fn logging_into_sdk_preserves_target_grants_canonical_user() {
        let json = r#"{
          "LoggingEnabled":{
            "TargetBucket":"log-bucket",
            "TargetPrefix":"logs/",
            "TargetGrants":[
              {
                "Grantee":{
                  "Type":"CanonicalUser",
                  "ID":"abcd1234",
                  "DisplayName":"alice"
                },
                "Permission":"FULL_CONTROL"
              }
            ]
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let le = cfg.logging_enabled().expect("logging_enabled");
        let grants = le.target_grants();
        assert_eq!(grants.len(), 1);
        let g = &grants[0];
        let grantee = g.grantee().expect("grantee");
        assert_eq!(grantee.r#type().as_str(), "CanonicalUser");
        assert_eq!(grantee.id(), Some("abcd1234"));
        assert_eq!(grantee.display_name(), Some("alice"));
        assert_eq!(g.permission().map(|p| p.as_str()), Some("FULL_CONTROL"));
    }

    #[test]
    fn logging_into_sdk_preserves_target_grants_group_uri() {
        let json = r#"{
          "LoggingEnabled":{
            "TargetBucket":"log-bucket",
            "TargetPrefix":"logs/",
            "TargetGrants":[
              {
                "Grantee":{
                  "Type":"Group",
                  "URI":"http://acs.amazonaws.com/groups/s3/LogDelivery"
                },
                "Permission":"WRITE"
              }
            ]
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let g = &cfg.logging_enabled().unwrap().target_grants()[0];
        let grantee = g.grantee().unwrap();
        assert_eq!(grantee.r#type().as_str(), "Group");
        assert_eq!(
            grantee.uri(),
            Some("http://acs.amazonaws.com/groups/s3/LogDelivery")
        );
        assert_eq!(g.permission().map(|p| p.as_str()), Some("WRITE"));
    }

    #[test]
    fn logging_into_sdk_target_grants_amazon_customer_by_email() {
        let json = r#"{
          "LoggingEnabled":{
            "TargetBucket":"log-bucket",
            "TargetPrefix":"logs/",
            "TargetGrants":[
              {
                "Grantee":{
                  "Type":"AmazonCustomerByEmail",
                  "EmailAddress":"alice@example.com"
                },
                "Permission":"READ"
              }
            ]
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let g = &cfg.logging_enabled().unwrap().target_grants()[0];
        let grantee = g.grantee().unwrap();
        assert_eq!(grantee.r#type().as_str(), "AmazonCustomerByEmail");
        assert_eq!(grantee.email_address(), Some("alice@example.com"));
        assert_eq!(g.permission().map(|p| p.as_str()), Some("READ"));
    }

    #[test]
    fn logging_into_sdk_no_target_grants_yields_empty_slice() {
        // Absent in JSON ⇒ SDK target_grants accessor is empty.
        let json = r#"{
          "LoggingEnabled":{
            "TargetBucket":"log-bucket",
            "TargetPrefix":"logs/"
          }
        }"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert!(cfg.logging_enabled().unwrap().target_grants().is_empty());
    }

    #[test]
    fn logging_into_sdk_target_grants_missing_type_errors() {
        // `Type` is required on Grantee — serde must reject at parse time.
        let json = r#"{
          "LoggingEnabled":{
            "TargetBucket":"log-bucket",
            "TargetPrefix":"logs/",
            "TargetGrants":[{"Grantee":{"ID":"abcd"},"Permission":"READ"}]
          }
        }"#;
        assert!(serde_json::from_str::<BucketLoggingStatusJson>(json).is_err());
    }

    // ----- absent-optional-field branches -----

    #[test]
    fn lifecycle_and_operator_without_tags_parses() {
        let json = r#"{"Rules":[{"Status":"Enabled","Filter":{"And":{"Prefix":"p/","ObjectSizeGreaterThan":10}},"Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().unwrap();
        assert_eq!(and.prefix(), Some("p/"));
        assert!(and.tags().is_empty());
        assert_eq!(and.object_size_greater_than(), Some(10));
    }

    #[test]
    fn encryption_rule_without_apply_default_parses() {
        let json = r#"{"Rules":[{"BucketKeyEnabled":true}]}"#;
        let parsed: ServerSideEncryptionConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rule = &cfg.rules()[0];
        assert!(rule.apply_server_side_encryption_by_default().is_none());
        assert_eq!(rule.bucket_key_enabled(), Some(true));
    }

    #[test]
    fn logging_target_grant_without_grantee_parses() {
        let json = r#"{"LoggingEnabled":{"TargetBucket":"b","TargetPrefix":"p/","TargetGrants":[{"Permission":"READ"}]}}"#;
        let parsed: BucketLoggingStatusJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let grants = cfg.logging_enabled().unwrap().target_grants();
        assert!(grants[0].grantee().is_none());
        assert!(grants[0].permission().is_some());
    }

    #[test]
    fn notification_s3_key_filter_without_rules_parses() {
        let json = r#"{"QueueConfigurations":[{"QueueArn":"arn:aws:sqs:us-east-1:1:q","Events":["s3:ObjectCreated:*"],"Filter":{"Key":{}}}]}"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let filter = cfg.queue_configurations()[0].filter().unwrap();
        assert!(filter.key().unwrap().filter_rules().is_empty());
    }

    #[test]
    fn replication_and_operator_without_tags_parses() {
        let json = r#"{"Role":"arn:aws:iam::1:role/r","Rules":[{"Status":"Enabled","Filter":{"And":{"Prefix":"p/"}},"Destination":{"Bucket":"arn:aws:s3:::d"}}]}"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().unwrap();
        assert_eq!(and.prefix(), Some("p/"));
        assert!(and.tags().is_empty());
    }

    #[test]
    fn replication_source_selection_with_only_replica_modifications_parses() {
        let json = r#"{"Role":"arn:aws:iam::1:role/r","Rules":[{"Status":"Enabled","Filter":{"Prefix":""},"SourceSelectionCriteria":{"ReplicaModifications":{"Status":"Enabled"}},"Destination":{"Bucket":"arn:aws:s3:::d"}}]}"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let ssc = cfg.rules()[0].source_selection_criteria().unwrap();
        assert!(ssc.sse_kms_encrypted_objects().is_none());
        assert!(ssc.replica_modifications().is_some());
    }

    #[test]
    fn replication_source_selection_with_only_sse_kms_parses() {
        let json = r#"{"Role":"arn:aws:iam::1:role/r","Rules":[{"Status":"Enabled","Filter":{"Prefix":""},"SourceSelectionCriteria":{"SseKmsEncryptedObjects":{"Status":"Enabled"}},"Destination":{"Bucket":"arn:aws:s3:::d"}}]}"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let ssc = cfg.rules()[0].source_selection_criteria().unwrap();
        assert!(ssc.sse_kms_encrypted_objects().is_some());
        assert!(ssc.replica_modifications().is_none());
    }

    // `DeleteMarkerReplication.Status`, `ReplicationTimeValue.Minutes` and
    // `FilterRule.Name`/`Value` are optional in the S3 model (and the SDK);
    // the mirrors must not demand them, or valid input — including our own
    // `get-*` output, which renders an absent value as `{}` — is rejected.

    #[test]
    fn replication_delete_marker_replication_without_status_parses() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "DeleteMarkerReplication": {},
            "Destination": { "Bucket": "arn:aws:s3:::d" }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let dmr = cfg.rules()[0].delete_marker_replication().unwrap();
        assert!(dmr.status().is_none());
    }

    #[test]
    fn replication_time_value_without_minutes_parses() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "ReplicationTime": { "Status": "Enabled", "Time": {} },
              "Metrics": { "Status": "Enabled", "EventThreshold": {} }
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let d = cfg.rules()[0].destination().unwrap();
        let time = d.replication_time().unwrap().time().unwrap();
        assert!(time.minutes().is_none());
        let et = d.metrics().unwrap().event_threshold().unwrap();
        assert!(et.minutes().is_none());
    }

    #[test]
    fn replication_time_value_with_minutes_still_set() {
        let json = r#"{
          "Role": "arn:aws:iam::111111111111:role/r",
          "Rules":[{
            "Status": "Enabled",
            "Destination": {
              "Bucket": "arn:aws:s3:::d",
              "ReplicationTime": { "Status": "Enabled", "Time": { "Minutes": 15 } },
              "Metrics": { "Status": "Enabled", "EventThreshold": { "Minutes": 15 } }
            }
          }]
        }"#;
        let parsed: ReplicationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let d = cfg.rules()[0].destination().unwrap();
        assert_eq!(
            d.replication_time().unwrap().time().unwrap().minutes(),
            Some(15)
        );
        assert_eq!(
            d.metrics().unwrap().event_threshold().unwrap().minutes(),
            Some(15)
        );
    }

    #[test]
    fn notification_filter_rule_without_name_or_value_parses() {
        let json = r#"{
          "QueueConfigurations":[{
            "QueueArn":"arn:aws:sqs:us-east-1:111111111111:q",
            "Events":["s3:ObjectCreated:*"],
            "Filter":{"Key":{"FilterRules":[{"Value":"a/"},{"Name":"suffix"}]}}
          }]
        }"#;
        let parsed: NotificationConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rules = cfg.queue_configurations()[0]
            .filter()
            .unwrap()
            .key()
            .unwrap()
            .filter_rules();
        assert!(rules[0].name().is_none());
        assert_eq!(rules[0].value(), Some("a/"));
        assert_eq!(rules[1].name(), Some(&FilterRuleName::Suffix));
        assert!(rules[1].value().is_none());
    }

    #[test]
    fn get_bucket_replication_output_round_trips_into_put() {
        use crate::output::json::get_bucket_replication_to_json;
        use aws_sdk_s3::operation::get_bucket_replication::GetBucketReplicationOutput;

        let rule = ReplicationRule::builder()
            .status(ReplicationRuleStatus::Enabled)
            .destination(
                Destination::builder()
                    .bucket("arn:aws:s3:::d")
                    .replication_time(
                        ReplicationTime::builder()
                            .status(ReplicationTimeStatus::Enabled)
                            .time(ReplicationTimeValue::builder().build())
                            .build()
                            .unwrap(),
                    )
                    .build()
                    .unwrap(),
            )
            .delete_marker_replication(DeleteMarkerReplication::builder().build())
            .build()
            .unwrap();
        let out = GetBucketReplicationOutput::builder()
            .replication_configuration(
                ReplicationConfiguration::builder()
                    .role("arn:aws:iam::111111111111:role/r")
                    .rules(rule)
                    .build()
                    .unwrap(),
            )
            .build();
        let inner = get_bucket_replication_to_json(&out)["ReplicationConfiguration"].to_string();
        let parsed: ReplicationConfigurationJson = serde_json::from_str(&inner)
            .unwrap_or_else(|e| panic!("own get output must parse: {e}: {inner}"));
        parsed.into_sdk().unwrap();
    }

    // ----- timestamps: AWS-CLI-accepted spellings -----

    fn expiration_date(date_json: &str) -> Result<DateTime> {
        let body =
            format!(r#"{{"Rules":[{{"Status":"Enabled","Expiration":{{"Date":{date_json}}}}}]}}"#);
        let cfg = from_json_str::<LifecycleConfigurationJson>(&body)?.into_sdk()?;
        Ok(*cfg.rules()[0].expiration().unwrap().date().unwrap())
    }

    /// 2024-01-01T00:00:00Z
    const NEW_YEAR_2024: i64 = 1_704_067_200;

    #[test]
    fn timestamp_spellings_the_aws_cli_accepts_all_parse_to_the_same_instant() {
        for date in [
            // ISO 8601 reduced precision (no seconds / no minutes), extended and basic.
            r#""2024-01-01T00:00Z""#,
            r#""2024-01-01T00:00""#,
            r#""2024-01-01T00Z""#,
            r#""20240101T0000Z""#,
            r#""20240101T00""#,
            // Comma as the decimal sign.
            r#""2024-01-01T00:00:00,000Z""#,
            r#""20240101T000000,0+0000""#,
            // Space / lowercase separators.
            r#""2024-01-01 00:00:00""#,
            r#""2024-01-01 00:00""#,
            r#""2024-01-01t00:00:00+0000""#,
            r#""2024-01-01T09:00:00 +09:00""#,
            r#""2024-01-01 09:00 +0900""#,
            // RFC 2822.
            r#""Mon, 01 Jan 2024 00:00:00 GMT""#,
            r#""Mon, 01 Jan 2024 09:00:00 +0900""#,
            // Epoch seconds, as a number and as a string.
            "1704067200",
            "1704067200.0",
            r#""1704067200""#,
            r#""1704067200.0""#,
        ] {
            let dt = expiration_date(date).unwrap_or_else(|e| panic!("{date} must parse: {e:#}"));
            assert_eq!(dt.secs(), NEW_YEAR_2024, "{date}");
            assert_eq!(dt.subsec_nanos(), 0, "{date}");
        }
    }

    #[test]
    fn timestamp_fraction_is_kept() {
        for date in [
            r#""2024-01-01T00:00:00,5Z""#,
            r#""2024-01-01 00:00:00.5""#,
            "1704067200.5",
            r#""1704067200.5""#,
        ] {
            let dt = expiration_date(date).unwrap();
            assert_eq!(dt.secs(), NEW_YEAR_2024, "{date}");
            assert_eq!(dt.subsec_nanos(), 500_000_000, "{date}");
        }
    }

    #[test]
    fn negative_epoch_seconds_parse() {
        let dt = expiration_date("-1.5").unwrap();
        assert_eq!((dt.secs(), dt.subsec_nanos()), (-2, 500_000_000));
        assert_eq!(expiration_date(r#""-1""#).unwrap().secs(), -1);
    }

    #[test]
    fn transition_date_accepts_epoch_number() {
        let body = r#"{"Rules":[{"Status":"Enabled",
            "Transitions":[{"Date":1704067200,"StorageClass":"GLACIER"}]}]}"#;
        let cfg = from_json_str::<LifecycleConfigurationJson>(body)
            .unwrap()
            .into_sdk()
            .unwrap();
        assert_eq!(
            cfg.rules()[0].transitions()[0].date().unwrap().secs(),
            NEW_YEAR_2024
        );
    }

    /// A leap second used to panic inside the SDK `DateTime` constructor.
    #[test]
    fn leap_second_is_an_error_not_a_panic() {
        for date in [
            r#""2024-12-31T23:59:60Z""#,
            r#""2024-12-31T23:59:60.5+09:00""#,
            r#""20241231T235960""#,
            r#""2024-12-31 23:59:60""#,
            r#""Tue, 31 Dec 2024 23:59:60 GMT""#,
        ] {
            let err = expiration_date(date).expect_err(date);
            assert!(
                format!("{err:#}").contains("invalid timestamp"),
                "{date}: {err:#}"
            );
        }
    }

    /// `time` would read a month-end leap second as `23:59:59.999999999` and
    /// reject any other `:60` as malformed; both must instead get the
    /// leap-second error, while a genuine `.999999999` still parses.
    #[test]
    fn leap_second_is_reported_as_such_wherever_it_falls() {
        for date in [
            r#""2024-12-31T23:59:60Z""#,          // month end: `time` accepts
            r#""2024-06-15T12:00:60Z""#,          // mid-month: `time` rejects
            r#""2024-06-15t12:00:60+09:00""#,     // lowercase separator
            r#""20240615T120060""#,               // basic format
            r#""Tue, 31 Dec 2024 23:59:60 GMT""#, // RFC 2822, month end
            r#""Sat, 15 Jun 2024 12:00:60 GMT""#, // RFC 2822, mid-month
            r#""15 Jun 2024 12:00:60 +0900""#,    // RFC 2822, no weekday
        ] {
            let err = expiration_date(date).expect_err(date);
            assert!(
                format!("{err:#}").contains("leap seconds"),
                "{date}: {err:#}"
            );
        }
        let dt = expiration_date(r#""2024-12-31T23:59:59.999999999Z""#).unwrap();
        assert_eq!(dt.subsec_nanos(), 999_999_999);
        // Minute 60 is not a leap second; it stays an ordinary error.
        for date in [
            r#""2024-06-15T12:60:00Z""#,
            r#""Sat, 15 Jun 2024 12:60:00 GMT""#,
        ] {
            let err = expiration_date(date).unwrap_err();
            assert!(!format!("{err:#}").contains("leap seconds"), "{err:#}");
        }
    }

    /// RFC 2822 section 4.3: a three-digit year means 1900 plus its value, as
    /// `chrono` read it; `time` itself rejects three-digit years.
    #[test]
    fn rfc2822_three_digit_year_adds_1900() {
        for date in [
            r#""Mon, 01 Jan 124 00:00:00 GMT""#,
            r#""01 Jan 124 00:00:00 +0000""#,
            r#""Mon,  01  Jan  124  09:00:00  +0900""#,
        ] {
            assert_eq!(
                expiration_date(date).unwrap().secs(),
                NEW_YEAR_2024,
                "{date}"
            );
        }
        // 009 -> 1909, as chrono read it.
        assert_eq!(
            expiration_date(r#""Fri, 01 Jan 009 00:00:00 GMT""#)
                .unwrap()
                .secs(),
            -1_924_992_000
        );
        // Two- and four-digit years are untouched.
        for date in [
            r#""Mon, 01 Jan 24 00:00:00 GMT""#,
            r#""Mon, 01 Jan 2024 00:00:00 GMT""#,
        ] {
            assert_eq!(
                expiration_date(date).unwrap().secs(),
                NEW_YEAR_2024,
                "{date}"
            );
        }
        // Lowercase month, as `time` accepts.
        assert_eq!(
            expiration_date(r#""mon, 01 jan 124 00:00:00 GMT""#)
                .unwrap()
                .secs(),
            NEW_YEAR_2024
        );
        // The expanded date is still checked: wrong weekday, bad day.
        assert!(expiration_date(r#""Tue, 01 Jan 124 00:00:00 GMT""#).is_err());
        assert!(expiration_date(r#""Mon, 32 Jan 124 00:00:00 GMT""#).is_err());
        // A leap second with a three-digit year still gets the leap-second error.
        let err = expiration_date(r#""Sat, 15 Jun 124 12:00:60 GMT""#).unwrap_err();
        assert!(format!("{err:#}").contains("leap seconds"), "{err:#}");
    }

    /// `time` ignores the RFC 2822 day of the week; a mismatch must still be
    /// rejected, as it was with `chrono`.
    #[test]
    fn rfc2822_weekday_must_match_the_date() {
        for date in [
            r#""Mon, 01 Jan 2024 00:00:00 GMT""#,
            r#""MON, 01 Jan 2024 00:00:00 GMT""#,
            r#""01 Jan 2024 00:00:00 GMT""#,
            // 2023-12-31T15:00:00Z: the weekday refers to the date as written.
            r#""Mon, 01 Jan 2024 00:00:00 +0900""#,
        ] {
            assert!(expiration_date(date).is_ok(), "{date}");
        }
        let err = expiration_date(r#""Tue, 01 Jan 2024 00:00:00 GMT""#).unwrap_err();
        assert!(
            format!("{err:#}").contains("day of the week does not match"),
            "{err:#}"
        );
    }

    #[test]
    fn eight_digit_string_is_a_basic_date_never_an_epoch() {
        assert_eq!(
            expiration_date(r#""20240101""#).unwrap().secs(),
            NEW_YEAR_2024
        );
        // A mistyped basic date must not fall through to the epoch reading.
        assert!(expiration_date(r#""20241301""#).is_err());
        assert!(expiration_date(r#""-20241301""#).is_err());
    }

    /// The JSON number must read the same as the string with the same digits.
    #[test]
    fn eight_digit_number_is_a_basic_date_never_an_epoch() {
        assert_eq!(expiration_date("20240101").unwrap().secs(), NEW_YEAR_2024);
        assert!(expiration_date("20241301").is_err());
        assert!(expiration_date("-20240101").is_err());
        // Other lengths, and a number written with a fraction, stay epochs.
        assert_eq!(expiration_date("9999999").unwrap().secs(), 9_999_999);
        assert_eq!(expiration_date("100000000").unwrap().secs(), 100_000_000);
        assert_eq!(expiration_date("20240101.0").unwrap().secs(), 20_240_101);
    }

    #[test]
    fn malformed_timestamps_still_error() {
        for date in [
            r#""2024-01-01T00:00.5Z""#, // fractional minutes
            r#""2024-01-01T0Z""#,
            r#""2024-01-01T000Z""#,
            r#""2024-01-01T00:00:00:00Z""#,
            r#""2024-01-01T00:00:00.Z""#,
            r#""2024-01-01T24:00:00Z""#,
            r#""2024-02-30""#,
            r#""2024-01""#,
            r#""1e9""#,
            r#""inf""#,
            r#""nan""#,
            r#"".""#,
            r#""-""#,
            r#""""#,
            r#"" ""#,
            r#""1704067200 ""#,
            "1e300",
            "-1e300",
            "253402300800",
            "true",
            "[]",
        ] {
            assert!(expiration_date(date).is_err(), "{date} must error");
        }
    }

    #[test]
    fn epoch_range_limits() {
        assert_eq!(
            expiration_date("253402300799").unwrap().secs(),
            253_402_300_799
        );
        assert_eq!(
            expiration_date("-62167219200").unwrap().secs(),
            -62_167_219_200
        );
        assert!(expiration_date("-62167219201").is_err());
        // Float rounding must never produce 1e9 nanoseconds (which panics).
        let dt = expiration_date("-0.00000000000000000001").unwrap();
        assert!(dt.subsec_nanos() < 1_000_000_000);
    }

    // ----- from_json_str: duplicate keys take the last value -----

    #[test]
    fn duplicate_key_takes_the_last_value() {
        let body = r#"{"Rules":[{"Status":"Enabled","Status":"Disabled","ID":"a","ID":"b"}],
                       "Rules":[{"Status":"Enabled","ID":"x","Status":"Disabled","ID":"y"}]}"#;
        let cfg = from_json_str::<LifecycleConfigurationJson>(body)
            .unwrap()
            .into_sdk()
            .unwrap();
        assert_eq!(cfg.rules().len(), 1);
        assert_eq!(cfg.rules()[0].id(), Some("y"));
        assert_eq!(cfg.rules()[0].status().as_str(), "Disabled");
    }

    #[test]
    fn duplicate_key_last_wins_even_when_an_earlier_value_is_invalid() {
        let body = r#"{"BlockPublicAcls":"yes","BlockPublicAcls":true}"#;
        let cfg = from_json_str::<PublicAccessBlockConfigurationJson>(body)
            .unwrap()
            .into_sdk()
            .unwrap();
        assert_eq!(cfg.block_public_acls(), Some(true));
    }

    #[test]
    fn duplicate_key_whose_last_value_is_invalid_errors() {
        let body = r#"{"BlockPublicAcls":true,"BlockPublicAcls":"yes"}"#;
        let err = from_json_str::<PublicAccessBlockConfigurationJson>(body).unwrap_err();
        assert!(err.to_string().contains("invalid type"), "got: {err}");
    }

    #[test]
    fn duplicates_do_not_hide_unknown_or_missing_fields() {
        let err =
            from_json_str::<CorsConfigurationJson>(r#"{"CORSRules":[],"CORSRules":[],"Bogus":1}"#)
                .unwrap_err();
        assert!(
            err.to_string().contains("unknown field `Bogus`"),
            "got: {err}"
        );
        assert!(
            from_json_str::<ReplicationConfigurationJson>(r#"{"Role":"a","Role":"b"}"#).is_err(),
            "missing `Rules` must still error"
        );
    }

    #[test]
    fn from_json_str_keeps_position_in_ordinary_errors() {
        let err = from_json_str::<CorsConfigurationJson>(r#"{"CORSRule":[]}"#).unwrap_err();
        assert!(err.to_string().contains("line 1 column"), "got: {err}");
        let err = from_json_str::<CorsConfigurationJson>("{not json").unwrap_err();
        assert!(err.is_syntax(), "got: {err}");
    }

    #[test]
    fn from_json_str_matches_from_str_on_valid_documents() {
        let body = r#"{"Rules":[{"Status":"Enabled","Filter":{"ObjectSizeGreaterThan":5497558138880},
            "Expiration":{"Days":30}}]}"#;
        let cfg = from_json_str::<LifecycleConfigurationJson>(body)
            .unwrap()
            .into_sdk()
            .unwrap();
        assert_eq!(
            cfg.rules()[0].filter().unwrap().object_size_greater_than(),
            Some(5_497_558_138_880)
        );
    }
}
