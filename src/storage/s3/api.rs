//! Thin async wrappers around `aws-sdk-s3` calls used by the v0.2.0
//! thin-wrapper subcommands. Each function builds the SDK operation,
//! awaits it, and maps the SDK error to `anyhow::Error` with operation
//! context.
//!
//! These wrappers exist so the per-subcommand runtime modules
//! (`bin/s3util/cli/<name>.rs`) can be written without referring to
//! `aws-sdk-s3` types beyond what's strictly necessary.

use anyhow::{Context, Result};
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::create_bucket::CreateBucketOutput;
use aws_sdk_s3::operation::delete_bucket::DeleteBucketOutput;
use aws_sdk_s3::operation::delete_bucket_policy::DeleteBucketPolicyOutput;
use aws_sdk_s3::operation::delete_bucket_tagging::DeleteBucketTaggingOutput;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
use aws_sdk_s3::operation::get_bucket_tagging::GetBucketTaggingOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_bucket_policy::PutBucketPolicyOutput;
use aws_sdk_s3::operation::put_bucket_tagging::PutBucketTaggingOutput;
use aws_sdk_s3::operation::put_bucket_versioning::PutBucketVersioningOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::{
    BucketInfo, BucketLocationConstraint, BucketType, BucketVersioningStatus, ChecksumMode,
    CreateBucketConfiguration, DataRedundancy, LocationInfo, LocationType, Tagging,
    VersioningConfiguration,
};

/// Error type for read wrappers that distinguish a 404 NotFound condition
/// from every other failure mode (network, auth, region mismatch, etc.).
/// Used by `head_object`, `head_bucket`, `get_object_tagging`,
/// `get_bucket_policy`, `get_bucket_tagging`, and `get_bucket_versioning`
/// so the runtime can map NotFound to a dedicated exit code (4) and emit
/// an accurate "bucket missing" vs "subresource missing" message.
#[derive(Debug, thiserror::Error)]
pub enum HeadError {
    /// S3 reported the bucket itself does not exist (`NoSuchBucket`).
    /// Distinct from `NotFound` so callers can say "bucket … not found"
    /// rather than the misleading "tags/policy … not found".
    #[error("bucket does not exist")]
    BucketNotFound,
    /// The addressed resource (key, version, tag set, policy) does not
    /// exist. Bucket existence is not implied by this variant.
    #[error("target does not exist")]
    NotFound,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// S3 error codes that `get-object-tagging` treats as a subresource
/// NotFound. `NoSuchBucket` is handled separately by `classify_not_found`
/// and mapped to `HeadError::BucketNotFound`.
const GET_OBJECT_TAGGING_NOT_FOUND_CODES: &[&str] = &["NoSuchKey", "NoSuchVersion"];
/// S3 error codes that `get-bucket-policy` treats as a subresource NotFound.
/// `NoSuchBucketPolicy` covers the case where the bucket exists but no
/// policy is attached. `NoSuchBucket` is handled separately.
const GET_BUCKET_POLICY_NOT_FOUND_CODES: &[&str] = &["NoSuchBucketPolicy"];
/// S3 error codes that `get-bucket-tagging` treats as a subresource NotFound.
/// `NoSuchTagSet` covers the case where the bucket exists but no tags
/// are configured. `NoSuchBucket` is handled separately.
const GET_BUCKET_TAGGING_NOT_FOUND_CODES: &[&str] = &["NoSuchTagSet"];
/// S3 error codes that `get-bucket-versioning` treats as a subresource
/// NotFound. `GetBucketVersioning` returns an empty body (not an error)
/// when versioning has never been configured, so the only NotFound case
/// is `NoSuchBucket`, which `classify_not_found` handles separately.
const GET_BUCKET_VERSIONING_NOT_FOUND_CODES: &[&str] = &[];

/// Options controlling `head_object` behaviour.
pub struct HeadObjectOpts {
    /// Version ID to target a specific object version.
    pub version_id: Option<String>,
    /// SSE-C algorithm (e.g. `"AES256"`) — required when the object was
    /// written with SSE-C.
    pub sse_c: Option<String>,
    /// Base64-encoded SSE-C key matching the one used to write the object.
    pub sse_c_key: Option<String>,
    /// Base64-encoded MD5 of the SSE-C key.
    pub sse_c_key_md5: Option<String>,
    /// When `true`, sets `ChecksumMode=ENABLED` so S3 includes the
    /// additional checksum in the response.
    pub enable_additional_checksum: bool,
}

/// Issue `HeadObject` against `bucket`/`key`. Returns the SDK response on
/// success, `HeadError::NotFound` if the SDK reports the object does not
/// exist (404), and `HeadError::Other` for any other failure (the original
/// SDK error is preserved with `anyhow::Context` describing the operation).
pub async fn head_object(
    client: &Client,
    bucket: &str,
    key: &str,
    opts: HeadObjectOpts,
) -> Result<HeadObjectOutput, HeadError> {
    let mut req = client.head_object().bucket(bucket).key(key);

    if let Some(vid) = opts.version_id {
        req = req.version_id(vid);
    }
    if let Some(alg) = opts.sse_c {
        req = req.sse_customer_algorithm(alg);
    }
    if let Some(k) = opts.sse_c_key {
        req = req.sse_customer_key(k);
    }
    if let Some(md5) = opts.sse_c_key_md5 {
        req = req.sse_customer_key_md5(md5);
    }
    if opts.enable_additional_checksum {
        req = req.checksum_mode(ChecksumMode::Enabled);
    }

    req.send().await.map_err(|e| {
        if e.as_service_error()
            .map(|s| s.is_not_found())
            .unwrap_or(false)
        {
            HeadError::NotFound
        } else {
            HeadError::Other(
                anyhow::Error::new(e).context(format!("head-object on s3://{bucket}/{key}")),
            )
        }
    })
}

/// Issue `DeleteObject` against `bucket`/`key`. Returns the SDK response on success.
///
/// If `version_id` is provided, only that specific version is deleted;
/// otherwise a delete marker is created (versioned bucket) or the object is
/// removed (non-versioned bucket).
pub async fn delete_object(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<DeleteObjectOutput> {
    let mut req = client.delete_object().bucket(bucket).key(key);
    if let Some(v) = version_id {
        req = req.version_id(v);
    }
    req.send()
        .await
        .with_context(|| format!("rm s3://{bucket}/{key}"))
}

/// Issue `GetObjectTagging` against `bucket`/`key`. Returns the SDK response
/// on success, `HeadError::BucketNotFound` when S3 returns `NoSuchBucket`,
/// `HeadError::NotFound` when S3 returns `NoSuchKey` or `NoSuchVersion`,
/// and `HeadError::Other` for any other failure.
///
/// If `version_id` is provided, tags for that specific object version are fetched.
pub async fn get_object_tagging(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<GetObjectTaggingOutput, HeadError> {
    let mut req = client.get_object_tagging().bucket(bucket).key(key);
    if let Some(v) = version_id {
        req = req.version_id(v);
    }
    req.send().await.map_err(|e| {
        let code = e
            .as_service_error()
            .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
        match classify_not_found(code, GET_OBJECT_TAGGING_NOT_FOUND_CODES) {
            Some(he) => he,
            None => HeadError::Other(
                anyhow::Error::new(e).context(format!("get-object-tagging on s3://{bucket}/{key}")),
            ),
        }
    })
}

fn matches_not_found_code(code: Option<&str>, candidates: &[&str]) -> bool {
    code.is_some_and(|c| candidates.contains(&c))
}

/// Classify an SDK error code into a `HeadError` for `get-*` wrappers.
/// `NoSuchBucket` is always mapped to `BucketNotFound`. Any code in
/// `subresource_codes` is mapped to `NotFound`. Anything else returns
/// `None`, signalling the caller should wrap the error as `Other`.
fn classify_not_found(code: Option<&str>, subresource_codes: &[&str]) -> Option<HeadError> {
    if code == Some("NoSuchBucket") {
        return Some(HeadError::BucketNotFound);
    }
    if matches_not_found_code(code, subresource_codes) {
        return Some(HeadError::NotFound);
    }
    None
}

/// Issue `PutObjectTagging` against `bucket`/`key`. Returns the SDK response on success.
///
/// Replaces all existing tags on the object with the provided `tagging`.
pub async fn put_object_tagging(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    tagging: Tagging,
) -> Result<PutObjectTaggingOutput> {
    let mut req = client
        .put_object_tagging()
        .bucket(bucket)
        .key(key)
        .tagging(tagging);
    if let Some(v) = version_id {
        req = req.version_id(v);
    }
    req.send()
        .await
        .with_context(|| format!("put-object-tagging on s3://{bucket}/{key}"))
}

/// Issue `DeleteObjectTagging` against `bucket`/`key`. Returns the SDK response on success.
///
/// Removes all tags from the object.
pub async fn delete_object_tagging(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<DeleteObjectTaggingOutput> {
    let mut req = client.delete_object_tagging().bucket(bucket).key(key);
    if let Some(v) = version_id {
        req = req.version_id(v);
    }
    req.send()
        .await
        .with_context(|| format!("delete-object-tagging on s3://{bucket}/{key}"))
}

/// Issue `HeadBucket` against `bucket`. Returns the SDK response on success,
/// `HeadError::BucketNotFound` if the SDK reports the bucket does not exist
/// (404), and `HeadError::Other` for any other failure.
pub async fn head_bucket(client: &Client, bucket: &str) -> Result<HeadBucketOutput, HeadError> {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            if e.as_service_error()
                .map(|s| s.is_not_found())
                .unwrap_or(false)
            {
                HeadError::BucketNotFound
            } else {
                HeadError::Other(
                    anyhow::Error::new(e).context(format!("head-bucket on s3://{bucket}")),
                )
            }
        })
}

/// Issue `CreateBucket` for `bucket`.
///
/// **Directory buckets (S3 Express One Zone, `--x-s3` suffix)** require a
/// different `CreateBucketConfiguration` (`Location` + `Bucket`) than
/// general-purpose buckets (`LocationConstraint`). The bucket name itself
/// encodes the zone ID — the segment between the last `--` and the
/// `--x-s3` suffix — so we parse it from the name and switch shapes.
///
/// **General-purpose buckets**: the `LocationConstraint` is derived from
/// the SDK client's resolved region (which honours `--target-region`,
/// `AWS_REGION`, and the active profile's region in that order). S3 rejects
/// a `LocationConstraint` of `us-east-1` (the API was designed before that
/// region existed), so the constraint is omitted there. It is also omitted
/// when the client has no resolved region.
pub async fn create_bucket(client: &Client, bucket: &str) -> Result<CreateBucketOutput> {
    let mut req = client.create_bucket().bucket(bucket);

    if let Some(loc) = parse_directory_bucket_zone(bucket) {
        let location = LocationInfo::builder()
            .r#type(loc.location_type)
            .name(loc.zone_id)
            .build();
        let bucket_info = BucketInfo::builder()
            .r#type(BucketType::Directory)
            .data_redundancy(loc.data_redundancy)
            .build();
        let cfg = CreateBucketConfiguration::builder()
            .location(location)
            .bucket(bucket_info)
            .build();
        req = req.create_bucket_configuration(cfg);
    } else if let Some(region) = client.config().region().map(|r| r.as_ref())
        && !region.is_empty()
        && region != "us-east-1"
    {
        let constraint = BucketLocationConstraint::from(region);
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        req = req.create_bucket_configuration(cfg);
    }

    req.send()
        .await
        .with_context(|| format!("create-bucket on s3://{bucket}"))
}

/// Parsed zone information for an S3 Express One Zone directory bucket
/// name. Returned by [`parse_directory_bucket_zone`].
struct DirectoryBucketZone {
    zone_id: String,
    location_type: LocationType,
    data_redundancy: DataRedundancy,
}

/// Parse the zone ID and zone type from a directory-bucket name.
///
/// Directory bucket names follow `<base>--<zone-id>--x-s3`. Returns `None`
/// for any name that does not match this shape (including names that end
/// in `--x-s3` but lack a zone segment — S3 will reject those itself).
///
/// Zone type is inferred from the zone-ID shape:
/// - one hyphen (e.g. `apne1-az4`) → Availability Zone
/// - two or more hyphens (e.g. `usw2-lax1-az1`) → Local Zone
fn parse_directory_bucket_zone(bucket: &str) -> Option<DirectoryBucketZone> {
    let stripped = bucket.strip_suffix("--x-s3")?;
    let (_, zone_id) = stripped.rsplit_once("--")?;
    if zone_id.is_empty() {
        return None;
    }
    let (location_type, data_redundancy) = if zone_id.matches('-').count() <= 1 {
        (
            LocationType::AvailabilityZone,
            DataRedundancy::SingleAvailabilityZone,
        )
    } else {
        (LocationType::LocalZone, DataRedundancy::SingleLocalZone)
    };
    Some(DirectoryBucketZone {
        zone_id: zone_id.to_string(),
        location_type,
        data_redundancy,
    })
}

/// Issue `DeleteBucket` for `bucket`. Returns the SDK response on success.
///
/// The bucket must be empty; S3 returns `409 BucketNotEmpty` otherwise.
pub async fn delete_bucket(client: &Client, bucket: &str) -> Result<DeleteBucketOutput> {
    client
        .delete_bucket()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket on s3://{bucket}"))
}

/// Issue `PutBucketTagging` for `bucket`. Returns the SDK response on success.
///
/// Replaces all existing tags on the bucket with the provided `tagging`.
pub async fn put_bucket_tagging(
    client: &Client,
    bucket: &str,
    tagging: Tagging,
) -> Result<PutBucketTaggingOutput> {
    client
        .put_bucket_tagging()
        .bucket(bucket)
        .tagging(tagging)
        .send()
        .await
        .with_context(|| format!("put-bucket-tagging on s3://{bucket}"))
}

/// Issue `GetBucketTagging` for `bucket`. Returns the SDK response on success,
/// `HeadError::BucketNotFound` when S3 returns `NoSuchBucket`,
/// `HeadError::NotFound` when S3 returns `NoSuchTagSet` (the bucket exists
/// but no tags are configured), and `HeadError::Other` for any other failure.
pub async fn get_bucket_tagging(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketTaggingOutput, HeadError> {
    client
        .get_bucket_tagging()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            let code = e
                .as_service_error()
                .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
            match classify_not_found(code, GET_BUCKET_TAGGING_NOT_FOUND_CODES) {
                Some(he) => he,
                None => HeadError::Other(
                    anyhow::Error::new(e).context(format!("get-bucket-tagging on s3://{bucket}")),
                ),
            }
        })
}

/// Issue `DeleteBucketTagging` for `bucket`. Returns the SDK response on success.
///
/// Removes all tags from the bucket. Silent on success.
pub async fn delete_bucket_tagging(
    client: &Client,
    bucket: &str,
) -> Result<DeleteBucketTaggingOutput> {
    client
        .delete_bucket_tagging()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket-tagging on s3://{bucket}"))
}

/// Issue `PutBucketVersioning` for `bucket` with the given `status`.
///
/// Sets the versioning state to `Enabled` or `Suspended`.
/// MFA-delete is intentionally not exposed in v0.2.0.
pub async fn put_bucket_versioning(
    client: &Client,
    bucket: &str,
    status: BucketVersioningStatus,
) -> Result<PutBucketVersioningOutput> {
    let versioning_config = VersioningConfiguration::builder().status(status).build();
    client
        .put_bucket_versioning()
        .bucket(bucket)
        .versioning_configuration(versioning_config)
        .send()
        .await
        .with_context(|| format!("put-bucket-versioning on s3://{bucket}"))
}

/// Issue `GetBucketVersioning` for `bucket`. Returns the SDK response on
/// success, `HeadError::BucketNotFound` when S3 returns `NoSuchBucket`,
/// and `HeadError::Other` for any other failure.
///
/// When versioning has never been configured, S3 returns an empty response
/// (no `Status` element) — that is `Ok`, not NotFound. The caller
/// (`get_bucket_versioning_to_json`) maps the empty payload to `{}`.
pub async fn get_bucket_versioning(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketVersioningOutput, HeadError> {
    client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            let code = e
                .as_service_error()
                .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
            match classify_not_found(code, GET_BUCKET_VERSIONING_NOT_FOUND_CODES) {
                Some(he) => he,
                None => HeadError::Other(
                    anyhow::Error::new(e)
                        .context(format!("get-bucket-versioning on s3://{bucket}")),
                ),
            }
        })
}

/// Issue `PutBucketPolicy` for `bucket` with the given `policy` JSON string.
///
/// The policy is sent verbatim; s3util performs no client-side validation.
/// S3 rejects malformed or invalid policies with `400 MalformedPolicy`.
pub async fn put_bucket_policy(
    client: &Client,
    bucket: &str,
    policy: &str,
) -> Result<PutBucketPolicyOutput> {
    client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy)
        .send()
        .await
        .with_context(|| format!("put-bucket-policy on s3://{bucket}"))
}

/// Issue `GetBucketPolicy` for `bucket`. Returns the SDK response on success,
/// `HeadError::BucketNotFound` when S3 returns `NoSuchBucket`,
/// `HeadError::NotFound` when S3 returns `NoSuchBucketPolicy` (the bucket
/// exists but no policy is attached), and `HeadError::Other` for any other
/// failure.
pub async fn get_bucket_policy(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketPolicyOutput, HeadError> {
    client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            let code = e
                .as_service_error()
                .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
            match classify_not_found(code, GET_BUCKET_POLICY_NOT_FOUND_CODES) {
                Some(he) => he,
                None => HeadError::Other(
                    anyhow::Error::new(e).context(format!("get-bucket-policy on s3://{bucket}")),
                ),
            }
        })
}

/// Issue `DeleteBucketPolicy` for `bucket`. Returns the SDK response on success.
///
/// Exits silently on success; the caller surfaces errors as exit code 1.
pub async fn delete_bucket_policy(
    client: &Client,
    bucket: &str,
) -> Result<DeleteBucketPolicyOutput> {
    client
        .delete_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket-policy on s3://{bucket}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_not_found_code_returns_false_for_none() {
        assert!(!matches_not_found_code(None, &["NoSuchKey"]));
    }

    #[test]
    fn matches_not_found_code_returns_false_for_empty_candidates() {
        assert!(!matches_not_found_code(Some("NoSuchKey"), &[]));
    }

    #[test]
    fn matches_not_found_code_returns_false_for_unrelated_code() {
        assert!(!matches_not_found_code(
            Some("AccessDenied"),
            &["NoSuchKey", "NoSuchBucket"]
        ));
    }

    #[test]
    fn matches_not_found_code_returns_true_for_matching_code() {
        assert!(matches_not_found_code(
            Some("NoSuchKey"),
            &["NoSuchKey", "NoSuchBucket"]
        ));
        assert!(matches_not_found_code(
            Some("NoSuchBucket"),
            &["NoSuchKey", "NoSuchBucket"]
        ));
    }

    #[test]
    fn matches_not_found_code_is_case_sensitive() {
        // S3 error codes use exact PascalCase; a case-folded comparison
        // would mask SDK changes.
        assert!(!matches_not_found_code(
            Some("nosuchkey"),
            &["NoSuchKey", "NoSuchBucket"]
        ));
    }

    /// Pin the candidate set so a typo or accidental edit shows up as a
    /// test failure rather than a behavioural regression at e2e time.
    /// The constants no longer include `NoSuchBucket` — that is handled
    /// uniformly by `classify_not_found` and mapped to `BucketNotFound`.
    #[test]
    fn get_object_tagging_not_found_codes_pinned() {
        assert_eq!(
            GET_OBJECT_TAGGING_NOT_FOUND_CODES,
            &["NoSuchKey", "NoSuchVersion"]
        );
    }

    #[test]
    fn get_bucket_policy_not_found_codes_pinned() {
        assert_eq!(GET_BUCKET_POLICY_NOT_FOUND_CODES, &["NoSuchBucketPolicy"]);
    }

    #[test]
    fn get_bucket_tagging_not_found_codes_pinned() {
        assert_eq!(GET_BUCKET_TAGGING_NOT_FOUND_CODES, &["NoSuchTagSet"]);
    }

    #[test]
    fn get_bucket_versioning_not_found_codes_pinned() {
        let empty: &[&str] = &[];
        assert_eq!(GET_BUCKET_VERSIONING_NOT_FOUND_CODES, empty);
    }

    #[test]
    fn classify_not_found_routes_no_such_bucket_to_bucket_not_found() {
        let got = classify_not_found(Some("NoSuchBucket"), &["NoSuchTagSet"]);
        assert!(matches!(got, Some(HeadError::BucketNotFound)));
    }

    #[test]
    fn classify_not_found_routes_subresource_code_to_not_found() {
        let got = classify_not_found(Some("NoSuchTagSet"), &["NoSuchTagSet"]);
        assert!(matches!(got, Some(HeadError::NotFound)));
    }

    #[test]
    fn classify_not_found_returns_none_for_unrelated_code() {
        assert!(classify_not_found(Some("AccessDenied"), &["NoSuchTagSet"]).is_none());
        assert!(classify_not_found(None, &["NoSuchTagSet"]).is_none());
    }

    #[test]
    fn classify_not_found_no_such_bucket_takes_priority_over_subresource_list() {
        // `NoSuchBucket` must always become `BucketNotFound` even if a
        // caller mistakenly leaves it in the subresource list.
        let got = classify_not_found(Some("NoSuchBucket"), &["NoSuchBucket", "NoSuchTagSet"]);
        assert!(matches!(got, Some(HeadError::BucketNotFound)));
    }

    #[test]
    fn parse_directory_bucket_zone_returns_none_for_general_purpose_name() {
        assert!(parse_directory_bucket_zone("my-bucket").is_none());
        assert!(parse_directory_bucket_zone("my-bucket--with-dashes").is_none());
    }

    #[test]
    fn parse_directory_bucket_zone_parses_availability_zone_id() {
        let z = parse_directory_bucket_zone("test-s3rm-e2e-0e1932b0b372--apne1-az4--x-s3")
            .expect("expected directory-bucket parse");
        assert_eq!(z.zone_id, "apne1-az4");
        assert_eq!(z.location_type, LocationType::AvailabilityZone);
        assert_eq!(z.data_redundancy, DataRedundancy::SingleAvailabilityZone);
    }

    #[test]
    fn parse_directory_bucket_zone_parses_local_zone_id() {
        let z = parse_directory_bucket_zone("mybucket--usw2-lax1-az1--x-s3")
            .expect("expected directory-bucket parse");
        assert_eq!(z.zone_id, "usw2-lax1-az1");
        assert_eq!(z.location_type, LocationType::LocalZone);
        assert_eq!(z.data_redundancy, DataRedundancy::SingleLocalZone);
    }

    #[test]
    fn parse_directory_bucket_zone_handles_base_with_embedded_double_dash() {
        // The base part can itself contain `--`; only the final `--<zone>--x-s3`
        // segment matters.
        let z = parse_directory_bucket_zone("my--weird--base--apne1-az4--x-s3")
            .expect("expected directory-bucket parse");
        assert_eq!(z.zone_id, "apne1-az4");
    }

    #[test]
    fn parse_directory_bucket_zone_rejects_missing_zone_segment() {
        // `bucket--x-s3` has no `--<zone>--` separator before the suffix.
        assert!(parse_directory_bucket_zone("bucket--x-s3").is_none());
        // Only the suffix, no base or zone.
        assert!(parse_directory_bucket_zone("--x-s3").is_none());
    }

    #[test]
    fn parse_directory_bucket_zone_rejects_empty_zone_id() {
        // `<base>----x-s3` parses to an empty zone-id segment.
        assert!(parse_directory_bucket_zone("base----x-s3").is_none());
    }
}
