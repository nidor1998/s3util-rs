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
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_bucket_policy::GetBucketPolicyOutput;
use aws_sdk_s3::operation::get_bucket_versioning::GetBucketVersioningOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_bucket_policy::PutBucketPolicyOutput;
use aws_sdk_s3::operation::put_bucket_tagging::PutBucketTaggingOutput;
use aws_sdk_s3::operation::put_bucket_versioning::PutBucketVersioningOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::{
    BucketLocationConstraint, BucketVersioningStatus, ChecksumMode, CreateBucketConfiguration,
    Tagging, VersioningConfiguration,
};

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
/// success.
///
/// Errors are wrapped with `anyhow::Context` describing the operation; callers
/// pretty-print the chain via `format!("{e:#}")`.
pub async fn head_object(
    client: &Client,
    bucket: &str,
    key: &str,
    opts: HeadObjectOpts,
) -> Result<HeadObjectOutput> {
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

    req.send()
        .await
        .with_context(|| format!("head-object on s3://{bucket}/{key}"))
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

/// Issue `GetObjectTagging` against `bucket`/`key`. Returns the SDK response on success.
///
/// If `version_id` is provided, tags for that specific object version are fetched.
pub async fn get_object_tagging(
    client: &Client,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> Result<GetObjectTaggingOutput> {
    let mut req = client.get_object_tagging().bucket(bucket).key(key);
    if let Some(v) = version_id {
        req = req.version_id(v);
    }
    req.send()
        .await
        .with_context(|| format!("get-object-tagging on s3://{bucket}/{key}"))
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

/// Issue `HeadBucket` against `bucket`. Returns the SDK response on success.
///
/// Errors are wrapped with `anyhow::Context` describing the operation; callers
/// pretty-print the chain via `format!("{e:#}")`.
pub async fn head_bucket(client: &Client, bucket: &str) -> Result<HeadBucketOutput> {
    client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("head-bucket on s3://{bucket}"))
}

/// Issue `CreateBucket` for `bucket` in the given `region`.
///
/// The `us-east-1` quirk: S3 rejects a `LocationConstraint` of `us-east-1`
/// (the API was designed before that region existed). When `region` is `None`
/// or `"us-east-1"`, the constraint is omitted entirely.
pub async fn create_bucket(
    client: &Client,
    bucket: &str,
    region: Option<&str>,
) -> Result<CreateBucketOutput> {
    let mut req = client.create_bucket().bucket(bucket);

    let needs_constraint = region
        .map(|r| !r.is_empty() && r != "us-east-1")
        .unwrap_or(false);

    if needs_constraint {
        let constraint = BucketLocationConstraint::from(region.unwrap());
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();
        req = req.create_bucket_configuration(cfg);
    }

    req.send()
        .await
        .with_context(|| format!("create-bucket on s3://{bucket}"))
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

/// Issue `GetBucketVersioning` for `bucket`. Returns the SDK response on success.
///
/// When versioning has never been configured, S3 returns an empty response
/// (no `Status` element). The caller (`get_bucket_versioning_to_json`) maps
/// this to `{}`.
pub async fn get_bucket_versioning(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketVersioningOutput> {
    client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("get-bucket-versioning on s3://{bucket}"))
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

/// Issue `GetBucketPolicy` for `bucket`. Returns the SDK response on success.
///
/// S3 returns `404 NoSuchBucketPolicy` when no policy is attached; this is
/// surfaced as an error with the original context.
pub async fn get_bucket_policy(client: &Client, bucket: &str) -> Result<GetBucketPolicyOutput> {
    client
        .get_bucket_policy()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("get-bucket-policy on s3://{bucket}"))
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
