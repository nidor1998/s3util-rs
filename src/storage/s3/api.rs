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
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::delete_object_tagging::DeleteObjectTaggingOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::types::{ChecksumMode, Tagging};

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
