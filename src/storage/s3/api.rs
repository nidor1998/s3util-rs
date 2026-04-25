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
use aws_sdk_s3::operation::head_bucket::HeadBucketOutput;

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
