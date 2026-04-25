use anyhow::Result;

use aws_sdk_s3::types::{Tag, Tagging};
use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::create_bucket::CreateBucketArgs;
use s3util_rs::storage::s3::api;

use super::ExitStatus;

/// Runtime entry for `s3util create-bucket s3://<BUCKET>`.
///
/// Issues `CreateBucket` using the region from `--target-region`. With
/// `--tagging`, follows up with `PutBucketTagging`. If the tagging step fails
/// after the bucket has been created, exits with [`ExitStatus::Warning`]
/// (exit code 3) and logs a warning explaining the partial state.
/// No automatic rollback is performed.
pub async fn run_create_bucket(
    args: CreateBucketArgs,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let region = client_config.region.clone();
    let client = client_config.create_client().await;

    api::create_bucket(&client, &bucket, region.as_deref()).await?;

    if let Some(raw_tagging) = args.tagging.as_deref() {
        let tags = parse_tagging_to_tags(raw_tagging)?;
        let tagging = Tagging::builder().set_tag_set(Some(tags)).build()?;
        if let Err(e) = api::put_bucket_tagging(&client, &bucket, tagging).await {
            tracing::warn!(
                error = format!("{e:#}"),
                "bucket s3://{bucket} was created but PutBucketTagging failed; \
                 the bucket exists untagged. Retry tagging or delete the bucket manually."
            );
            return Ok(ExitStatus::Warning);
        }
    }

    Ok(ExitStatus::Success)
}

/// Parse a URL-encoded tagging string (e.g. `"key1=val1&key2=val2"`) into a
/// `Vec<Tag>`. Each `key=value` pair is percent-decoded before being passed to
/// the SDK.
fn parse_tagging_to_tags(s: &str) -> Result<Vec<Tag>> {
    if s.is_empty() {
        return Ok(vec![]);
    }
    let mut tags = Vec::new();
    for pair in s.split('&') {
        let mut parts = pair.splitn(2, '=');
        let raw_key = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("invalid tagging pair: {pair}"))?;
        let raw_val = parts.next().unwrap_or("");
        let key = urlencoding::decode(raw_key)
            .map_err(|e| anyhow::anyhow!("invalid percent-encoding in tag key: {e}"))?
            .into_owned();
        let value = urlencoding::decode(raw_val)
            .map_err(|e| anyhow::anyhow!("invalid percent-encoding in tag value: {e}"))?
            .into_owned();
        tags.push(Tag::builder().key(key).value(value).build()?);
    }
    Ok(tags)
}
