use anyhow::Result;

use aws_sdk_s3::types::{Tag, Tagging};
use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::put_bucket_tagging::PutBucketTaggingArgs;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util put-bucket-tagging s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `PutBucketTagging`,
/// replacing all existing tags with the supplied ones. Silent on success.
pub async fn run_put_bucket_tagging(
    args: PutBucketTaggingArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let tagging_str = args.tagging.as_deref().unwrap_or("");
    let tags = parse_tagging_to_tags(tagging_str)?;
    let tagging = Tagging::builder().set_tag_set(Some(tags)).build()?;

    let client = client_config.create_client().await;

    api::put_bucket_tagging(&client, &bucket, tagging).await?;
    Ok(())
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
