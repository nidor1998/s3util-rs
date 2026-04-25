use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_tagging::GetBucketTaggingArgs;
use s3util_rs::output::json::get_bucket_tagging_to_json;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util get-bucket-tagging s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `GetBucketTagging`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON
/// followed by a newline.
pub async fn run_get_bucket_tagging(
    args: GetBucketTaggingArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let client = client_config.create_client().await;

    let out = api::get_bucket_tagging(&client, &bucket).await?;
    let json = get_bucket_tagging_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
