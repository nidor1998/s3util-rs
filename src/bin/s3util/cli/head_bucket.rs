use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::head_bucket::HeadBucketArgs;
use s3util_rs::output::json::head_bucket_to_json;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util head-bucket s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `HeadBucket`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON
/// followed by a newline.
// TODO(task-10): remove once main.rs dispatches HeadBucket
#[allow(dead_code)]
pub async fn run_head_bucket(args: HeadBucketArgs, client_config: ClientConfig) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    let out = api::head_bucket(&client, &bucket).await?;
    let json = head_bucket_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
