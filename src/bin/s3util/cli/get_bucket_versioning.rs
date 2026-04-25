use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_versioning::GetBucketVersioningArgs;
use s3util_rs::output::json::get_bucket_versioning_to_json;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util get-bucket-versioning s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `GetBucketVersioning`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON followed by
/// a newline.  When the bucket has never had versioning configured, S3 returns
/// no `Status` element and the output is `{}`.
pub async fn run_get_bucket_versioning(
    args: GetBucketVersioningArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    let out = api::get_bucket_versioning(&client, &bucket).await?;
    let json = get_bucket_versioning_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
