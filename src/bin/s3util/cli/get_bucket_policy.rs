use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_policy::GetBucketPolicyArgs;
use s3util_rs::output::json::get_bucket_policy_to_json;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util get-bucket-policy s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `GetBucketPolicy`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON followed by
/// a newline. Mirrors `aws s3api get-bucket-policy --output json`:
/// `{"Policy": "<escaped-JSON-string>"}` where `Policy` is the raw policy
/// JSON double-encoded as a JSON string.
///
/// If S3 returns `404 NoSuchBucketPolicy`, exits 1 with the original error context.
pub async fn run_get_bucket_policy(
    args: GetBucketPolicyArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    let out = api::get_bucket_policy(&client, &bucket).await?;
    let json = get_bucket_policy_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
