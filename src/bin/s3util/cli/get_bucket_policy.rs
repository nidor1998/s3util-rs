use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_policy::GetBucketPolicyArgs;
use s3util_rs::output::json::get_bucket_policy_to_json;
use s3util_rs::storage::s3::api::{self, HeadError};

use super::ExitStatus;

/// Runtime entry for `s3util get-bucket-policy s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `GetBucketPolicy`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON followed by
/// a newline. Mirrors `aws s3api get-bucket-policy --output json`:
/// `{"Policy": "<escaped-JSON-string>"}` where `Policy` is the raw policy
/// JSON double-encoded as a JSON string.
///
/// Returns `ExitStatus::NotFound` (exit code 4) when S3 reports
/// `NoSuchBucket` (bucket missing) or `NoSuchBucketPolicy` (no policy attached).
pub async fn run_get_bucket_policy(
    args: GetBucketPolicyArgs,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    match api::get_bucket_policy(&client, &bucket).await {
        Ok(out) => {
            let json = get_bucket_policy_to_json(&out);
            let pretty = serde_json::to_string_pretty(&json)?;
            println!("{pretty}");
            Ok(ExitStatus::Success)
        }
        Err(HeadError::NotFound) => {
            tracing::error!("policy for s3://{bucket} not found");
            Ok(ExitStatus::NotFound)
        }
        Err(HeadError::Other(e)) => Err(e),
    }
}
