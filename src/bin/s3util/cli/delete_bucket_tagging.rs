use anyhow::Result;
use tracing::info;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::delete_bucket_tagging::DeleteBucketTaggingArgs;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util delete-bucket-tagging s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `DeleteBucketTagging`,
/// removing all tags from the bucket. Silent on success.
pub async fn run_delete_bucket_tagging(
    args: DeleteBucketTaggingArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let client = client_config.create_client().await;

    if args.dry_run {
        info!(bucket = %bucket, "[dry-run] would delete bucket tagging.");
        return Ok(());
    }

    api::delete_bucket_tagging(&client, &bucket).await?;
    info!(bucket = %bucket, "Bucket tagging deleted.");
    Ok(())
}
