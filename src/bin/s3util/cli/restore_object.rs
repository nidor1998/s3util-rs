use anyhow::Result;
use aws_sdk_s3::types::{GlacierJobParameters, RestoreRequest};
use tracing::info;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::restore_object::RestoreObjectArgs;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util restore-object s3://<BUCKET>/<KEY> --days N --tier T`.
///
/// Builds the SDK client from `client_config`, builds a `RestoreRequest`
/// from `--days` and `--tier`, and issues `RestoreObject`. Exits silently
/// on success.
pub async fn run_restore_object(
    args: RestoreObjectArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let (bucket, key) = args
        .bucket_key()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let mut req = RestoreRequest::builder();
    if let Some(d) = args.days {
        req = req.days(d);
    }
    if let Some(t) = args.tier.clone() {
        // For archive (Glacier-class) restore, S3 expects `<Tier>` inside
        // `<GlacierJobParameters>`. The top-level `<Tier>` element is only
        // valid for the (now-deprecated) SELECT type, and S3 rejects it
        // with MalformedXML for archive restores.
        req = req.glacier_job_parameters(GlacierJobParameters::builder().tier(t).build()?);
    }
    let restore_request = req.build();

    let client = client_config.create_client().await;
    if args.dry_run {
        info!(
            bucket = %bucket,
            key = %key,
            version_id = %args.source_version_id.as_deref().unwrap_or_default(),
            days = ?args.days,
            tier = ?args.tier,
            "[dry-run] would restore object."
        );
        return Ok(());
    }
    api::restore_object(
        &client,
        &bucket,
        &key,
        args.source_version_id.as_deref(),
        restore_request,
    )
    .await?;
    info!(
        bucket = %bucket,
        key = %key,
        version_id = %args.source_version_id.as_deref().unwrap_or_default(),
        "Restore initiated."
    );
    Ok(())
}
