use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_object_tagging::GetObjectTaggingArgs;
use s3util_rs::output::json::get_object_tagging_to_json;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util get-object-tagging s3://<BUCKET>/<KEY>`.
///
/// Builds the SDK client from `client_config`, issues `GetObjectTagging`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON
/// followed by a newline.
pub async fn run_get_object_tagging(
    args: GetObjectTaggingArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let (bucket, key) = args
        .bucket_key()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let client = client_config.create_client().await;

    let out =
        api::get_object_tagging(&client, &bucket, &key, args.source_version_id.as_deref()).await?;
    let json = get_object_tagging_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
