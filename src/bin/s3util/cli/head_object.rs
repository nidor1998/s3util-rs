use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::head_object::HeadObjectArgs;
use s3util_rs::output::json::head_object_to_json;
use s3util_rs::storage::s3::api::{self, HeadObjectOpts};

/// Runtime entry for `s3util head-object s3://<BUCKET>/<KEY>`.
///
/// Builds the SDK client from `client_config`, issues `HeadObject`,
/// and prints the response as AWS-CLI-shape pretty-printed JSON
/// followed by a newline.
pub async fn run_head_object(args: HeadObjectArgs, client_config: ClientConfig) -> Result<()> {
    let (bucket, key) = args
        .bucket_key()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let client = client_config.create_client().await;

    let opts = HeadObjectOpts {
        version_id: args.source_version_id.clone(),
        sse_c: args.source_sse_c.clone(),
        sse_c_key: args.source_sse_c_key.clone(),
        sse_c_key_md5: args.source_sse_c_key_md5.clone(),
        enable_additional_checksum: args.enable_additional_checksum,
    };

    let out = api::head_object(&client, &bucket, &key, opts).await?;
    let json = head_object_to_json(&out);
    let pretty = serde_json::to_string_pretty(&json)?;
    println!("{pretty}");
    Ok(())
}
