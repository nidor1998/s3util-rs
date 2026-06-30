use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result};
use aws_sdk_s3::operation::get_object_annotation::GetObjectAnnotationOutput;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumType};
use tempfile::NamedTempFile;
use tracing::{info, warn};

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_object_annotation::GetObjectAnnotationArgs;
use s3util_rs::output::json::get_object_annotation_to_json;
use s3util_rs::storage::annotation;
use s3util_rs::storage::s3::api::{self, GetObjectAnnotationParams, HeadError};

use super::ExitStatus;

/// Pick the single additional checksum S3 returned, mapped to the algorithm we
/// can recompute locally. Returns `None` when no supported checksum is present.
fn detect_checksum(out: &GetObjectAnnotationOutput) -> Option<(ChecksumAlgorithm, String)> {
    if let Some(v) = out.checksum_crc64_nvme() {
        return Some((ChecksumAlgorithm::Crc64Nvme, v.to_string()));
    }
    if let Some(v) = out.checksum_crc32() {
        return Some((ChecksumAlgorithm::Crc32, v.to_string()));
    }
    if let Some(v) = out.checksum_crc32_c() {
        return Some((ChecksumAlgorithm::Crc32C, v.to_string()));
    }
    if let Some(v) = out.checksum_sha1() {
        return Some((ChecksumAlgorithm::Sha1, v.to_string()));
    }
    if let Some(v) = out.checksum_sha256() {
        return Some((ChecksumAlgorithm::Sha256, v.to_string()));
    }
    None
}

/// Runtime entry for
/// `s3util get-object-annotation s3://<BUCKET>/<KEY> <OUTFILE> --annotation-name N`.
///
/// Fetches the annotation payload (checksum-mode ENABLED), buffers it in memory
/// (≤1 MiB), verifies content length, the ETag/MD5 (only for AES256 objects),
/// and the additional checksum (if any); warns when neither check applies. Then
/// writes the payload to a temp file and atomically renames it to `<OUTFILE>`
/// (or streams to stdout when `<OUTFILE>` is `-`), and prints AWS-CLI-shape JSON
/// metadata (file mode only). Returns `ExitStatus::NotFound` (exit 4) when the
/// bucket, object, or version does not exist; a verification mismatch returns
/// `Err` (exit 1).
pub async fn run_get_object_annotation(
    args: GetObjectAnnotationArgs,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let (bucket, key) = args
        .bucket_key()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let annotation_name = args
        .annotation_name
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--annotation-name is required"))?;
    annotation::validate_annotation_name(annotation_name)?;
    let outfile = args
        .outfile
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("outfile is required"))?;

    let request_payer = client_config.request_payer.clone();
    let client = client_config.create_client().await;

    let params = GetObjectAnnotationParams {
        bucket: &bucket,
        key: &key,
        annotation_name,
        version_id: args.target_version_id.as_deref(),
        request_payer,
    };

    let out = match api::get_object_annotation(&client, params).await {
        Ok(out) => out,
        Err(HeadError::BucketNotFound) => {
            tracing::error!("bucket s3://{bucket} not found");
            return Ok(ExitStatus::NotFound);
        }
        Err(HeadError::NotFound) => {
            match args.target_version_id.as_deref() {
                Some(v) => tracing::error!("s3://{bucket}/{key} (versionId={v}) not found"),
                None => tracing::error!("object s3://{bucket}/{key} not found"),
            }
            return Ok(ExitStatus::NotFound);
        }
        Err(HeadError::Other(e)) => return Err(e),
    };

    // Gather everything we need from `out` before consuming its payload body.
    let json = get_object_annotation_to_json(&out);
    let e_tag = out.e_tag().map(str::to_string);
    let sse = out.server_side_encryption().cloned();
    let content_length = out.content_length();
    let is_composite = matches!(out.checksum_type(), Some(ct) if *ct == ChecksumType::Composite);
    let checksum = if is_composite {
        None
    } else {
        detect_checksum(&out)
    };

    // Consume the payload body with a bounded read (moves the public
    // `annotation_payload` field). Streaming with a hard cap means a buggy or
    // hostile endpoint returning more than the 1 MiB annotation limit can't OOM
    // the process before the content-length check runs.
    let mut body = out.annotation_payload;
    let cap = annotation::MAX_ANNOTATION_PAYLOAD_LEN;
    let mut payload: Vec<u8> = Vec::new();
    while let Some(chunk) = body
        .try_next()
        .await
        .context("reading annotation payload body")?
    {
        payload.extend_from_slice(&chunk);
        if payload.len() > cap {
            anyhow::bail!(
                "annotation payload for s3://{bucket}/{key} exceeds the 1 MiB limit ({cap} bytes)"
            );
        }
    }

    // Content-length sanity check (always possible; mismatch is fatal).
    if let Some(len) = content_length
        && len != payload.len() as i64
    {
        anyhow::bail!(
            "content length mismatch for s3://{bucket}/{key}: response said {len} bytes, received {}",
            payload.len()
        );
    }

    // Integrity verification.
    let mut verified = false;
    match annotation::verify_etag_md5(&payload, e_tag.as_deref(), sse.as_ref()) {
        Some(true) => verified = true,
        Some(false) => anyhow::bail!("ETag (MD5) verification failed for s3://{bucket}/{key}"),
        None => {}
    }
    if let Some((algo, expected)) = &checksum {
        if annotation::verify_additional_checksum(&payload, algo.clone(), expected) {
            verified = true;
        } else {
            anyhow::bail!(
                "{} checksum verification failed for s3://{bucket}/{key}",
                algo.as_str()
            );
        }
    }
    if !verified {
        warn!(
            bucket = %bucket,
            key = %key,
            "payload integrity could not be verified (no AES256 ETag and no additional checksum)."
        );
    }

    // Output.
    if outfile == "-" {
        std::io::stdout()
            .write_all(&payload)
            .context("writing annotation payload to stdout")?;
        return Ok(ExitStatus::Success);
    }

    let path = Path::new(outfile);
    let parent = match path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };
    let mut tmp = NamedTempFile::new_in(parent)
        .with_context(|| format!("creating temp file next to {outfile}"))?;
    tmp.write_all(&payload)
        .context("writing annotation payload to temp file")?;
    tmp.flush()
        .context("flushing annotation payload temp file")?;
    tmp.persist(path)
        .map_err(|e| anyhow::anyhow!("persisting annotation payload to {outfile}: {e}"))?;

    println!("{}", serde_json::to_string_pretty(&json)?);
    let outcome = if verified {
        "written and verified"
    } else {
        "written, but integrity could NOT be verified"
    };
    info!(
        bucket = %bucket,
        key = %key,
        annotation_name = %annotation_name,
        outfile = %outfile,
        "Annotation payload {}.",
        outcome
    );
    Ok(ExitStatus::Success)
}
