use anyhow::Result;
use tracing::error;

use s3util_rs::Config;
use s3util_rs::storage::s3::api::{self, HeadError, HeadObjectOpts};
use s3util_rs::types::StoragePath;

use crate::cli::{ExitStatus, extract_keys, run_copy_phase};

pub async fn run_cp(config: Config) -> Result<ExitStatus> {
    if config.skip_existing && target_exists(&config).await? {
        let (_, target_key) = extract_keys(&config)?;
        let target_display = match &config.target {
            StoragePath::S3 { bucket, .. } => format!("s3://{bucket}/{target_key}"),
            StoragePath::Local(_) => target_key,
            StoragePath::Stdio => "-".to_string(),
        };
        if config.dry_run {
            tracing::info!(target = %target_display, "[dry-run] would skip: target exists.");
        } else {
            tracing::info!(target = %target_display, "Target exists; skipping copy.");
        }
        return Ok(ExitStatus::Success);
    }

    let phase = run_copy_phase(config).await?;
    if phase.cancelled {
        return Ok(ExitStatus::Cancelled);
    }
    if let Err(e) = phase.transfer_result {
        error!(error = format!("{e:#}"), "copy failed.");
        return Err(e);
    }
    if phase.has_warning {
        return Ok(ExitStatus::Warning);
    }
    Ok(ExitStatus::Success)
}

/// Check whether the target already exists. For S3 targets this issues
/// HeadObject; for local targets this is a filesystem exists check. Stdio
/// targets are rejected by `validate_storage_config`, so they cannot reach
/// here — the match arm is `unreachable!`.
async fn target_exists(config: &Config) -> anyhow::Result<bool> {
    let (_, target_key) = extract_keys(config)?;
    match &config.target {
        StoragePath::S3 { bucket, .. } => {
            let target_client_config = config.target_client_config.as_ref().ok_or_else(|| {
                anyhow::anyhow!("internal error: target_client_config missing for s3 target")
            })?;
            let client = target_client_config.create_client().await;
            let opts = HeadObjectOpts {
                version_id: None,
                sse_c: config.target_sse_c.clone(),
                sse_c_key: config.target_sse_c_key.key.clone(),
                sse_c_key_md5: config.target_sse_c_key_md5.clone(),
                enable_additional_checksum: false,
            };
            match api::head_object(&client, bucket, &target_key, opts).await {
                Ok(_) => Ok(true),
                Err(HeadError::NotFound) | Err(HeadError::BucketNotFound) => Ok(false),
                Err(HeadError::Other(e)) => Err(e),
            }
        }
        StoragePath::Local(_) => Ok(tokio::fs::try_exists(&target_key).await?),
        StoragePath::Stdio => {
            unreachable!("validate_storage_config rejects --skip-existing with stdout target")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3util_rs::types::SseCustomerKey;
    use std::collections::HashMap;
    use std::path::PathBuf;

    fn build_local_target_config(target_path: &str) -> Config {
        // Build a minimally-populated Config with a Local target. We bypass
        // CpArgs::TryFrom because `target_exists` only reads `target` and
        // `target_key`-derived fields — no client config or SDK calls are
        // exercised on the Local branch.
        Config {
            source: StoragePath::S3 {
                bucket: "src".to_string(),
                prefix: "k".to_string(),
            },
            target: StoragePath::Local(PathBuf::from(target_path)),
            show_progress: false,
            source_client_config: None,
            target_client_config: None,
            tracing_config: None,
            transfer_config: s3util_rs::config::TransferConfig {
                multipart_threshold: 8 * 1024 * 1024,
                multipart_chunksize: 8 * 1024 * 1024,
                auto_chunksize: false,
            },
            disable_tagging: false,
            server_side_copy: false,
            no_guess_mime_type: false,
            disable_multipart_verify: false,
            disable_etag_verify: false,
            disable_additional_checksum_verify: false,
            storage_class: None,
            sse: None,
            sse_kms_key_id: s3util_rs::types::SseKmsKeyId { id: None },
            source_sse_c: None,
            source_sse_c_key: SseCustomerKey { key: None },
            source_sse_c_key_md5: None,
            target_sse_c: None,
            target_sse_c_key: SseCustomerKey { key: None },
            target_sse_c_key_md5: None,
            canned_acl: None,
            additional_checksum_mode: None,
            additional_checksum_algorithm: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            content_language: None,
            content_type: None,
            expires: None,
            metadata: Some(HashMap::new()),
            no_sync_system_metadata: false,
            no_sync_user_defined_metadata: false,
            website_redirect: None,
            tagging: None,
            put_last_modified_metadata: false,
            disable_payload_signing: false,
            disable_content_md5_header: false,
            full_object_checksum: false,
            source_accelerate: false,
            target_accelerate: false,
            source_request_payer: false,
            target_request_payer: false,
            if_none_match: false,
            disable_stalled_stream_protection: false,
            disable_express_one_zone_additional_checksum: false,
            max_parallel_uploads: 16,
            rate_limit_bandwidth: None,
            version_id: None,
            is_stdio_source: false,
            is_stdio_target: false,
            no_fail_on_verify_error: false,
            skip_existing: true,
            dry_run: false,
        }
    }

    #[tokio::test]
    async fn target_exists_local_returns_true_for_existing_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_string_lossy().to_string();
        let config = build_local_target_config(&path);
        let exists = target_exists(&config).await.unwrap();
        assert!(
            exists,
            "expected existing tempfile to be reported as exists"
        );
    }

    #[tokio::test]
    async fn target_exists_local_returns_false_for_missing_path() {
        // Build a path that definitely does not exist within a real tempdir
        // so the parent directory is valid but the file itself is absent.
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("definitely-missing-file.dat");
        let path = missing.to_string_lossy().to_string();
        let config = build_local_target_config(&path);
        let exists = target_exists(&config).await.unwrap();
        assert!(
            !exists,
            "expected missing path to be reported as not exists"
        );
    }

    fn build_s3_target_config_without_client() -> Config {
        // S3 target but with target_client_config left as None — exercises
        // the defensive "internal error" branch in target_exists.
        let mut config = build_local_target_config("ignored");
        config.target = StoragePath::S3 {
            bucket: "tgt".to_string(),
            prefix: "k".to_string(),
        };
        config
    }

    #[tokio::test]
    async fn target_exists_s3_without_client_config_errors() {
        let config = build_s3_target_config_without_client();
        let err = target_exists(&config).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("internal error"),
            "expected internal error, got: {msg}"
        );
    }
}
