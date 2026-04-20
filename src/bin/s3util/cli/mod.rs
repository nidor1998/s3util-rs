use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Result, anyhow};
use aws_sdk_s3::types::RequestPayer;
use leaky_bucket::RateLimiter;
use tracing::{error, trace};

use s3util_rs::Config;
use s3util_rs::storage::StorageFactory;
use s3util_rs::storage::local::LocalStorageFactory;
use s3util_rs::storage::s3::S3StorageFactory;
use s3util_rs::transfer::{TransferDirection, detect_direction};
use s3util_rs::types::StoragePath;
use s3util_rs::types::error::is_cancelled_error;
use s3util_rs::types::token::create_pipeline_cancellation_token;

pub mod ctrl_c_handler;
pub mod indicator;
pub mod ui_config;

// Default refill interval is 100ms (= 10 refills per second).
const REFILL_PER_INTERVAL_DIVIDER: usize = 10;

fn build_rate_limiter(config: &Config) -> Option<Arc<RateLimiter>> {
    config.rate_limit_bandwidth.map(|bandwidth| {
        let refill = bandwidth as usize / REFILL_PER_INTERVAL_DIVIDER;
        Arc::new(
            RateLimiter::builder()
                .max(bandwidth as usize)
                .initial(bandwidth as usize)
                .refill(refill)
                .fair(true)
                .build(),
        )
    })
}

pub enum ExitStatus {
    Success,
    Warning,
}

impl ExitStatus {
    pub fn code(&self) -> i32 {
        match self {
            ExitStatus::Success => EXIT_CODE_SUCCESS,
            ExitStatus::Warning => EXIT_CODE_WARNING,
        }
    }
}

pub const EXIT_CODE_SUCCESS: i32 = 0;
pub const EXIT_CODE_ERROR: i32 = 1;
pub const EXIT_CODE_WARNING: i32 = 3;

///
/// and `Err` for errors.
pub async fn run_cp(config: Config) -> Result<ExitStatus> {
    let cancellation_token = create_pipeline_cancellation_token();
    ctrl_c_handler::spawn_ctrl_c_handler(cancellation_token.clone());

    let (stats_sender, stats_receiver) = async_channel::unbounded();

    // Determine transfer direction
    let (source_str, target_str) = get_path_strings(&config.source, &config.target);
    let direction = detect_direction(&source_str, &target_str)?;

    trace!(direction = ?direction, "detected transfer direction");

    if let Err(e) = check_local_source_not_directory(&config.source, &direction) {
        error!(error = format!("{e:#}"), "copy failed.");
        return Err(e);
    }

    // For cp, the full path is always passed as the key to get_object/put_object.
    // Storage instances are created with an empty base path so that key = full path.
    let (source_key, target_key) = match extract_keys(&config) {
        Ok(keys) => keys,
        Err(e) => {
            error!(error = format!("{e:#}"), "copy failed.");
            return Err(e);
        }
    };

    // When the user gave a bare `s3://bucket` (no key), show the resolved
    // `s3://bucket/<basename>` in the indicator. Other directory-style targets
    // (local dir, S3 prefix ending in `/`) aren't rewritten here.
    let resolved_target_display = {
        let original = match &config.target {
            StoragePath::S3 { bucket, prefix } => format!("s3://{bucket}/{prefix}"),
            StoragePath::Local(path) => path.to_string_lossy().to_string(),
            StoragePath::Stdio => "-".to_string(),
        };
        if original != target_str {
            Some(format_target_path(&config.target, &target_key))
        } else {
            None
        }
    };

    let show_progress = ui_config::is_progress_indicator_needed(&config);
    let show_result = ui_config::is_show_result_needed(&config);
    let log_sync_summary = config.tracing_config.is_some();

    // Start indicator
    let indicator_handle = indicator::show_indicator(
        stats_receiver,
        show_progress,
        show_result,
        log_sync_summary,
        resolved_target_display,
    );

    let has_warning = Arc::new(AtomicBool::new(false));
    let rate_limit_bandwidth = build_rate_limiter(&config);

    let result = match direction {
        TransferDirection::LocalToS3 => {
            let target_request_payer = if config.target_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = LocalStorageFactory::create(
                config.clone(),
                empty_local_storage_path(),
                cancellation_token.clone(),
                stats_sender.clone(),
                None,
                None,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            let target = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.target),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::local_to_s3::transfer(
                &config,
                source,
                target,
                &source_key,
                &target_key,
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
        TransferDirection::S3ToLocal => {
            let source_request_payer = if config.source_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.source),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            let target = LocalStorageFactory::create(
                config.clone(),
                empty_local_storage_path(),
                cancellation_token.clone(),
                stats_sender.clone(),
                None,
                None,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::s3_to_local::transfer(
                &config,
                source,
                target,
                &source_key,
                &target_key,
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
        TransferDirection::S3ToS3 => {
            let source_request_payer = if config.source_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };
            let target_request_payer = if config.target_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.source),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            let target = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.target),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::s3_to_s3::transfer(
                &config,
                source,
                target,
                &source_key,
                &target_key,
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
        TransferDirection::StdioToS3 => {
            let target_request_payer = if config.target_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let target = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.target),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::stdio_to_s3::transfer(
                &config,
                target,
                &target_key,
                tokio::io::stdin(),
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
        TransferDirection::S3ToStdio => {
            let source_request_payer = if config.source_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = S3StorageFactory::create(
                config.clone(),
                empty_s3_storage_path(&config.source),
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                rate_limit_bandwidth.clone(),
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::s3_to_stdio::transfer(
                &config,
                source,
                &source_key,
                tokio::io::stdout(),
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
    };

    // Send error stat if transfer failed, so indicator can suppress summary
    if result.is_err() {
        let _ = stats_sender
            .send(s3util_rs::types::SyncStatistics::SyncError { key: String::new() })
            .await;
    }

    // Close stats channel to signal indicator to finish
    stats_sender.close();

    // Wait for indicator to finish
    let _ = indicator_handle.await;

    if let Err(e) = &result {
        if is_cancelled_error(e) {
            // ctrl-c-handler already warned about shutdown; match s3sync
            // convention and exit 0 on user cancellation rather than logging
            // a misleading "copy failed." with the inner upload context.
            return Ok(ExitStatus::Success);
        }
        error!(error = format!("{e:#}"), "copy failed.");
        return Err(result.unwrap_err());
    }

    if has_warning.load(std::sync::atomic::Ordering::SeqCst) {
        return Ok(ExitStatus::Warning);
    }

    Ok(ExitStatus::Success)
}

fn get_path_strings(source: &StoragePath, target: &StoragePath) -> (String, String) {
    let source_str = match source {
        StoragePath::S3 { bucket, prefix } => {
            if prefix.is_empty() {
                format!("s3://{}", bucket)
            } else {
                format!("s3://{}/{}", bucket, prefix)
            }
        }
        StoragePath::Local(path) => path.to_string_lossy().to_string(),
        StoragePath::Stdio => "-".to_string(),
    };
    let target_str = match target {
        StoragePath::S3 { bucket, prefix } => {
            if prefix.is_empty() {
                format!("s3://{}", bucket)
            } else {
                format!("s3://{}/{}", bucket, prefix)
            }
        }
        StoragePath::Local(path) => path.to_string_lossy().to_string(),
        StoragePath::Stdio => "-".to_string(),
    };
    (source_str, target_str)
}

/// Extract the full path as the key for each side.
/// For cp, the full path is always passed to get_object/put_object.
/// Storage instances are created with empty base paths.
fn extract_keys(config: &Config) -> Result<(String, String)> {
    let source_key = match &config.source {
        StoragePath::S3 { prefix, .. } => {
            if prefix.is_empty() {
                return Err(anyhow!("source S3 key is required (e.g. s3://bucket/key)"));
            }
            prefix.clone()
        }
        StoragePath::Local(path) => path.to_string_lossy().to_string(),
        StoragePath::Stdio => String::new(),
    };
    let source_basename = std::path::Path::new(&source_key)
        .file_name()
        .map(|f| f.to_string_lossy().to_string())
        .unwrap_or(source_key.clone());

    let target_key = match &config.target {
        StoragePath::S3 { prefix, .. } => {
            // If target is empty or ends with '/', treat as directory prefix — append source basename
            if prefix.is_empty() || prefix.ends_with('/') {
                // With a stdin source there's no basename to derive, so the user must
                // spell the target key explicitly (e.g. `s3://bucket/key`).
                if source_basename.is_empty() {
                    return Err(anyhow!(
                        "target S3 key is required when source is stdin (e.g. s3://bucket/key)"
                    ));
                }
                format!("{prefix}{source_basename}")
            } else {
                prefix.clone()
            }
        }
        StoragePath::Local(path) => {
            let p = path.clone();
            // If target is a directory (existing dir or ends with separator),
            // append the source object's basename — like `aws s3 cp s3://bucket/key .`
            if p.is_dir() || p.to_string_lossy().ends_with(std::path::MAIN_SEPARATOR) {
                p.join(&source_basename).to_string_lossy().to_string()
            } else {
                p.to_string_lossy().to_string()
            }
        }
        StoragePath::Stdio => String::new(),
    };
    Ok((source_key, target_key))
}

/// Format the resolved target path for display.
fn format_target_path(target: &StoragePath, target_key: &str) -> String {
    match target {
        StoragePath::S3 { bucket, .. } => format!("s3://{bucket}/{target_key}"),
        StoragePath::Local(_) => target_key.to_string(),
        StoragePath::Stdio => "-".to_string(),
    }
}

/// Reject local source directories for `cp`.
///
/// LocalStorage::head_object returns a 0-byte success for directories (inherited
/// from s3sync's recursive-sync semantics). `s3util cp` is single-file only, so
/// without this guard a command like `s3util cp /tmp/ s3://bucket/` would silently
/// upload an empty object.
fn check_local_source_not_directory(
    source: &StoragePath,
    direction: &TransferDirection,
) -> Result<()> {
    if !matches!(direction, TransferDirection::LocalToS3) {
        return Ok(());
    }
    if let StoragePath::Local(path) = source
        && path.is_dir()
    {
        return Err(anyhow!(
            "source is a directory: {}. cp command copies a single file.",
            path.display()
        ));
    }
    Ok(())
}

/// Create a LocalStorage base path (empty — full path is passed as the key).
fn empty_local_storage_path() -> StoragePath {
    StoragePath::Local(".".into())
}

/// Create an S3Storage base path with empty prefix (full key is passed to operations).
fn empty_s3_storage_path(original: &StoragePath) -> StoragePath {
    match original {
        StoragePath::S3 { bucket, .. } => StoragePath::S3 {
            bucket: bucket.clone(),
            prefix: String::new(),
        },
        _ => unreachable!("expected S3 storage path"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3util_rs::config::args::{Commands, parse_from_args};
    use std::path::PathBuf;

    fn build_config(args: Vec<&str>) -> Config {
        let cli = parse_from_args(args).unwrap();
        let Commands::Cp(cp_args) = cli.command;
        Config::try_from(cp_args).unwrap()
    }

    #[test]
    fn exit_status_codes() {
        assert_eq!(ExitStatus::Success.code(), EXIT_CODE_SUCCESS);
        assert_eq!(ExitStatus::Warning.code(), EXIT_CODE_WARNING);
        assert_eq!(EXIT_CODE_SUCCESS, 0);
        assert_eq!(EXIT_CODE_ERROR, 1);
        assert_eq!(EXIT_CODE_WARNING, 3);
    }

    #[test]
    fn get_path_strings_formats_each_storage_kind() {
        let s3_with_prefix = StoragePath::S3 {
            bucket: "b".to_string(),
            prefix: "k/v".to_string(),
        };
        let s3_no_prefix = StoragePath::S3 {
            bucket: "b".to_string(),
            prefix: String::new(),
        };
        let local = StoragePath::Local(PathBuf::from("/tmp/x"));
        let stdio = StoragePath::Stdio;

        let (src, tgt) = get_path_strings(&s3_with_prefix, &local);
        assert_eq!(src, "s3://b/k/v");
        assert_eq!(tgt, "/tmp/x");

        let (src, tgt) = get_path_strings(&s3_no_prefix, &stdio);
        assert_eq!(src, "s3://b");
        assert_eq!(tgt, "-");

        let (src, tgt) = get_path_strings(&stdio, &s3_with_prefix);
        assert_eq!(src, "-");
        assert_eq!(tgt, "s3://b/k/v");
    }

    #[test]
    fn format_target_path_for_each_storage_kind() {
        let s3 = StoragePath::S3 {
            bucket: "mybucket".to_string(),
            prefix: String::new(),
        };
        assert_eq!(format_target_path(&s3, "k/v.dat"), "s3://mybucket/k/v.dat");

        let local = StoragePath::Local(PathBuf::from("/x"));
        assert_eq!(format_target_path(&local, "ignored"), "ignored");

        assert_eq!(format_target_path(&StoragePath::Stdio, "ignored"), "-");
    }

    #[test]
    fn empty_local_storage_path_is_dot() {
        match empty_local_storage_path() {
            StoragePath::Local(p) => assert_eq!(p, PathBuf::from(".")),
            _ => panic!("expected Local"),
        }
    }

    #[test]
    fn empty_s3_storage_path_clears_prefix_keeps_bucket() {
        let original = StoragePath::S3 {
            bucket: "mybucket".to_string(),
            prefix: "some/key".to_string(),
        };
        match empty_s3_storage_path(&original) {
            StoragePath::S3 { bucket, prefix } => {
                assert_eq!(bucket, "mybucket");
                assert_eq!(prefix, "");
            }
            _ => panic!("expected S3"),
        }
    }

    #[test]
    fn build_rate_limiter_returns_none_when_unset() {
        let config = build_config(vec!["s3util", "cp", "/tmp/a", "s3://b/k"]);
        assert!(config.rate_limit_bandwidth.is_none());
        assert!(build_rate_limiter(&config).is_none());
    }

    #[test]
    fn build_rate_limiter_returns_some_when_set() {
        let config = build_config(vec![
            "s3util",
            "cp",
            "--rate-limit-bandwidth",
            "10MiB",
            "/tmp/a",
            "s3://b/k",
        ]);
        assert!(config.rate_limit_bandwidth.is_some());
        assert!(build_rate_limiter(&config).is_some());
    }

    #[test]
    fn extract_keys_local_to_s3_object_target() {
        let config = build_config(vec!["s3util", "cp", "/tmp/source.dat", "s3://b/key.dat"]);
        let (src, tgt) = extract_keys(&config).unwrap();
        assert_eq!(src, "/tmp/source.dat");
        assert_eq!(tgt, "key.dat");
    }

    #[test]
    fn extract_keys_local_to_s3_bucket_only_uses_basename() {
        // s3://b with no key → tgt becomes basename of source.
        let config = build_config(vec!["s3util", "cp", "/tmp/source.dat", "s3://b"]);
        let (_, tgt) = extract_keys(&config).unwrap();
        assert_eq!(tgt, "source.dat");
    }

    #[test]
    fn extract_keys_local_to_s3_prefix_with_slash_appends_basename() {
        let config = build_config(vec!["s3util", "cp", "/tmp/source.dat", "s3://b/dir/"]);
        let (_, tgt) = extract_keys(&config).unwrap();
        assert_eq!(tgt, "dir/source.dat");
    }

    #[test]
    fn extract_keys_s3_to_local_with_no_source_key_errors() {
        let config = build_config(vec!["s3util", "cp", "s3://b", "/tmp/dst"]);
        let err = extract_keys(&config).unwrap_err();
        assert!(err.to_string().contains("source S3 key is required"));
    }

    #[test]
    fn extract_keys_stdio_target_yields_empty_target_key() {
        let config = build_config(vec!["s3util", "cp", "s3://b/key", "-"]);
        let (src, tgt) = extract_keys(&config).unwrap();
        assert_eq!(src, "key");
        assert_eq!(tgt, "");
    }

    #[test]
    fn extract_keys_stdio_source_yields_empty_source_key() {
        let config = build_config(vec!["s3util", "cp", "-", "s3://b/key"]);
        let (src, tgt) = extract_keys(&config).unwrap();
        assert_eq!(src, "");
        assert_eq!(tgt, "key");
    }

    #[test]
    fn extract_keys_stdio_to_s3_bucket_only_errors() {
        let config = build_config(vec!["s3util", "cp", "-", "s3://b"]);
        let err = extract_keys(&config).unwrap_err();
        assert!(err.to_string().contains("target S3 key is required"));
    }

    #[test]
    fn extract_keys_stdio_to_s3_prefix_with_slash_errors() {
        let config = build_config(vec!["s3util", "cp", "-", "s3://b/dir/"]);
        let err = extract_keys(&config).unwrap_err();
        assert!(err.to_string().contains("target S3 key is required"));
    }

    #[test]
    fn check_local_source_not_directory_rejects_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let source = StoragePath::Local(tmp.path().to_path_buf());
        let err =
            check_local_source_not_directory(&source, &TransferDirection::LocalToS3).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("source is a directory"), "message: {msg}");
    }

    #[test]
    fn check_local_source_not_directory_allows_file() {
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let source = StoragePath::Local(tmp.path().to_path_buf());
        check_local_source_not_directory(&source, &TransferDirection::LocalToS3).unwrap();
    }

    #[test]
    fn check_local_source_not_directory_allows_nonexistent_path() {
        // head_object downstream turns this into a "file not found" error; the
        // directory guard should not pre-empt that path.
        let source = StoragePath::Local(PathBuf::from("/nonexistent/path/for/test"));
        check_local_source_not_directory(&source, &TransferDirection::LocalToS3).unwrap();
    }

    #[test]
    fn check_local_source_not_directory_skips_non_local_to_s3_direction() {
        // A Local source only reaches transfer for LocalToS3. Guard must be a
        // no-op for every other direction so we don't stat paths that aren't
        // the local source.
        let tmp = tempfile::tempdir().unwrap();
        let source = StoragePath::Local(tmp.path().to_path_buf());
        for direction in [
            TransferDirection::S3ToLocal,
            TransferDirection::S3ToS3,
            TransferDirection::StdioToS3,
            TransferDirection::S3ToStdio,
        ] {
            check_local_source_not_directory(&source, &direction).unwrap();
        }
    }

    #[test]
    fn extract_keys_s3_to_existing_local_directory_appends_basename() {
        // `aws s3 cp s3://bucket/key /existing/dir` resolves the target to
        // /existing/dir/<basename> — exercises the p.is_dir() branch.
        let tmp = tempfile::tempdir().unwrap();
        let target_arg = tmp.path().to_string_lossy().to_string();
        let config = build_config(vec![
            "s3util",
            "cp",
            "s3://b/remote/file.dat",
            target_arg.as_str(),
        ]);
        let (_, tgt) = extract_keys(&config).unwrap();
        let expected = tmp.path().join("file.dat").to_string_lossy().to_string();
        assert_eq!(tgt, expected);
    }

    #[test]
    fn extract_keys_s3_to_local_path_with_trailing_separator_appends_basename() {
        // Non-existent local path ending with the platform separator is also
        // treated as a directory target — the second half of the branch
        // condition in extract_keys.
        let sep = std::path::MAIN_SEPARATOR;
        let target_arg = format!("/tmp/s3util_nonexistent_dir_{sep}");
        let config = build_config(vec![
            "s3util",
            "cp",
            "s3://b/remote/object.bin",
            target_arg.as_str(),
        ]);
        let (_, tgt) = extract_keys(&config).unwrap();
        assert!(tgt.ends_with("object.bin"), "target was: {tgt}");
    }
}
