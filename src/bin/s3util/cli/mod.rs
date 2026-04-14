use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::{Result, anyhow};
use aws_sdk_s3::types::RequestPayer;
use tracing::{error, trace};

use s3util_rs::Config;
use s3util_rs::storage::StorageFactory;
use s3util_rs::storage::local::LocalStorageFactory;
use s3util_rs::storage::s3::S3StorageFactory;
use s3util_rs::transfer::{TransferDirection, detect_direction};
use s3util_rs::types::StoragePath;
use s3util_rs::types::token::create_pipeline_cancellation_token;

pub mod ctrl_c_handler;
pub mod indicator;
pub mod ui_config;

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

    // Determine source/target keys and storage paths for cp.
    // s3sync's Storage model uses base_path + key internally.
    // For local: base = parent dir, key = filename.
    // For S3: prefix is already the full object key, key = "".
    let (source_storage_path, source_key, target_storage_path, target_key) =
        extract_storage_paths_and_keys(&config, &direction)?;

    let show_progress = ui_config::is_progress_indicator_needed(&config);
    let show_result = ui_config::is_show_result_needed(&config);
    let log_sync_summary = config.tracing_config.is_some();

    // Start indicator
    let indicator_handle =
        indicator::show_indicator(stats_receiver, show_progress, show_result, log_sync_summary);

    let has_warning = Arc::new(AtomicBool::new(false));

    let result = match direction {
        TransferDirection::LocalToS3 => {
            let target_request_payer = if config.target_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = LocalStorageFactory::create(
                config.clone(),
                source_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                None,
                None,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            let target = S3StorageFactory::create(
                config.clone(),
                target_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                None,
                None,
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
                source_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            let target = LocalStorageFactory::create(
                config.clone(),
                target_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                None,
                None,
                None,
                None,
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
                source_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            let target = S3StorageFactory::create(
                config.clone(),
                target_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                None,
                None,
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
                target_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.target_client_config.clone(),
                target_request_payer,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::stdio_to_s3::transfer(
                &config,
                target,
                &target_key,
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
                source_storage_path,
                cancellation_token.clone(),
                stats_sender.clone(),
                config.source_client_config.clone(),
                source_request_payer,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            s3util_rs::transfer::s3_to_stdio::transfer(
                &config,
                source,
                &source_key,
                cancellation_token.clone(),
                stats_sender.clone(),
            )
            .await
        }
    };

    // Close stats channel to signal indicator to finish
    stats_sender.close();

    // Wait for indicator to finish
    let _ = indicator_handle.await;

    if let Err(e) = &result {
        error!(error = %e, "transfer failed.");
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

/// For cp, source and target paths are independent.
/// s3sync's Storage uses base_path + key internally:
///   - S3: full_key = prefix + key
///   - Local: file_path = path / key
///
/// For cp we split each side so that:
///   - Local: StoragePath = parent dir, key = filename
///   - S3: StoragePath = S3 { bucket, prefix="" }, key = full object key
///   - Stdio: no Storage needed, key = target S3 key or source S3 key
///
/// Returns (source_storage_path, source_key, target_storage_path, target_key).
fn extract_storage_paths_and_keys(
    config: &Config,
    direction: &TransferDirection,
) -> Result<(StoragePath, String, StoragePath, String)> {
    let (source_storage_path, source_key) = split_for_storage(&config.source)?;
    let (target_storage_path, target_key) = split_for_storage(&config.target)?;

    // For stdio directions, the stdio side has no Storage — use a dummy.
    match direction {
        TransferDirection::LocalToS3 | TransferDirection::S3ToLocal | TransferDirection::S3ToS3 => {
        }
        TransferDirection::StdioToS3 => {
            // source is stdio — only target matters
        }
        TransferDirection::S3ToStdio => {
            // target is stdio — only source matters
        }
    }

    Ok((
        source_storage_path,
        source_key,
        target_storage_path,
        target_key,
    ))
}

/// Split a StoragePath into (base StoragePath for Storage creation, key for operations).
fn split_for_storage(path: &StoragePath) -> Result<(StoragePath, String)> {
    match path {
        StoragePath::S3 { bucket, prefix } => {
            // S3: Storage gets bucket with empty prefix, key is the full object key.
            Ok((
                StoragePath::S3 {
                    bucket: bucket.clone(),
                    prefix: String::new(),
                },
                prefix.clone(),
            ))
        }
        StoragePath::Local(path) => {
            // Local: Storage gets parent dir, key is the filename.
            let parent = path
                .parent()
                .ok_or_else(|| anyhow!("local path has no parent directory: {:?}", path))?;
            let filename = path
                .file_name()
                .ok_or_else(|| anyhow!("local path has no filename: {:?}", path))?
                .to_string_lossy()
                .to_string();
            Ok((StoragePath::Local(parent.to_path_buf()), filename))
        }
        StoragePath::Stdio => {
            // Stdio has no Storage — return dummy values.
            Ok((StoragePath::Stdio, String::new()))
        }
    }
}
