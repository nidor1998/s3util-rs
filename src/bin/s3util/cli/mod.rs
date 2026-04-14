use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use anyhow::Result;
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

    // For cp, the full path is always passed as the key to get_object/put_object.
    // Storage instances are created with an empty base path so that key = full path.
    let (source_key, target_key) = extract_keys(&config)?;

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
                empty_local_storage_path(),
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
                empty_s3_storage_path(&config.target),
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
                empty_s3_storage_path(&config.source),
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
                empty_local_storage_path(),
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
                empty_s3_storage_path(&config.source),
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
                empty_s3_storage_path(&config.target),
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
                empty_s3_storage_path(&config.target),
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
                empty_s3_storage_path(&config.source),
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

/// Extract the full path as the key for each side.
/// For cp, the full path is always passed to get_object/put_object.
/// Storage instances are created with empty base paths.
fn extract_keys(config: &Config) -> Result<(String, String)> {
    let source_key = match &config.source {
        StoragePath::S3 { prefix, .. } => prefix.clone(),
        StoragePath::Local(path) => path.to_string_lossy().to_string(),
        StoragePath::Stdio => String::new(),
    };
    let target_key = match &config.target {
        StoragePath::S3 { prefix, .. } => prefix.clone(),
        StoragePath::Local(path) => path.to_string_lossy().to_string(),
        StoragePath::Stdio => String::new(),
    };
    Ok((source_key, target_key))
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
