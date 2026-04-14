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

    // Determine key for the transfer
    let key = extract_key(&config, &direction)?;

    let show_progress = ui_config::is_progress_indicator_needed(&config);
    let show_result = ui_config::is_show_result_needed(&config);
    let log_sync_summary = config.tracing_config.is_some();

    // Start indicator
    let indicator_handle =
        indicator::show_indicator(stats_receiver, show_progress, show_result, log_sync_summary);

    let has_warning = Arc::new(AtomicBool::new(false));

    let result = match direction {
        TransferDirection::LocalToS3 => {
            let source_request_payer = None;
            let target_request_payer = if config.target_request_payer {
                Some(RequestPayer::Requester)
            } else {
                None
            };

            let source = LocalStorageFactory::create(
                config.clone(),
                config.source.clone(),
                cancellation_token.clone(),
                stats_sender.clone(),
                None,
                source_request_payer,
                None,
                None,
                has_warning.clone(),
                None,
            )
            .await;

            let target = S3StorageFactory::create(
                config.clone(),
                config.target.clone(),
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
                &key,
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
                config.source.clone(),
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
                config.target.clone(),
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
                &key,
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
                config.source.clone(),
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
                config.target.clone(),
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
                &key,
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
                config.target.clone(),
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
                &key,
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
                config.source.clone(),
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
                &key,
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

fn extract_key(config: &Config, direction: &TransferDirection) -> Result<String> {
    match direction {
        TransferDirection::LocalToS3 => {
            // Key is the filename from the local path
            if let StoragePath::Local(path) = &config.source {
                Ok(path
                    .file_name()
                    .map(|f| f.to_string_lossy().to_string())
                    .unwrap_or_default())
            } else {
                Err(anyhow!("expected local source path for LocalToS3"))
            }
        }
        TransferDirection::S3ToLocal => {
            // Key is the S3 object key (prefix)
            if let StoragePath::S3 { prefix, .. } = &config.source {
                Ok(extract_object_key(prefix))
            } else {
                Err(anyhow!("expected S3 source path for S3ToLocal"))
            }
        }
        TransferDirection::S3ToS3 => {
            // Key is the S3 object key from the source
            if let StoragePath::S3 { prefix, .. } = &config.source {
                Ok(extract_object_key(prefix))
            } else {
                Err(anyhow!("expected S3 source path for S3ToS3"))
            }
        }
        TransferDirection::StdioToS3 => {
            // Key is from the target S3 path
            if let StoragePath::S3 { prefix, .. } = &config.target {
                Ok(extract_object_key(prefix))
            } else {
                Err(anyhow!("expected S3 target path for StdioToS3"))
            }
        }
        TransferDirection::S3ToStdio => {
            // Key is the S3 object key from the source
            if let StoragePath::S3 { prefix, .. } = &config.source {
                Ok(extract_object_key(prefix))
            } else {
                Err(anyhow!("expected S3 source path for S3ToStdio"))
            }
        }
    }
}

/// Extract the object key from an S3 prefix.
/// For "dir1/dir2/file.txt" -> "file.txt"
/// For "file.txt" -> "file.txt"
/// For "" -> ""
fn extract_object_key(prefix: &str) -> String {
    if prefix.is_empty() {
        return String::new();
    }
    // For cp (single object), the prefix IS the key
    prefix.to_string()
}
