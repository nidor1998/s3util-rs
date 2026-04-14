use crate::types::{ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId, StoragePath};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, RequestPayer, ServerSideEncryption,
    StorageClass,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub mod args;

#[derive(Debug, Clone)]
pub struct Config {
    pub source: StoragePath,
    pub target: StoragePath,
    pub show_progress: bool,
    pub source_client_config: Option<ClientConfig>,
    pub target_client_config: Option<ClientConfig>,
    pub force_retry_config: ForceRetryConfig,
    pub tracing_config: Option<TracingConfig>,
    pub transfer_config: TransferConfig,
    pub disable_tagging: bool,
    pub sync_latest_tagging: bool,
    pub server_side_copy: bool,
    pub no_guess_mime_type: bool,
    pub disable_multipart_verify: bool,
    pub disable_etag_verify: bool,
    pub disable_additional_checksum_verify: bool,
    pub storage_class: Option<StorageClass>,
    pub sse: Option<ServerSideEncryption>,
    pub sse_kms_key_id: SseKmsKeyId,
    pub source_sse_c: Option<String>,
    pub source_sse_c_key: SseCustomerKey,
    pub source_sse_c_key_md5: Option<String>,
    pub target_sse_c: Option<String>,
    pub target_sse_c_key: SseCustomerKey,
    pub target_sse_c_key_md5: Option<String>,
    pub canned_acl: Option<ObjectCannedAcl>,
    pub additional_checksum_mode: Option<ChecksumMode>,
    pub additional_checksum_algorithm: Option<ChecksumAlgorithm>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<DateTime<Utc>>,
    pub metadata: Option<HashMap<String, String>>,
    pub no_sync_system_metadata: bool,
    pub no_sync_user_defined_metadata: bool,
    pub website_redirect: Option<String>,
    pub tagging: Option<String>,
    pub put_last_modified_metadata: bool,
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,
    pub disable_payload_signing: bool,
    pub disable_content_md5_header: bool,
    pub full_object_checksum: bool,
    pub source_accelerate: bool,
    pub target_accelerate: bool,
    pub source_request_payer: bool,
    pub target_request_payer: bool,
    pub if_match: bool,
    pub if_none_match: bool,
    pub copy_source_if_match: bool,
    pub disable_stalled_stream_protection: bool,
    pub disable_express_one_zone_additional_checksum: bool,
    pub max_parallel_uploads: u16,
    pub version_id: Option<String>,
    pub is_stdio_source: bool,
    pub is_stdio_target: bool,
}

#[derive(Debug, Clone)]
pub struct ClientConfig {
    pub client_config_location: ClientConfigLocation,
    pub credential: S3Credentials,
    pub region: Option<String>,
    pub endpoint_url: Option<String>,
    pub force_path_style: bool,
    pub accelerate: bool,
    pub request_payer: Option<RequestPayer>,
    pub retry_config: RetryConfig,
    pub cli_timeout_config: CLITimeoutConfig,
    pub disable_stalled_stream_protection: bool,
    pub request_checksum_calculation: RequestChecksumCalculation,
    pub parallel_upload_semaphore: Arc<Semaphore>,
}

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub aws_max_attempts: u32,
    pub initial_backoff_milliseconds: u64,
}

#[derive(Debug, Clone)]
pub struct CLITimeoutConfig {
    pub operation_timeout_milliseconds: Option<u64>,
    pub operation_attempt_timeout_milliseconds: Option<u64>,
    pub connect_timeout_milliseconds: Option<u64>,
    pub read_timeout_milliseconds: Option<u64>,
}

#[derive(Debug, Clone, Copy)]
pub struct TracingConfig {
    pub tracing_level: log::Level,
    pub json_tracing: bool,
    pub aws_sdk_tracing: bool,
    pub span_events_tracing: bool,
    pub disable_color_tracing: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct ForceRetryConfig {
    pub force_retry_count: u32,
    pub force_retry_interval_milliseconds: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct TransferConfig {
    pub multipart_threshold: u64,
    pub multipart_chunksize: u64,
    pub auto_chunksize: bool,
}

impl TransferConfig {
    pub fn is_multipart_upload_required(&self, content_length: u64) -> bool {
        self.multipart_threshold <= content_length
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_multipart_upload_necessary() {
        let transfer_config = TransferConfig {
            multipart_threshold: 8 * 1024 * 1024,
            multipart_chunksize: 8 * 1024 * 1024,
            auto_chunksize: false,
        };

        assert!(transfer_config.is_multipart_upload_required(8 * 1024 * 1024));
        assert!(transfer_config.is_multipart_upload_required((8 * 1024 * 1024) + 1));
        assert!(!transfer_config.is_multipart_upload_required((8 * 1024 * 1024) - 1));
    }
}
