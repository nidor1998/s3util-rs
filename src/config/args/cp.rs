use crate::config::Config;
use crate::config::args::common::{self, CommonTransferArgs};
use crate::config::args::value_parser::{human_bytes, metadata, storage_path, tagging};
use crate::types::{SseCustomerKey, SseKmsKeyId, StoragePath};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, ServerSideEncryption, StorageClass,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation;
use clap::Parser;
use std::str::FromStr;

const BOTH_STDIO_SPECIFIED: &str = "source and target cannot both be stdin/stdout (-)\n";
const STDIO_INCOMPATIBLE_WITH_SERVER_SIDE_COPY: &str =
    "stdin/stdout (-) is incompatible with --server-side-copy\n";

#[derive(Parser, Clone, Debug)]
pub struct CpArgs {
    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix], local path, or - for stdin/stdout", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub source: Option<String>,

    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix], local path, or - for stdin/stdout", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub target: Option<String>,

    #[command(flatten)]
    pub common: CommonTransferArgs,
}

impl CpArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    pub(crate) fn source_str(&self) -> &str {
        self.source.as_deref().unwrap_or("")
    }

    pub(crate) fn target_str(&self) -> &str {
        self.target.as_deref().unwrap_or("")
    }

    pub(crate) fn is_source_stdio(&self) -> bool {
        common::is_source_stdio(self.source_str())
    }

    pub(crate) fn is_target_stdio(&self) -> bool {
        common::is_target_stdio(self.target_str())
    }

    pub(crate) fn check_both_stdio(&self) -> Result<(), String> {
        if self.is_source_stdio() && self.is_target_stdio() {
            return Err(BOTH_STDIO_SPECIFIED.to_string());
        }
        Ok(())
    }

    pub(crate) fn check_stdio_server_side_copy_conflict(&self) -> Result<(), String> {
        if self.common.server_side_copy && (self.is_source_stdio() || self.is_target_stdio()) {
            return Err(STDIO_INCOMPATIBLE_WITH_SERVER_SIDE_COPY.to_string());
        }
        Ok(())
    }

    /// Kept as a `&self` method so the existing direct-call test in
    /// `tests.rs` continues to compile.
    #[cfg(test)]
    pub(crate) fn check_at_least_one_s3_or_stdio(&self) -> Result<(), String> {
        common::check_at_least_one_s3_or_stdio(self.source_str(), self.target_str())
    }

    pub(crate) fn validate_storage_config(&self) -> Result<(), String> {
        self.check_both_stdio()?;
        self.check_stdio_server_side_copy_conflict()?;
        self.common
            .validate_common_storage_config(self.source_str(), self.target_str())
    }
}

impl TryFrom<CpArgs> for Config {
    type Error = String;

    fn try_from(value: CpArgs) -> Result<Self, Self::Error> {
        value.validate_storage_config()?;

        let original_cloned_value = value.clone();

        let tracing_config =
            value
                .common
                .verbosity
                .log_level()
                .map(|log_level| crate::config::TracingConfig {
                    tracing_level: log_level,
                    json_tracing: value.common.json_tracing,
                    aws_sdk_tracing: value.common.aws_sdk_tracing,
                    span_events_tracing: value.common.span_events_tracing,
                    disable_color_tracing: value.common.disable_color_tracing,
                });

        let storage_class = value
            .common
            .storage_class
            .map(|storage_class| StorageClass::from_str(&storage_class).unwrap());

        let sse = value
            .common
            .sse
            .map(|sse| ServerSideEncryption::from_str(&sse).unwrap());

        let canned_acl = value
            .common
            .acl
            .map(|acl| ObjectCannedAcl::from_str(&acl).unwrap());

        let mut additional_checksum_algorithm = value
            .common
            .additional_checksum_algorithm
            .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

        let mut checksum_mode = if value.common.enable_additional_checksum {
            Some(ChecksumMode::Enabled)
        } else {
            None
        };

        let tagging = value
            .common
            .tagging
            .map(|tagging| tagging::parse_tagging(&tagging).unwrap());

        let metadata_parsed = if value.common.metadata.is_some() {
            Some(metadata::parse_metadata(&value.common.metadata.unwrap())?)
        } else {
            None
        };

        let mut full_object_checksum = if additional_checksum_algorithm
            .as_ref()
            .is_some_and(|algorithm| algorithm == &ChecksumAlgorithm::Crc64Nvme)
        {
            true
        } else {
            value.common.full_object_checksum
        };

        let source_str = value.source.as_deref().unwrap_or("");
        let target_str = value.target.as_deref().unwrap_or("");

        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(source_str) {
            if super::is_express_onezone_storage(&bucket)
                && !value.common.disable_express_one_zone_additional_checksum
            {
                checksum_mode = Some(ChecksumMode::Enabled);
            }
        }

        let mut request_checksum_calculation = RequestChecksumCalculation::WhenRequired;
        if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(target_str) {
            if super::is_express_onezone_storage(&bucket)
                && additional_checksum_algorithm.is_none()
                && !value.common.disable_express_one_zone_additional_checksum
            {
                additional_checksum_algorithm = Some(ChecksumAlgorithm::Crc64Nvme);
                full_object_checksum = true;
                request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
            } else if additional_checksum_algorithm.is_some() {
                request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
            }
        }

        let (source_client_config, target_client_config) = original_cloned_value
            .common
            .build_client_configs(request_checksum_calculation);

        let is_stdio_source = original_cloned_value.is_source_stdio();
        let is_stdio_target = original_cloned_value.is_target_stdio();

        let rate_limit_bandwidth = value
            .common
            .rate_limit_bandwidth
            .as_ref()
            .map(|bandwidth| human_bytes::parse_human_bandwidth(bandwidth).unwrap());

        Ok(Config {
            source: storage_path::parse_storage_path(
                original_cloned_value.source.as_deref().unwrap_or(""),
            ),
            target: storage_path::parse_storage_path(
                original_cloned_value.target.as_deref().unwrap_or(""),
            ),

            show_progress: value.common.show_progress,

            source_client_config,
            target_client_config,

            tracing_config,

            transfer_config: crate::config::TransferConfig {
                multipart_threshold: human_bytes::parse_human_bytes(
                    &value.common.multipart_threshold,
                )?,
                multipart_chunksize: human_bytes::parse_human_bytes(
                    &value.common.multipart_chunksize,
                )?,
                auto_chunksize: value.common.auto_chunksize,
            },

            disable_tagging: value.common.disable_tagging,
            server_side_copy: value.common.server_side_copy,
            no_guess_mime_type: value.common.no_guess_mime_type,
            disable_multipart_verify: value.common.disable_multipart_verify,
            disable_etag_verify: value.common.disable_etag_verify,
            disable_additional_checksum_verify: value.common.disable_additional_checksum_verify,
            storage_class,
            sse,
            sse_kms_key_id: SseKmsKeyId {
                id: value.common.sse_kms_key_id,
            },
            source_sse_c: value.common.source_sse_c,
            source_sse_c_key: SseCustomerKey {
                key: value.common.source_sse_c_key,
            },
            source_sse_c_key_md5: value.common.source_sse_c_key_md5,
            target_sse_c: value.common.target_sse_c,
            target_sse_c_key: SseCustomerKey {
                key: value.common.target_sse_c_key,
            },
            target_sse_c_key_md5: value.common.target_sse_c_key_md5,
            canned_acl,
            additional_checksum_algorithm,
            additional_checksum_mode: checksum_mode,
            cache_control: value.common.cache_control,
            content_disposition: value.common.content_disposition,
            content_encoding: value.common.content_encoding,
            content_language: value.common.content_language,
            content_type: value.common.content_type,
            expires: value.common.expires,
            metadata: metadata_parsed,
            website_redirect: value.common.website_redirect,
            no_sync_system_metadata: value.common.no_sync_system_metadata,
            no_sync_user_defined_metadata: value.common.no_sync_user_defined_metadata,
            tagging,
            put_last_modified_metadata: value.common.put_last_modified_metadata,
            disable_payload_signing: value.common.disable_payload_signing,
            disable_content_md5_header: value.common.disable_content_md5_header,
            full_object_checksum,
            source_accelerate: value.common.source_accelerate,
            target_accelerate: value.common.target_accelerate,
            source_request_payer: value.common.source_request_payer,
            target_request_payer: value.common.target_request_payer,
            if_none_match: value.common.if_none_match,
            disable_stalled_stream_protection: value.common.disable_stalled_stream_protection,
            disable_express_one_zone_additional_checksum: value
                .common
                .disable_express_one_zone_additional_checksum,
            max_parallel_uploads: value.common.max_parallel_uploads,
            rate_limit_bandwidth,
            version_id: value.common.source_version_id,
            is_stdio_source,
            is_stdio_target,
        })
    }
}
