use crate::config::args::value_parser::{
    canned_acl, checksum_algorithm, human_bytes, metadata, sse, storage_class, storage_path,
    tagging, url,
};
use crate::config::{CLITimeoutConfig, ClientConfig, Config, RetryConfig};
use crate::types::{
    AccessKeys, ClientConfigLocation, S3Credentials, SseCustomerKey, SseKmsKeyId, StoragePath,
};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectCannedAcl, RequestPayer, ServerSideEncryption,
    StorageClass,
};
use aws_smithy_types::checksum_config::RequestChecksumCalculation;
use chrono::{DateTime, Utc};
use clap::Parser;
use clap::builder::NonEmptyStringValueParser;
use clap_verbosity_flag::{Verbosity, WarnLevel};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;

pub(crate) const DEFAULT_AWS_MAX_ATTEMPTS: u32 = 10;
pub(crate) const DEFAULT_INITIAL_BACKOFF_MILLISECONDS: u64 = 100;
pub(crate) const DEFAULT_JSON_TRACING: bool = false;
pub(crate) const DEFAULT_AWS_SDK_TRACING: bool = false;
pub(crate) const DEFAULT_SPAN_EVENTS_TRACING: bool = false;
pub(crate) const DEFAULT_DISABLE_COLOR_TRACING: bool = false;
pub(crate) const DEFAULT_MULTIPART_THRESHOLD: &str = "8MiB";
pub(crate) const DEFAULT_MULTIPART_CHUNKSIZE: &str = "8MiB";
pub(crate) const DEFAULT_AUTO_CHUNKSIZE: bool = false;
pub(crate) const DEFAULT_NO_SYNC_SYSTEM_METADATA: bool = false;
pub(crate) const DEFAULT_NO_SYNC_USER_DEFINED_METADATA: bool = false;
pub(crate) const DEFAULT_FORCE_PATH_STYLE: bool = false;
pub(crate) const DEFAULT_DISABLE_TAGGING: bool = false;
pub(crate) const DEFAULT_NO_GUESS_MIME_TYPE: bool = false;
pub(crate) const DEFAULT_SERVER_SIDE_COPY: bool = false;
pub(crate) const DEFAULT_DISABLE_MULTIPART_VERIFY: bool = false;
pub(crate) const DEFAULT_DISABLE_ETAG_VERIFY: bool = false;
pub(crate) const DEFAULT_DISABLE_ADDITIONAL_CHECKSUM_VERIFY: bool = false;
pub(crate) const DEFAULT_ENABLE_ADDITIONAL_CHECKSUM: bool = false;
pub(crate) const DEFAULT_PUT_LAST_MODIFIED_METADATA: bool = false;
pub(crate) const DEFAULT_DISABLE_STALLED_STREAM_PROTECTION: bool = false;
pub(crate) const DEFAULT_DISABLE_PAYLOAD_SIGNING: bool = false;
pub(crate) const DEFAULT_DISABLE_CONTENT_MD5_HEADER: bool = false;
pub(crate) const DEFAULT_FULL_OBJECT_CHECKSUM: bool = false;
pub(crate) const DEFAULT_DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM: bool = false;
pub(crate) const DEFAULT_MAX_PARALLEL_MULTIPART_UPLOADS: u16 = 16;
pub(crate) const DEFAULT_ACCELERATE: bool = false;
pub(crate) const DEFAULT_REQUEST_PAYER: bool = false;
pub(crate) const DEFAULT_SOURCE_NO_SIGN_REQUEST: bool = false;
pub(crate) const DEFAULT_SHOW_PROGRESS: bool = false;
pub(crate) const DEFAULT_IF_NONE_MATCH: bool = false;

pub(crate) const NO_S3_STORAGE_SPECIFIED: &str = "either SOURCE or TARGET must be s3://\n";
pub(crate) const BOTH_LOCAL_SPECIFIED: &str = "source and target cannot both be local paths\n";
pub(crate) const SERVER_SIDE_COPY_REQUIRES_BOTH_S3: &str =
    "with --server-side-copy, both source and target must be s3://\n";
pub(crate) const VERSION_ID_REQUIRES_S3_SOURCE: &str =
    "with --source-version-id, source must be s3://\n";
pub(crate) const LOCAL_STORAGE_SPECIFIED_WITH_STORAGE_CLASS: &str =
    "with --storage-class, target storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SSE: &str =
    "with --sse/--sse-kms-key-id, target storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACL: &str =
    "with --acl, target storage must be s3://\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_AUTO_CHUNKSIZE: &str =
    "with --auto-chunksize, source storage must be s3://\n";
pub(crate) const LOCAL_STORAGE_SPECIFIED_WITH_SSE_C: &str =
    "with --source-sse-c/--target-sse-c, remote storage must be s3://\n";
pub(crate) const SSE_KMS_KEY_ID_ARGUMENTS_CONFLICT: &str =
    "--sse-kms-key-id must be used with --sse aws:kms | aws:kms:dsse\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL: &str =
    "with --source-endpoint-url, source storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL: &str =
    "with --target-endpoint-url, target storage must be s3://\n";
pub(crate) const NO_SOURCE_CREDENTIAL_REQUIRED: &str = "no source credential required\n";
pub(crate) const NO_TARGET_CREDENTIAL_REQUIRED: &str = "no target credential required\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_METADATA_OPTION: &str =
    "with metadata related option, target storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_PAYLOAD_SIGNING: &str =
    "with --disable-payload-signing, target storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_CONTENT_MD5_HEADER: &str =
    "with --disable-content-md5-header, target storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_FULL_OBJECT_CHECKSUM: &str =
    "with --full-object-checksum, target storage must be s3://\n";
pub(crate) const FULL_OBJECT_CHECKSUM_NOT_SUPPORTED: &str =
    "Only CRC32/CRC32C/CRC64NVME supports full object checksum\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE: &str =
    "with --source-accelerate, source storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE: &str =
    "with --target-accelerate, target storage must be s3://\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER: &str =
    "with --source-request-payer, source storage must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER: &str =
    "with --target-request-payer, target storage must be s3://\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST: &str =
    "with --source-no-sign-request, source must be s3://\n";
pub(crate) const TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ADDITIONAL_CHECKSUM_ALGORITHM: &str =
    "with --additional-checksum-algorithm, target storage must be s3://\n";
pub(crate) const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENABLE_ADDITIONAL_CHECKSUM: &str =
    "with --enable-additional-checksum, source storage must be s3://\n";
pub(crate) const TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX: &str =
    "target directory does not exist";

#[derive(Parser, Clone, Debug)]
pub struct CommonTransferArgs {
    /// Show progress bar.
    #[arg(long, env, default_value_t = DEFAULT_SHOW_PROGRESS, help_heading = "General")]
    pub show_progress: bool,

    #[arg(long, env, default_value_t = DEFAULT_SERVER_SIDE_COPY, help_heading = "General",
    long_help = r#"Use server-side copy. This option is only available when both source and target are S3 storage.
It cannot work between different object storages or regions."#)]
    pub server_side_copy: bool,

    /// Location of the file that the AWS CLI uses to store configuration profiles
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    pub aws_config_file: Option<PathBuf>,

    /// Location of the file that the AWS CLI uses to store access keys
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    pub aws_shared_credentials_file: Option<PathBuf>,

    /// Source AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["source_access_key", "source_secret_access_key", "source_session_token"], help_heading = "AWS Configuration")]
    pub source_profile: Option<String>,

    /// Source access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_secret_access_key", help_heading = "AWS Configuration")]
    pub source_access_key: Option<String>,

    /// Source secret access key
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key", help_heading = "AWS Configuration")]
    pub source_secret_access_key: Option<String>,

    /// Source session token
    #[arg(long, env, conflicts_with_all = ["source_profile"], requires = "source_access_key", help_heading = "AWS Configuration")]
    pub source_session_token: Option<String>,

    /// Source region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "Source Options")]
    pub source_region: Option<String>,

    /// Source endpoint url
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "Source Options")]
    pub source_endpoint_url: Option<String>,

    /// Use Amazon S3 Transfer Acceleration for the source bucket.
    #[arg(long, env, default_value_t = DEFAULT_ACCELERATE, help_heading = "Source Options")]
    pub source_accelerate: bool,

    /// Use request payer for the source bucket.
    #[arg(long, env, default_value_t = DEFAULT_REQUEST_PAYER, help_heading = "Source Options")]
    pub source_request_payer: bool,

    /// Do not sign the request. If this argument is specified, credentials will not be loaded
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_SOURCE_NO_SIGN_REQUEST,
        conflicts_with_all = [
            "source_profile",
            "source_access_key",
            "source_secret_access_key",
            "source_session_token",
            "source_request_payer",
        ],
        help_heading = "AWS Configuration"
    )]
    pub source_no_sign_request: bool,

    /// Force path-style addressing for source endpoint.
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE, help_heading = "Source Options")]
    pub source_force_path_style: bool,

    /// Target AWS CLI profile
    #[arg(long, env, conflicts_with_all = ["target_access_key", "target_secret_access_key", "target_session_token"], help_heading = "AWS Configuration")]
    pub target_profile: Option<String>,

    /// Target access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_secret_access_key", help_heading = "AWS Configuration")]
    pub target_access_key: Option<String>,

    /// Target secret access key
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", help_heading = "AWS Configuration")]
    pub target_secret_access_key: Option<String>,

    /// Target session token
    #[arg(long, env, conflicts_with_all = ["target_profile"], requires = "target_access_key", help_heading = "AWS Configuration")]
    pub target_session_token: Option<String>,

    /// Target region
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "Target Options")]
    pub target_region: Option<String>,

    /// Target endpoint url
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "Target Options")]
    pub target_endpoint_url: Option<String>,

    /// Use Amazon S3 Transfer Acceleration for the target bucket.
    #[arg(long, env, default_value_t = DEFAULT_ACCELERATE, help_heading = "Target Options")]
    pub target_accelerate: bool,

    /// Use request payer for the target bucket.
    #[arg(long, env, default_value_t = DEFAULT_REQUEST_PAYER, help_heading = "Target Options")]
    pub target_request_payer: bool,

    /// Force path-style addressing for target endpoint.
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE, help_heading = "Target Options")]
    pub target_force_path_style: bool,

    #[arg(long, env, value_parser = storage_class::parse_storage_class, help_heading = "Target Options",
    long_help = r#"Type of storage to use for the target object.
Valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER |
               DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE"#)]
    pub storage_class: Option<String>,

    /// Additional checksum algorithm for upload
    #[arg(long, env, value_parser = checksum_algorithm::parse_checksum_algorithm, help_heading = "Verification")]
    pub additional_checksum_algorithm: Option<String>,

    #[arg(long, env, default_value_t = DEFAULT_FULL_OBJECT_CHECKSUM, help_heading = "Verification", long_help=r#"Use full object checksum for verification. CRC64NVME automatically uses full object checksum.
This option cannot be used with SHA1/SHA256 additional checksum."#)]
    pub full_object_checksum: bool,

    /// Enable additional checksum for download
    #[arg(long, env, default_value_t = DEFAULT_ENABLE_ADDITIONAL_CHECKSUM, help_heading = "Verification")]
    pub enable_additional_checksum: bool,

    /// Disable multipart upload verification with ETag/additional checksum.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_MULTIPART_VERIFY, help_heading = "Verification")]
    pub disable_multipart_verify: bool,

    /// Disable ETag verification.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_ETAG_VERIFY, help_heading = "Verification")]
    pub disable_etag_verify: bool,

    #[arg(long, env, requires = "additional_checksum_algorithm", default_value_t = DEFAULT_DISABLE_ADDITIONAL_CHECKSUM_VERIFY, help_heading = "Verification",
    long_help=r#"Disable additional checksum verification
But use additional checksum for upload (The hash value is stored in the target object)."#)]
    pub disable_additional_checksum_verify: bool,

    /// Maximum number of parallel multipart uploads/downloads
    #[arg(long, env, default_value_t = DEFAULT_MAX_PARALLEL_MULTIPART_UPLOADS, value_parser = clap::value_parser!(u16).range(1..), help_heading = "Performance")]
    pub max_parallel_uploads: u16,

    /// Rate limit bandwidth (bytes per sec). Allow suffixes: MB, MiB, GB, GiB
    #[arg(long, env, value_parser = human_bytes::check_human_bandwidth, help_heading = "Performance")]
    pub rate_limit_bandwidth: Option<String>,

    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_THRESHOLD, value_parser = human_bytes::check_human_bytes, help_heading = "Multipart Settings",
    long_help=r#"Object size threshold for multipart upload.
Allow suffixes: MB, MiB, GB, GiB.
The larger the size, the larger the memory usage."#)]
    pub multipart_threshold: String,

    #[arg(long, env, conflicts_with_all = ["auto_chunksize"], default_value = DEFAULT_MULTIPART_CHUNKSIZE, value_parser = human_bytes::check_human_bytes, help_heading = "Multipart Settings",
    long_help=r#"Chunk size for multipart upload of individual files.
Allow suffixes: MB, MiB, GB, GiB.
The larger the size, the larger the memory usage."#)]
    pub multipart_chunksize: String,

    #[arg(long, env, conflicts_with_all = ["multipart_threshold", "multipart_chunksize"], default_value_t = DEFAULT_AUTO_CHUNKSIZE, help_heading = "Multipart Settings",
    long_help=r#"Automatically adjusts a chunk size to match the source or target.
It takes extra HEAD requests(1 API call per part)."#)]
    pub auto_chunksize: bool,

    /// Cache-Control HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub cache_control: Option<String>,

    /// Content-Disposition HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub content_disposition: Option<String>,

    /// Content-Encoding HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub content_encoding: Option<String>,

    /// Content-Language HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub content_language: Option<String>,

    /// Content-Type HTTP header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub content_type: Option<String>,

    #[arg(
        long,
        env,
        help_heading = "Metadata/Headers",
        long_help = r#"Expires HTTP header to set on the target object(RFC3339 datetime)
Example: 2023-02-19T12:00:00Z"#
    )]
    pub expires: Option<DateTime<Utc>>,

    #[arg(long, env, value_parser = metadata::check_metadata, help_heading = "Metadata/Headers", long_help=r#"Metadata to set on the target object
Example: key1=value1,key2=value2"#)]
    pub metadata: Option<String>,

    /// x-amz-website-redirect-location header to set on the target object
    #[arg(long, env, help_heading = "Metadata/Headers")]
    pub website_redirect: Option<String>,

    #[arg(long, env, default_value_t = DEFAULT_NO_SYNC_SYSTEM_METADATA, help_heading = "Metadata/Headers",
    long_help= r#"Do not sync system metadata
System metadata: content-disposition, content-encoding, content-language, content-type,
                 cache-control, expires, website-redirect"#)]
    pub no_sync_system_metadata: bool,

    /// Do not sync user-defined metadata.
    #[arg(long, env, default_value_t = DEFAULT_NO_SYNC_USER_DEFINED_METADATA, help_heading = "Metadata/Headers")]
    pub no_sync_user_defined_metadata: bool,

    #[arg(long, env, conflicts_with_all = ["disable_tagging"], value_parser = tagging::parse_tagging, help_heading = "Tagging",
    long_help=r#"Tagging to set on the target object.
Key/value must be encoded as UTF-8 then URLEncoded URL query parameters without tag name duplicates.

Example: key1=value1&key2=value2"#)]
    pub tagging: Option<String>,

    /// Do not copy tagging.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_TAGGING, help_heading = "Tagging")]
    pub disable_tagging: bool,

    /// Version ID of the source object (requires S3 source)
    #[arg(long, env, help_heading = "Versioning")]
    pub source_version_id: Option<String>,

    /// Server-side encryption. Valid choices: AES256 | aws:kms | aws:kms:dsse
    #[arg(long, env, value_parser = sse::parse_sse, help_heading = "Encryption")]
    pub sse: Option<String>,

    /// SSE KMS ID key
    #[arg(long, env, help_heading = "Encryption")]
    pub sse_kms_key_id: Option<String>,

    /// Source SSE-C algorithm. Valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "source_sse_c_key", value_parser = sse::parse_sse_c, help_heading = "Encryption")]
    pub source_sse_c: Option<String>,

    /// Source SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(
        long,
        env,
        requires = "source_sse_c_key_md5",
        help_heading = "Encryption"
    )]
    pub source_sse_c_key: Option<String>,

    /// Source base64 encoded MD5 digest of source_sse_c_key
    #[arg(long, env, requires = "source_sse_c", help_heading = "Encryption")]
    pub source_sse_c_key_md5: Option<String>,

    /// Target SSE-C algorithm. Valid choices: AES256
    #[arg(long, env, conflicts_with_all = ["sse", "sse_kms_key_id"], requires = "target_sse_c_key", value_parser = sse::parse_sse_c, help_heading = "Encryption")]
    pub target_sse_c: Option<String>,

    /// Target SSE-C customer-provided encryption key(256bit key. must be base64 encoded)
    #[arg(
        long,
        env,
        requires = "target_sse_c_key_md5",
        help_heading = "Encryption"
    )]
    pub target_sse_c_key: Option<String>,

    /// Target base64 encoded MD5 digest of target-sse-c-key
    #[arg(long, env, requires = "target_sse_c", help_heading = "Encryption")]
    pub target_sse_c_key_md5: Option<String>,

    /// Trace verbosity(-v: show info, -vv: show debug, -vvv show trace)
    #[clap(flatten)]
    pub verbosity: Verbosity<WarnLevel>,

    /// Show trace as json format.
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING, help_heading = "Tracing/Logging")]
    pub json_tracing: bool,

    /// Enable aws sdk tracing.
    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING, help_heading = "Tracing/Logging")]
    pub aws_sdk_tracing: bool,

    /// Show span event tracing.
    #[arg(long, env, default_value_t = DEFAULT_SPAN_EVENTS_TRACING, help_heading = "Tracing/Logging")]
    pub span_events_tracing: bool,

    /// Disable ANSI terminal colors.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_COLOR_TRACING, help_heading = "Tracing/Logging")]
    pub disable_color_tracing: bool,

    /// Maximum retry attempts
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, value_name = "max_attempts", help_heading = "Retry Options")]
    pub aws_max_attempts: u32,

    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, value_name = "initial_backoff", help_heading = "Retry Options",
    long_help=r#"A multiplier value used when calculating backoff times as part of an exponential backoff with jitter strategy.
"#)]
    pub initial_backoff_milliseconds: u64,

    #[arg(
        long,
        env,
        value_name = "operation_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Operation timeout (milliseconds). The default has no timeout."#
    )]
    pub operation_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "operation_attempt_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Operation attempt timeout (milliseconds). The default has no timeout."#
    )]
    pub operation_attempt_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "connect_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Connect timeout (milliseconds). The default uses AWS SDK default timeout."#
    )]
    pub connect_timeout_milliseconds: Option<u64>,

    #[arg(
        long,
        env,
        value_name = "read_timeout",
        help_heading = "Timeout Options",
        long_help = r#"Read timeout (milliseconds). The default has no timeout."#
    )]
    pub read_timeout_milliseconds: Option<u64>,

    #[arg(long, env, value_parser = canned_acl::parse_canned_acl, help_heading = "Advanced",
    long_help=r#"ACL for the objects
Valid choices: private | public-read | public-read-write | authenticated-read | aws-exec-read |
               bucket-owner-read | bucket-owner-full-control"#)]
    pub acl: Option<String>,

    /// Do not try to guess the mime type of local file.
    #[arg(long, env, default_value_t = DEFAULT_NO_GUESS_MIME_TYPE, help_heading = "Advanced")]
    pub no_guess_mime_type: bool,

    /// Put last modified of the source to metadata.
    #[arg(long, env, default_value_t = DEFAULT_PUT_LAST_MODIFIED_METADATA, help_heading = "Advanced")]
    pub put_last_modified_metadata: bool,

    #[arg(long, env, value_name = "SHELL", value_parser = clap_complete::shells::Shell::from_str, help_heading = "Advanced",
    long_help=r#"Generate a auto completions script.
Valid choices: bash, fish, zsh, powershell, elvish."#)]
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,

    /// Disable stalled stream protection.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_STALLED_STREAM_PROTECTION, help_heading = "Advanced")]
    pub disable_stalled_stream_protection: bool,

    /// Disable payload signing for object uploads.
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_PAYLOAD_SIGNING, help_heading = "Advanced")]
    pub disable_payload_signing: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_CONTENT_MD5_HEADER, help_heading = "Advanced",
    long_help=r#"Disable Content-MD5 header for object uploads. It disables the ETag verification for the uploaded object.
"#)]
    pub disable_content_md5_header: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM, help_heading = "Advanced",
    long_help=r#"Disable default additional checksum verification in Express One Zone storage class.
 "#)]
    pub disable_express_one_zone_additional_checksum: bool,

    #[arg(long, env, default_value_t = DEFAULT_IF_NONE_MATCH, help_heading = "Advanced", long_help=r#"Uploads the object only if the object key name does not already exist in the specified bucket.
This is for like an optimistic lock."#)]
    pub if_none_match: bool,
}

// Free helpers — only depend on the source/target strings, not on the common flags.

pub(crate) fn is_source_s3(source: &str) -> bool {
    matches!(
        storage_path::parse_storage_path(source),
        StoragePath::S3 { .. }
    )
}

pub(crate) fn is_target_s3(target: &str) -> bool {
    matches!(
        storage_path::parse_storage_path(target),
        StoragePath::S3 { .. }
    )
}

pub(crate) fn is_source_local(source: &str) -> bool {
    matches!(
        storage_path::parse_storage_path(source),
        StoragePath::Local(_)
    )
}

pub(crate) fn is_target_local(target: &str) -> bool {
    matches!(
        storage_path::parse_storage_path(target),
        StoragePath::Local(_)
    )
}

pub(crate) fn is_source_stdio(source: &str) -> bool {
    matches!(storage_path::parse_storage_path(source), StoragePath::Stdio)
}

pub(crate) fn is_target_stdio(target: &str) -> bool {
    matches!(storage_path::parse_storage_path(target), StoragePath::Stdio)
}

pub(crate) fn check_both_local(source: &str, target: &str) -> Result<(), String> {
    if is_source_local(source) && is_target_local(target) {
        return Err(BOTH_LOCAL_SPECIFIED.to_string());
    }
    Ok(())
}

pub(crate) fn check_at_least_one_s3_or_stdio(source: &str, target: &str) -> Result<(), String> {
    let source_path = storage_path::parse_storage_path(source);
    let target_path = storage_path::parse_storage_path(target);

    if storage_path::is_both_storage_local(&source_path, &target_path) {
        return Err(NO_S3_STORAGE_SPECIFIED.to_string());
    }
    Ok(())
}

pub(crate) fn check_source_s3_key(source: &str) -> Result<(), String> {
    if !is_source_s3(source) {
        return Ok(());
    }
    // Validate the raw CLI input. `url::Url::parse` in the value-parser
    // layer normalizes `s3://` paths (strips trailing `.`, collapses
    // `/foo/..`), so the post-parsed prefix no longer carries the signal
    // we want to reject. Read directly from the raw argument instead.
    if source.ends_with('/') {
        return Err("source S3 URL ending in '/' is not supported: \
             `s3util cp` copies a single object, not a prefix."
            .to_string());
    }
    if source.ends_with("/.") || source.ends_with("/..") {
        return Err(format!(
            "source S3 key has an invalid final segment ('.' or '..'): {source}"
        ));
    }
    Ok(())
}

pub(crate) fn check_target_local_directory_exists(target: &str) -> Result<(), String> {
    let target_path = storage_path::parse_storage_path(target);
    let target_path = match target_path {
        StoragePath::Local(p) => p,
        StoragePath::S3 { .. } | StoragePath::Stdio => return Ok(()),
    };

    let ends_with_sep = target_path
        .to_string_lossy()
        .ends_with(std::path::MAIN_SEPARATOR);

    let effective_dir: PathBuf = if ends_with_sep {
        // e.g. "/tmp/" → "/tmp"
        let mut s = target_path.to_string_lossy().into_owned();
        s.pop();
        if s.is_empty() {
            // Degenerate input like "/" alone — root directory, trivially exists.
            return Ok(());
        }
        PathBuf::from(s)
    } else if target_path.is_dir() {
        target_path.clone()
    } else {
        // File-style target — the parent directory is what must exist.
        match target_path.parent() {
            None => return Ok(()),
            Some(p) if p.as_os_str().is_empty() => return Ok(()),
            Some(p) => p.to_path_buf(),
        }
    };

    if effective_dir.try_exists().unwrap_or(false) {
        return Ok(());
    }

    Err(format!(
        "{}: '{}'. Please create it before running this command.\n",
        TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX,
        effective_dir.to_string_lossy()
    ))
}

impl CommonTransferArgs {
    /// Run all the validation checks that depend on common's flag fields plus the
    /// source/target strings. The cp-only checks (both-stdio, stdio-vs-server-side-copy)
    /// are run by `CpArgs::validate_storage_config` before delegating here.
    pub fn validate_common_storage_config(&self, source: &str, target: &str) -> Result<(), String> {
        check_both_local(source, target)?;
        check_at_least_one_s3_or_stdio(source, target)?;
        self.check_server_side_copy_conflict(source, target)?;
        self.check_version_id_conflict(source)?;
        self.check_storage_class_conflict(target)?;
        self.check_storage_credentials_conflict(source, target)?;
        self.check_sse_conflict(target)?;
        self.check_sse_c_conflict(source, target)?;
        self.check_acl_conflict(target)?;
        self.check_enable_additional_checksum_conflict(source)?;
        self.check_additional_checksum_algorithm_conflict(target)?;
        self.check_auto_chunksize_conflict(source)?;
        self.check_metadata_conflict(target)?;
        self.check_endpoint_url_conflict(source, target)?;
        self.check_disable_payload_signing_conflict(target)?;
        self.check_disable_content_md5_header_conflict(target)?;
        self.check_full_object_checksum_conflict(target)?;
        self.check_accelerate_conflict(source, target)?;
        self.check_request_payer_conflict(source, target)?;
        self.check_source_no_sign_request_conflict(source)?;
        check_source_s3_key(source)?;
        check_target_local_directory_exists(target)?;

        Ok(())
    }

    fn check_server_side_copy_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if !self.server_side_copy {
            return Ok(());
        }
        if !is_source_s3(source) || !is_target_s3(target) {
            return Err(SERVER_SIDE_COPY_REQUIRES_BOTH_S3.to_string());
        }
        Ok(())
    }

    fn check_version_id_conflict(&self, source: &str) -> Result<(), String> {
        if self.source_version_id.is_none() {
            return Ok(());
        }
        if !is_source_s3(source) {
            return Err(VERSION_ID_REQUIRES_S3_SOURCE.to_string());
        }
        Ok(())
    }

    fn check_storage_class_conflict(&self, target: &str) -> Result<(), String> {
        if self.storage_class.is_some() && !is_target_s3(target) {
            return Err(LOCAL_STORAGE_SPECIFIED_WITH_STORAGE_CLASS.to_string());
        }
        Ok(())
    }

    fn check_storage_credentials_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if !is_source_s3(source)
            && (self.source_profile.is_some() || self.source_access_key.is_some())
        {
            return Err(NO_SOURCE_CREDENTIAL_REQUIRED.to_string());
        }

        if !is_target_s3(target)
            && (self.target_profile.is_some() || self.target_access_key.is_some())
        {
            return Err(NO_TARGET_CREDENTIAL_REQUIRED.to_string());
        }
        Ok(())
    }

    fn check_sse_conflict(&self, target: &str) -> Result<(), String> {
        if self.sse.is_none() && self.sse_kms_key_id.is_none() {
            return Ok(());
        }

        if !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_SSE.to_string());
        }

        if self.sse_kms_key_id.is_some()
            && (self.sse.is_none()
                || (ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap()
                    != ServerSideEncryption::AwsKms
                    && ServerSideEncryption::from_str(self.sse.as_ref().unwrap()).unwrap()
                        != ServerSideEncryption::AwsKmsDsse))
        {
            return Err(SSE_KMS_KEY_ID_ARGUMENTS_CONFLICT.to_string());
        }
        Ok(())
    }

    fn check_sse_c_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if self.source_sse_c.is_some() && !is_source_s3(source) {
            return Err(LOCAL_STORAGE_SPECIFIED_WITH_SSE_C.to_string());
        }
        if self.target_sse_c.is_some() && !is_target_s3(target) {
            return Err(LOCAL_STORAGE_SPECIFIED_WITH_SSE_C.to_string());
        }
        Ok(())
    }

    fn check_acl_conflict(&self, target: &str) -> Result<(), String> {
        if self.acl.is_some() && !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACL.to_string());
        }
        Ok(())
    }

    fn check_additional_checksum_algorithm_conflict(&self, target: &str) -> Result<(), String> {
        if self.additional_checksum_algorithm.is_some() && !is_target_s3(target) {
            return Err(
                TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ADDITIONAL_CHECKSUM_ALGORITHM.to_string(),
            );
        }
        Ok(())
    }

    fn check_enable_additional_checksum_conflict(&self, source: &str) -> Result<(), String> {
        if self.enable_additional_checksum && !is_source_s3(source) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENABLE_ADDITIONAL_CHECKSUM.to_string());
        }
        Ok(())
    }

    fn check_auto_chunksize_conflict(&self, source: &str) -> Result<(), String> {
        if self.auto_chunksize && !is_source_s3(source) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_AUTO_CHUNKSIZE.to_string());
        }
        Ok(())
    }

    fn check_metadata_conflict(&self, target: &str) -> Result<(), String> {
        if self.cache_control.is_none()
            && self.content_disposition.is_none()
            && self.content_encoding.is_none()
            && self.content_language.is_none()
            && self.content_type.is_none()
            && self.website_redirect.is_none()
            && self.expires.is_none()
            && self.tagging.is_none()
            && !self.put_last_modified_metadata
        {
            return Ok(());
        }

        if !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_METADATA_OPTION.to_string());
        }
        Ok(())
    }

    fn check_endpoint_url_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if !is_source_s3(source) && self.source_endpoint_url.is_some() {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string());
        }
        if !is_target_s3(target) && self.target_endpoint_url.is_some() {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ENDPOINT_URL.to_string());
        }
        Ok(())
    }

    fn check_disable_payload_signing_conflict(&self, target: &str) -> Result<(), String> {
        if self.disable_payload_signing && !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_PAYLOAD_SIGNING.to_string());
        }
        Ok(())
    }

    fn check_disable_content_md5_header_conflict(&self, target: &str) -> Result<(), String> {
        if self.disable_content_md5_header && !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_DISABLE_CONTENT_MD5_HEADER.to_string());
        }
        Ok(())
    }

    fn check_full_object_checksum_conflict(&self, target: &str) -> Result<(), String> {
        if !self.full_object_checksum {
            return Ok(());
        }

        if !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_FULL_OBJECT_CHECKSUM.to_string());
        }

        if let Some(additional_checksum_algorithm) = &self.additional_checksum_algorithm {
            if additional_checksum_algorithm == "SHA1" || additional_checksum_algorithm == "SHA256"
            {
                return Err(FULL_OBJECT_CHECKSUM_NOT_SUPPORTED.to_string());
            }
        }
        Ok(())
    }

    fn check_accelerate_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if self.source_accelerate && !is_source_s3(source) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE.to_string());
        }
        if self.target_accelerate && !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_ACCELERATE.to_string());
        }
        Ok(())
    }

    fn check_request_payer_conflict(&self, source: &str, target: &str) -> Result<(), String> {
        if self.source_request_payer && !is_source_s3(source) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER.to_string());
        }
        if self.target_request_payer && !is_target_s3(target) {
            return Err(TARGET_LOCAL_STORAGE_SPECIFIED_WITH_REQUEST_PAYER.to_string());
        }
        Ok(())
    }

    fn check_source_no_sign_request_conflict(&self, source: &str) -> Result<(), String> {
        if self.source_no_sign_request && !is_source_s3(source) {
            return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST.to_string());
        }
        Ok(())
    }

    pub fn build_client_configs(
        &self,
        request_checksum_calculation: RequestChecksumCalculation,
    ) -> (Option<ClientConfig>, Option<ClientConfig>) {
        let source_credential = if self.source_no_sign_request {
            Some(S3Credentials::NoSignRequest)
        } else if let Some(source_profile) = self.source_profile.clone() {
            Some(S3Credentials::Profile(source_profile))
        } else if self.source_access_key.is_some() {
            self.source_access_key
                .clone()
                .map(|access_key| S3Credentials::Credentials {
                    access_keys: AccessKeys {
                        access_key,
                        secret_access_key: self
                            .source_secret_access_key
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        session_token: self.source_session_token.clone(),
                    },
                })
        } else {
            Some(S3Credentials::FromEnvironment)
        };

        let target_credential = if let Some(target_profile) = self.target_profile.clone() {
            Some(S3Credentials::Profile(target_profile))
        } else if self.target_access_key.is_some() {
            self.target_access_key
                .clone()
                .map(|access_key| S3Credentials::Credentials {
                    access_keys: AccessKeys {
                        access_key,
                        secret_access_key: self
                            .target_secret_access_key
                            .as_ref()
                            .unwrap()
                            .to_string(),
                        session_token: self.target_session_token.clone(),
                    },
                })
        } else {
            Some(S3Credentials::FromEnvironment)
        };

        let parallel_upload_semaphore =
            Arc::new(Semaphore::new(self.max_parallel_uploads as usize));

        let source_request_payer = if self.source_request_payer {
            Some(RequestPayer::Requester)
        } else {
            None
        };

        let source_client_config = source_credential.map(|source_credential| ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential: source_credential,
            region: self.source_region.clone(),
            endpoint_url: self.source_endpoint_url.clone(),
            force_path_style: self.source_force_path_style,
            retry_config: RetryConfig {
                aws_max_attempts: self.aws_max_attempts,
                initial_backoff_milliseconds: self.initial_backoff_milliseconds,
            },
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: self.operation_timeout_milliseconds,
                operation_attempt_timeout_milliseconds: self.operation_attempt_timeout_milliseconds,
                connect_timeout_milliseconds: self.connect_timeout_milliseconds,
                read_timeout_milliseconds: self.read_timeout_milliseconds,
            },
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
            request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
            parallel_upload_semaphore: parallel_upload_semaphore.clone(),
            accelerate: self.source_accelerate,
            request_payer: source_request_payer,
        });

        let target_request_payer = if self.target_request_payer {
            Some(RequestPayer::Requester)
        } else {
            None
        };

        let target_client_config = target_credential.map(|target_credential| ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential: target_credential,
            region: self.target_region.clone(),
            endpoint_url: self.target_endpoint_url.clone(),
            force_path_style: self.target_force_path_style,
            retry_config: RetryConfig {
                aws_max_attempts: self.aws_max_attempts,
                initial_backoff_milliseconds: self.initial_backoff_milliseconds,
            },
            cli_timeout_config: CLITimeoutConfig {
                operation_timeout_milliseconds: self.operation_timeout_milliseconds,
                operation_attempt_timeout_milliseconds: self.operation_attempt_timeout_milliseconds,
                connect_timeout_milliseconds: self.connect_timeout_milliseconds,
                read_timeout_milliseconds: self.read_timeout_milliseconds,
            },
            disable_stalled_stream_protection: self.disable_stalled_stream_protection,
            request_checksum_calculation,
            parallel_upload_semaphore,
            accelerate: self.target_accelerate,
            request_payer: target_request_payer,
        });

        (source_client_config, target_client_config)
    }
}

/// Build a `Config` from already-validated common transfer args plus the
/// raw source/target strings. Callers (the per-subcommand `TryFrom` impls)
/// are responsible for running their own structural validation
/// (`validate_storage_config`) before invoking this helper.
///
/// This sets `no_fail_on_verify_error: false`. Subcommands whose args
/// expose that flag (currently only mv) override it on the returned
/// `Config` after this returns.
pub(crate) fn build_config_from_common(
    common: CommonTransferArgs,
    source: Option<String>,
    target: Option<String>,
) -> Result<Config, String> {
    let original_common = common.clone();

    let tracing_config =
        common
            .verbosity
            .log_level()
            .map(|log_level| crate::config::TracingConfig {
                tracing_level: log_level,
                json_tracing: common.json_tracing,
                aws_sdk_tracing: common.aws_sdk_tracing,
                span_events_tracing: common.span_events_tracing,
                disable_color_tracing: common.disable_color_tracing,
            });

    let storage_class = common
        .storage_class
        .as_ref()
        .map(|storage_class| StorageClass::from_str(storage_class).unwrap());

    let sse = common
        .sse
        .as_ref()
        .map(|sse| ServerSideEncryption::from_str(sse).unwrap());

    let canned_acl = common
        .acl
        .as_ref()
        .map(|acl| ObjectCannedAcl::from_str(acl).unwrap());

    let mut additional_checksum_algorithm = common
        .additional_checksum_algorithm
        .as_ref()
        .map(|algorithm| ChecksumAlgorithm::from(algorithm.as_str()));

    let mut checksum_mode = if common.enable_additional_checksum {
        Some(ChecksumMode::Enabled)
    } else {
        None
    };

    let tagging = common
        .tagging
        .as_ref()
        .map(|tagging| tagging::parse_tagging(tagging).unwrap());

    let metadata_parsed = if let Some(metadata) = common.metadata.as_ref() {
        Some(metadata::parse_metadata(metadata)?)
    } else {
        None
    };

    let mut full_object_checksum = if additional_checksum_algorithm
        .as_ref()
        .is_some_and(|algorithm| algorithm == &ChecksumAlgorithm::Crc64Nvme)
    {
        true
    } else {
        common.full_object_checksum
    };

    let source_str = source.as_deref().unwrap_or("");
    let target_str = target.as_deref().unwrap_or("");

    if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(source_str) {
        if super::is_express_onezone_storage(&bucket)
            && !common.disable_express_one_zone_additional_checksum
        {
            checksum_mode = Some(ChecksumMode::Enabled);
        }
    }

    let mut request_checksum_calculation = RequestChecksumCalculation::WhenRequired;
    if let StoragePath::S3 { bucket, .. } = storage_path::parse_storage_path(target_str) {
        if super::is_express_onezone_storage(&bucket)
            && additional_checksum_algorithm.is_none()
            && !common.disable_express_one_zone_additional_checksum
        {
            additional_checksum_algorithm = Some(ChecksumAlgorithm::Crc64Nvme);
            full_object_checksum = true;
            request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
        } else if additional_checksum_algorithm.is_some() {
            request_checksum_calculation = RequestChecksumCalculation::WhenSupported;
        }
    }

    let (source_client_config, target_client_config) =
        original_common.build_client_configs(request_checksum_calculation);

    let is_stdio_source = is_source_stdio(source_str);
    let is_stdio_target = is_target_stdio(target_str);

    let rate_limit_bandwidth = common
        .rate_limit_bandwidth
        .as_ref()
        .map(|bandwidth| human_bytes::parse_human_bandwidth(bandwidth).unwrap());

    Ok(Config {
        source: storage_path::parse_storage_path(source_str),
        target: storage_path::parse_storage_path(target_str),

        show_progress: common.show_progress,

        source_client_config,
        target_client_config,

        tracing_config,

        transfer_config: crate::config::TransferConfig {
            multipart_threshold: human_bytes::parse_human_bytes(&common.multipart_threshold)?,
            multipart_chunksize: human_bytes::parse_human_bytes(&common.multipart_chunksize)?,
            auto_chunksize: common.auto_chunksize,
        },

        disable_tagging: common.disable_tagging,
        server_side_copy: common.server_side_copy,
        no_guess_mime_type: common.no_guess_mime_type,
        disable_multipart_verify: common.disable_multipart_verify,
        disable_etag_verify: common.disable_etag_verify,
        disable_additional_checksum_verify: common.disable_additional_checksum_verify,
        storage_class,
        sse,
        sse_kms_key_id: SseKmsKeyId {
            id: common.sse_kms_key_id,
        },
        source_sse_c: common.source_sse_c,
        source_sse_c_key: SseCustomerKey {
            key: common.source_sse_c_key,
        },
        source_sse_c_key_md5: common.source_sse_c_key_md5,
        target_sse_c: common.target_sse_c,
        target_sse_c_key: SseCustomerKey {
            key: common.target_sse_c_key,
        },
        target_sse_c_key_md5: common.target_sse_c_key_md5,
        canned_acl,
        additional_checksum_algorithm,
        additional_checksum_mode: checksum_mode,
        cache_control: common.cache_control,
        content_disposition: common.content_disposition,
        content_encoding: common.content_encoding,
        content_language: common.content_language,
        content_type: common.content_type,
        expires: common.expires,
        metadata: metadata_parsed,
        website_redirect: common.website_redirect,
        no_sync_system_metadata: common.no_sync_system_metadata,
        no_sync_user_defined_metadata: common.no_sync_user_defined_metadata,
        tagging,
        put_last_modified_metadata: common.put_last_modified_metadata,
        disable_payload_signing: common.disable_payload_signing,
        disable_content_md5_header: common.disable_content_md5_header,
        full_object_checksum,
        source_accelerate: common.source_accelerate,
        target_accelerate: common.target_accelerate,
        source_request_payer: common.source_request_payer,
        target_request_payer: common.target_request_payer,
        if_none_match: common.if_none_match,
        disable_stalled_stream_protection: common.disable_stalled_stream_protection,
        disable_express_one_zone_additional_checksum: common
            .disable_express_one_zone_additional_checksum,
        max_parallel_uploads: common.max_parallel_uploads,
        rate_limit_bandwidth,
        version_id: common.source_version_id,
        is_stdio_source,
        is_stdio_target,
        no_fail_on_verify_error: false,
    })
}
