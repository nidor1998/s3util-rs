use clap::Parser;
use clap::builder::NonEmptyStringValueParser;
use clap_verbosity_flag::{Verbosity, WarnLevel};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::config::args::value_parser::{storage_path, url};
use crate::config::{CLITimeoutConfig, ClientConfig, RetryConfig, TracingConfig};
use crate::types::{AccessKeys, ClientConfigLocation, S3Credentials, StoragePath};
use aws_smithy_types::checksum_config::RequestChecksumCalculation;

use crate::config::args::common::{
    DEFAULT_AWS_MAX_ATTEMPTS, DEFAULT_AWS_SDK_TRACING, DEFAULT_DISABLE_COLOR_TRACING,
    DEFAULT_DISABLE_STALLED_STREAM_PROTECTION, DEFAULT_FORCE_PATH_STYLE,
    DEFAULT_INITIAL_BACKOFF_MILLISECONDS, DEFAULT_JSON_TRACING, DEFAULT_SPAN_EVENTS_TRACING,
};

const SOURCE_NOT_S3: &str = "rename source must be s3://<BUCKET>/<KEY>\n";
const SOURCE_KEY_EMPTY: &str = "rename source must include a key (s3://<BUCKET>/<KEY>)\n";
const TARGET_NOT_S3: &str = "rename target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str = "rename target must include a key (s3://<BUCKET>/<KEY>)\n";
const DIFFERENT_BUCKETS: &str = "rename source and target must be in the same bucket\n";
pub(crate) const NOT_EXPRESS_ONEZONE: &str =
    "rename is only supported on S3 Express One Zone buckets (bucket name must end with --x-s3)\n";

#[derive(Parser, Clone, Debug)]
pub struct RenameArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub source: Option<String>,

    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Rename only if the source object ETag matches this value
    #[arg(
        long,
        env,
        value_name = "ETAG",
        value_parser = NonEmptyStringValueParser::new(),
        conflicts_with = "source_if_none_match",
        help_heading = "Conditional Checks"
    )]
    pub source_if_match: Option<String>,

    /// Rename only if the source object ETag does not match this value
    #[arg(
        long,
        env,
        value_name = "ETAG",
        value_parser = NonEmptyStringValueParser::new(),
        conflicts_with = "source_if_match",
        help_heading = "Conditional Checks"
    )]
    pub source_if_none_match: Option<String>,

    /// Rename only if the destination object ETag matches this value
    #[arg(
        long,
        env,
        value_name = "ETAG",
        value_parser = NonEmptyStringValueParser::new(),
        conflicts_with = "target_if_none_match",
        help_heading = "Conditional Checks"
    )]
    pub target_if_match: Option<String>,

    /// Rename only if the destination object ETag does not match this value
    #[arg(
        long,
        env,
        value_name = "ETAG",
        value_parser = NonEmptyStringValueParser::new(),
        conflicts_with = "target_if_match",
        help_heading = "Conditional Checks"
    )]
    pub target_if_none_match: Option<String>,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    // === Tracing / Logging ===
    /// Show trace as json format.
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING, help_heading = "Tracing/Logging")]
    pub json_tracing: bool,

    /// Enable aws sdk tracing.
    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING, help_heading = "Tracing/Logging")]
    pub aws_sdk_tracing: bool,

    /// Show span event tracing.
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_SPAN_EVENTS_TRACING,
        help_heading = "Tracing/Logging"
    )]
    pub span_events_tracing: bool,

    /// Disable ANSI terminal colors.
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_DISABLE_COLOR_TRACING,
        help_heading = "Tracing/Logging"
    )]
    pub disable_color_tracing: bool,

    #[command(flatten)]
    pub verbosity: Verbosity<WarnLevel>,

    // === AWS Configuration ===
    /// Location of the file that the AWS CLI uses to store configuration profiles
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    pub aws_config_file: Option<PathBuf>,

    /// Location of the file that the AWS CLI uses to store access keys
    #[arg(long, env, value_name = "FILE", help_heading = "AWS Configuration")]
    pub aws_shared_credentials_file: Option<PathBuf>,

    /// AWS CLI profile
    #[arg(
        long,
        env,
        conflicts_with_all = ["source_access_key", "source_secret_access_key", "source_session_token"],
        help_heading = "AWS Configuration"
    )]
    pub source_profile: Option<String>,

    /// Access key
    #[arg(
        long,
        env,
        hide_env_values = true,
        conflicts_with_all = ["source_profile"],
        requires = "source_secret_access_key",
        help_heading = "AWS Configuration"
    )]
    pub source_access_key: Option<String>,

    /// Secret access key
    #[arg(
        long,
        env,
        hide_env_values = true,
        conflicts_with_all = ["source_profile"],
        requires = "source_access_key",
        help_heading = "AWS Configuration"
    )]
    pub source_secret_access_key: Option<String>,

    /// Session token
    #[arg(
        long,
        env,
        hide_env_values = true,
        conflicts_with_all = ["source_profile"],
        requires = "source_access_key",
        help_heading = "AWS Configuration"
    )]
    pub source_session_token: Option<String>,

    /// AWS region for the bucket
    #[arg(
        long,
        env,
        value_parser = NonEmptyStringValueParser::new(),
        help_heading = "AWS Configuration"
    )]
    pub source_region: Option<String>,

    /// Custom S3-compatible endpoint URL (e.g. MinIO, Wasabi)
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "AWS Configuration")]
    pub source_endpoint_url: Option<String>,

    /// Use path-style access (required by some S3-compatible services)
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_FORCE_PATH_STYLE,
        help_heading = "AWS Configuration"
    )]
    pub source_force_path_style: bool,

    /// Disable stalled stream protection
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_DISABLE_STALLED_STREAM_PROTECTION,
        help_heading = "AWS Configuration"
    )]
    pub disable_stalled_stream_protection: bool,

    // === Retry Options ===
    /// Maximum retry attempts for AWS SDK operations
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_AWS_MAX_ATTEMPTS,
        help_heading = "Retry Options"
    )]
    pub aws_max_attempts: u32,

    /// Initial backoff in milliseconds for retries
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS,
        help_heading = "Retry Options"
    )]
    pub initial_backoff_milliseconds: u64,

    // === Timeout Options ===
    /// Overall operation timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_timeout_milliseconds: Option<u64>,

    /// Per-attempt operation timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub operation_attempt_timeout_milliseconds: Option<u64>,

    /// Connection timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub connect_timeout_milliseconds: Option<u64>,

    /// Read timeout in milliseconds
    #[arg(long, env, help_heading = "Timeout Options")]
    pub read_timeout_milliseconds: Option<u64>,

    // === Advanced ===
    #[arg(
        long,
        env,
        value_name = "SHELL",
        value_parser = clap_complete::shells::Shell::from_str,
        help_heading = "Advanced",
        long_help = r#"Generate a auto completions script.
Valid choices: bash, fish, zsh, powershell, elvish."#
    )]
    pub auto_complete_shell: Option<clap_complete::shells::Shell>,
}

impl RenameArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.auto_complete_shell
    }

    pub fn source_bucket_key(&self) -> Result<(String, String), String> {
        let raw = self
            .source
            .as_deref()
            .ok_or_else(|| SOURCE_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    return Err(SOURCE_KEY_EMPTY.to_string());
                }
                Ok((bucket, prefix))
            }
            _ => Err(SOURCE_NOT_S3.to_string()),
        }
    }

    pub fn target_bucket_key(&self) -> Result<(String, String), String> {
        let raw = self
            .target
            .as_deref()
            .ok_or_else(|| TARGET_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if prefix.is_empty() {
                    return Err(TARGET_KEY_EMPTY.to_string());
                }
                Ok((bucket, prefix))
            }
            _ => Err(TARGET_NOT_S3.to_string()),
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        let (src_bucket, _) = self.source_bucket_key()?;
        let (dst_bucket, _) = self.target_bucket_key()?;
        if !super::is_express_onezone_storage(&src_bucket) {
            return Err(NOT_EXPRESS_ONEZONE.to_string());
        }
        if src_bucket != dst_bucket {
            return Err(DIFFERENT_BUCKETS.to_string());
        }
        Ok(())
    }

    pub fn build_client_config(&self) -> ClientConfig {
        let credential = if let Some(profile) = self.source_profile.clone() {
            S3Credentials::Profile(profile)
        } else if let Some(access_key) = self.source_access_key.clone() {
            S3Credentials::Credentials {
                access_keys: AccessKeys {
                    access_key,
                    secret_access_key: self.source_secret_access_key.clone().expect(
                        "clap requires --source-secret-access-key alongside --source-access-key",
                    ),
                    session_token: self.source_session_token.clone(),
                },
            }
        } else {
            S3Credentials::FromEnvironment
        };

        ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential,
            region: self.source_region.clone(),
            endpoint_url: self.source_endpoint_url.clone(),
            force_path_style: self.source_force_path_style,
            accelerate: false,
            request_payer: None,
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
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    pub fn build_tracing_config(&self) -> Option<TracingConfig> {
        self.verbosity
            .log_level()
            .map(|tracing_level| TracingConfig {
                tracing_level,
                json_tracing: self.json_tracing,
                aws_sdk_tracing: self.aws_sdk_tracing,
                span_events_tracing: self.span_events_tracing,
                disable_color_tracing: self.disable_color_tracing,
            })
    }

    pub fn build_tracing_config_dry_run(&self, dry_run: bool) -> Option<TracingConfig> {
        if !dry_run {
            return self.build_tracing_config();
        }
        let tracing_level = self
            .verbosity
            .log_level()
            .map_or(log::Level::Info, |l| l.max(log::Level::Info));
        Some(TracingConfig {
            tracing_level,
            json_tracing: self.json_tracing,
            aws_sdk_tracing: self.aws_sdk_tracing,
            span_events_tracing: self.span_events_tracing,
            disable_color_tracing: self.disable_color_tracing,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(name = "test")]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestSub,
    }

    #[derive(clap::Subcommand, Debug)]
    enum TestSub {
        Rename(RenameArgs),
    }

    fn parse(args: &[&str]) -> RenameArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::Rename(a) = cli.cmd;
        a
    }

    // --- source_bucket_key ---

    #[test]
    fn source_bucket_key_accepts_s3_key() {
        let a = parse(&[
            "test",
            "rename",
            "s3://my-bucket/my-key",
            "s3://my-bucket/other-key",
        ]);
        let (bucket, key) = a.source_bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    fn source_bucket_key_rejects_bucket_only() {
        let a = parse(&["test", "rename", "s3://my-bucket", "s3://my-bucket/dst"]);
        let err = a.source_bucket_key().unwrap_err();
        assert!(
            err.contains("key"),
            "expected key-missing message, got: {err}"
        );
    }

    #[test]
    fn source_bucket_key_rejects_bucket_with_trailing_slash() {
        let a = parse(&["test", "rename", "s3://my-bucket/", "s3://my-bucket/dst"]);
        assert!(a.source_bucket_key().is_err());
    }

    #[test]
    fn source_bucket_key_rejects_non_s3_path() {
        // check_storage_path accepts local paths; source_bucket_key must reject them.
        let res =
            TestCli::try_parse_from(["test", "rename", "/tmp/local-file", "s3://my-bucket/dst"]);
        if let Ok(cli) = res {
            let TestSub::Rename(a) = cli.cmd;
            let err = a.source_bucket_key().unwrap_err();
            assert!(err.contains("s3://"), "expected not-S3 error, got: {err}");
        }
    }

    // --- target_bucket_key ---

    #[test]
    fn target_bucket_key_accepts_s3_key() {
        let a = parse(&[
            "test",
            "rename",
            "s3://my-bucket/src",
            "s3://my-bucket/my-key",
        ]);
        let (bucket, key) = a.target_bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    fn target_bucket_key_rejects_bucket_only() {
        let a = parse(&["test", "rename", "s3://my-bucket/src", "s3://my-bucket"]);
        let err = a.target_bucket_key().unwrap_err();
        assert!(
            err.contains("key"),
            "expected key-missing message, got: {err}"
        );
    }

    #[test]
    fn target_bucket_key_rejects_bucket_with_trailing_slash() {
        let a = parse(&["test", "rename", "s3://my-bucket/src", "s3://my-bucket/"]);
        assert!(a.target_bucket_key().is_err());
    }

    #[test]
    fn target_bucket_key_rejects_non_s3_path() {
        let res =
            TestCli::try_parse_from(["test", "rename", "s3://my-bucket/src", "/tmp/local-file"]);
        if let Ok(cli) = res {
            let TestSub::Rename(a) = cli.cmd;
            let err = a.target_bucket_key().unwrap_err();
            assert!(err.contains("s3://"), "expected not-S3 error, got: {err}");
        }
    }

    // --- validate ---

    #[test]
    fn validate_passes_for_same_express_onezone_bucket() {
        let a = parse(&[
            "test",
            "rename",
            "s3://my-bucket--apne1-az4--x-s3/src-key",
            "s3://my-bucket--apne1-az4--x-s3/dst-key",
        ]);
        assert!(a.validate().is_ok());
    }

    #[test]
    fn validate_rejects_non_express_onezone() {
        let a = parse(&[
            "test",
            "rename",
            "s3://regular-bucket/src-key",
            "s3://regular-bucket/dst-key",
        ]);
        let err = a.validate().unwrap_err();
        assert!(
            err.contains("Express"),
            "expected Express One Zone error, got: {err}"
        );
    }

    #[test]
    fn validate_rejects_different_buckets() {
        let a = parse(&[
            "test",
            "rename",
            "s3://bucket-a--apne1-az4--x-s3/src-key",
            "s3://bucket-b--apne1-az4--x-s3/dst-key",
        ]);
        let err = a.validate().unwrap_err();
        assert!(
            err.contains("same bucket"),
            "expected same-bucket error, got: {err}"
        );
    }

    // --- conditional flag mutual exclusion ---

    #[test]
    fn source_if_match_and_source_if_none_match_are_mutually_exclusive() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-match",
            "\"abc123\"",
            "--source-if-none-match",
            "\"def456\"",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --source-if-match with --source-if-none-match"
        );
    }

    #[test]
    fn target_if_match_and_target_if_none_match_are_mutually_exclusive() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-match",
            "\"abc123\"",
            "--target-if-none-match",
            "\"def456\"",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --target-if-match with --target-if-none-match"
        );
    }

    #[test]
    fn source_if_match_without_value_is_rejected() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-match",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --source-if-match without an ETAG value"
        );
    }

    #[test]
    fn target_if_match_without_value_is_rejected() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-match",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --target-if-match without an ETAG value"
        );
    }

    #[test]
    fn source_if_none_match_without_value_is_rejected() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-none-match",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --source-if-none-match without an ETAG value"
        );
    }

    #[test]
    fn target_if_none_match_without_value_is_rejected() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-none-match",
        ]);
        assert!(
            res.is_err(),
            "clap should reject --target-if-none-match without an ETAG value"
        );
    }

    #[test]
    fn source_if_none_match_accepts_etag_value() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-none-match",
            "\"abc123\"",
        ]);
        assert_eq!(a.source_if_none_match.as_deref(), Some("\"abc123\""));
    }

    #[test]
    fn target_if_none_match_accepts_etag_value() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-none-match",
            "\"abc123\"",
        ]);
        assert_eq!(a.target_if_none_match.as_deref(), Some("\"abc123\""));
    }

    #[test]
    fn source_if_match_rejects_empty_string() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-match",
            "",
        ]);
        assert!(
            res.is_err(),
            "--source-if-match with empty string should be rejected"
        );
    }

    #[test]
    fn source_if_none_match_rejects_empty_string() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-if-none-match",
            "",
        ]);
        assert!(
            res.is_err(),
            "--source-if-none-match with empty string should be rejected"
        );
    }

    #[test]
    fn target_if_match_rejects_empty_string() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-match",
            "",
        ]);
        assert!(
            res.is_err(),
            "--target-if-match with empty string should be rejected"
        );
    }

    #[test]
    fn target_if_none_match_rejects_empty_string() {
        let res = TestCli::try_parse_from([
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--target-if-none-match",
            "",
        ]);
        assert!(
            res.is_err(),
            "--target-if-none-match with empty string should be rejected"
        );
    }

    // --- build_client_config credential branches ---

    #[test]
    fn build_client_config_uses_environment_credentials_by_default() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
        ]);
        let cfg = a.build_client_config();
        assert!(matches!(
            cfg.credential,
            crate::types::S3Credentials::FromEnvironment
        ));
    }

    #[test]
    fn build_client_config_uses_profile_when_set() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-profile",
            "prod",
        ]);
        let cfg = a.build_client_config();
        match cfg.credential {
            crate::types::S3Credentials::Profile(name) => assert_eq!(name, "prod"),
            other => panic!("expected Profile credential, got {other:?}"),
        }
    }

    #[test]
    fn build_client_config_uses_explicit_keys() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-access-key",
            "AKIAIOSFODNN7EXAMPLE",
            "--source-secret-access-key",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ]);
        let cfg = a.build_client_config();
        assert!(
            matches!(
                cfg.credential,
                crate::types::S3Credentials::Credentials { .. }
            ),
            "expected Credentials variant"
        );
    }

    #[test]
    fn build_client_config_explicit_keys_with_session_token() {
        let a = parse(&[
            "test",
            "rename",
            "s3://b--az--x-s3/src",
            "s3://b--az--x-s3/dst",
            "--source-access-key",
            "AKID",
            "--source-secret-access-key",
            "SECRET",
            "--source-session-token",
            "TOKEN",
        ]);
        let cfg = a.build_client_config();
        if let crate::types::S3Credentials::Credentials { access_keys } = cfg.credential {
            assert_eq!(access_keys.session_token.as_deref(), Some("TOKEN"));
        } else {
            panic!("expected Credentials variant");
        }
    }
}
