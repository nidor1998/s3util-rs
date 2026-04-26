use crate::config::args::value_parser::url;
use crate::config::{CLITimeoutConfig, ClientConfig, RetryConfig, TracingConfig};
use crate::types::{AccessKeys, ClientConfigLocation, S3Credentials};
use aws_sdk_s3::types::RequestPayer;
use aws_smithy_types::checksum_config::RequestChecksumCalculation;
use clap::Parser;
use clap::builder::NonEmptyStringValueParser;
use clap_verbosity_flag::{Verbosity, WarnLevel};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Semaphore;

use super::common::{
    DEFAULT_ACCELERATE, DEFAULT_AWS_MAX_ATTEMPTS, DEFAULT_AWS_SDK_TRACING,
    DEFAULT_DISABLE_COLOR_TRACING, DEFAULT_DISABLE_STALLED_STREAM_PROTECTION,
    DEFAULT_FORCE_PATH_STYLE, DEFAULT_INITIAL_BACKOFF_MILLISECONDS, DEFAULT_JSON_TRACING,
    DEFAULT_REQUEST_PAYER, DEFAULT_SPAN_EVENTS_TRACING,
};

const DEFAULT_TARGET_NO_SIGN_REQUEST: bool = false;

#[derive(Parser, Clone, Debug)]
pub struct CommonClientArgs {
    // === Tracing / Logging ===
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING, help_heading = "Tracing/Logging")]
    pub json_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING, help_heading = "Tracing/Logging")]
    pub aws_sdk_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_SPAN_EVENTS_TRACING, help_heading = "Tracing/Logging")]
    pub span_events_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_COLOR_TRACING, help_heading = "Tracing/Logging")]
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

    /// AWS region for the target
    #[arg(long, env, value_parser = NonEmptyStringValueParser::new(), help_heading = "AWS Configuration")]
    pub target_region: Option<String>,

    /// Custom S3-compatible endpoint URL (e.g. MinIO, Wasabi)
    #[arg(long, env, value_parser = url::check_scheme, help_heading = "AWS Configuration")]
    pub target_endpoint_url: Option<String>,

    /// Use path-style access (required by some S3-compatible services)
    #[arg(long, env, default_value_t = DEFAULT_FORCE_PATH_STYLE, help_heading = "AWS Configuration")]
    pub target_force_path_style: bool,

    /// Enable S3 Transfer Acceleration
    #[arg(long, env, default_value_t = DEFAULT_ACCELERATE, help_heading = "AWS Configuration")]
    pub target_accelerate: bool,

    /// Enable requester-pays for the target bucket
    #[arg(long, env, default_value_t = DEFAULT_REQUEST_PAYER, help_heading = "AWS Configuration")]
    pub target_request_payer: bool,

    /// Do not sign the request. If this argument is specified, credentials will not be loaded
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_TARGET_NO_SIGN_REQUEST,
        conflicts_with_all = [
            "target_profile",
            "target_access_key",
            "target_secret_access_key",
            "target_session_token",
            "target_request_payer",
        ],
        help_heading = "AWS Configuration"
    )]
    pub target_no_sign_request: bool,

    /// Disable stalled stream protection
    #[arg(long, env, default_value_t = DEFAULT_DISABLE_STALLED_STREAM_PROTECTION, help_heading = "AWS Configuration")]
    pub disable_stalled_stream_protection: bool,

    // === Retry Options ===
    /// Maximum retry attempts for AWS SDK operations
    #[arg(long, env, default_value_t = DEFAULT_AWS_MAX_ATTEMPTS, help_heading = "Retry Options")]
    pub aws_max_attempts: u32,

    /// Initial backoff in milliseconds for retries
    #[arg(long, env, default_value_t = DEFAULT_INITIAL_BACKOFF_MILLISECONDS, help_heading = "Retry Options")]
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

impl CommonClientArgs {
    /// Translate the parsed flags into the existing `ClientConfig` used by
    /// `storage::s3::client_builder`. Single endpoint (target), so no source/target
    /// dichotomy. The parallel-upload semaphore is unused by thin-wrapper commands
    /// but must be present because `ClientConfig` requires it.
    pub fn build_client_config(&self) -> ClientConfig {
        let credential = if self.target_no_sign_request {
            S3Credentials::NoSignRequest
        } else if let Some(profile) = self.target_profile.clone() {
            S3Credentials::Profile(profile)
        } else if let Some(access_key) = self.target_access_key.clone() {
            S3Credentials::Credentials {
                access_keys: AccessKeys {
                    access_key,
                    secret_access_key: self.target_secret_access_key.clone().expect(
                        "clap requires --target-secret-access-key alongside --target-access-key",
                    ),
                    session_token: self.target_session_token.clone(),
                },
            }
        } else {
            S3Credentials::FromEnvironment
        };

        let request_payer = if self.target_request_payer {
            Some(RequestPayer::Requester)
        } else {
            None
        };

        ClientConfig {
            client_config_location: ClientConfigLocation {
                aws_config_file: self.aws_config_file.clone(),
                aws_shared_credentials_file: self.aws_shared_credentials_file.clone(),
            },
            credential,
            region: self.target_region.clone(),
            endpoint_url: self.target_endpoint_url.clone(),
            force_path_style: self.target_force_path_style,
            accelerate: self.target_accelerate,
            request_payer,
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
            // Thin-wrapper commands don't multipart-upload, but ClientConfig
            // demands a semaphore. A 1-permit semaphore is harmless.
            parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    /// Build a `TracingConfig` from verbosity + tracing flags. Returns `None`
    /// when verbosity is below Error so the global tracing subscriber is not
    /// installed (matches cp/mv `-qqq` behaviour).
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    struct TestCli {
        #[command(flatten)]
        common: CommonClientArgs,
    }

    #[test]
    fn parses_with_no_flags() {
        let cli = TestCli::try_parse_from(["test"]).unwrap();
        assert!(cli.common.target_region.is_none());
        assert_eq!(cli.common.aws_max_attempts, DEFAULT_AWS_MAX_ATTEMPTS);
        assert!(!cli.common.json_tracing);
    }

    #[test]
    fn target_access_key_requires_secret() {
        let res = TestCli::try_parse_from(["test", "--target-access-key", "AKIA"]);
        assert!(res.is_err(), "must require --target-secret-access-key");
    }

    #[test]
    fn target_no_sign_request_conflicts_with_profile() {
        let res = TestCli::try_parse_from([
            "test",
            "--target-no-sign-request",
            "--target-profile",
            "default",
        ]);
        assert!(res.is_err(), "no-sign-request must conflict with profile");
    }

    #[test]
    fn target_region_rejects_empty() {
        let res = TestCli::try_parse_from(["test", "--target-region", ""]);
        assert!(res.is_err(), "empty region must be rejected");
    }

    #[test]
    fn build_client_config_uses_environment_credentials_by_default() {
        let cli = TestCli::try_parse_from(["test"]).unwrap();
        let cfg = cli.common.build_client_config();
        assert!(matches!(
            cfg.credential,
            crate::types::S3Credentials::FromEnvironment
        ));
        assert_eq!(cfg.retry_config.aws_max_attempts, DEFAULT_AWS_MAX_ATTEMPTS);
        assert!(!cfg.disable_stalled_stream_protection);
    }

    #[test]
    fn build_client_config_uses_no_sign_request_when_set() {
        let cli = TestCli::try_parse_from(["test", "--target-no-sign-request"]).unwrap();
        let cfg = cli.common.build_client_config();
        assert!(matches!(
            cfg.credential,
            crate::types::S3Credentials::NoSignRequest
        ));
    }

    #[test]
    fn build_client_config_uses_explicit_keys() {
        let cli = TestCli::try_parse_from([
            "test",
            "--target-access-key",
            "AKIA",
            "--target-secret-access-key",
            "secret",
        ])
        .unwrap();
        let cfg = cli.common.build_client_config();
        assert!(matches!(
            cfg.credential,
            crate::types::S3Credentials::Credentials { .. }
        ));
    }

    #[test]
    fn build_client_config_uses_profile_when_set() {
        let cli = TestCli::try_parse_from(["test", "--target-profile", "prod"]).unwrap();
        let cfg = cli.common.build_client_config();
        match cfg.credential {
            crate::types::S3Credentials::Profile(name) => assert_eq!(name, "prod"),
            other => panic!("expected Profile, got {other:?}"),
        }
    }

    #[test]
    fn build_client_config_propagates_request_payer_and_accelerate() {
        let cli =
            TestCli::try_parse_from(["test", "--target-request-payer", "--target-accelerate"])
                .unwrap();
        let cfg = cli.common.build_client_config();
        assert_eq!(
            cfg.request_payer,
            Some(aws_sdk_s3::types::RequestPayer::Requester)
        );
        assert!(cfg.accelerate);
    }

    #[test]
    fn build_tracing_config_returns_some_at_default_verbosity() {
        let cli = TestCli::try_parse_from(["test"]).unwrap();
        let cfg = cli.common.build_tracing_config();
        assert!(cfg.is_some());
        let cfg = cfg.unwrap();
        assert_eq!(cfg.tracing_level, log::Level::Warn);
        assert!(!cfg.json_tracing);
    }

    #[test]
    fn build_tracing_config_returns_none_when_silenced() {
        let cli = TestCli::try_parse_from(["test", "-qqq"]).unwrap();
        let cfg = cli.common.build_tracing_config();
        assert!(cfg.is_none(), "expected None when verbosity below Error");
    }

    #[test]
    fn build_tracing_config_propagates_flags() {
        let cli = TestCli::try_parse_from([
            "test",
            "--json-tracing",
            "--aws-sdk-tracing",
            "--span-events-tracing",
            "--disable-color-tracing",
        ])
        .unwrap();
        let cfg = cli.common.build_tracing_config().unwrap();
        assert!(cfg.json_tracing);
        assert!(cfg.aws_sdk_tracing);
        assert!(cfg.span_events_tracing);
        assert!(cfg.disable_color_tracing);
    }
}
