use crate::config::args::value_parser::url;
use clap::Parser;
use clap::builder::NonEmptyStringValueParser;
use clap_verbosity_flag::{Verbosity, WarnLevel};
use std::path::PathBuf;
use std::str::FromStr;

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
    #[arg(long, env, default_value_t = DEFAULT_JSON_TRACING, help_heading = "tracing/Logging")]
    pub json_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_AWS_SDK_TRACING, help_heading = "tracing/Logging")]
    pub aws_sdk_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_SPAN_EVENTS_TRACING, help_heading = "tracing/Logging")]
    pub span_events_tracing: bool,

    #[arg(long, env, default_value_t = DEFAULT_DISABLE_COLOR_TRACING, help_heading = "tracing/Logging")]
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
}
