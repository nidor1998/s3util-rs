use crate::config::Config;
use crate::config::args::common::CommonTransferArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const STDIO_NOT_SUPPORTED: &str = "stdin/stdout (-) is not supported by mv\n";

#[derive(Parser, Clone, Debug)]
pub struct MvArgs {
    #[arg(help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub source: Option<String>,

    #[arg(help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub target: Option<String>,

    #[command(flatten)]
    pub common: CommonTransferArgs,

    /// Treat ETag/checksum verify warnings as success: delete source and exit 0.
    #[arg(long, env, default_value_t = false, help_heading = "Verification")]
    pub no_fail_on_verify_error: bool,
}

impl MvArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    fn source_str(&self) -> &str {
        self.source.as_deref().unwrap_or("")
    }

    fn target_str(&self) -> &str {
        self.target.as_deref().unwrap_or("")
    }

    fn is_source_stdio(&self) -> bool {
        matches!(
            storage_path::parse_storage_path(self.source_str()),
            StoragePath::Stdio
        )
    }

    fn is_target_stdio(&self) -> bool {
        matches!(
            storage_path::parse_storage_path(self.target_str()),
            StoragePath::Stdio
        )
    }

    fn check_no_stdio(&self) -> Result<(), String> {
        if self.is_source_stdio() || self.is_target_stdio() {
            return Err(STDIO_NOT_SUPPORTED.to_string());
        }
        Ok(())
    }

    fn validate_storage_config(&self) -> Result<(), String> {
        let source = self.source_str();
        let target = self.target_str();
        self.check_no_stdio()?;
        self.common.validate_common_storage_config(source, target)?;
        Ok(())
    }
}

impl TryFrom<MvArgs> for Config {
    type Error = String;

    fn try_from(value: MvArgs) -> Result<Self, Self::Error> {
        value.validate_storage_config()?;
        let no_fail_on_verify_error = value.no_fail_on_verify_error;
        let mut config = crate::config::args::common::build_config_from_common(
            value.common,
            value.source,
            value.target,
        )?;
        config.no_fail_on_verify_error = no_fail_on_verify_error;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::{Commands, parse_from_args};

    fn mv_args_from(extra: &[&str]) -> MvArgs {
        let mut args: Vec<String> = vec!["s3util".to_string(), "mv".to_string()];
        for e in extra {
            args.push((*e).to_string());
        }
        let cli = parse_from_args(args).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        mv_args
    }

    #[test]
    fn stdio_source_is_rejected() {
        let mv_args = mv_args_from(&["-", "s3://b/k"]);
        let err = mv_args.validate_storage_config().unwrap_err();
        assert!(
            err.contains("not supported by mv"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn no_fail_on_verify_error_reaches_config() {
        let mv_args = mv_args_from(&["--no-fail-on-verify-error", "s3://a/k", "s3://b/k"]);
        let config = Config::try_from(mv_args).unwrap();
        assert!(config.no_fail_on_verify_error);
    }

    #[test]
    fn build_config_error_propagates_through_try_from() {
        // clap validates --multipart-threshold before it ever reaches
        // build_config_from_common, so corrupt the already-parsed args the
        // way a programmatic caller could: validation passes, the build
        // itself must be the step that fails.
        let mut mv_args = mv_args_from(&["s3://a/k", "s3://b/k"]);
        mv_args.common.multipart_threshold = "not-a-size".to_string();
        let err = Config::try_from(mv_args).unwrap_err();
        assert!(
            err.contains("is not a number"),
            "expected a human-bytes parse error: {err}"
        );
    }
}
