use crate::config::Config;
use crate::config::args::common::CommonTransferArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const STDIO_NOT_SUPPORTED: &str = "stdin/stdout (-) is not supported by mv\n";

#[derive(Parser, Clone, Debug)]
pub struct MvArgs {
    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub source: Option<String>,

    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix] or local path", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
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
