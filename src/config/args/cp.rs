use crate::config::Config;
use crate::config::args::common::{self, CommonTransferArgs};
use crate::config::args::value_parser::storage_path;
use clap::Parser;

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
        crate::config::args::common::build_config_from_common(
            value.common,
            value.source,
            value.target,
        )
    }
}
