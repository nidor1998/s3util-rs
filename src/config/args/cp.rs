use crate::config::Config;
use crate::config::args::common::{self, CommonTransferArgs};
use crate::config::args::value_parser::storage_path;
use clap::Parser;

const BOTH_STDIO_SPECIFIED: &str = "source and target cannot both be stdin/stdout (-)\n";
const STDIO_INCOMPATIBLE_WITH_SERVER_SIDE_COPY: &str =
    "stdin/stdout (-) is incompatible with --server-side-copy\n";
const SKIP_EXISTING_INCOMPATIBLE_WITH_STDIO_TARGET: &str =
    "--skip-existing is not supported with stdout target (-)\n";
const SKIP_EXISTING_INCOMPATIBLE_WITH_IF_NONE_MATCH: &str =
    "--skip-existing cannot be used with --if-none-match\n";

#[derive(Parser, Clone, Debug)]
pub struct CpArgs {
    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix], local path, or - for stdin/stdout", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub source: Option<String>,

    #[arg(env, help = "s3://<BUCKET_NAME>[/prefix], local path, or - for stdin/stdout", value_parser = storage_path::check_storage_path, required_unless_present = "auto_complete_shell")]
    pub target: Option<String>,

    #[command(flatten)]
    pub common: CommonTransferArgs,

    /// Skip the copy if the target already exists.
    #[arg(
        long,
        env,
        default_value_t = false,
        help_heading = "Advanced",
        long_help = r#"Skip the copy if the target already exists.
The target's content is not verified — only its existence is checked
(S3 HeadObject for s3:// targets, filesystem exists check for local targets).
When the target exists the command exits 0 with no transfer performed.
Cannot be used with --if-none-match or with stdout target (-)."#
    )]
    pub skip_existing: bool,
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

    pub(crate) fn check_skip_existing_stdio_target(&self) -> Result<(), String> {
        if self.skip_existing && self.is_target_stdio() {
            return Err(SKIP_EXISTING_INCOMPATIBLE_WITH_STDIO_TARGET.to_string());
        }
        Ok(())
    }

    pub(crate) fn check_skip_existing_if_none_match_conflict(&self) -> Result<(), String> {
        if self.skip_existing && self.common.if_none_match {
            return Err(SKIP_EXISTING_INCOMPATIBLE_WITH_IF_NONE_MATCH.to_string());
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
        self.check_skip_existing_stdio_target()?;
        self.check_skip_existing_if_none_match_conflict()?;
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
        let skip_existing = value.skip_existing;
        let mut config = crate::config::args::common::build_config_from_common(
            value.common,
            value.source,
            value.target,
        )?;
        config.skip_existing = skip_existing;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::{Commands, parse_from_args};

    fn cp_args_from(extra: &[&str]) -> CpArgs {
        let mut args: Vec<String> = vec!["s3util".to_string(), "cp".to_string()];
        for e in extra {
            args.push((*e).to_string());
        }
        let cli = parse_from_args(args).unwrap();
        let Commands::Cp(cp_args) = cli.command else {
            panic!("expected Cp variant");
        };
        cp_args
    }

    #[test]
    fn skip_existing_parses_to_true() {
        let cp_args = cp_args_from(&["--skip-existing", "/tmp/a", "s3://b/k"]);
        assert!(cp_args.skip_existing);
    }

    #[test]
    fn skip_existing_default_is_false() {
        let cp_args = cp_args_from(&["/tmp/a", "s3://b/k"]);
        assert!(!cp_args.skip_existing);
    }

    #[test]
    fn skip_existing_with_stdio_target_rejected() {
        let cp_args = cp_args_from(&["--skip-existing", "s3://b/k", "-"]);
        let err = cp_args.validate_storage_config().unwrap_err();
        assert!(
            err.contains("stdout target"),
            "expected stdout-target error, got: {err}"
        );
    }

    #[test]
    fn skip_existing_with_if_none_match_rejected() {
        let cp_args = cp_args_from(&["--skip-existing", "--if-none-match", "/tmp/a", "s3://b/k"]);
        let err = cp_args.validate_storage_config().unwrap_err();
        assert!(
            err.contains("--if-none-match"),
            "expected --if-none-match error, got: {err}"
        );
    }

    #[test]
    fn skip_existing_alone_with_s3_target_accepted() {
        let cp_args = cp_args_from(&["--skip-existing", "s3://src/k", "s3://dst/k"]);
        cp_args
            .validate_storage_config()
            .expect("validation must succeed");
    }

    #[test]
    fn skip_existing_alone_with_local_target_accepted() {
        // Local source + local target would fail check_both_local; use S3 source + local target.
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("dst.dat").to_string_lossy().to_string();
        let cp_args = cp_args_from(&["--skip-existing", "s3://b/k", &target]);
        cp_args
            .validate_storage_config()
            .expect("validation must succeed");
    }

    #[test]
    fn skip_existing_with_stdio_source_accepted() {
        let cp_args = cp_args_from(&["--skip-existing", "-", "s3://b/k"]);
        cp_args
            .validate_storage_config()
            .expect("validation must succeed");
    }

    #[test]
    fn skip_existing_with_server_side_copy_accepted() {
        let cp_args = cp_args_from(&[
            "--skip-existing",
            "--server-side-copy",
            "s3://src/k",
            "s3://dst/k",
        ]);
        cp_args
            .validate_storage_config()
            .expect("validation must succeed");
    }
}
