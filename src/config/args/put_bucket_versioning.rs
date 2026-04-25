use aws_sdk_s3::types::BucketVersioningStatus;
use clap::{ArgGroup, CommandFactory, Parser};

use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;

const TARGET_NOT_S3: &str = "put-bucket-versioning target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-versioning target must be s3://<BUCKET> with no key or prefix\n";

/// `--enabled` and `--suspended` are mutually exclusive; exactly one is
/// required (unless `--auto-complete-shell` is used).
///
/// The `ArgGroup` enforces the mutual-exclusion (`multiple(false)`) only.
/// The "required" check is done post-parse via [`validate_state_flag`] so
/// that `--auto-complete-shell` can short-circuit before validation fires.
#[derive(Parser, Clone, Debug)]
#[command(group(
    ArgGroup::new("versioning_state")
        .args(["enabled", "suspended"])
        .multiple(false)
))]
pub struct PutBucketVersioningArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Enable versioning on the bucket
    #[arg(long)]
    pub enabled: bool,

    /// Suspend versioning on the bucket
    #[arg(long)]
    pub suspended: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketVersioningArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    /// Returns the validated bucket name (without the `s3://` scheme or any
    /// trailing `/`). Errors on non-S3 paths or paths with a key/prefix.
    pub fn bucket_name(&self) -> Result<String, String> {
        let raw = self
            .target
            .as_deref()
            .ok_or_else(|| TARGET_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if !prefix.is_empty() {
                    return Err(TARGET_HAS_KEY_OR_PREFIX.to_string());
                }
                Ok(bucket)
            }
            _ => Err(TARGET_NOT_S3.to_string()),
        }
    }

    /// Validates that exactly one of `--enabled` / `--suspended` is provided.
    ///
    /// Called from dispatch code after `--auto-complete-shell` has already been
    /// handled.  Exits with code 2 (clap convention for argument errors) if
    /// neither flag was supplied.
    pub fn validate_state_flag(&self) {
        if !self.enabled && !self.suspended {
            let mut cmd = Self::command();
            cmd.error(
                clap::error::ErrorKind::MissingRequiredArgument,
                "one of --enabled or --suspended must be specified",
            )
            .exit();
        }
    }

    /// Returns the versioning status derived from the mutually-exclusive flags.
    /// [`validate_state_flag`] must be called first to ensure one is set.
    pub fn versioning_status(&self) -> BucketVersioningStatus {
        if self.enabled {
            BucketVersioningStatus::Enabled
        } else {
            BucketVersioningStatus::Suspended
        }
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
        PutBucketVersioning(PutBucketVersioningArgs),
    }

    fn parse(args: &[&str]) -> PutBucketVersioningArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketVersioning(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketVersioningArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketVersioning(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_enabled_flag() {
        let a = parse(&[
            "test",
            "put-bucket-versioning",
            "s3://my-bucket",
            "--enabled",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(a.versioning_status(), BucketVersioningStatus::Enabled);
    }

    #[test]
    fn accepts_suspended_flag() {
        let a = parse(&[
            "test",
            "put-bucket-versioning",
            "s3://my-bucket",
            "--suspended",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(a.versioning_status(), BucketVersioningStatus::Suspended);
    }

    #[test]
    fn rejects_both_flags() {
        let res = try_parse(&[
            "test",
            "put-bucket-versioning",
            "s3://my-bucket",
            "--enabled",
            "--suspended",
        ]);
        assert!(
            res.is_err(),
            "cannot specify both --enabled and --suspended"
        );
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-versioning",
            "s3://my-bucket/key",
            "--enabled",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn accepts_bucket_with_trailing_slash() {
        let a = parse(&[
            "test",
            "put-bucket-versioning",
            "s3://my-bucket/",
            "--enabled",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-versioning",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn enabled_and_suspended_both_false_detect_via_validate() {
        // validate_state_flag() exits the process; we verify the flags are
        // both false and rely on the binary-level CLI test for the exit-2
        // behaviour.
        let a = parse(&["test", "put-bucket-versioning", "s3://my-bucket"]);
        assert!(!a.enabled);
        assert!(!a.suspended);
    }
}
