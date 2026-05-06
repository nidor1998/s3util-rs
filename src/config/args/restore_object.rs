use aws_sdk_s3::types::Tier;
use clap::Parser;

use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;

const TARGET_NOT_S3: &str = "restore-object target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str = "restore-object target must include a key (s3://<BUCKET>/<KEY>)\n";
const INVALID_TIER: &str = "invalid tier. valid choices: Standard | Bulk | Expedited.";

fn parse_tier(s: &str) -> Result<Tier, String> {
    match s {
        "Standard" => Ok(Tier::Standard),
        "Bulk" => Ok(Tier::Bulk),
        "Expedited" => Ok(Tier::Expedited),
        _ => Err(INVALID_TIER.to_string()),
    }
}

#[derive(Parser, Clone, Debug)]
pub struct RestoreObjectArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Lifetime of the active copy in days
    #[arg(long, env, help_heading = "Object Options")]
    pub days: Option<i32>,

    /// Retrieval tier (Standard, Bulk, or Expedited)
    #[arg(long, env, value_parser = parse_tier, help_heading = "Object Options")]
    pub tier: Option<Tier>,

    /// Optional description for the restore job
    #[arg(long, env, help_heading = "Object Options")]
    pub description: Option<String>,

    /// Version ID of the source object
    #[arg(long, env, help_heading = "Object Options")]
    pub source_version_id: Option<String>,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl RestoreObjectArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    /// Returns the validated `(bucket, key)` pair.
    pub fn bucket_key(&self) -> Result<(String, String), String> {
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
        RestoreObject(RestoreObjectArgs),
    }

    fn parse(args: &[&str]) -> RestoreObjectArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::RestoreObject(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<RestoreObjectArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::RestoreObject(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_key() {
        let a = parse(&["test", "restore-object", "s3://my-bucket/my-key"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    fn accepts_days_and_tier() {
        let a = parse(&[
            "test",
            "restore-object",
            "s3://my-bucket/my-key",
            "--days",
            "7",
            "--tier",
            "Standard",
        ]);
        assert_eq!(a.days, Some(7));
        assert_eq!(a.tier, Some(Tier::Standard));
    }

    #[test]
    fn accepts_bulk_and_expedited_tiers() {
        let a = parse(&[
            "test",
            "restore-object",
            "s3://my-bucket/my-key",
            "--tier",
            "Bulk",
        ]);
        assert_eq!(a.tier, Some(Tier::Bulk));

        let a = parse(&[
            "test",
            "restore-object",
            "s3://my-bucket/my-key",
            "--tier",
            "Expedited",
        ]);
        assert_eq!(a.tier, Some(Tier::Expedited));
    }

    #[test]
    fn rejects_unknown_tier() {
        let res = try_parse(&[
            "test",
            "restore-object",
            "s3://my-bucket/my-key",
            "--tier",
            "TurboMax",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn accepts_source_version_id() {
        let a = parse(&[
            "test",
            "restore-object",
            "s3://my-bucket/my-key",
            "--source-version-id",
            "v123",
        ]);
        assert_eq!(a.source_version_id.as_deref(), Some("v123"));
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&["test", "restore-object", "s3://my-bucket"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn rejects_bucket_with_trailing_slash() {
        let a = parse(&["test", "restore-object", "s3://my-bucket/"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&["test", "restore-object", "--auto-complete-shell", "bash"]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn bucket_key_rejects_non_s3_target() {
        let a = parse(&["test", "restore-object", "/tmp/local"]);
        let err = a.bucket_key().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }
}
