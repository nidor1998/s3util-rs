use aws_sdk_s3::types::Payer;
use clap::{ArgGroup, CommandFactory, Parser};

use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;

const TARGET_NOT_S3: &str = "put-bucket-request-payment target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-request-payment target must be s3://<BUCKET> with no key or prefix\n";

/// `--requester` and `--bucket-owner` are mutually exclusive; exactly one is
/// required (unless `--auto-complete-shell` is used). Mirrors the
/// `put-bucket-versioning` flag-style pattern.
#[derive(Parser, Clone, Debug)]
#[command(group(
    ArgGroup::new("payer_state")
        .args(["requester", "bucket_owner"])
        .multiple(false)
))]
pub struct PutBucketRequestPaymentArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Requester pays for download and request fees
    #[arg(long)]
    pub requester: bool,

    /// Bucket owner pays for download and request fees (default for new buckets)
    #[arg(long)]
    pub bucket_owner: bool,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketRequestPaymentArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

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

    /// Exits with code 2 (clap convention for argument errors) if neither
    /// `--requester` nor `--bucket-owner` was supplied.
    pub fn validate_state_flag(&self) {
        if !self.requester && !self.bucket_owner {
            let mut cmd = Self::command();
            cmd.error(
                clap::error::ErrorKind::MissingRequiredArgument,
                "one of --requester or --bucket-owner must be specified",
            )
            .exit();
        }
    }

    /// Returns the payer derived from the mutually-exclusive flags.
    /// [`validate_state_flag`] must be called first to ensure one is set.
    pub fn payer(&self) -> Payer {
        if self.requester {
            Payer::Requester
        } else {
            Payer::BucketOwner
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
        PutBucketRequestPayment(PutBucketRequestPaymentArgs),
    }

    fn parse(args: &[&str]) -> PutBucketRequestPaymentArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketRequestPayment(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketRequestPaymentArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketRequestPayment(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_requester_flag() {
        let a = parse(&[
            "test",
            "put-bucket-request-payment",
            "s3://my-bucket",
            "--requester",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(a.payer(), Payer::Requester);
    }

    #[test]
    fn accepts_bucket_owner_flag() {
        let a = parse(&[
            "test",
            "put-bucket-request-payment",
            "s3://my-bucket",
            "--bucket-owner",
        ]);
        assert_eq!(a.payer(), Payer::BucketOwner);
    }

    #[test]
    fn rejects_both_flags() {
        let res = try_parse(&[
            "test",
            "put-bucket-request-payment",
            "s3://my-bucket",
            "--requester",
            "--bucket-owner",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-request-payment",
            "s3://my-bucket/key",
            "--requester",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-request-payment",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn neither_flag_set_detect_via_validate() {
        let a = parse(&["test", "put-bucket-request-payment", "s3://my-bucket"]);
        assert!(!a.requester);
        assert!(!a.bucket_owner);
    }

    #[test]
    fn bucket_name_rejects_non_s3_target() {
        let a = parse(&[
            "test",
            "put-bucket-request-payment",
            "/tmp/local",
            "--requester",
        ]);
        let err = a.bucket_name().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }
}
