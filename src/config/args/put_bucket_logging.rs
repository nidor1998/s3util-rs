use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "put-bucket-logging target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-logging target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct PutBucketLoggingArgs {
    #[arg(
        env = "TARGET",
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Path to a file containing the bucket-logging-status JSON (AWS-CLI shape,
    /// top-level optional `LoggingEnabled` object), or `-` to read from stdin.
    /// An empty object (`{}`) disables logging on the bucket — there is no
    /// `delete-bucket-logging` API.
    #[arg(
        env = "BUCKET_LOGGING_STATUS",
        required_unless_present = "auto_complete_shell"
    )]
    pub bucket_logging_status: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketLoggingArgs {
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
        PutBucketLogging(PutBucketLoggingArgs),
    }

    fn parse(args: &[&str]) -> PutBucketLoggingArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketLogging(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketLoggingArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketLogging(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_file_path() {
        let a = parse(&[
            "test",
            "put-bucket-logging",
            "s3://my-bucket",
            "/tmp/logging.json",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(
            a.bucket_logging_status.as_deref(),
            Some("/tmp/logging.json")
        );
    }

    #[test]
    fn accepts_bucket_and_stdin_dash() {
        let a = parse(&["test", "put-bucket-logging", "s3://my-bucket", "-"]);
        assert_eq!(a.bucket_logging_status.as_deref(), Some("-"));
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-logging",
            "s3://my-bucket/key",
            "/tmp/x.json",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_config_positional_errors() {
        let res = try_parse(&["test", "put-bucket-logging", "s3://my-bucket"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_both_positionals_errors() {
        let res = try_parse(&["test", "put-bucket-logging"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-logging",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.bucket_logging_status.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
