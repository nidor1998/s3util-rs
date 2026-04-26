use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::config::args::value_parser::tagging::parse_tagging;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "create-bucket target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "create-bucket target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct CreateBucketArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>  (directory bucket: s3://<base>--<zone-id>--x-s3)",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Tags to apply to the bucket, in URL-encoded query format (e.g. "key1=val1&key2=val2")
    #[arg(long, env, value_parser = parse_tagging, help_heading = "Bucket Options")]
    pub tagging: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl CreateBucketArgs {
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
        CreateBucket(CreateBucketArgs),
    }

    fn parse(args: &[&str]) -> CreateBucketArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::CreateBucket(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_bucket_only_path() {
        let a = parse(&["test", "create-bucket", "s3://my-bucket"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn accepts_bucket_with_trailing_slash() {
        let a = parse(&["test", "create-bucket", "s3://my-bucket/"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn accepts_bucket_with_tagging() {
        let a = parse(&[
            "test",
            "create-bucket",
            "s3://my-bucket",
            "--tagging",
            "env=prod&team=sre",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(a.tagging.as_deref(), Some("env=prod&team=sre"));
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&["test", "create-bucket", "s3://my-bucket/key"]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn rejects_local_path() {
        let res = TestCli::try_parse_from(["test", "create-bucket", "/tmp/foo"]);
        if let Ok(cli) = res {
            let TestSub::CreateBucket(a) = cli.cmd;
            assert!(a.bucket_name().is_err());
        }
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&["test", "create-bucket", "--auto-complete-shell", "bash"]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
