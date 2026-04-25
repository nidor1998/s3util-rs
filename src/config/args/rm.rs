use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "rm target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str = "rm target must include a key (s3://<BUCKET>/<KEY>)\n";

#[derive(Parser, Clone, Debug)]
pub struct RmArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Version ID of the object version to delete
    #[arg(long, env, help_heading = "Object Options")]
    pub source_version_id: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl RmArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    /// Returns the validated `(bucket, key)` pair.
    ///
    /// Errors if the target is not an S3 path or if the key is empty.
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
        Rm(RmArgs),
    }

    fn parse(args: &[&str]) -> RmArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::Rm(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_bucket_and_key() {
        let a = parse(&["test", "rm", "s3://my-bucket/my-key"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    fn accepts_nested_key() {
        let a = parse(&["test", "rm", "s3://my-bucket/dir/subdir/file.txt"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "dir/subdir/file.txt");
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&["test", "rm", "s3://my-bucket"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn rejects_bucket_with_trailing_slash() {
        let a = parse(&["test", "rm", "s3://my-bucket/"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn rejects_local_path() {
        let res = TestCli::try_parse_from(["test", "rm", "/tmp/foo"]);
        // storage_path::check_storage_path may accept it; bucket_key() rejects it.
        if let Ok(cli) = res {
            let TestSub::Rm(a) = cli.cmd;
            assert!(a.bucket_key().is_err());
        }
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&["test", "rm", "--auto-complete-shell", "bash"]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn accepts_source_version_id() {
        let a = parse(&[
            "test",
            "rm",
            "s3://my-bucket/my-key",
            "--source-version-id",
            "abc123",
        ]);
        assert_eq!(a.source_version_id.as_deref(), Some("abc123"));
    }
}
