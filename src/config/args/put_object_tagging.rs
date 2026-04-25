use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::config::args::value_parser::tagging::parse_tagging;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "put-object-tagging target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str =
    "put-object-tagging target must include a key (s3://<BUCKET>/<KEY>)\n";

#[derive(Parser, Clone, Debug)]
pub struct PutObjectTaggingArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Tags to apply, in URL-encoded query format (e.g. "key1=val1&key2=val2")
    #[arg(long, env, value_parser = parse_tagging, required_unless_present = "auto_complete_shell", help_heading = "Object Options")]
    pub tagging: Option<String>,

    /// Version ID of the source object
    #[arg(long, env, help_heading = "Object Options")]
    pub source_version_id: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutObjectTaggingArgs {
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
        PutObjectTagging(PutObjectTaggingArgs),
    }

    fn parse(args: &[&str]) -> PutObjectTaggingArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutObjectTagging(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_bucket_key_and_tagging() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket/my-key",
            "--tagging",
            "env=prod",
        ]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
        assert_eq!(a.tagging.as_deref(), Some("env=prod"));
    }

    #[test]
    fn accepts_nested_key() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket/dir/subdir/file.txt",
            "--tagging",
            "k=v",
        ]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "dir/subdir/file.txt");
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket",
            "--tagging",
            "k=v",
        ]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn rejects_bucket_with_trailing_slash() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket/",
            "--tagging",
            "k=v",
        ]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn accepts_source_version_id() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket/my-key",
            "--tagging",
            "k=v",
            "--source-version-id",
            "abc123",
        ]);
        assert_eq!(a.source_version_id.as_deref(), Some("abc123"));
    }

    #[test]
    fn accepts_multiple_tags() {
        let a = parse(&[
            "test",
            "put-object-tagging",
            "s3://my-bucket/my-key",
            "--tagging",
            "key1=val1&key2=val2",
        ]);
        assert_eq!(a.tagging.as_deref(), Some("key1=val1&key2=val2"));
    }
}
