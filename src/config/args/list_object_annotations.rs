use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "list-object-annotations target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str =
    "list-object-annotations target must include a key (s3://<BUCKET>/<KEY>)\n";

#[derive(Parser, Clone, Debug)]
pub struct ListObjectAnnotationsArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Only list annotations whose name starts with this prefix.
    #[arg(long, env, help_heading = "Object Options")]
    pub annotation_prefix: Option<String>,

    /// Version ID of the object to list annotations for.
    #[arg(long, env, help_heading = "Object Options")]
    pub target_version_id: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl ListObjectAnnotationsArgs {
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
        ListObjectAnnotations(ListObjectAnnotationsArgs),
    }

    fn parse(args: &[&str]) -> ListObjectAnnotationsArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::ListObjectAnnotations(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_target_only() {
        let a = parse(&["test", "list-object-annotations", "s3://my-bucket/my-key"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
        assert!(a.annotation_prefix.is_none());
        assert!(a.target_version_id.is_none());
    }

    #[test]
    fn accepts_prefix_and_version_id() {
        let a = parse(&[
            "test",
            "list-object-annotations",
            "s3://my-bucket/dir/sub/file.txt",
            "--annotation-prefix",
            "xs",
            "--target-version-id",
            "v123",
        ]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "dir/sub/file.txt");
        assert_eq!(a.annotation_prefix.as_deref(), Some("xs"));
        assert_eq!(a.target_version_id.as_deref(), Some("v123"));
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&["test", "list-object-annotations", "s3://my-bucket"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn bucket_key_rejects_non_s3_target() {
        let a = parse(&["test", "list-object-annotations", "/tmp/local"]);
        let err = a.bucket_key().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "list-object-annotations",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
