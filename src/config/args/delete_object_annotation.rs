use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "delete-object-annotation target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str =
    "delete-object-annotation target must include a key (s3://<BUCKET>/<KEY>)\n";

#[derive(Parser, Clone, Debug)]
pub struct DeleteObjectAnnotationArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Name of the annotation to delete (1-512 bytes).
    #[arg(
        long,
        env,
        required_unless_present = "auto_complete_shell",
        help_heading = "Object Options"
    )]
    pub annotation_name: Option<String>,

    /// Version ID of the object to delete the annotation from.
    #[arg(long, env, help_heading = "Object Options")]
    pub target_version_id: Option<String>,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl DeleteObjectAnnotationArgs {
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
        DeleteObjectAnnotation(DeleteObjectAnnotationArgs),
    }

    fn parse(args: &[&str]) -> DeleteObjectAnnotationArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::DeleteObjectAnnotation(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<DeleteObjectAnnotationArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::DeleteObjectAnnotation(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_required_args() {
        let a = parse(&[
            "test",
            "delete-object-annotation",
            "s3://my-bucket/my-key",
            "--annotation-name",
            "note",
        ]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
        assert_eq!(a.annotation_name.as_deref(), Some("note"));
    }

    #[test]
    fn accepts_nested_key_and_version_id() {
        let a = parse(&[
            "test",
            "delete-object-annotation",
            "s3://my-bucket/dir/sub/file.txt",
            "--annotation-name",
            "note",
            "--target-version-id",
            "v123",
        ]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "dir/sub/file.txt");
        assert_eq!(a.target_version_id.as_deref(), Some("v123"));
    }

    #[test]
    fn missing_annotation_name_is_error() {
        let res = try_parse(&["test", "delete-object-annotation", "s3://my-bucket/my-key"]);
        assert!(res.is_err());
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&[
            "test",
            "delete-object-annotation",
            "s3://my-bucket",
            "--annotation-name",
            "note",
        ]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn bucket_key_rejects_non_s3_target() {
        let a = parse(&[
            "test",
            "delete-object-annotation",
            "/tmp/local",
            "--annotation-name",
            "note",
        ]);
        let err = a.bucket_key().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "delete-object-annotation",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
