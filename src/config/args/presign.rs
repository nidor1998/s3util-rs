use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "presign target must be s3://<BUCKET>/<KEY>\n";
const TARGET_KEY_EMPTY: &str = "presign target must include a key (s3://<BUCKET>/<KEY>)\n";

const DEFAULT_EXPIRES_IN: u64 = 3600;
const MAX_EXPIRES_IN: u64 = 604800;

fn parse_expires_in(s: &str) -> Result<u64, String> {
    let n: u64 = s
        .parse()
        .map_err(|_| format!("invalid --expires-in: {s}"))?;
    if n == 0 {
        return Err("--expires-in must be greater than 0".to_string());
    }
    if n > MAX_EXPIRES_IN {
        return Err(format!(
            "--expires-in must be no more than {MAX_EXPIRES_IN}"
        ));
    }
    Ok(n)
}

#[derive(Parser, Clone, Debug)]
pub struct PresignArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>/<KEY>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Number of seconds until the pre-signed URL expires. Default is 3600 seconds. Maximum is 604800 seconds.
    #[arg(
        long,
        env,
        default_value_t = DEFAULT_EXPIRES_IN,
        value_parser = parse_expires_in,
        help_heading = "Object Options"
    )]
    pub expires_in: u64,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PresignArgs {
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
        Presign(PresignArgs),
    }

    fn parse(args: &[&str]) -> PresignArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::Presign(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PresignArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::Presign(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_key() {
        let a = parse(&["test", "presign", "s3://my-bucket/my-key"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key");
    }

    #[test]
    fn accepts_nested_key() {
        let a = parse(&["test", "presign", "s3://my-bucket/dir/subdir/file.txt"]);
        let (bucket, key) = a.bucket_key().unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "dir/subdir/file.txt");
    }

    #[test]
    fn rejects_bucket_only_path() {
        let a = parse(&["test", "presign", "s3://my-bucket"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn rejects_bucket_with_trailing_slash() {
        let a = parse(&["test", "presign", "s3://my-bucket/"]);
        assert!(a.bucket_key().is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&["test", "presign", "--auto-complete-shell", "bash"]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn expires_in_defaults_to_3600() {
        let a = parse(&["test", "presign", "s3://my-bucket/my-key"]);
        assert_eq!(a.expires_in, 3600);
    }

    #[test]
    fn accepts_custom_expires_in() {
        let a = parse(&[
            "test",
            "presign",
            "s3://my-bucket/my-key",
            "--expires-in",
            "60",
        ]);
        assert_eq!(a.expires_in, 60);
    }

    #[test]
    fn accepts_max_expires_in() {
        let a = parse(&[
            "test",
            "presign",
            "s3://my-bucket/my-key",
            "--expires-in",
            "604800",
        ]);
        assert_eq!(a.expires_in, 604800);
    }

    #[test]
    fn rejects_zero_expires_in() {
        let res = try_parse(&[
            "test",
            "presign",
            "s3://my-bucket/my-key",
            "--expires-in",
            "0",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn rejects_expires_in_over_one_week() {
        let res = try_parse(&[
            "test",
            "presign",
            "s3://my-bucket/my-key",
            "--expires-in",
            "604801",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn rejects_negative_expires_in() {
        let res = try_parse(&[
            "test",
            "presign",
            "s3://my-bucket/my-key",
            "--expires-in",
            "-1",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn bucket_key_rejects_non_s3_target() {
        let a = parse(&["test", "presign", "/tmp/local"]);
        let err = a.bucket_key().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }
}
