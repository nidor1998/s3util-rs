use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "put-bucket-website target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-website target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct PutBucketWebsiteArgs {
    #[arg(
        env = "TARGET",
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Path to a file containing the website-configuration JSON (AWS-CLI shape,
    /// flat top-level with `IndexDocument`, `ErrorDocument`,
    /// `RedirectAllRequestsTo`, `RoutingRules`), or `-` to read from stdin.
    #[arg(
        env = "WEBSITE_CONFIGURATION",
        required_unless_present = "auto_complete_shell"
    )]
    pub website_configuration: Option<String>,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketWebsiteArgs {
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
        PutBucketWebsite(PutBucketWebsiteArgs),
    }

    fn parse(args: &[&str]) -> PutBucketWebsiteArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketWebsite(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketWebsiteArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketWebsite(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_file_path() {
        let a = parse(&[
            "test",
            "put-bucket-website",
            "s3://my-bucket",
            "/tmp/website.json",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(
            a.website_configuration.as_deref(),
            Some("/tmp/website.json")
        );
    }

    #[test]
    fn accepts_bucket_and_stdin_dash() {
        let a = parse(&["test", "put-bucket-website", "s3://my-bucket", "-"]);
        assert_eq!(a.website_configuration.as_deref(), Some("-"));
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-website",
            "s3://my-bucket/key",
            "/tmp/x.json",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_config_positional_errors() {
        let res = try_parse(&["test", "put-bucket-website", "s3://my-bucket"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_both_positionals_errors() {
        let res = try_parse(&["test", "put-bucket-website"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-website",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.website_configuration.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
