use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str = "put-bucket-lifecycle-configuration target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-lifecycle-configuration target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct PutBucketLifecycleConfigurationArgs {
    #[arg(
        env = "TARGET",
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Path to a file containing the lifecycle-configuration JSON (AWS-CLI
    /// shape, top-level `Rules` array), or `-` to read from stdin.
    #[arg(
        env = "LIFECYCLE_CONFIGURATION",
        required_unless_present = "auto_complete_shell"
    )]
    pub lifecycle_configuration: Option<String>,

    /// Bucket-level default minimum object size for transitions.
    ///
    /// `get-bucket-lifecycle-configuration` reports this at the top level of its
    /// JSON, but S3 takes it as a request parameter rather than a member of the
    /// lifecycle configuration itself (the AWS CLI exposes it the same way), so
    /// it cannot be supplied inside the configuration file. Without it S3
    /// applies its own default of `all_storage_classes_128K`, which silently
    /// resets a bucket set to `varies_by_storage_class`.
    #[arg(
        long,
        env,
        value_parser = ["varies_by_storage_class", "all_storage_classes_128K"],
        help_heading = "General"
    )]
    pub transition_default_minimum_object_size: Option<String>,

    /// Show what would happen without performing any S3 mutating operation.
    #[arg(long, env, default_value_t = false, help_heading = "General")]
    pub dry_run: bool,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketLifecycleConfigurationArgs {
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
        PutBucketLifecycleConfiguration(PutBucketLifecycleConfigurationArgs),
    }

    fn parse(args: &[&str]) -> PutBucketLifecycleConfigurationArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketLifecycleConfiguration(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketLifecycleConfigurationArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketLifecycleConfiguration(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_file_path() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket",
            "/tmp/lifecycle.json",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(
            a.lifecycle_configuration.as_deref(),
            Some("/tmp/lifecycle.json")
        );
    }

    #[test]
    fn accepts_bucket_and_stdin_dash() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket",
            "-",
        ]);
        assert_eq!(a.lifecycle_configuration.as_deref(), Some("-"));
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket/key",
            "/tmp/x.json",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_config_positional_errors() {
        let res = try_parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket",
        ]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_both_positionals_errors() {
        let res = try_parse(&["test", "put-bucket-lifecycle-configuration"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.lifecycle_configuration.is_none());
        assert!(a.auto_complete_shell().is_some());
    }

    #[test]
    fn bucket_name_rejects_non_s3_target() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "/tmp/local",
            "/tmp/lc.json",
        ]);
        let err = a.bucket_name().unwrap_err();
        assert!(err.contains("must be s3://"), "unexpected err: {err}");
    }
}

/// `--transition-default-minimum-object-size` exists because
/// `get-bucket-lifecycle-configuration` reports the value at the top level of
/// its JSON while S3 accepts it only as a request parameter. Without the flag,
/// putting back that same output silently reset the bucket to S3's default of
/// `all_storage_classes_128K`.
#[cfg(test)]
mod transition_default_minimum_object_size_tests {
    use super::tests_support::parse_args;

    #[test]
    fn defaults_to_none_so_s3_keeps_its_own_default() {
        let args = parse_args(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://b",
            "cfg.json",
        ]);
        assert!(
            args.transition_default_minimum_object_size.is_none(),
            "the parameter must be omitted unless the user asks for it"
        );
    }

    #[test]
    fn accepts_both_values_s3_defines() {
        for value in ["varies_by_storage_class", "all_storage_classes_128K"] {
            let args = parse_args(&[
                "test",
                "put-bucket-lifecycle-configuration",
                "s3://b",
                "cfg.json",
                "--transition-default-minimum-object-size",
                value,
            ]);
            assert_eq!(
                args.transition_default_minimum_object_size.as_deref(),
                Some(value)
            );
        }
    }

    #[test]
    fn rejects_an_unknown_value() {
        assert!(
            super::tests_support::try_parse_args(&[
                "test",
                "put-bucket-lifecycle-configuration",
                "s3://b",
                "cfg.json",
                "--transition-default-minimum-object-size",
                "128KB",
            ])
            .is_err(),
            "only the two values S3 defines may be accepted"
        );
    }

    /// The value must map onto the SDK enum, not just parse as a string.
    #[test]
    fn values_map_onto_the_sdk_enum() {
        use aws_sdk_s3::types::TransitionDefaultMinimumObjectSize;
        assert_eq!(
            TransitionDefaultMinimumObjectSize::from("varies_by_storage_class"),
            TransitionDefaultMinimumObjectSize::VariesByStorageClass
        );
        assert_eq!(
            TransitionDefaultMinimumObjectSize::from("all_storage_classes_128K"),
            TransitionDefaultMinimumObjectSize::AllStorageClasses128K
        );
    }
}

#[cfg(test)]
mod tests_support {
    use super::PutBucketLifecycleConfigurationArgs;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(name = "test")]
    struct SupportCli {
        #[command(subcommand)]
        cmd: SupportSub,
    }

    #[derive(clap::Subcommand, Debug)]
    enum SupportSub {
        PutBucketLifecycleConfiguration(PutBucketLifecycleConfigurationArgs),
    }

    pub(super) fn parse_args(args: &[&str]) -> PutBucketLifecycleConfigurationArgs {
        try_parse_args(args).unwrap()
    }

    pub(super) fn try_parse_args(
        args: &[&str],
    ) -> Result<PutBucketLifecycleConfigurationArgs, clap::Error> {
        let cli = SupportCli::try_parse_from(args)?;
        let SupportSub::PutBucketLifecycleConfiguration(a) = cli.cmd;
        Ok(a)
    }
}
