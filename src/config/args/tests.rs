#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::config::args::build_config_from_args;

    fn args_with(source: &str, target: &str) -> Vec<String> {
        vec![
            "s3util".to_string(),
            "cp".to_string(),
            source.to_string(),
            target.to_string(),
        ]
    }

    fn args_with_extra(source: &str, target: &str, extra: &[&str]) -> Vec<String> {
        let mut args = args_with(source, target);
        for e in extra {
            args.push(e.to_string());
        }
        args
    }

    #[test]
    fn both_stdio_rejected() {
        let result = build_config_from_args(args_with("-", "-"));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("source and target cannot both be stdin/stdout")
        );
    }

    #[test]
    fn both_local_rejected() {
        let result = build_config_from_args(args_with("/tmp/source", "/tmp/target"));
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("both be local"));
    }

    #[test]
    fn server_side_copy_requires_both_s3() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/source",
            "s3://my-bucket/prefix",
            &["--server-side-copy"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--server-side-copy, both source and target must be s3://")
        );
    }

    #[test]
    fn stdio_incompatible_with_server_side_copy() {
        let result = build_config_from_args(args_with_extra(
            "-",
            "s3://my-bucket/prefix",
            &["--server-side-copy"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("incompatible with --server-side-copy")
        );
    }

    #[test]
    fn version_id_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/source",
            "s3://my-bucket/prefix",
            &["--version-id", "abc123"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--version-id, source must be s3://")
        );
    }

    #[test]
    fn valid_local_to_s3() {
        let result = build_config_from_args(args_with("/tmp", "s3://my-bucket/prefix"));
        assert!(result.is_ok());
    }

    #[test]
    fn valid_s3_to_local() {
        let result = build_config_from_args(args_with("s3://my-bucket/prefix", "/tmp"));
        assert!(result.is_ok());
    }

    #[test]
    fn valid_stdio_to_s3() {
        let result = build_config_from_args(args_with("-", "s3://my-bucket/key"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(config.is_stdio_source);
        assert!(!config.is_stdio_target);
    }

    #[test]
    fn valid_s3_to_stdio() {
        let result = build_config_from_args(args_with("s3://my-bucket/key", "-"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert!(!config.is_stdio_source);
        assert!(config.is_stdio_target);
    }

    #[test]
    fn rate_limit_bandwidth_parsed_mib() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/source",
            "s3://my-bucket/key",
            &["--rate-limit-bandwidth", "10MiB"],
        ));
        assert!(result.is_ok());
        assert_eq!(result.unwrap().rate_limit_bandwidth, Some(10 * 1024 * 1024),);
    }

    #[test]
    fn rate_limit_bandwidth_below_min_rejected() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/source",
            "s3://my-bucket/key",
            &["--rate-limit-bandwidth", "512KiB"],
        ));
        assert!(result.is_err());
    }

    #[test]
    fn rate_limit_defaults_none() {
        let result = build_config_from_args(args_with("/tmp/source", "s3://my-bucket/key"));
        assert!(result.is_ok());
        let config = result.unwrap();
        assert_eq!(config.rate_limit_bandwidth, None);
    }
}
