#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use crate::config::Config;
    use crate::config::args::{Cli, Commands, build_config_from_args, parse_from_args};
    use crate::types::{S3Credentials, StoragePath};

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
    fn check_at_least_one_s3_or_stdio_rejects_both_local_direct() {
        // check_at_least_one_s3_or_stdio is normally preempted by
        // check_both_local in the validation chain, so the NO_S3_STORAGE
        // branch is defensive. Call the method directly on a CpArgs with
        // two local paths to exercise that branch.
        let cli = parse_from_args(args_with("/tmp/a", "/tmp/b")).unwrap();
        let Commands::Cp(cp_args) = cli.command else {
            panic!("expected Cp variant");
        };
        let err = cp_args.check_at_least_one_s3_or_stdio().unwrap_err();
        assert!(
            err.contains("either SOURCE or TARGET must be s3://"),
            "unexpected error: {err}"
        );
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
            &["--source-version-id", "abc123"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-version-id, source must be s3://")
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

    #[test]
    fn storage_class_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--storage-class", "STANDARD"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--storage-class, target storage must be s3://")
        );
    }

    #[test]
    fn storage_credentials_rejected_on_local_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--source-profile", "p"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("no source credential required")
        );
    }

    #[test]
    fn storage_credentials_rejected_on_local_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--target-profile", "p"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("no target credential required")
        );
    }

    #[test]
    fn sse_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--sse", "AES256"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--sse/--sse-kms-key-id, target storage must be s3://")
        );
    }

    #[test]
    fn sse_kms_key_id_requires_aws_kms_sse() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--sse", "AES256", "--sse-kms-key-id", "alias/my-key"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--sse-kms-key-id must be used with --sse aws:kms")
        );
    }

    #[test]
    fn sse_c_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &[
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
                "--source-sse-c-key-md5",
                "zZ5FnqcIqUjVwvWmyog4zw==",
            ],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-sse-c/--target-sse-c, remote storage must be s3://")
        );
    }

    #[test]
    fn sse_c_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &[
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                "MDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
                "--target-sse-c-key-md5",
                "zZ5FnqcIqUjVwvWmyog4zw==",
            ],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-sse-c/--target-sse-c, remote storage must be s3://")
        );
    }

    #[test]
    fn acl_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--acl", "private"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--acl, target storage must be s3://")
        );
    }

    #[test]
    fn additional_checksum_algorithm_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--additional-checksum-algorithm", "SHA1"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--additional-checksum-algorithm, target storage must be s3://")
        );
    }

    #[test]
    fn enable_additional_checksum_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--enable-additional-checksum"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--enable-additional-checksum, source storage must be s3://")
        );
    }

    #[test]
    fn auto_chunksize_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--auto-chunksize"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--auto-chunksize, source storage must be s3://")
        );
    }

    #[test]
    fn metadata_option_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--cache-control", "no-cache"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("metadata related option, target storage must be s3://")
        );
    }

    #[test]
    fn source_endpoint_url_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--source-endpoint-url", "http://localhost:9000"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-endpoint-url, source storage must be s3://")
        );
    }

    #[test]
    fn target_endpoint_url_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--target-endpoint-url", "http://localhost:9000"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--target-endpoint-url, target storage must be s3://")
        );
    }

    #[test]
    fn disable_payload_signing_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--disable-payload-signing"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--disable-payload-signing, target storage must be s3://")
        );
    }

    #[test]
    fn disable_content_md5_header_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--disable-content-md5-header"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--disable-content-md5-header, target storage must be s3://")
        );
    }

    #[test]
    fn full_object_checksum_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--full-object-checksum"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--full-object-checksum, target storage must be s3://")
        );
    }

    #[test]
    fn full_object_checksum_rejects_sha1() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &[
                "--full-object-checksum",
                "--additional-checksum-algorithm",
                "SHA1",
            ],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Only CRC32/CRC32C/CRC64NVME supports full object checksum")
        );
    }

    #[test]
    fn full_object_checksum_rejects_sha256() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &[
                "--full-object-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
            ],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Only CRC32/CRC32C/CRC64NVME supports full object checksum")
        );
    }

    #[test]
    fn source_accelerate_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--source-accelerate"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-accelerate, source storage must be s3://")
        );
    }

    #[test]
    fn target_accelerate_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--target-accelerate"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--target-accelerate, target storage must be s3://")
        );
    }

    #[test]
    fn source_request_payer_requires_s3_source() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://b/k",
            &["--source-request-payer"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--source-request-payer, source storage must be s3://")
        );
    }

    #[test]
    fn target_request_payer_requires_s3_target() {
        let result = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/dst",
            &["--target-request-payer"],
        ));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("--target-request-payer, target storage must be s3://")
        );
    }

    #[test]
    fn source_access_key_builds_credentials_variant() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("dst").to_string_lossy().to_string();
        let result = build_config_from_args(args_with_extra(
            "s3://src/k",
            &target,
            &[
                "--source-access-key",
                "AKIATEST",
                "--source-secret-access-key",
                "SECRET",
            ],
        ));
        let config = result.unwrap();
        match config.source_client_config.unwrap().credential {
            S3Credentials::Credentials { access_keys } => {
                assert_eq!(access_keys.access_key, "AKIATEST");
                assert_eq!(access_keys.secret_access_key, "SECRET");
                assert!(access_keys.session_token.is_none());
            }
            other => panic!("expected Credentials variant, got {:?}", other),
        }
    }

    #[test]
    fn target_access_key_builds_credentials_variant() {
        let result = build_config_from_args(args_with_extra(
            "/tmp/src",
            "s3://tgt/k",
            &[
                "--target-access-key",
                "AKIATGT",
                "--target-secret-access-key",
                "SECRET2",
            ],
        ));
        let config = result.unwrap();
        match config.target_client_config.unwrap().credential {
            S3Credentials::Credentials { access_keys } => {
                assert_eq!(access_keys.access_key, "AKIATGT");
                assert_eq!(access_keys.secret_access_key, "SECRET2");
                assert!(access_keys.session_token.is_none());
            }
            other => panic!("expected Credentials variant, got {:?}", other),
        }
    }

    #[test]
    fn missing_target_rejected() {
        let result = build_config_from_args(vec![
            "s3util".to_string(),
            "cp".to_string(),
            "s3://bucket/key".to_string(),
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn missing_source_and_target_rejected() {
        let result = build_config_from_args(vec!["s3util".to_string(), "cp".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn auto_complete_shell_allows_missing_source_and_target() {
        // Clap must accept --auto-complete-shell without source/target so
        // main.rs can short-circuit into completion generation before
        // Config::try_from runs.
        let result = parse_from_args(vec![
            "s3util".to_string(),
            "cp".to_string(),
            "--auto-complete-shell".to_string(),
            "bash".to_string(),
        ]);
        assert!(result.is_ok());
    }

    #[test]
    fn target_in_existing_directory_passes() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("out.bin").to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://my-bucket/key", &target));
        assert!(result.is_ok(), "expected Ok, got {:?}", result.err());
    }

    #[test]
    fn target_inside_nonexistent_directory_rejected() {
        let target = format!(
            "/definitely/does/not/exist/abc123{}out.bin",
            std::path::MAIN_SEPARATOR
        );
        let result = build_config_from_args(args_with("s3://my-bucket/key", &target));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains(crate::config::args::TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn target_existing_directory_trailing_separator_passes() {
        let dir = tempfile::tempdir().unwrap();
        let target = format!(
            "{}{}",
            dir.path().to_string_lossy(),
            std::path::MAIN_SEPARATOR
        );
        let result = build_config_from_args(args_with("s3://my-bucket/key", &target));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn target_nonexistent_directory_trailing_separator_rejected() {
        let target = format!(
            "/definitely/does/not/exist/abc123{}",
            std::path::MAIN_SEPARATOR
        );
        let result = build_config_from_args(args_with("s3://my-bucket/key", &target));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.contains(crate::config::args::TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn target_existing_directory_no_trailing_separator_passes() {
        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://my-bucket/key", &target));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn target_relative_filename_no_parent_passes() {
        // Bare filename → parent is "" → treated as cwd → check skipped.
        let result = build_config_from_args(args_with("s3://my-bucket/key", "out.bin"));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn target_s3_skips_directory_check() {
        // S3→S3 transfer — local directory logic not exercised.
        let result = build_config_from_args(args_with("s3://src-bucket/k", "s3://dst-bucket/k"));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn target_stdio_skips_directory_check() {
        // Stdio target (`-`) — local directory logic not exercised.
        let result = build_config_from_args(args_with("s3://my-bucket/key", "-"));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn target_parent_dir_slash_accepted_when_parent_exists() {
        // Regression guard for the user-reported command:
        // `s3util cp s3://bucket/key ../` must pass arg validation.
        // `..` (parent of CWD) is a real directory on every platform we
        // run on; the defer branch previously short-circuited this —
        // now the standard existence check handles it uniformly.
        let result = build_config_from_args(args_with("s3://my-bucket/key", "../"));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn source_s3_url_trailing_slash_rejected() {
        let result = build_config_from_args(args_with("s3://b/dir/", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(err.contains("URL ending in '/'"), "unexpected error: {err}");
    }

    #[test]
    fn source_s3_url_trailing_dot_rejected() {
        let result = build_config_from_args(args_with("s3://b/foo/.", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_trailing_dotdot_rejected() {
        let result = build_config_from_args(args_with("s3://b/foo/..", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_bare_dot_rejected() {
        let result = build_config_from_args(args_with("s3://b/.", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_bare_dotdot_rejected() {
        let result = build_config_from_args(args_with("s3://b/..", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_mid_path_dotdot_accepted() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://b/foo/../etc/passwd", &dst));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn source_s3_url_filename_ending_in_dot_accepted() {
        // `foo.` is a legitimate filename ending in `.` (not a path
        // segment equal to `.`). Raw `ends_with("/.")` does NOT match.
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://b/foo.", &dst));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn source_no_sign_request_produces_no_sign_request_credential() {
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().join("out").to_string_lossy().to_string();
        let config = build_config_from_args(args_with_extra(
            "s3://public-bucket/key",
            &dst,
            &["--source-no-sign-request"],
        ))
        .unwrap();

        let source_credential = config.source_client_config.unwrap().credential;
        assert!(
            matches!(source_credential, S3Credentials::NoSignRequest),
            "expected NoSignRequest, got {source_credential:?}"
        );
    }

    #[test]
    fn source_no_sign_request_conflicts_with_source_profile() {
        let err = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/out",
            &["--source-no-sign-request", "--source-profile", "myprofile"],
        ))
        .unwrap_err();
        assert!(
            err.contains("cannot be used with"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_no_sign_request_conflicts_with_source_access_key() {
        let err = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/out",
            &[
                "--source-no-sign-request",
                "--source-access-key",
                "AKIA...",
                "--source-secret-access-key",
                "secret",
            ],
        ))
        .unwrap_err();
        assert!(
            err.contains("cannot be used with"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_no_sign_request_conflicts_with_source_session_token() {
        let err = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/out",
            &[
                "--source-no-sign-request",
                "--source-access-key",
                "AKIA...",
                "--source-secret-access-key",
                "secret",
                "--source-session-token",
                "token",
            ],
        ))
        .unwrap_err();
        assert!(
            err.contains("cannot be used with"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_no_sign_request_conflicts_with_source_request_payer() {
        let err = build_config_from_args(args_with_extra(
            "s3://b/k",
            "/tmp/out",
            &["--source-no-sign-request", "--source-request-payer"],
        ))
        .unwrap_err();
        assert!(
            err.contains("cannot be used with"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_no_sign_request_requires_s3_source() {
        // Local source + --source-no-sign-request is nonsensical; reject at
        // parse-time with a clear message (mirrors --source-endpoint-url).
        let err = build_config_from_args(args_with_extra(
            "/tmp/local-source",
            "s3://my-bucket/key",
            &["--source-no-sign-request"],
        ))
        .unwrap_err();
        assert!(
            err.contains("--source-no-sign-request, source must be s3://"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_no_sign_request_rejects_stdio_source() {
        // `is_source_s3()` returns false for Stdio just as it does for Local;
        // the guard should apply uniformly. Target must be S3 so we don't trip
        // the "both stdio" check first.
        let err = build_config_from_args(args_with_extra(
            "-",
            "s3://my-bucket/key",
            &["--source-no-sign-request"],
        ))
        .unwrap_err();
        assert!(
            err.contains("--source-no-sign-request, source must be s3://"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn mv_parses_with_local_to_s3() {
        let cli = parse_from_args(vec!["s3util", "mv", "/tmp/a", "s3://b/k"]).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let config = Config::try_from(mv_args).unwrap();
        assert!(matches!(config.source, StoragePath::Local(_)));
        assert!(matches!(config.target, StoragePath::S3 { .. }));
        assert!(!config.no_fail_on_verify_error);
    }

    #[test]
    fn mv_no_fail_on_verify_error_default_false() {
        let cli = parse_from_args(vec!["s3util", "mv", "/tmp/a", "s3://b/k"]).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let config = Config::try_from(mv_args).unwrap();
        assert!(!config.no_fail_on_verify_error);
    }

    #[test]
    fn mv_no_fail_on_verify_error_can_be_set() {
        let cli = parse_from_args(vec![
            "s3util",
            "mv",
            "--no-fail-on-verify-error",
            "/tmp/a",
            "s3://b/k",
        ])
        .unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let config = Config::try_from(mv_args).unwrap();
        assert!(config.no_fail_on_verify_error);
    }

    #[test]
    fn mv_rejects_stdio_source() {
        let cli = parse_from_args(vec!["s3util", "mv", "-", "s3://b/k"]).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let err = Config::try_from(mv_args).unwrap_err();
        assert!(
            err.contains("stdin/stdout (-) is not supported by mv"),
            "actual: {err}"
        );
    }

    #[test]
    fn mv_rejects_stdio_target() {
        let cli = parse_from_args(vec!["s3util", "mv", "s3://b/k", "-"]).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let err = Config::try_from(mv_args).unwrap_err();
        assert!(
            err.contains("stdin/stdout (-) is not supported by mv"),
            "actual: {err}"
        );
    }

    #[test]
    fn mv_rejects_both_local() {
        let cli = parse_from_args(vec!["s3util", "mv", "/tmp/a", "/tmp/b"]).unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let err = Config::try_from(mv_args).unwrap_err();
        assert!(err.contains("source and target cannot both be local"));
    }

    #[test]
    fn mv_inherits_storage_class_validation() {
        let cli = parse_from_args(vec![
            "s3util",
            "mv",
            "--storage-class",
            "STANDARD",
            "s3://b/k",
            "/tmp/a",
        ])
        .unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let err = Config::try_from(mv_args).unwrap_err();
        assert!(err.contains("--storage-class"));
    }

    #[test]
    fn parses_head_bucket_subcommand() {
        use clap::Parser;
        let cli = Cli::try_parse_from(["s3util", "head-bucket", "s3://my-bucket"]).unwrap();
        assert!(matches!(cli.command, Commands::HeadBucket(_)));
    }

    // build_config_from_args should reject every thin-wrapper subcommand with
    // a clear Err message — those commands are dispatched in main.rs without
    // going through Config. Each arm asserted individually so a regression
    // produces a precise, actionable failure (rather than a single test that
    // dumps fourteen subcommands' output).
    //
    // Keeps the message contract stable: it must mention the subcommand name
    // and route the operator to main.rs so the next maintainer can navigate
    // from the error to the actual dispatch site.

    fn assert_rejects_with(args: &[&str], cmd_name: &str) {
        let err = build_config_from_args(args.iter().map(|s| s.to_string())).unwrap_err();
        assert!(
            err.contains(cmd_name),
            "expected error to mention subcommand `{cmd_name}`, got: {err}"
        );
        assert!(
            err.contains("dispatched in main.rs"),
            "expected error to point at main.rs, got: {err}"
        );
    }

    #[test]
    fn build_config_rejects_create_bucket() {
        assert_rejects_with(&["s3util", "create-bucket", "s3://b"], "create-bucket");
    }

    #[test]
    fn build_config_rejects_delete_bucket() {
        assert_rejects_with(&["s3util", "delete-bucket", "s3://b"], "delete-bucket");
    }

    #[test]
    fn build_config_rejects_delete_bucket_policy() {
        assert_rejects_with(
            &["s3util", "delete-bucket-policy", "s3://b"],
            "delete-bucket-policy",
        );
    }

    #[test]
    fn build_config_rejects_delete_bucket_tagging() {
        assert_rejects_with(
            &["s3util", "delete-bucket-tagging", "s3://b"],
            "delete-bucket-tagging",
        );
    }

    #[test]
    fn build_config_rejects_delete_object_tagging() {
        assert_rejects_with(
            &["s3util", "delete-object-tagging", "s3://b/k"],
            "delete-object-tagging",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_policy() {
        assert_rejects_with(
            &["s3util", "get-bucket-policy", "s3://b"],
            "get-bucket-policy",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_tagging() {
        assert_rejects_with(
            &["s3util", "get-bucket-tagging", "s3://b"],
            "get-bucket-tagging",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_versioning() {
        assert_rejects_with(
            &["s3util", "get-bucket-versioning", "s3://b"],
            "get-bucket-versioning",
        );
    }

    #[test]
    fn build_config_rejects_get_object_tagging() {
        assert_rejects_with(
            &["s3util", "get-object-tagging", "s3://b/k"],
            "get-object-tagging",
        );
    }

    #[test]
    fn build_config_rejects_head_bucket() {
        assert_rejects_with(&["s3util", "head-bucket", "s3://b"], "head-bucket");
    }

    #[test]
    fn build_config_rejects_head_object() {
        assert_rejects_with(&["s3util", "head-object", "s3://b/k"], "head-object");
    }

    #[test]
    fn build_config_rejects_put_bucket_policy() {
        // put-bucket-policy needs a second positional (policy file path or `-`);
        // we don't read the file in arg-parsing, so a non-existent path is fine.
        assert_rejects_with(
            &["s3util", "put-bucket-policy", "s3://b", "/tmp/policy.json"],
            "put-bucket-policy",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_tagging() {
        assert_rejects_with(
            &["s3util", "put-bucket-tagging", "--tagging", "k=v", "s3://b"],
            "put-bucket-tagging",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_versioning() {
        assert_rejects_with(
            &["s3util", "put-bucket-versioning", "--enabled", "s3://b"],
            "put-bucket-versioning",
        );
    }

    #[test]
    fn build_config_rejects_put_object_tagging() {
        assert_rejects_with(
            &[
                "s3util",
                "put-object-tagging",
                "--tagging",
                "k=v",
                "s3://b/k",
            ],
            "put-object-tagging",
        );
    }

    #[test]
    fn build_config_rejects_rm() {
        assert_rejects_with(&["s3util", "rm", "s3://b/k"], "rm");
    }

    // The remaining bucket-config CRUD subcommands. Same contract as the
    // tests above: build_config_from_args must reject every thin-wrapper
    // subcommand with a message naming the subcommand and pointing at
    // main.rs. put-* commands take a JSON-file positional that is not read
    // at parse time, so a placeholder path is fine.

    #[test]
    fn build_config_rejects_delete_bucket_cors() {
        assert_rejects_with(
            &["s3util", "delete-bucket-cors", "s3://b"],
            "delete-bucket-cors",
        );
    }

    #[test]
    fn build_config_rejects_delete_bucket_encryption() {
        assert_rejects_with(
            &["s3util", "delete-bucket-encryption", "s3://b"],
            "delete-bucket-encryption",
        );
    }

    #[test]
    fn build_config_rejects_delete_bucket_lifecycle_configuration() {
        assert_rejects_with(
            &["s3util", "delete-bucket-lifecycle-configuration", "s3://b"],
            "delete-bucket-lifecycle-configuration",
        );
    }

    #[test]
    fn build_config_rejects_delete_bucket_website() {
        assert_rejects_with(
            &["s3util", "delete-bucket-website", "s3://b"],
            "delete-bucket-website",
        );
    }

    #[test]
    fn build_config_rejects_delete_public_access_block() {
        assert_rejects_with(
            &["s3util", "delete-public-access-block", "s3://b"],
            "delete-public-access-block",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_cors() {
        assert_rejects_with(&["s3util", "get-bucket-cors", "s3://b"], "get-bucket-cors");
    }

    #[test]
    fn build_config_rejects_get_bucket_encryption() {
        assert_rejects_with(
            &["s3util", "get-bucket-encryption", "s3://b"],
            "get-bucket-encryption",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_lifecycle_configuration() {
        assert_rejects_with(
            &["s3util", "get-bucket-lifecycle-configuration", "s3://b"],
            "get-bucket-lifecycle-configuration",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_logging() {
        assert_rejects_with(
            &["s3util", "get-bucket-logging", "s3://b"],
            "get-bucket-logging",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_notification_configuration() {
        assert_rejects_with(
            &["s3util", "get-bucket-notification-configuration", "s3://b"],
            "get-bucket-notification-configuration",
        );
    }

    #[test]
    fn build_config_rejects_get_bucket_website() {
        assert_rejects_with(
            &["s3util", "get-bucket-website", "s3://b"],
            "get-bucket-website",
        );
    }

    #[test]
    fn build_config_rejects_get_public_access_block() {
        assert_rejects_with(
            &["s3util", "get-public-access-block", "s3://b"],
            "get-public-access-block",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_cors() {
        assert_rejects_with(
            &["s3util", "put-bucket-cors", "s3://b", "/tmp/cors.json"],
            "put-bucket-cors",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_encryption() {
        assert_rejects_with(
            &[
                "s3util",
                "put-bucket-encryption",
                "s3://b",
                "/tmp/encryption.json",
            ],
            "put-bucket-encryption",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_lifecycle_configuration() {
        assert_rejects_with(
            &[
                "s3util",
                "put-bucket-lifecycle-configuration",
                "s3://b",
                "/tmp/lifecycle.json",
            ],
            "put-bucket-lifecycle-configuration",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_logging() {
        assert_rejects_with(
            &[
                "s3util",
                "put-bucket-logging",
                "s3://b",
                "/tmp/logging.json",
            ],
            "put-bucket-logging",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_notification_configuration() {
        assert_rejects_with(
            &[
                "s3util",
                "put-bucket-notification-configuration",
                "s3://b",
                "/tmp/notification.json",
            ],
            "put-bucket-notification-configuration",
        );
    }

    #[test]
    fn build_config_rejects_put_bucket_website() {
        assert_rejects_with(
            &[
                "s3util",
                "put-bucket-website",
                "s3://b",
                "/tmp/website.json",
            ],
            "put-bucket-website",
        );
    }

    #[test]
    fn build_config_rejects_put_public_access_block() {
        assert_rejects_with(
            &[
                "s3util",
                "put-public-access-block",
                "s3://b",
                "/tmp/pab.json",
            ],
            "put-public-access-block",
        );
    }

    // -----------------------------------------------------------------
    // dry-run plumbing: build_config_from_common must (a) propagate the
    // dry_run bool into Config and (b) force tracing_level to >= Info so
    // the [dry-run] log line is visible at the default WarnLevel. The
    // bump must NOT downgrade levels already above info (debug/trace).
    // -----------------------------------------------------------------

    fn cp_config(extra: &[&str]) -> Config {
        let cli =
            parse_from_args(args_with_extra("/tmp/source.txt", "s3://bucket/key", extra)).unwrap();
        let Commands::Cp(cp_args) = cli.command else {
            panic!("expected Cp variant");
        };
        Config::try_from(cp_args).unwrap()
    }

    #[test]
    fn cp_dry_run_false_leaves_tracing_at_warn() {
        let config = cp_config(&[]);
        assert!(!config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Warn
        );
    }

    #[test]
    fn cp_dry_run_true_bumps_warn_to_info() {
        let config = cp_config(&["--dry-run"]);
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Info
        );
    }

    #[test]
    fn cp_dry_run_true_keeps_explicit_info() {
        let config = cp_config(&["--dry-run", "-v"]);
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Info
        );
    }

    #[test]
    fn cp_dry_run_true_preserves_debug() {
        let config = cp_config(&["--dry-run", "-vv"]);
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Debug,
            "debug must not be downgraded to info"
        );
    }

    #[test]
    fn cp_dry_run_true_preserves_trace() {
        let config = cp_config(&["--dry-run", "-vvv"]);
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Trace,
            "trace must not be downgraded to info"
        );
    }

    #[test]
    fn cp_dry_run_true_bumps_error_to_info() {
        let config = cp_config(&["--dry-run", "-q"]);
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Info
        );
    }

    #[test]
    fn cp_dry_run_true_overrides_silenced_qqq() {
        // Without --dry-run, -qqq returns None (no global subscriber). With
        // --dry-run, the bump produces Some(Info) so the [dry-run] line
        // remains visible even when the user asked for full silence.
        let config = cp_config(&["--dry-run", "-qqq"]);
        assert!(config.dry_run);
        let tc = config.tracing_config.expect("dry_run must override -qqq");
        assert_eq!(tc.tracing_level, log::Level::Info);
    }

    #[test]
    fn cp_dry_run_false_silenced_returns_none() {
        let config = cp_config(&["-qqq"]);
        assert!(!config.dry_run);
        assert!(
            config.tracing_config.is_none(),
            "without dry_run, -qqq must still silence tracing"
        );
    }

    #[test]
    fn mv_dry_run_propagates_via_build_config() {
        // mv has its own TryFrom that goes through build_config_from_common
        // — confirm dry_run reaches Config on that path too.
        let cli = parse_from_args(vec![
            "s3util".to_string(),
            "mv".to_string(),
            "s3://src/k".to_string(),
            "s3://dst/k".to_string(),
            "--dry-run".to_string(),
        ])
        .unwrap();
        let Commands::Mv(mv_args) = cli.command else {
            panic!("expected Mv variant");
        };
        let config = Config::try_from(mv_args).unwrap();
        assert!(config.dry_run);
        assert_eq!(
            config.tracing_config.unwrap().tracing_level,
            log::Level::Info
        );
    }

    #[test]
    fn build_config_clap_parse_error_propagates() {
        // A non-existent subcommand is a clap parse error, surfaced as Err
        // via `Cli::try_parse_from(...).map_err(...)?` at the top of
        // build_config_from_args.
        let err = build_config_from_args(
            ["s3util", "no-such-subcommand"]
                .iter()
                .map(|s| s.to_string()),
        )
        .unwrap_err();
        assert!(!err.is_empty());
    }
}
