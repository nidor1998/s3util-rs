#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    // Process exit codes are defined in src/bin/s3util/cli/mod.rs:
    //   EXIT_CODE_SUCCESS = 0
    //   EXIT_CODE_ERROR   = 1
    //   EXIT_CODE_WARNING = 3
    //
    // These tests invoke the actual binary as a subprocess and assert the
    // process-level exit code. They are the only tests that exercise
    // src/bin/s3util/main.rs's exit-code mapping end to end.

    const EXIT_CODE_SUCCESS: i32 = 0;
    const EXIT_CODE_ERROR: i32 = 1;
    const EXIT_CODE_WARNING: i32 = 3;

    /// Successful local→S3 cp must exit 0.
    #[tokio::test]
    async fn exit_code_success_on_normal_cp() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "ok.bin", 1024);
        let target = format!("s3://{}/ok.bin", bucket);

        let status = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        assert_eq!(
            status.code(),
            Some(EXIT_CODE_SUCCESS),
            "successful cp must exit 0, got: {status}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// cp against a nonexistent bucket must exit 1 (run_cp returns Err).
    #[tokio::test]
    async fn exit_code_error_on_missing_bucket() {
        TestHelper::init_dummy_tracing_subscriber();

        // No bucket creation — the target bucket is intentionally absent. Use a
        // unique name so we don't collide with an existing bucket.
        let bucket = format!("nonexistent-{}", uuid::Uuid::new_v4());
        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "err.bin", 1024);
        let target = format!("s3://{}/err.bin", bucket);

        let status = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        assert_eq!(
            status.code(),
            Some(EXIT_CODE_ERROR),
            "cp to nonexistent bucket must exit 1, got: {status}"
        );

        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // CLI paths in src/bin/s3util/main.rs that don't reach run_cp.
    // These cover the early-return and validation branches that aren't
    // exercised by lib unit tests (they need the actual binary).
    // ---------------------------------------------------------------

    /// `--auto-complete-shell bash` short-circuits before Config::try_from,
    /// generates a shell completion script to stdout, and exits 0.
    /// Covers the early-return branch in main.rs at the `auto_complete_shell`
    /// check.
    #[tokio::test]
    async fn auto_complete_shell_emits_script_and_exits_zero() {
        let output = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                "--auto-complete-shell",
                "bash",
            ])
            .stderr(std::process::Stdio::null())
            .output()
            .unwrap();

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "--auto-complete-shell must exit 0, got: {}",
            output.status
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        // bash completion scripts contain `complete -F <funcname> s3util`.
        assert!(
            stdout.contains("s3util"),
            "expected bash completion output mentioning 's3util', got first 200 chars: {}",
            &stdout.chars().take(200).collect::<String>()
        );
    }

    /// Invalid `--multipart-threshold` value (below the 5 MiB minimum) is
    /// rejected by the value parser, causing clap to exit with its standard
    /// arg-error code (2). Exercises the value-validation branch reached via
    /// `clap::Error::raw(...).exit()` in main.rs.
    #[tokio::test]
    async fn invalid_multipart_threshold_exits_with_clap_error() {
        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "x.bin", 64);

        let status = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                // 1KiB is below the documented 5 MiB minimum → value parser rejects.
                "--multipart-threshold",
                "1KiB",
                test_file.to_str().unwrap(),
                "s3://any-bucket/key",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        // Clap's default exit code for argument-validation errors is 2.
        // We assert "non-zero, non-success, non-warning" rather than exactly 2
        // to stay robust if clap changes its convention.
        let code = status.code();
        assert!(
            !status.success(),
            "invalid arg must not exit success, got: {status}"
        );
        assert_ne!(
            code,
            Some(EXIT_CODE_WARNING),
            "arg-validation error must not be reported as a runtime warning"
        );

        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// A cp that produces a warning (no errors) must exit 3.
    ///
    /// Mirrors the trigger used by `local_to_s3_multipart_e_tag_ng` /
    /// `s3_to_local_multipart_e_tag_ng` in `tests/e2e_integrity_check.rs`:
    /// upload a 9 MiB file with `--multipart-chunksize=5MiB`, then download
    /// without specifying chunksize — the local recompute uses the default
    /// 8 MiB and the resulting ETag won't match the source's stored ETag,
    /// causing the cp to emit a sync_warning and exit 3.
    #[tokio::test]
    async fn exit_code_warning_on_etag_mismatch_after_chunksize_change() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(&local_dir, "warn.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/warn.bin", bucket);

        // Step 1: upload with non-default chunksize so the stored ETag is built
        // from 5 MiB parts.
        let upload_status = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();
        assert_eq!(
            upload_status.code(),
            Some(EXIT_CODE_SUCCESS),
            "warning-test setup upload must succeed first, got: {upload_status}"
        );

        // Step 2: download without chunksize override. Local ETag recompute will
        // use defaults and won't match the stored multipart ETag → warning.
        let dl_file = local_dir.join("warn_dl.bin");
        let dl_status = std::process::Command::new("cargo")
            .args([
                "run",
                "--quiet",
                "--",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                dl_file.to_str().unwrap(),
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap();

        assert_eq!(
            dl_status.code(),
            Some(EXIT_CODE_WARNING),
            "ETag mismatch from chunksize change must exit 3, got: {dl_status}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
