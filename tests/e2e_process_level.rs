//! Process-level happy-path e2e tests.
//!
//! These tests invoke the real `s3util` binary via `env!("CARGO_BIN_EXE_s3util")`
//! as a subprocess and assert that the process exit code is 0. They cover:
//! - The four transfer directions S3ToLocal, S3ToS3, StdioToS3, and S3ToStdio,
//!   across every valid `--source-request-payer` / `--target-request-payer`
//!   flag combination.
//! - The `--show-progress` indicator, which prints a one-line summary to stderr
//!   on success.
//! - The `--disable-payload-signing` flag on a LocalToS3 upload.
//!
//! Gated by `cfg(e2e_test)` because they hit real AWS (the user runs e2e tests).

#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::io::Write;
    use std::process::{Command, Output, Stdio};

    const EXIT_CODE_SUCCESS: i32 = 0;

    /// Run the compiled `s3util` binary with the given args and no stdin.
    /// Stdout and stderr are piped so they can be embedded in assertion
    /// messages and, for S3ToStdio, inspected by the caller.
    fn run_s3util(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap()
    }

    /// Run the compiled `s3util` binary with the given args, piping
    /// `stdin_bytes` into the child's stdin. Stdin is closed after the
    /// write so the binary sees EOF. Stdout and stderr are piped so they
    /// can be embedded in assertion messages.
    fn run_s3util_with_stdin(args: &[&str], stdin_bytes: &[u8]) -> Output {
        let mut child = Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        {
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(stdin_bytes).unwrap();
        }
        // Drop stdin to close the pipe so the child sees EOF.
        drop(child.stdin.take());

        child.wait_with_output().unwrap()
    }

    // ---------------------------------------------------------------
    // S3ToLocal (2)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_local_process_level() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "s3_to_local.txt";
        let body = b"s3_to_local body".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join("s3_to_local.txt");
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_local_process_level must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        let downloaded = std::fs::read(&dst).unwrap();
        assert_eq!(downloaded, body);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_process_level_with_source_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --source-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "s3_to_local_rp.txt";
        let body = b"s3_to_local rp body".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join("s3_to_local_rp.txt");
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--source-request-payer",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_local_process_level_with_source_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        let downloaded = std::fs::read(&dst).unwrap();
        assert_eq!(downloaded, body);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // S3ToS3 (4)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_s3_process_level() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let source_bucket = TestHelper::generate_bucket_name();
        let target_bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&source_bucket, REGION).await;
        helper.create_bucket(&target_bucket, REGION).await;

        let key = "s3_to_s3.txt";
        let body = b"s3_to_s3 body".to_vec();
        helper.put_object(&source_bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", source_bucket, key);
        let target = format!("s3://{}/{}", target_bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_s3_process_level must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&target_bucket, key, None).await);
        let fetched = helper.get_object_bytes(&target_bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&source_bucket).await;
        helper.delete_bucket_with_cascade(&target_bucket).await;
    }

    #[tokio::test]
    async fn s3_to_s3_process_level_with_source_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --source-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let source_bucket = TestHelper::generate_bucket_name();
        let target_bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&source_bucket, REGION).await;
        helper.create_bucket(&target_bucket, REGION).await;

        let key = "s3_to_s3_src_rp.txt";
        let body = b"s3_to_s3 src rp body".to_vec();
        helper.put_object(&source_bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", source_bucket, key);
        let target = format!("s3://{}/{}", target_bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-request-payer",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_s3_process_level_with_source_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&target_bucket, key, None).await);
        let fetched = helper.get_object_bytes(&target_bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&source_bucket).await;
        helper.delete_bucket_with_cascade(&target_bucket).await;
    }

    #[tokio::test]
    async fn s3_to_s3_process_level_with_target_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --target-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let source_bucket = TestHelper::generate_bucket_name();
        let target_bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&source_bucket, REGION).await;
        helper.create_bucket(&target_bucket, REGION).await;

        let key = "s3_to_s3_tgt_rp.txt";
        let body = b"s3_to_s3 tgt rp body".to_vec();
        helper.put_object(&source_bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", source_bucket, key);
        let target = format!("s3://{}/{}", target_bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-request-payer",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_s3_process_level_with_target_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&target_bucket, key, None).await);
        let fetched = helper.get_object_bytes(&target_bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&source_bucket).await;
        helper.delete_bucket_with_cascade(&target_bucket).await;
    }

    #[tokio::test]
    async fn s3_to_s3_process_level_with_both_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --source-request-payer / --target-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let source_bucket = TestHelper::generate_bucket_name();
        let target_bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&source_bucket, REGION).await;
        helper.create_bucket(&target_bucket, REGION).await;

        let key = "s3_to_s3_both_rp.txt";
        let body = b"s3_to_s3 both rp body".to_vec();
        helper.put_object(&source_bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", source_bucket, key);
        let target = format!("s3://{}/{}", target_bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-request-payer",
            "--target-request-payer",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_s3_process_level_with_both_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&target_bucket, key, None).await);
        let fetched = helper.get_object_bytes(&target_bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&source_bucket).await;
        helper.delete_bucket_with_cascade(&target_bucket).await;
    }

    // ---------------------------------------------------------------
    // StdioToS3 (2)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdio_to_s3_process_level() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "stdio_to_s3.txt";
        let body = b"stdio_to_s3 body".to_vec();
        let target = format!("s3://{}/{}", bucket, key);

        let output = run_s3util_with_stdin(
            &["cp", "--target-profile", "s3sync-e2e-test", "-", &target],
            &body,
        );

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "stdio_to_s3_process_level must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&bucket, key, None).await);
        let fetched = helper.get_object_bytes(&bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdio_to_s3_process_level_with_target_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --target-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "stdio_to_s3_rp.txt";
        let body = b"stdio_to_s3 rp body".to_vec();
        let target = format!("s3://{}/{}", bucket, key);

        let output = run_s3util_with_stdin(
            &[
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-request-payer",
                "-",
                &target,
            ],
            &body,
        );

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "stdio_to_s3_process_level_with_target_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&bucket, key, None).await);
        let fetched = helper.get_object_bytes(&bucket, key, None).await;
        assert_eq!(fetched, body);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // S3ToStdio (2)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_stdio_process_level() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "s3_to_stdio.txt";
        let body = b"s3_to_stdio body".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&["cp", "--source-profile", "s3sync-e2e-test", &source, "-"]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_stdio_process_level must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert_eq!(output.stdout, body);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn s3_to_stdio_process_level_with_source_request_payer() {
        // Limitation: Without a Requester-Pays bucket, we can only verify that the
        // CLI accepts --source-request-payer and exits 0.
        // On a non-requester-pays bucket the header is ignored server-side.
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "s3_to_stdio_rp.txt";
        let body = b"s3_to_stdio rp body".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--source-request-payer",
            &source,
            "-",
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "s3_to_stdio_process_level_with_source_request_payer must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert_eq!(output.stdout, body);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Indicator
    // ---------------------------------------------------------------

    /// `--show-progress` prints a final summary line to stderr on success.
    /// Uses LocalToS3 as the simplest direction that can exercise the
    /// indicator path (no stdin/stdout piping to interfere with stderr
    /// capture).
    #[tokio::test]
    async fn show_indicator_emits_summary() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "indicator.txt", b"indicator body");
        let target = format!("s3://{}/indicator.txt", bucket);

        let output = run_s3util(&[
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--show-progress",
            test_file.to_str().unwrap(),
            &target,
        ]);

        let stderr = String::from_utf8_lossy(&output.stderr);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "show_indicator_emits_summary must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            stderr,
        );

        // The indicator's final summary line contains throughput (e.g.
        // "1.00 KB | 5.00 KB/sec") and "etag verify: <status>".
        assert!(
            stderr.contains("/sec"),
            "expected indicator summary with '/sec' on stderr, got: {stderr}"
        );
        assert!(
            stderr.contains("etag verify:"),
            "expected indicator summary with 'etag verify:' on stderr, got: {stderr}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Disable payload signing
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn disable_payload_signing_process_level() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let body = b"disable payload signing body";
        let test_file =
            TestHelper::create_test_file(&local_dir, "disable_payload_signing.txt", body);
        let key = "disable_payload_signing.txt";
        let target = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--disable-payload-signing",
            test_file.to_str().unwrap(),
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "disable_payload_signing_process_level must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        assert!(helper.is_object_exist(&bucket, key, None).await);
        let fetched = helper.get_object_bytes(&bucket, key, None).await;
        assert_eq!(fetched, body.to_vec());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
