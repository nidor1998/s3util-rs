#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Test uploading from stdin to S3 using a child process.
    #[tokio::test]
    async fn stdin_to_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let target = format!("s3://{}/stdin_test.txt", bucket);

        // Use a child process to pipe data through s3util
        let child = std::process::Command::new("cargo")
            .args([
                "run",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        if let Ok(mut child) = child {
            if let Some(ref mut stdin) = child.stdin {
                use std::io::Write;
                let _ = stdin.write_all(b"stdin data for s3");
            }
            // Drop stdin to close the pipe
            child.stdin.take();
            let status = child.wait().unwrap();
            assert!(status.success() || status.code() == Some(0));

            assert!(
                helper
                    .is_object_exist(&bucket, "stdin_test.txt", None)
                    .await
            );
        }

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Test downloading from S3 to stdout using a child process.
    #[tokio::test]
    async fn s3_to_stdout() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let test_content = b"content for stdout test";
        helper
            .put_object(&bucket, "stdout_test.txt", test_content.to_vec())
            .await;

        let source = format!("s3://{}/stdout_test.txt", bucket);

        let output = std::process::Command::new("cargo")
            .args([
                "run",
                "--",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                "-",
            ])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .output();

        if let Ok(output) = output {
            assert!(output.status.success());
            assert_eq!(output.stdout, test_content);
        }

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// In-process smoke: verifies `cp_test_data_stdin_to_s3` works end-to-end
    /// with StatsCount collection. Does not assert any checksum/ETag — that is
    /// covered by `e2e_stdio_integrity_check.rs`.
    #[tokio::test]
    async fn stdin_to_s3_basic_smoke() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let target = format!("s3://{}/smoke.txt", bucket);
        let stdin_bytes = b"stdin smoke test".to_vec();

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert!(helper.is_object_exist(&bucket, "smoke.txt", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// In-process smoke: verifies `cp_test_data_s3_to_stdout` captures stdout
    /// and returns StatsCount.
    #[tokio::test]
    async fn s3_to_stdout_basic_smoke() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let test_content = b"stdout smoke test";
        helper
            .put_object(&bucket, "smoke.txt", test_content.to_vec())
            .await;

        let source = format!("s3://{}/smoke.txt", bucket);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stdout_bytes, test_content);

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
