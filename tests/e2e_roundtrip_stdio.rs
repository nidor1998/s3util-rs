#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    /// Pipe data to stdin → upload to S3 → download via SDK → verify content.
    #[tokio::test]
    async fn stdin_to_s3_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let target = format!("s3://{}/stdin_roundtrip.txt", bucket);
        let test_content = b"stdin roundtrip verification data";

        let mut child = std::process::Command::new("cargo")
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
            .spawn()
            .unwrap();

        {
            use std::io::Write;
            let stdin = child.stdin.as_mut().unwrap();
            stdin.write_all(test_content).unwrap();
        }
        // Drop stdin to close the pipe
        child.stdin.take();

        let status = child.wait().unwrap();
        assert!(
            status.success(),
            "s3util cp stdin->s3 exited with {}",
            status
        );

        // Verify content via SDK
        let downloaded = helper
            .get_object_bytes(&bucket, "stdin_roundtrip.txt", None)
            .await;
        assert_eq!(downloaded, test_content);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Upload to S3 → pipe to stdout → verify stdout content.
    #[tokio::test]
    async fn s3_to_stdout_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let test_content = b"stdout roundtrip verification data";
        helper
            .put_object(&bucket, "stdout_roundtrip.txt", test_content.to_vec())
            .await;

        let source = format!("s3://{}/stdout_roundtrip.txt", bucket);

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
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "s3util cp s3->stdout exited with {}",
            output.status
        );
        assert_eq!(output.stdout, test_content);

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
