#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;
    use uuid::Uuid;

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

            assert!(helper.is_object_exist(&bucket, "stdin_test.txt", None).await);
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
        helper.put_object(&bucket, "stdout_test.txt", test_content.to_vec()).await;

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
}
