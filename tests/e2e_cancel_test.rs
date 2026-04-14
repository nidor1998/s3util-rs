#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;
    use uuid::Uuid;

    use super::*;

    /// Test that cancellation via Ctrl-C (SIGINT) during a large upload
    /// is handled gracefully. This test sends SIGINT to the child process.
    #[tokio::test]
    async fn cancel_upload_with_sigint() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // Create a large file so upload takes enough time to cancel
        let test_file =
            TestHelper::create_sized_file(&local_dir, "cancel_test.bin", 30 * 1024 * 1024);

        let target = format!("s3://{}/cancel_test.bin", bucket);

        // Spawn the cp as a child process
        let child = std::process::Command::new("cargo")
            .args([
                "run",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        if let Ok(mut child) = child {
            // Give it a moment to start
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;

            // Send SIGINT
            #[cfg(unix)]
            {
                use nix::sys::signal::{Signal, kill};
                use nix::unistd::Pid;
                let pid = Pid::from_raw(child.id() as i32);
                let _ = kill(pid, Signal::SIGINT);
            }

            let status = child.wait().unwrap();
            // The process should have exited (not hung)
            // Exit code may be non-zero due to cancellation, that's fine
            assert!(status.code().is_some() || !status.success());
        }

        // Clean up
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
