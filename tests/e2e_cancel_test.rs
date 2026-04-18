#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Baseline cancel test: sends SIGINT to a running `s3util cp` and
    /// asserts the process exits with a non-zero status (i.e. does not
    /// hang or report success). The richer orphan-MPU assertions live in
    /// `cancel_multipart_upload_sigint_no_orphan_mpu` below.
    #[tokio::test]
    async fn cancel_upload_with_sigint_exit_nonzero() {
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
            // After SIGINT, the process must exit non-successfully.
            // (`code().is_some()` alone would also accept exit 0, so we
            // require either non-success or a signal exit.)
            assert!(
                !status.success(),
                "process exited successfully after SIGINT: {status:?}"
            );
        }

        // Clean up — abort any MPUs that may have been left pending so
        // the bucket can be deleted cleanly.
        helper.abort_all_multipart_uploads(&bucket).await;
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Strengthened cancel test: SIGINTs a multipart upload and verifies
    /// three properties:
    ///   1. The process exits non-successfully (interrupted or error).
    ///   2. The final target object is NOT present in the bucket (or, if
    ///      present, its size is 0) — i.e. cancellation did not race and
    ///      complete the upload anyway.
    ///   3. No orphan multipart uploads remain in the bucket — s3util-rs
    ///      is expected to abort the MPU on cancel/error via
    ///      `abort_multipart_upload` in `upload_manager.rs`.
    ///
    /// Property (3) is the most load-bearing assertion. If the tool
    /// DOES leak MPUs under SIGINT, this test will fail — in that case
    /// investigate the abort logic rather than weakening the assertion.
    /// The teardown still aborts any pending MPUs so the bucket can be
    /// deleted.
    #[tokio::test]
    async fn cancel_multipart_upload_sigint_no_orphan_mpu() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // Use a large enough file that the upload takes long enough for
        // SIGINT to land mid-transfer and trigger the multipart path.
        let test_file =
            TestHelper::create_sized_file(&local_dir, "cancel_mpu.bin", 30 * 1024 * 1024);

        let target_key = "cancel_mpu.bin";
        let target = format!("s3://{}/{}", bucket, target_key);

        let child = std::process::Command::new("cargo")
            .args([
                "run",
                "--",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                // Small chunksize+threshold guarantees the upload uses the
                // multipart code path (which is where the abort logic
                // lives).
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        #[cfg(unix)]
        let cancelled = {
            let mut delivered = false;
            if let Ok(mut child) = child {
                // Give it a moment to start and begin the MPU (create
                // multipart upload + start uploading parts).
                tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

                use nix::sys::signal::{Signal, kill};
                use nix::unistd::Pid;
                let pid = Pid::from_raw(child.id() as i32);
                let _ = kill(pid, Signal::SIGINT);
                delivered = true;

                let status = child.wait().unwrap();

                // (1) Non-successful exit after SIGINT.
                assert!(
                    !status.success(),
                    "process exited successfully after SIGINT: {status:?}"
                );

                // Give S3 a brief moment to reflect AbortMultipartUpload
                // completing on the server side.
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            delivered
        };

        #[cfg(not(unix))]
        let cancelled = {
            // On non-unix platforms we cannot deliver SIGINT; still spin
            // the child down politely if we managed to spawn it.
            if let Ok(mut child) = child {
                let _ = child.kill();
                let _ = child.wait();
            }
            false
        };

        // Only run the bucket-state assertions if we actually delivered a
        // SIGINT — on a platform without unix signals this test becomes a
        // no-op.
        if cancelled {
            // (2) The final object must not be a completed, non-empty
            // upload. It's acceptable for it to be absent or zero-length.
            let exists = helper.is_object_exist(&bucket, target_key, None).await;
            if exists {
                let head = helper.head_object(&bucket, target_key, None).await;
                let content_length = head.content_length().unwrap_or(0);
                assert_eq!(
                    content_length, 0,
                    "cancelled upload left a non-empty object of {content_length} bytes in the bucket"
                );
            }

            // (3) No orphan multipart uploads should remain.
            let mpu_count = helper.count_multipart_uploads(&bucket).await;
            assert_eq!(
                mpu_count, 0,
                "expected no orphan multipart uploads after SIGINT, found {mpu_count}"
            );
        }

        // Teardown — abort any pending MPUs so the bucket is deletable
        // even if an earlier assertion failed (best-effort).
        helper.abort_all_multipart_uploads(&bucket).await;
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
