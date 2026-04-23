#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Baseline cancel test: sends SIGINT to a running `s3util cp` and
    /// asserts the process exits (i.e. does not hang) within `child.wait()`.
    /// Exit status is intentionally not checked here — ctrl-c cancellation
    /// is expected to exit 130 (standard Unix SIGINT convention, 128 + 2).
    /// The exit-code assertion for ctrl-c lives in
    /// `cancel_s3_to_stdout_sigint_exits_130`; the richer orphan-MPU
    /// assertions live in `cancel_multipart_upload_sigint_no_orphan_mpu`
    /// below.
    #[tokio::test]
    async fn cancel_upload_with_sigint_does_not_hang() {
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
                // Throttle to keep the upload running long enough for
                // SIGINT to land. Without this, fast networks finish a
                // 30 MiB upload during the 500ms sleep and the test sees
                // exit 0 instead of the expected cancellation.
                "--rate-limit-bandwidth",
                "2MiB",
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

            // The process must reach exit (not hang). Exit code is not asserted
            // here; ctrl-c exit-code is covered by
            // `cancel_s3_to_stdout_sigint_exits_130`.
            let _status = child.wait().unwrap();
        }

        // Clean up — abort any MPUs that may have been left pending so
        // the bucket can be deleted cleanly.
        helper.abort_all_multipart_uploads(&bucket).await;
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Strengthened cancel test: SIGINTs a multipart upload and verifies
    /// two properties:
    ///   1. The final target object is NOT present in the bucket — i.e.
    ///      cancellation did not race and complete the upload anyway.
    ///   2. No orphan multipart uploads remain in the bucket — s3util-rs
    ///      is expected to abort the MPU on cancel/error via
    ///      `abort_multipart_upload` in `upload_manager.rs`.
    ///
    /// Exit status is not asserted here — ctrl-c cancellation exits 130
    /// (standard Unix SIGINT convention); the exit-code assertion lives in
    /// `cancel_s3_to_stdout_sigint_exits_130`.
    ///
    /// Property (2) is the most load-bearing assertion. If the tool
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
                // Throttle so SIGINT reliably lands mid-stream regardless
                // of network speed: 2 MiB/s on a 30 MiB file gives ~15s of
                // upload window, well beyond the 1500ms sleep below.
                // Without this, fast networks finish the upload before
                // the SIGINT arrives and the test sees exit 0.
                "--rate-limit-bandwidth",
                "2MiB",
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

                // Process must reach exit (not hang). Exit code is not checked
                // here; ctrl-c exit-code is covered by
                // `cancel_s3_to_stdout_sigint_exits_130`.
                let _status = child.wait().unwrap();

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
            // (1) The final object must not be present. S3 multipart
            // upload is atomic: either CompleteMultipartUpload ran
            // (object present at full size) or it didn't (object
            // absent). Any object here means cancellation lost the
            // race to completion.
            assert!(
                !helper.is_object_exist(&bucket, target_key, None).await,
                "cancelled upload left an object in the bucket"
            );

            // (2) No orphan multipart uploads should remain.
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

    /// SIGINTs an in-progress S3→stdout transfer and asserts the process
    /// exits 130 — the standard Unix SIGINT convention (128 + 2), shared
    /// across all five transfer paths (Local→S3, S3→Local, S3→S3, stdin→S3,
    /// S3→stdout).
    ///
    /// Without the cancel check inside `s3_to_stdio`, the read loop would
    /// just `break` on cancellation, fall through to ETag verification over
    /// a truncated body, trigger a mismatch, and the process would either
    /// exit 3 (warning — default verification on) or exit 1 (if the
    /// checksum happens to be full-object). Both are wrong for a ctrl-c.
    ///
    /// Spawns the built binary directly via `CARGO_BIN_EXE_s3util` instead
    /// of `cargo run`: cargo is a middle-man process and sending SIGINT to
    /// `child.id()` hits cargo, not the binary. Cargo's own SIGINT handling
    /// can obscure the binary's exit code (signal-terminated status vs
    /// clean exit 130). Spawning the binary directly makes the assertion
    /// meaningful.
    #[cfg(unix)]
    #[tokio::test]
    async fn cancel_s3_to_stdout_sigint_exits_130() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_file =
            TestHelper::create_sized_file(&local_dir, "cancel_stdout.bin", 30 * 1024 * 1024);
        let s3_path = format!("s3://{}/cancel_stdout.bin", bucket);

        // Seed the bucket.
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                src_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        // Discard stdout — otherwise the body fills the pipe buffer and the
        // child blocks waiting on a reader, which would prevent SIGINT from
        // being observed inside the read loop.
        let bin = env!("CARGO_BIN_EXE_s3util");
        let child = std::process::Command::new(bin)
            .args(["cp", "--source-profile", "s3sync-e2e-test", &s3_path, "-"])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        if let Ok(mut child) = child {
            // Give the binary time to start, authenticate, issue GetObject,
            // and enter the read loop before delivering SIGINT. 1500ms is
            // generous — too short risks pre-loop cancel, which passes
            // trivially and doesn't exercise the fix.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

            use nix::sys::signal::{Signal, kill};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(child.id() as i32);
            let _ = kill(pid, Signal::SIGINT);

            let status = child.wait().unwrap();
            assert_eq!(
                status.code(),
                Some(130),
                "ctrl-c during s3→stdout must exit 130; status={status:?}, code={:?}",
                status.code(),
            );
        }

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
