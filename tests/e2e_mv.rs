//! E2E tests for `s3util mv`.
//!
//! These tests hit real AWS and are reserved for the user to run. The
//! executor only compile-checks them (under `RUSTFLAGS="--cfg e2e_test"`)
//! to keep mv's end-to-end behavior covered without spending money or
//! touching production buckets in CI.
//!
//! Pattern: invoke the compiled `s3util` binary directly via
//! `env!("CARGO_BIN_EXE_s3util")` (matching `tests/e2e_process_level.rs`
//! and `tests/e2e_cancel_test.rs`). Process-level invocation is the only
//! way to assert mv's exit-code mapping (success/error/warning/cancelled)
//! together with its source-delete side effect, since `cli::run_mv` lives
//! inside the binary and is not exposed by the library.
//!
//! Coverage:
//! - Direction × delete-correctness for the four mv-supported directions.
//! - Verification gate: warning blocks delete; `--no-fail-on-verify-error`
//!   bypasses the gate.
//! - Versioning: default (latest) capture, explicit `--source-version-id`,
//!   and unversioned bucket.
//! - Failure modes: source 404 keeps source absent and exits 1; delete-
//!   permission failure (marked `#[ignore]`) leaves target present and
//!   source intact.
//! - Cancellation: SIGINT mid-transfer exits 130, source intact.
//! - Argument rejection: stdio source/target rejected at clap with exit 2,
//!   no AWS contact.

#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::process::{Command, Output, Stdio};

    /// Process exit codes are defined in src/bin/s3util/cli/mod.rs:
    ///   EXIT_CODE_SUCCESS   = 0
    ///   EXIT_CODE_ERROR     = 1
    ///   EXIT_CODE_WARNING   = 3
    ///   EXIT_CODE_CANCELLED = 130 (SIGINT/ctrl-c)
    ///
    /// Clap argument errors exit 2 (mapped by `clap::Error::exit`).
    const EXIT_CODE_SUCCESS: i32 = 0;
    const EXIT_CODE_ERROR: i32 = 1;
    #[cfg(unix)]
    const EXIT_CODE_CANCELLED: i32 = 130;
    const EXIT_CODE_CLAP_ARG_ERROR: i32 = 2;

    /// Run the compiled `s3util` binary with the given args. Stdout/stderr
    /// are captured so callers can embed them in assertion messages.
    fn run_s3util(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap()
    }

    // ---------------------------------------------------------------
    // Direction × delete-correctness (4 tests)
    //
    // Each test runs mv in one of the four supported directions and
    // asserts: exit 0, target present with correct content, source
    // absent. Stdio is rejected at config validation and is covered by
    // the argument-rejection tests at the bottom of this file.
    // ---------------------------------------------------------------

    /// LocalToS3: mv must upload the file and unlink the local source.
    #[tokio::test]
    async fn mv_local_to_s3_deletes_local_source() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let body = b"local mv body";
        let test_file = TestHelper::create_test_file(&local_dir, "mv_local.txt", body);
        let target = format!("s3://{}/mv_local.txt", bucket);

        let output = run_s3util(&[
            "mv",
            "--target-profile",
            "s3sync-e2e-test",
            test_file.to_str().unwrap(),
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            !TestHelper::is_file_exist(test_file.to_str().unwrap()),
            "mv must unlink local source"
        );
        assert!(helper.is_object_exist(&bucket, "mv_local.txt", None).await);
        helper
            .verify_object_content_md5(&bucket, "mv_local.txt", body)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// S3ToLocal: mv must download the object and DeleteObject the source.
    #[tokio::test]
    async fn mv_s3_to_local_deletes_s3_source() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "mv_s3l.txt";
        let body = b"s3-to-local mv body".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join(key);
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert_eq!(std::fs::read(&dst).unwrap(), body);
        assert!(
            !helper.is_object_exist(&bucket, key, None).await,
            "mv must DeleteObject the S3 source"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// S3ToS3 download+upload (default): mv must transfer via local pipe and
    /// DeleteObject the source. `--server-side-copy` deliberately NOT set so
    /// the cp pipeline takes the GetObject/PutObject path.
    #[tokio::test]
    async fn mv_s3_to_s3_deletes_s3_source() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "mv_s3s.txt";
        let body = b"s3-to-s3 mv body".to_vec();
        helper.put_object(&bucket1, key, body.clone()).await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);

        let output = run_s3util(&[
            "mv",
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
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(helper.is_object_exist(&bucket2, key, None).await);
        helper.verify_object_content_md5(&bucket2, key, &body).await;
        assert!(
            !helper.is_object_exist(&bucket1, key, None).await,
            "mv must DeleteObject the S3 source"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// S3ToS3 server-side-copy: mv must use CopyObject/UploadPartCopy then
    /// DeleteObject the source.
    #[tokio::test]
    async fn mv_s3_to_s3_server_side_copy_deletes_s3_source() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "mv_ssc.txt";
        let body = b"s3-to-s3 ssc mv body".to_vec();
        helper.put_object(&bucket1, key, body.clone()).await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--server-side-copy",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv --server-side-copy must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(helper.is_object_exist(&bucket2, key, None).await);
        helper.verify_object_content_md5(&bucket2, key, &body).await;
        assert!(
            !helper.is_object_exist(&bucket1, key, None).await,
            "mv --server-side-copy must DeleteObject the S3 source"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    // ---------------------------------------------------------------
    // Verification gate & bypass flag (2 tests)
    //
    // Strategy: seed the source with a 9 MiB single-part object, then
    // run mv across S3→S3 with `--multipart-chunksize 5MiB` so the
    // target is reuploaded as a 2-part multipart object. The source
    // ETag (single-part MD5) won't match the target's multipart ETag
    // (`<md5>-2`), producing a verify warning. cf. `e2e_s3_to_s3.rs`'s
    // `s3_to_s3_large_object` which exercises the same warning shape.
    // ---------------------------------------------------------------

    /// Verify warning without bypass flag must keep the source and exit 1.
    #[tokio::test]
    async fn mv_verify_mismatch_keeps_source_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "mv_verify_fail.bin";
        helper
            .put_sized_object(&bucket1, key, 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--multipart-threshold",
            "5MiB",
            "--multipart-chunksize",
            "5MiB",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_ERROR),
            "verify mismatch without bypass must exit 1; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket1, key, None).await,
            "source must be preserved on verify warning"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Verify warning WITH `--no-fail-on-verify-error` must delete the
    /// source and exit 0.
    #[tokio::test]
    async fn mv_verify_mismatch_with_bypass_flag_deletes_source_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "mv_verify_bypass.bin";
        helper
            .put_sized_object(&bucket1, key, 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            "--multipart-threshold",
            "5MiB",
            "--multipart-chunksize",
            "5MiB",
            "--no-fail-on-verify-error",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "verify mismatch with bypass must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            !helper.is_object_exist(&bucket1, key, None).await,
            "source must be deleted under --no-fail-on-verify-error"
        );
        assert!(helper.is_object_exist(&bucket2, key, None).await);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    // ---------------------------------------------------------------
    // Versioning (3 tests)
    //
    // Versioned-bucket capture path: cp seeds `TransferOutcome.source_version_id`
    // with the version it actually downloaded (the bucket's "latest"); mv
    // uses that captured value for DeleteObject so it deletes exactly the
    // version that was moved, not whatever happens to be latest at delete
    // time. Without that capture, racing puts could delete the wrong version.
    // ---------------------------------------------------------------

    /// Default capture: no `--source-version-id` supplied. cp downloads v2
    /// (latest) and reports it as the captured version. mv must delete v2
    /// specifically, leaving v1 reachable.
    #[tokio::test]
    async fn mv_versioned_bucket_default_captures_version_id_and_deletes_that_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "mv_ver_default.txt";
        let v1_content = b"v1 content".to_vec();
        let v2_content = b"v2 content".to_vec();
        let v1_id = helper
            .put_object_with_version(&bucket, key, v1_content.clone())
            .await;
        let v2_id = helper
            .put_object_with_version(&bucket, key, v2_content.clone())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join(key);
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        // The downloaded file must be v2 (latest at the time of cp).
        assert_eq!(std::fs::read(&dst).unwrap(), v2_content);

        // v1 must still be reachable; v2 must be gone.
        assert!(
            helper
                .is_object_exist(&bucket, key, Some(v1_id.clone()))
                .await,
            "v1 must remain present"
        );
        assert!(
            !helper
                .is_object_exist(&bucket, key, Some(v2_id.clone()))
                .await,
            "v2 must be deleted"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Explicit `--source-version-id` wins over the captured "latest". mv
    /// downloads the requested version and deletes that exact version.
    #[tokio::test]
    async fn mv_versioned_bucket_with_explicit_source_version_id_deletes_that_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "mv_ver_explicit.txt";
        let v1_content = b"v1 explicit".to_vec();
        let v2_content = b"v2 explicit".to_vec();
        let v1_id = helper
            .put_object_with_version(&bucket, key, v1_content.clone())
            .await;
        let v2_id = helper
            .put_object_with_version(&bucket, key, v2_content.clone())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join(key);
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v1_id,
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        // Downloaded file must be v1 (the explicitly requested version).
        assert_eq!(std::fs::read(&dst).unwrap(), v1_content);

        // v1 must be gone; v2 must remain.
        assert!(
            !helper.is_object_exist(&bucket, key, Some(v1_id)).await,
            "v1 must be deleted"
        );
        assert!(
            helper.is_object_exist(&bucket, key, Some(v2_id)).await,
            "v2 must remain present"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Unversioned bucket: DeleteObject without a version-id removes the
    /// object entirely (no delete marker, no surviving version).
    #[tokio::test]
    async fn mv_unversioned_bucket_deletes_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "mv_unversioned.txt";
        let body = b"unversioned mv".to_vec();
        helper.put_object(&bucket, key, body.clone()).await;

        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join(key);
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "mv must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert_eq!(std::fs::read(&dst).unwrap(), body);
        assert!(
            !helper.is_object_exist(&bucket, key, None).await,
            "unversioned source object must be gone after mv"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Failure modes (2 tests)
    // ---------------------------------------------------------------

    /// cp returns 404 (source object doesn't exist). mv must exit 1
    /// without any DeleteObject call. Source absence is the natural
    /// state — there's nothing to verify post-condition beyond the exit
    /// code, but we still assert it because a regression that calls
    /// DeleteObject on a 404'd source would silently mask the bug.
    #[tokio::test]
    async fn mv_cp_404_keeps_source_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "mv_missing.txt";
        // Deliberately do NOT put the object — cp must 404.
        let local_dir = TestHelper::create_temp_dir();
        let dst = local_dir.join(key);
        let source = format!("s3://{}/{}", bucket, key);

        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            dst.to_str().unwrap(),
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_ERROR),
            "mv on 404 source must exit 1; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        // Source remains absent (it never existed). The post-condition
        // we care about is that mv didn't somehow create then delete it —
        // any reachable bug would surface a different exit code, but we
        // double-check explicitly.
        assert!(!helper.is_object_exist(&bucket, key, None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// cp succeeds but DeleteObject on the source fails (e.g. IAM permits
    /// PutObject on the target but not DeleteObject on the source). mv must
    /// exit 1, target present, source intact.
    ///
    /// Marked `#[ignore]`: the existing test infrastructure uses a single
    /// IAM identity (`s3sync-e2e-test`) that can both put and delete in
    /// every test bucket. Reproducing this case requires either:
    ///   (a) a second AWS profile scoped down to PutObject-only on a
    ///       dedicated source bucket, or
    ///   (b) a bucket policy that denies `s3:DeleteObject` for the test
    ///       principal on a specific source bucket.
    /// Neither is wired into `tests/common/mod.rs` today. Run manually with:
    ///   1. Configure profile `s3util-mv-noperm` with PutObject only.
    ///   2. Apply a Deny-DeleteObject bucket policy to the source bucket.
    ///   3. `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_mv \
    ///       mv_delete_failure_after_successful_cp_exits_1 -- --ignored`
    #[tokio::test]
    #[ignore = "requires per-test IAM with PutObject but not DeleteObject; see test docstring"]
    async fn mv_delete_failure_after_successful_cp_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "mv_no_delete.txt";
        let body = b"mv no-delete-perm".to_vec();
        helper.put_object(&bucket1, key, body.clone()).await;

        // The user is expected to apply a Deny-DeleteObject policy on
        // bucket1 for the test principal before running this test
        // manually. The cp will succeed; the post-cp DeleteObject will
        // fail with AccessDenied; mv exits 1.
        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);
        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3sync-e2e-test",
            "--target-profile",
            "s3sync-e2e-test",
            &source,
            &target,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_ERROR),
            "mv with delete-permission failure must exit 1; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket2, key, None).await,
            "target must be present (cp succeeded before delete failed)"
        );
        assert!(
            helper.is_object_exist(&bucket1, key, None).await,
            "source must remain (DeleteObject failed)"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    // ---------------------------------------------------------------
    // Cancellation (1 test)
    //
    // Pattern adapted from `e2e_cancel_test.rs::cancel_s3_to_stdout_sigint_exits_130`:
    // spawn the binary directly (NOT via `cargo run`), let it begin the
    // upload, deliver SIGINT, then assert exit 130 and source intact.
    // SIGINT before completion runs run_copy_phase's gate-1 cancelled
    // branch which short-circuits before the DeleteObject call.
    // ---------------------------------------------------------------

    /// Ctrl-C mid-transfer: process exits 130, source is not deleted.
    /// Throttled to 2 MiB/s so SIGINT reliably lands mid-stream regardless
    /// of network speed.
    #[cfg(unix)]
    #[tokio::test]
    async fn mv_ctrl_c_during_transfer_keeps_source_exits_130() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "mv_cancel.bin";
        // Use a sized file large enough that the upload window outlasts
        // the post-spawn sleep at 2 MiB/s — a 30 MiB file gives ~15s of
        // streaming, well beyond the 1500ms wait.
        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, key, 30 * 1024 * 1024);
        let target = format!("s3://{}/{}", bucket, key);

        // Spawn the built binary directly (cargo run is a middle-man and
        // SIGINT to the cargo PID does not reliably propagate the binary's
        // own exit code; cf. cancel_s3_to_stdout_sigint_exits_130).
        let bin = env!("CARGO_BIN_EXE_s3util");
        let child = std::process::Command::new(bin)
            .args([
                "mv",
                "--target-profile",
                "s3sync-e2e-test",
                "--rate-limit-bandwidth",
                "2MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn();

        if let Ok(mut child) = child {
            // Give it time to start, authenticate, and enter the upload
            // loop before delivering SIGINT.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;

            use nix::sys::signal::{Signal, kill};
            use nix::unistd::Pid;
            let pid = Pid::from_raw(child.id() as i32);
            let _ = kill(pid, Signal::SIGINT);

            let status = child.wait().unwrap();
            assert_eq!(
                status.code(),
                Some(EXIT_CODE_CANCELLED),
                "ctrl-c during mv must exit 130; status={status:?}, code={:?}",
                status.code(),
            );

            // Local source must still exist — gate 1 (cancelled) short-
            // circuits before the source-delete step.
            assert!(
                TestHelper::is_file_exist(test_file.to_str().unwrap()),
                "local source must be intact after ctrl-c"
            );
        }

        // Teardown — abort any MPUs left pending, delete bucket cleanly.
        helper.abort_all_multipart_uploads(&bucket).await;
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Argument rejection — no AWS contact (2 tests)
    //
    // mv rejects stdio at config validation (see src/config/args/mv.rs's
    // STDIO_NOT_SUPPORTED). Clap maps `Config::try_from` errors to
    // `ErrorKind::ValueValidation`, which exits 2.
    // ---------------------------------------------------------------

    /// `s3util mv - s3://b/k` — stdio source is rejected at clap exit 2.
    #[tokio::test]
    async fn mv_stdio_source_exits_2() {
        // No bucket creation — clap rejects before any AWS call.
        let output = run_s3util(&["mv", "-", "s3://nonexistent-bucket/key"]);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_CLAP_ARG_ERROR),
            "mv with stdin source must exit 2 (clap arg error); stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// `s3util mv s3://b/k -` — stdio target is rejected at clap exit 2.
    #[tokio::test]
    async fn mv_stdio_target_exits_2() {
        // No bucket creation — clap rejects before any AWS call.
        let output = run_s3util(&["mv", "s3://nonexistent-bucket/key", "-"]);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_CLAP_ARG_ERROR),
            "mv with stdout target must exit 2 (clap arg error); stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
