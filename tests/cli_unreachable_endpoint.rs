//! Process-level CLI tests that drive every wrapper's "unexpected S3 error"
//! path (`HeadError::Other` / `ObjectAnnotationError::Other`) without AWS.
//!
//! Each invocation points the SDK at an unreachable endpoint
//! (`http://127.0.0.1:1` — connect refused immediately), with static dummy
//! credentials so no profile / IMDS lookup happens and `--aws-max-attempts 1`
//! so nothing is retried. A dispatch failure carries no S3 service error code,
//! so `classify_not_found` yields `None` and the wrapper must take its
//! `Other` branch: the CLI has to exit 1 (not 4 — the failure is *not* a
//! NotFound) and stderr has to carry the operation-specific context string
//! attached in `api.rs`, which pins the exact `map_err` closure that ran.
//!
//! The same invocations also exercise the request-parameter plumbing that is
//! only observable against paid AWS features (`--target-request-payer`,
//! `--enable-additional-checksum`, `--source-version-id`): the builder lines
//! run before `send()`, dead endpoint or not.

use std::process::{Command, Stdio};

fn run_with_dead_endpoint(args: &[&str]) -> (Option<i32>, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_s3util"))
        .args(args)
        .args([
            "--target-endpoint-url",
            "http://127.0.0.1:1",
            "--target-access-key",
            "AKIAIOSFODNN7EXAMPLE",
            "--target-secret-access-key",
            "wJalrXUtnFEMIK7MDENGbPxRfiCYEXAMPLEKEY",
            "--target-region",
            "us-east-1",
            "--aws-max-attempts",
            "1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

/// Assert the run failed with exit 1 (generic error, NOT the NotFound exit 4)
/// and that the api.rs context string for the operation made it to stderr.
fn assert_other_error(args: &[&str], expected_context: &str) {
    let (code, stderr) = run_with_dead_endpoint(args);
    assert_eq!(
        code,
        Some(1),
        "expected exit 1 (unexpected-error path) for {args:?}; stderr: {stderr}"
    );
    assert!(
        stderr.contains(expected_context),
        "expected context {expected_context:?} on stderr for {args:?}; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// get-bucket-* wrappers: each has its own `HeadError::Other` closure in
// api.rs and its own `Err(HeadError::Other(e)) => Err(e)` arm in the CLI.
// ---------------------------------------------------------------------------

#[test]
fn get_bucket_tagging_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-tagging", "s3://cov-bucket"],
        "get-bucket-tagging on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_versioning_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-versioning", "s3://cov-bucket"],
        "get-bucket-versioning on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_policy_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-policy", "s3://cov-bucket"],
        "get-bucket-policy on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_policy_status_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-policy-status", "s3://cov-bucket"],
        "get-bucket-policy-status on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_lifecycle_configuration_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-lifecycle-configuration", "s3://cov-bucket"],
        "get-bucket-lifecycle-configuration on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_encryption_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-encryption", "s3://cov-bucket"],
        "get-bucket-encryption on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_cors_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-cors", "s3://cov-bucket"],
        "get-bucket-cors on s3://cov-bucket",
    );
}

#[test]
fn get_public_access_block_unexpected_error_exits_1() {
    assert_other_error(
        &["get-public-access-block", "s3://cov-bucket"],
        "get-public-access-block on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_website_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-website", "s3://cov-bucket"],
        "get-bucket-website on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_logging_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-logging", "s3://cov-bucket"],
        "get-bucket-logging on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_notification_configuration_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-notification-configuration", "s3://cov-bucket"],
        "get-bucket-notification-configuration on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_replication_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-replication", "s3://cov-bucket"],
        "get-bucket-replication on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_accelerate_configuration_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-accelerate-configuration", "s3://cov-bucket"],
        "get-bucket-accelerate-configuration on s3://cov-bucket",
    );
}

#[test]
fn get_bucket_request_payment_unexpected_error_exits_1() {
    assert_other_error(
        &["get-bucket-request-payment", "s3://cov-bucket"],
        "get-bucket-request-payment on s3://cov-bucket",
    );
}

// ---------------------------------------------------------------------------
// Object wrappers. `--target-request-payer` (and `--enable-additional-checksum`
// on head-object, `--source-version-id` on restore-object) additionally walk
// the optional request-builder lines in api.rs before the send fails.
// ---------------------------------------------------------------------------

#[test]
fn get_object_tagging_unexpected_error_exits_1() {
    assert_other_error(
        &[
            "get-object-tagging",
            "s3://cov-bucket/key",
            "--target-request-payer",
        ],
        "get-object-tagging on s3://cov-bucket/key",
    );
}

#[test]
fn put_object_tagging_unexpected_error_exits_1() {
    let (code, stderr) = run_with_dead_endpoint(&[
        "put-object-tagging",
        "s3://cov-bucket/key",
        "--tagging",
        "k=v",
        "--target-request-payer",
    ]);
    assert_eq!(code, Some(1), "expected exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}

#[test]
fn rm_unexpected_error_exits_1() {
    let (code, stderr) =
        run_with_dead_endpoint(&["rm", "s3://cov-bucket/key", "--target-request-payer"]);
    assert_eq!(code, Some(1), "expected exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}

#[test]
fn head_object_with_checksum_and_request_payer_unexpected_error_exits_1() {
    let (code, stderr) = run_with_dead_endpoint(&[
        "head-object",
        "s3://cov-bucket/key",
        "--enable-additional-checksum",
        "--target-request-payer",
    ]);
    assert_eq!(code, Some(1), "expected exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}

#[test]
fn restore_object_with_version_and_request_payer_unexpected_error_exits_1() {
    let (code, stderr) = run_with_dead_endpoint(&[
        "restore-object",
        "s3://cov-bucket/key",
        "--days",
        "1",
        "--source-version-id",
        "dummy-version",
        "--target-request-payer",
    ]);
    assert_eq!(code, Some(1), "expected exit 1; stderr: {stderr}");
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}

// ---------------------------------------------------------------------------
// Object-annotation wrappers. Every annotation CLI issues its annotation API
// call first (no preliminary HeadObject), so the dead endpoint hits the
// annotation wrapper's own `Other` closure.
// ---------------------------------------------------------------------------

#[test]
fn get_object_annotation_unexpected_error_exits_1() {
    assert_other_error(
        &[
            "get-object-annotation",
            "s3://cov-bucket/key",
            "-",
            "--annotation-name",
            "cov_annotation",
            "--target-request-payer",
        ],
        "get-object-annotation on s3://cov-bucket/key",
    );
}

#[test]
fn put_object_annotation_unexpected_error_exits_1() {
    // --annotation-payload takes a file path; the payload is read before the
    // API call, so it must exist for the request to be attempted at all.
    let payload = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(payload.path(), b"payload").unwrap();
    let payload_path = payload.path().to_string_lossy().to_string();

    assert_other_error(
        &[
            "put-object-annotation",
            "s3://cov-bucket/key",
            "--annotation-name",
            "cov_annotation",
            "--annotation-payload",
            &payload_path,
            "--target-request-payer",
        ],
        "put-object-annotation on s3://cov-bucket/key",
    );
}

#[test]
fn delete_object_annotation_unexpected_error_exits_1() {
    assert_other_error(
        &[
            "delete-object-annotation",
            "s3://cov-bucket/key",
            "--annotation-name",
            "cov_annotation",
            "--target-request-payer",
        ],
        "delete-object-annotation on s3://cov-bucket/key",
    );
}

#[test]
fn list_object_annotations_unexpected_error_exits_1() {
    assert_other_error(
        &[
            "list-object-annotations",
            "s3://cov-bucket/key",
            "--target-request-payer",
        ],
        "list-object-annotations on s3://cov-bucket/key",
    );
}

// ---------------------------------------------------------------------------
// Composite flows that probe with HeadObject/HeadBucket before acting: the
// probe's unexpected failure must abort the command with exit 1, not be
// misread as "does not exist".
// ---------------------------------------------------------------------------

#[test]
fn cp_skip_existing_probe_unexpected_error_exits_1() {
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"data").unwrap();
    let src_path = src.path().to_string_lossy().to_string();

    let (code, stderr) =
        run_with_dead_endpoint(&["cp", &src_path, "s3://cov-bucket/key", "--skip-existing"]);
    assert_eq!(
        code,
        Some(1),
        "a failed --skip-existing probe must abort with exit 1, not skip or copy; stderr: {stderr}"
    );
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}

#[test]
fn create_bucket_if_not_exists_probe_unexpected_error_exits_1() {
    let (code, stderr) =
        run_with_dead_endpoint(&["create-bucket", "s3://cov-bucket", "--if-not-exists"]);
    assert_eq!(
        code,
        Some(1),
        "a failed --if-not-exists probe must abort with exit 1, not fall through to create; stderr: {stderr}"
    );
    assert!(
        stderr.contains("dispatch failure") || stderr.contains("Connection refused"),
        "expected a dispatch failure on stderr; got: {stderr}"
    );
}
