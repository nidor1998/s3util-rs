//! Process-level CLI tests for the `rename` subcommand.
//! These run without AWS credentials or network access.

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

fn run(cmd: &mut Command) -> (bool, String, String, Option<i32>) {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (
        output.status.success(),
        stdout,
        stderr,
        output.status.code(),
    )
}

// Express One Zone bucket name usable in no-AWS tests: passes validate() because
// the bucket ends with --x-s3, and source/target share the same bucket name.
const EXPR_BUCKET: &str = "s3://fake-bucket--apne1-az4--x-s3";

#[test]
fn help_succeeds_and_lists_option_groups() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["rename", "--help"]));
    assert!(ok, "rename --help must succeed");
    assert!(
        stdout.contains("AWS Configuration"),
        "expected 'AWS Configuration' section; got: {stdout}"
    );
    assert!(
        stdout.contains("Conditional Checks"),
        "expected 'Conditional Checks' section; got: {stdout}"
    );
    assert!(
        stdout.contains("--source-if-match"),
        "expected --source-if-match flag; got: {stdout}"
    );
    assert!(
        stdout.contains("--target-if-none-match"),
        "expected --target-if-none-match flag; got: {stdout}"
    );
    assert!(
        stdout.contains("--dry-run"),
        "expected --dry-run flag; got: {stdout}"
    );
}

#[test]
fn missing_args_exits_2() {
    let (ok, _stdout, _stderr, code) = run(s3util().arg("rename"));
    assert!(!ok);
    assert_eq!(code, Some(2), "no-arg rename must exit 2");
}

#[test]
fn missing_target_exits_2() {
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["rename", &format!("{EXPR_BUCKET}/src-key")]));
    assert!(!ok);
    assert_eq!(code, Some(2), "rename with only source must exit 2");
}

#[test]
fn auto_complete_shell_short_circuits_without_positional_args() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["rename", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without source/target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn source_bucket_only_exits_2() {
    // s3://bucket with no key → validate() → source_bucket_key() error → exit 2
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        "s3://fake-bucket--apne1-az4--x-s3",
        &format!("{EXPR_BUCKET}/dst"),
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "bucket-only source must fail validation (exit 2); stderr: {stderr}"
    );
}

#[test]
fn target_bucket_only_exits_2() {
    // source is valid, but target has no key → exit 2
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        &format!("{EXPR_BUCKET}/src"),
        "s3://fake-bucket--apne1-az4--x-s3",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "bucket-only target must fail validation (exit 2); stderr: {stderr}"
    );
}

#[test]
fn non_express_onezone_bucket_exits_2() {
    // Regular (non --x-s3) bucket → NOT_EXPRESS_ONEZONE validation error → exit 2
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        "s3://regular-bucket/src-key",
        "s3://regular-bucket/dst-key",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "non-Express One Zone bucket must fail validation (exit 2); stderr: {stderr}"
    );
    assert!(
        stderr.contains("Express") || stderr.contains("x-s3"),
        "expected Express One Zone error in stderr; got: {stderr}"
    );
}

#[test]
fn different_buckets_exits_2() {
    // Source and target are different Express One Zone buckets → DIFFERENT_BUCKETS → exit 2
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        "s3://bucket-a--apne1-az4--x-s3/src-key",
        "s3://bucket-b--apne1-az4--x-s3/dst-key",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "different-bucket rename must fail validation (exit 2); stderr: {stderr}"
    );
    assert!(
        stderr.contains("same bucket"),
        "expected same-bucket error in stderr; got: {stderr}"
    );
}

#[test]
fn source_access_key_without_secret_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        &format!("{EXPR_BUCKET}/src"),
        &format!("{EXPR_BUCKET}/dst"),
        "--source-access-key",
        "AKIAIOSFODNN7EXAMPLE",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "access-key without secret must exit 2; stderr: {stderr}"
    );
}

#[test]
fn source_profile_conflicts_with_access_key_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "rename",
        &format!("{EXPR_BUCKET}/src"),
        &format!("{EXPR_BUCKET}/dst"),
        "--source-profile",
        "myprofile",
        "--source-access-key",
        "AKID",
        "--source-secret-access-key",
        "SECRET",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "--source-profile conflicts with --source-access-key; stderr: {stderr}"
    );
}
