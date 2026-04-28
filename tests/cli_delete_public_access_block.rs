//! Process-level CLI tests for the `delete-public-access-block` subcommand.
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

#[test]
fn help_succeeds_and_lists_option_groups() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["delete-public-access-block", "--help"]));
    assert!(ok);
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("delete-public-access-block"));
    assert!(!ok);
    assert_eq!(code, Some(2), "stderr: {stderr}");
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "delete-public-access-block",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(ok);
    assert!(stdout.contains("_s3util"));
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "delete-public-access-block",
        "s3://example",
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key"),
        "expected clap error; got: {stderr}"
    );
}

#[test]
fn target_no_sign_request_conflicts_with_target_profile() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "delete-public-access-block",
        "s3://example",
        "--target-no-sign-request",
        "--target-profile",
        "default",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("cannot be used")
            || stderr.to_lowercase().contains("conflict"),
        "expected clap conflict; got: {stderr}"
    );
}

#[test]
fn bucket_with_key_exits_1() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["delete-public-access-block", "s3://example/key"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "bucket path with key should exit 1 (validation)"
    );
    assert!(
        !stderr.is_empty(),
        "should have an error message on stderr; got empty"
    );
}
