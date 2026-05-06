//! Process-level CLI tests for the `get-bucket-request-payment` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["get-bucket-request-payment", "--help"]));
    assert!(ok, "get-bucket-request-payment --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("get-bucket-request-payment"));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap missing-arg should exit 2; stderr: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "get-bucket-request-payment",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn bucket_with_key_exits_1() {
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["get-bucket-request-payment", "s3://example/key"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "bucket path with key should exit 1 (validation)"
    );
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "get-bucket-request-payment",
        "s3://example",
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap missing-arg should exit 2; stderr: {stderr}"
    );
}
