//! Process-level CLI tests for the `put-bucket-request-payment` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["put-bucket-request-payment", "--help"]));
    assert!(ok, "put-bucket-request-payment --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
}

#[test]
fn missing_both_state_flags_exits_2() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["put-bucket-request-payment", "s3://example"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "missing state flags should exit 2; got {code:?}; stderr: {stderr}"
    );
}

#[test]
fn both_flags_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-request-payment",
        "s3://example",
        "--requester",
        "--bucket-owner",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "conflicting flags should exit 2; got {code:?}; stderr: {stderr}"
    );
}

#[test]
fn requester_alone_with_valid_bucket_parses_ok() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["put-bucket-request-payment", "s3://example", "--requester"]));
    assert!(
        code != Some(2),
        "--requester alone must parse without clap error; code={code:?}; stderr={stderr}"
    );
    let _ = ok;
}

#[test]
fn auto_complete_shell_short_circuits_without_target_or_state_flags() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "put-bucket-request-payment",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(
        ok,
        "auto-complete-shell must succeed without a target or state flags"
    );
    assert!(stdout.contains("_s3util"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["put-bucket-request-payment", "--requester"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "missing target should exit 2; stderr: {stderr}"
    );
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-request-payment",
        "s3://example",
        "--requester",
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

#[test]
fn bucket_owner_alone_with_valid_bucket_parses_ok() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-request-payment",
        "s3://example",
        "--bucket-owner",
    ]));
    assert!(
        code != Some(2),
        "--bucket-owner alone must parse without clap error; code={code:?}; stderr={stderr}"
    );
    let _ = ok;
}

#[test]
fn bucket_with_key_exits_1() {
    let (ok, _stdout, _stderr, code) = run(s3util().args([
        "put-bucket-request-payment",
        "s3://example/key",
        "--requester",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "bucket path with key should exit 1 (validation)"
    );
}
