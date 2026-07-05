//! Process-level CLI tests for the `delete-object-annotation` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["delete-object-annotation", "--help"]));
    assert!(ok, "delete-object-annotation --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("--annotation-name"));
    assert!(stdout.contains("--target-version-id"));
}

#[test]
fn missing_positional_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("delete-object-annotation"));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-arg should exit 2");
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn missing_annotation_name_exits_2() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["delete-object-annotation", "s3://bucket/key"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "--annotation-name is required; should exit 2"
    );
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn bucket_only_path_no_key_exits_1() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "delete-object-annotation",
        "s3://bucket",
        "--annotation-name",
        "note",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(1), "bucket-only path should exit 1 (validation)");
    assert!(!stderr.is_empty());
}

#[test]
fn dry_run_succeeds_without_network() {
    let (ok, stdout, stderr, _code) = run(s3util().args([
        "delete-object-annotation",
        "s3://bucket/key",
        "--annotation-name",
        "note",
        "--dry-run",
        "--target-region",
        "us-east-1",
    ]));
    assert!(ok, "dry-run should exit 0; stderr: {stderr}");
    assert!(
        stdout.is_empty(),
        "success path must print nothing to stdout; stdout: {stdout}"
    );
    assert!(
        stderr.contains("[dry-run]"),
        "expected dry-run log; stderr: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["delete-object-annotation", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}
