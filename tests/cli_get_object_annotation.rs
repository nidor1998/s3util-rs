//! Process-level CLI tests for the `get-object-annotation` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["get-object-annotation", "--help"]));
    assert!(ok, "get-object-annotation --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("--annotation-name"));
    assert!(stdout.contains("--target-version-id"));
}

#[test]
fn missing_positionals_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("get-object-annotation"));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-arg should exit 2");
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn missing_annotation_name_exits_2() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["get-object-annotation", "s3://bucket/key", "/tmp/out.bin"]));
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
        "get-object-annotation",
        "s3://bucket",
        "/tmp/out.bin",
        "--annotation-name",
        "note",
        "--target-region",
        "us-east-1",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(1), "bucket-only path should exit 1 (validation)");
    assert!(!stderr.is_empty());
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["get-object-annotation", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}
