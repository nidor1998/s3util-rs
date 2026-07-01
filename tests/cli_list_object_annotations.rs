//! Process-level CLI tests for the `list-object-annotations` subcommand.
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
fn help_succeeds_and_lists_options() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["list-object-annotations", "--help"]));
    assert!(ok, "list-object-annotations --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("--annotation-prefix"));
    assert!(stdout.contains("--target-version-id"));
}

#[test]
fn missing_positional_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("list-object-annotations"));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-arg should exit 2");
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn bucket_only_path_no_key_exits_1() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "list-object-annotations",
        "s3://bucket",
        "--target-region",
        "us-east-1",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(1), "bucket-only path should exit 1 (validation)");
    assert!(!stderr.is_empty());
}

#[test]
fn non_s3_target_exits_1() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "list-object-annotations",
        "/tmp/local",
        "--target-region",
        "us-east-1",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(1));
    assert!(!stderr.is_empty());
}
