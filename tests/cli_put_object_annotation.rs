//! Process-level CLI tests for the `put-object-annotation` subcommand.
//! These run without AWS credentials or network access.

use std::io::Write;
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["put-object-annotation", "--help"]));
    assert!(ok, "put-object-annotation --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("--annotation-name"));
    assert!(stdout.contains("--annotation-payload"));
    assert!(stdout.contains("--target-version-id"));
}

#[test]
fn missing_positional_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("put-object-annotation"));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-arg should exit 2");
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn missing_annotation_name_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-object-annotation",
        "s3://bucket/key",
        "--annotation-payload",
        "-",
    ]));
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
        "put-object-annotation",
        "s3://bucket",
        "--annotation-name",
        "note",
        "--annotation-payload",
        "/dev/null",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(1), "bucket-only path should exit 1 (validation)");
    assert!(!stderr.is_empty());
}

#[test]
fn dry_run_succeeds_without_network() {
    // --dry-run must short-circuit before any S3 call. Payload is read and
    // validated first, so provide a valid one-line payload file via stdin "-".
    let mut child = s3util()
        .args([
            "put-object-annotation",
            "s3://bucket/key",
            "--annotation-name",
            "note",
            "--annotation-payload",
            "-",
            "--dry-run",
            "--target-region",
            "us-east-1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    child.stdin.take().unwrap().write_all(b"hello").unwrap();
    let output = child.wait_with_output().expect("wait");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "dry-run should exit 0; stderr: {stderr}"
    );
    assert!(
        stderr.contains("[dry-run]"),
        "expected dry-run log; stderr: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["put-object-annotation", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn oversize_file_payload_exits_1_without_network() {
    // Create a unique temp file slightly larger than 1 MiB to trigger local
    // validation. A unique path (not a fixed shared name) keeps concurrent test
    // runs / shared $TMPDIR from racing. No AWS credentials or network needed.
    let max: usize = 1024 * 1024; // MAX_ANNOTATION_PAYLOAD_LEN
    let mut tmp = tempfile::NamedTempFile::new().expect("create temp file");
    tmp.write_all(&vec![0u8; max + 1])
        .expect("write temp oversize file");
    tmp.flush().expect("flush temp file");
    let path = tmp.path().to_path_buf();
    let path_str = path.to_str().expect("UTF-8 temp path");
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-object-annotation",
        "s3://bucket/key",
        "--annotation-name",
        "note",
        "--annotation-payload",
        path_str,
    ]));
    // `tmp` is deleted when dropped at end of scope.
    assert!(!ok, "oversize payload should fail; stderr: {stderr}");
    assert_eq!(code, Some(1), "expected exit code 1; stderr: {stderr}");
    assert!(
        stderr.contains("1 MiB") || stderr.contains("1048576"),
        "expected size-limit error message; stderr: {stderr}"
    );
}
