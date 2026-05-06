//! Process-level CLI tests for the `restore-object` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["restore-object", "--help"]));
    assert!(ok, "restore-object --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(
        stdout.contains("--days") || stdout.contains("DAYS"),
        "expected --days flag in help; got: {stdout}"
    );
    assert!(
        stdout.contains("--tier") || stdout.contains("TIER"),
        "expected --tier flag in help; got: {stdout}"
    );
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("restore-object"));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap missing-arg should exit 2; stderr: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["restore-object", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn bucket_only_target_exits_1() {
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["restore-object", "s3://example", "--days", "1"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "bucket-only path (no key) should exit 1 (validation)"
    );
}

#[test]
fn unknown_tier_exits_2() {
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["restore-object", "s3://example/key", "--tier", "TurboMax"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "unknown tier value should exit 2 (clap arg-validation)"
    );
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "restore-object",
        "s3://example/key",
        "--days",
        "1",
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
fn non_numeric_days_value_exits_2() {
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["restore-object", "s3://example/key", "--days", "many"]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "non-numeric --days value should exit 2 (clap arg-validation)"
    );
}

#[test]
fn dry_run_help_exposes_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["restore-object", "--help"]));
    assert!(ok);
    assert!(
        stdout.contains("--dry-run"),
        "restore-object should expose --dry-run; help: {stdout}"
    );
}
