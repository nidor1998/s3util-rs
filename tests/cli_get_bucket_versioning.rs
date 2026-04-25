//! Process-level CLI tests for the `get-bucket-versioning` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["get-bucket-versioning", "--help"]));
    assert!(ok, "get-bucket-versioning --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("get-bucket-versioning"));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap missing-arg should exit 2; stderr: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"),
        "expected 'required' or 'usage' in stderr; got: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["get-bucket-versioning", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn target_with_key_component_fails_gracefully() {
    // The bucket_name() helper rejects paths with a key — this exercises
    // that path at the parse-assertion level (exit 1, not exit 2).
    let (ok, _stdout, _stderr, code) =
        run(s3util().args(["get-bucket-versioning", "s3://example/key"]));
    // key-bearing paths are accepted by clap but rejected by bucket_name().
    // We only assert it does not exit 2 (parse error) — the runtime (exit 1)
    // failure is expected without real credentials.
    assert!(
        code != Some(2),
        "key-bearing path must not trigger a clap parse error; code={code:?}"
    );
    let _ = ok;
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "get-bucket-versioning",
        "s3://example",
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key"),
        "expected clap error about missing secret key; got: {stderr}"
    );
}
