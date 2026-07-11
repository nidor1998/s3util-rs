//! Process-level CLI tests for the `create-bucket` subcommand.
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
    let (ok, stdout, _stderr, _code) = run(s3util().args(["create-bucket", "--help"]));
    assert!(ok, "create-bucket --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn help_mentions_tagging_option() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["create-bucket", "--help"]));
    assert!(ok);
    assert!(
        stdout.contains("--tagging"),
        "expected --tagging in help output: {stdout}"
    );
}

#[test]
fn help_mentions_if_not_exists_option() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["create-bucket", "--help"]));
    assert!(ok);
    assert!(
        stdout.contains("--if-not-exists"),
        "expected --if-not-exists in help output: {stdout}"
    );
}

#[test]
fn help_mentions_bucket_namespace_and_configuration_options() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["create-bucket", "--help"]));
    assert!(ok);
    assert!(
        stdout.contains("--bucket-namespace"),
        "expected --bucket-namespace in help output: {stdout}"
    );
    assert!(
        stdout.contains("--create-bucket-configuration"),
        "expected --create-bucket-configuration in help output: {stdout}"
    );
}

#[test]
fn bucket_namespace_and_configuration_parse_together() {
    // A well-formed account-regional invocation gets past clap value-parsing
    // and arg-relation checks (it fails later, at the AWS call, for lack of
    // credentials — not with the exit-2 clap usage error).
    let (ok, _stdout, _stderr, code) = run(s3util().args([
        "create-bucket",
        "s3://mybucket2-111111111-ap-northeast-1-an",
        "--bucket-namespace",
        "account-regional",
        "--create-bucket-configuration",
        "LocationConstraint=ap-northeast-1",
        "--target-no-sign-request",
        "--target-region",
        "ap-northeast-1",
    ]));
    assert!(!ok, "invocation still fails at the AWS call without creds");
    assert_ne!(
        code,
        Some(2),
        "a valid account-regional invocation must not be a clap usage error"
    );
}

#[test]
fn bucket_namespace_without_configuration_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "create-bucket",
        "s3://my-bucket",
        "--bucket-namespace",
        "account-regional",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-requirement should exit 2");
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.contains("--create-bucket-configuration"),
        "expected a missing --create-bucket-configuration message; got: {stderr}"
    );
}

#[test]
fn configuration_without_bucket_namespace_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "create-bucket",
        "s3://my-bucket",
        "--create-bucket-configuration",
        "LocationConstraint=ap-northeast-1",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-requirement should exit 2");
    assert!(
        stderr.to_lowercase().contains("required") || stderr.contains("--bucket-namespace"),
        "expected a missing --bucket-namespace message; got: {stderr}"
    );
}

#[test]
fn invalid_bucket_namespace_value_rejected() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "create-bucket",
        "s3://my-bucket",
        "--bucket-namespace",
        "global",
        "--create-bucket-configuration",
        "LocationConstraint=ap-northeast-1",
    ]));
    assert!(
        !ok,
        "only account-regional is accepted for --bucket-namespace"
    );
    assert!(!stderr.is_empty());
}

#[test]
fn invalid_create_bucket_configuration_value_rejected() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "create-bucket",
        "s3://my-bucket",
        "--bucket-namespace",
        "account-regional",
        "--create-bucket-configuration",
        "LocationType=AvailabilityZone",
    ]));
    assert!(
        !ok,
        "only LocationConstraint= is accepted for --create-bucket-configuration"
    );
    assert!(!stderr.is_empty());
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("create-bucket"));
    assert!(!ok);
    assert_eq!(code, Some(2), "clap missing-arg should exit 2");
    assert!(stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"));
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["create-bucket", "--auto-complete-shell", "bash"]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "create-bucket",
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
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key")
    );
}

#[test]
fn target_no_sign_request_conflicts_with_target_profile() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "create-bucket",
        "s3://example",
        "--target-no-sign-request",
        "--target-profile",
        "default",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap conflict should exit 2; stderr: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("cannot be used")
            || stderr.to_lowercase().contains("conflict"),
        "expected clap conflict message; got: {stderr}"
    );
}
