//! Process-level CLI tests: invocations whose validation happens entirely
//! before any AWS call. These run without credentials and without network.
//!
//! Each test launches the real `s3util` binary and asserts on exit status and
//! stderr — exercising the clap value_parsers, arg conflicts, and
//! `Config::try_from` validations that gate every command before it ever
//! touches the SDK.

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

fn run(cmd: &mut Command) -> (bool, String, String) {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (output.status.success(), stdout, stderr)
}

#[test]
fn no_subcommand_exits_non_zero_and_prints_usage() {
    let (ok, _stdout, stderr) = run(&mut s3util());
    assert!(!ok, "no-subcommand invocation must exit non-zero");
    assert!(
        stderr.to_lowercase().contains("usage"),
        "expected usage message on stderr.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn help_succeeds_and_lists_subcommands() {
    let (ok, stdout, _stderr) = run(s3util().arg("--help"));
    assert!(ok, "--help must succeed");
    // Top-level help should mention the cp/mv subcommands.
    assert!(stdout.contains("cp"), "--help output must mention cp");
    assert!(stdout.contains("mv"), "--help output must mention mv");
}

#[test]
fn version_flag_succeeds_and_emits_version_string() {
    let (ok, stdout, _stderr) = run(s3util().arg("--version"));
    assert!(ok, "--version must succeed");
    assert!(
        stdout.contains("s3util"),
        "--version output should contain the binary name.\n--- stdout ---\n{stdout}"
    );
}

#[test]
fn cp_without_args_exits_with_usage_on_stderr() {
    let (ok, _stdout, stderr) = run(s3util().arg("cp"));
    assert!(!ok, "cp without args must exit non-zero");
    assert!(
        stderr.to_lowercase().contains("usage")
            || stderr.contains("required")
            || stderr.contains("missing"),
        "cp without args should produce a usage/required-arg message.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_single_arg_exits_with_usage_on_stderr() {
    let (ok, _stdout, stderr) = run(s3util().args(["cp", "s3://b/k"]));
    assert!(!ok, "cp with only one positional arg must exit non-zero");
    assert!(
        stderr.to_lowercase().contains("usage")
            || stderr.contains("required")
            || stderr.contains("missing"),
        "cp with single arg must complain about missing target.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_invalid_source_path_rejected_by_value_parser() {
    // Neither a valid s3:// URI, nor an existing local file, nor "-".
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/this/path/definitely/does/not/exist/12345",
        "s3://bucket/key",
    ]));
    assert!(!ok, "cp with non-existent local source must exit non-zero");
    // file_exist value parser surfaces an error message.
    assert!(
        !stderr.is_empty(),
        "value parser error should write something to stderr"
    );
}

#[test]
fn mv_help_succeeds() {
    let (ok, stdout, _stderr) = run(s3util().args(["mv", "--help"]));
    assert!(ok, "mv --help must succeed");
    assert!(
        stdout.to_lowercase().contains("mv") || stdout.to_lowercase().contains("move"),
        "mv --help should describe mv.\n--- stdout ---\n{stdout}"
    );
}

#[test]
fn cp_help_succeeds_and_mentions_known_flags() {
    let (ok, stdout, _stderr) = run(s3util().args(["cp", "--help"]));
    assert!(ok, "cp --help must succeed");
    // A handful of flags that are part of the public CLI.
    for flag in [
        "--target-profile",
        "--source-profile",
        "--additional-checksum-algorithm",
    ] {
        assert!(
            stdout.contains(flag),
            "cp --help missing expected flag: {flag}\n--- stdout ---\n{stdout}"
        );
    }
}

#[test]
fn cp_unknown_flag_exits_with_error() {
    let (ok, _stdout, stderr) = run(s3util().args(["cp", "--this-flag-does-not-exist"]));
    assert!(!ok, "unknown flag must exit non-zero");
    assert!(
        stderr.contains("unexpected") || stderr.contains("unknown") || stderr.contains("Found"),
        "expected an unknown-flag error.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn mv_both_local_paths_rejected_with_validation_message() {
    let (ok, _stdout, stderr) =
        run(s3util().args(["mv", "/tmp/s3util_e2e_src", "/tmp/s3util_e2e_dst"]));
    assert!(!ok, "mv with two local paths must exit non-zero");
    assert!(
        stderr.contains("source and target cannot both be local paths"),
        "expected check_both_local validation on stderr for mv.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_source_no_sign_request_with_credentials_conflict() {
    // --source-no-sign-request conflicts with --source-profile per the clap spec.
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "s3://b/k",
        "/tmp/anywhere",
        "--source-no-sign-request",
        "--source-profile",
        "p",
    ]));
    assert!(
        !ok,
        "source-no-sign + source-profile must conflict and exit non-zero"
    );
    assert!(
        stderr.contains("cannot be used with") || stderr.contains("conflict"),
        "expected conflict message on stderr.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_invalid_storage_class_rejected() {
    // A garbage storage class should fail clap's value_parser.
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/tmp/anywhere",
        "s3://b/k",
        "--storage-class",
        "TOTALLY_FAKE_CLASS",
    ]));
    assert!(!ok, "invalid storage class must exit non-zero");
    assert!(
        !stderr.is_empty(),
        "expected a storage-class validation error.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_invalid_checksum_algorithm_rejected() {
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/tmp/anywhere",
        "s3://b/k",
        "--checksum-algorithm",
        "MD5",
    ]));
    assert!(!ok, "MD5 (not in AWS additional checksum) must be rejected");
    assert!(!stderr.is_empty());
}

#[test]
fn cp_invalid_metadata_format_rejected() {
    // metadata format expects "k=v" pairs joined with comma.
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/tmp/anywhere",
        "s3://b/k",
        "--metadata",
        "no_equal_sign",
    ]));
    assert!(!ok, "malformed metadata value must be rejected");
    assert!(!stderr.is_empty());
}

#[test]
fn cp_negative_max_parallel_uploads_rejected() {
    // u16 type means clap rejects negatives at parse time.
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/tmp/anywhere",
        "s3://b/k",
        "--max-parallel-uploads",
        "-5",
    ]));
    assert!(!ok, "negative max-parallel-uploads must be rejected");
    assert!(!stderr.is_empty());
}

#[test]
fn cp_invalid_endpoint_url_rejected() {
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "/tmp/anywhere",
        "s3://b/k",
        "--target-endpoint-url",
        "not a url",
    ]));
    assert!(!ok, "malformed endpoint URL must be rejected");
    assert!(!stderr.is_empty());
}

#[test]
fn cp_skip_existing_with_stdio_target_rejected() {
    let (ok, _stdout, stderr) = run(s3util().args(["cp", "--skip-existing", "s3://b/k", "-"]));
    assert!(
        !ok,
        "cp --skip-existing with stdout target must exit non-zero"
    );
    assert!(
        stderr.contains("stdout target"),
        "expected stdout target error.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn cp_skip_existing_with_if_none_match_rejected() {
    let (ok, _stdout, stderr) = run(s3util().args([
        "cp",
        "--skip-existing",
        "--if-none-match",
        "/tmp/a",
        "s3://b/k",
    ]));
    assert!(!ok, "cp --skip-existing --if-none-match must exit non-zero");
    assert!(
        stderr.contains("--if-none-match"),
        "expected --if-none-match error.\n--- stderr ---\n{stderr}"
    );
}

#[test]
fn invalid_subcommand_exits_with_error() {
    let (ok, _stdout, stderr) = run(s3util().args(["fly_to_the_moon"]));
    assert!(!ok, "unknown subcommand must exit non-zero");
    assert!(
        stderr.contains("unrecognized") || stderr.contains("unknown") || stderr.contains("Found"),
        "expected unknown-subcommand error.\n--- stderr ---\n{stderr}"
    );
}
