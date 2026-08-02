//! Process-level broken-pipe regression tests. No AWS access is needed:
//! presign signs locally with dummy env credentials, and completions/help
//! never leave the process.
//!
//! Each test hands the child a stdout whose read end is already closed, so
//! every stdout write in the child fails with `BrokenPipe` from the first
//! byte — exactly the reported `s3util ... | head 1` case, where `head`
//! fails to open the file `1` and exits without reading its input (and the
//! worst case of any consumer that stops reading early). The binary
//! must exit through its normal success path: never panic ("failed printing
//! to stdout: Broken pipe") and never die of SIGPIPE (which would surface
//! here as `status.code() == None`).

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

/// Run `cmd` with a pre-closed stdout pipe and return (exit_code, stderr).
fn run_with_closed_stdout(cmd: &mut Command) -> (Option<i32>, String) {
    let (reader, writer) = std::io::pipe().expect("failed to create pipe");
    // Close the read end before the child even starts: with no readers left,
    // every stdout write in the child fails with EPIPE immediately.
    drop(reader);

    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::from(writer))
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    (
        output.status.code(),
        String::from_utf8_lossy(&output.stderr).to_string(),
    )
}

fn assert_exits_zero_without_panic(code: Option<i32>, stderr: &str, what: &str) {
    assert!(
        !stderr.contains("panicked"),
        "{what} must not panic on a closed stdout pipe; stderr: {stderr}"
    );
    assert_eq!(
        code,
        Some(0),
        "{what} must exit 0 on a closed stdout pipe (None = killed by \
         SIGPIPE); stderr: {stderr}"
    );
}

/// `presign` prints its URL through `pipe_safe::println_pipe_safe` — the
/// same helper every JSON-report subcommand (get-bucket-versioning,
/// head-object, ...) uses — so this pins the fix for the reported
/// `s3util get-bucket-versioning ... | head 1` panic without needing AWS.
/// Credentials are the AWS documentation example values; presign resolves
/// them from the environment and signs locally, no network I/O.
#[test]
fn presign_with_closed_stdout_exits_zero() {
    let mut cmd = s3util();
    cmd.args(["presign", "s3://closed-stdout-test-bucket/test-key"])
        .env("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE")
        .env(
            "AWS_SECRET_ACCESS_KEY",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        )
        .env("AWS_REGION", "us-east-1")
        // Keep the developer's real AWS setup out of the credential chain.
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .env("AWS_CONFIG_FILE", "/nonexistent/aws-config")
        .env(
            "AWS_SHARED_CREDENTIALS_FILE",
            "/nonexistent/aws-credentials",
        )
        .env_remove("AWS_PROFILE")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_ENDPOINT_URL")
        .env_remove("AWS_ENDPOINT_URL_S3");
    let (code, stderr) = run_with_closed_stdout(&mut cmd);
    assert_exits_zero_without_panic(code, &stderr, "presign");
}

/// Completion scripts are rendered to a buffer and written pipe-safely —
/// clap_complete itself would panic ("failed to write completion file") if
/// its generator wrote straight into a closed stdout.
#[test]
fn completion_script_with_closed_stdout_exits_zero() {
    for shell in ["bash", "zsh", "fish"] {
        let (code, stderr) =
            run_with_closed_stdout(s3util().args(["cp", "--auto-complete-shell", shell]));
        assert_exits_zero_without_panic(code, &stderr, &format!("--auto-complete-shell {shell}"));
    }
}

/// The hand-rendered categorized top-level help ignores write errors.
#[test]
fn top_level_help_with_closed_stdout_exits_zero() {
    let (code, stderr) = run_with_closed_stdout(s3util().arg("--help"));
    assert_exits_zero_without_panic(code, &stderr, "--help");
}

/// Subcommand help and --version are printed by clap, whose `Error::exit`
/// swallows write errors ("Swallow broken pipe errors" in clap itself).
/// Pinned here so a clap regression would be caught.
#[test]
fn subcommand_help_and_version_with_closed_stdout_exit_zero() {
    let (code, stderr) = run_with_closed_stdout(s3util().args(["get-bucket-versioning", "--help"]));
    assert_exits_zero_without_panic(code, &stderr, "get-bucket-versioning --help");

    let (code, stderr) = run_with_closed_stdout(s3util().arg("--version"));
    assert_exits_zero_without_panic(code, &stderr, "--version");
}
