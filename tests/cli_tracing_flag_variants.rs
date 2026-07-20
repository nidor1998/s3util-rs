//! Process-level tests for the tracing-init flag variants. The in-module unit
//! tests in `src/bin/s3util/tracing_init/mod.rs` can only exercise a copy of
//! the builder logic (a process can install one global subscriber), so each
//! variant here runs the real binary with the corresponding flag and asserts
//! on the stderr shape produced by the real `init_tracing`.
//!
//! Like `cli_tracing_to_stderr.rs`, none of this needs AWS: the cp invocation
//! fails on the nonexistent local source, but only after tracing has been
//! initialized and the `config = ...` trace line has fired.

use std::io::Read;
use std::process::{Command, Stdio};

fn s3util_cp_failing(extra_args: &[&str]) -> std::process::Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_s3util"));
    cmd.args([
        "cp",
        "-vvv",
        "--disable-color-tracing",
        "/nonexistent/source/file/for/tracing/variants.bin",
        "s3://nonexistent-bucket-for-tracing-variants/key",
    ])
    .args(extra_args)
    .env_remove("RUST_LOG")
    .env_remove("NO_COLOR")
    .env_remove("CLICOLOR")
    .stdin(Stdio::null())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    cmd.output().expect("failed to spawn s3util binary")
}

#[test]
fn json_tracing_emits_json_lines_on_stderr() {
    let output = s3util_cp_failing(&["--json-tracing"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    // Every tracing line must be a JSON object carrying the standard
    // tracing-subscriber fields.
    let mut saw_json_line = false;
    for line in stderr.lines().filter(|l| !l.trim().is_empty()) {
        let parsed: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("--json-tracing stderr line is not JSON ({e}): {line}"));
        assert!(
            parsed.get("level").is_some() && parsed.get("timestamp").is_some(),
            "JSON tracing line missing level/timestamp: {line}"
        );
        saw_json_line = true;
    }
    assert!(
        saw_json_line,
        "expected at least one JSON tracing line on stderr; got: {stderr}"
    );
}

#[test]
fn aws_sdk_tracing_still_traces_config_summary() {
    let output = s3util_cp_failing(&["--aws-sdk-tracing"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("config ="),
        "expected the 'config = ...' trace with --aws-sdk-tracing; got: {stderr}"
    );
}

#[test]
fn span_events_tracing_still_traces_config_summary() {
    let output = s3util_cp_failing(&["--span-events-tracing"]);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("config ="),
        "expected the 'config = ...' trace with --span-events-tracing; got: {stderr}"
    );
}

/// Closing the stderr pipe early must not kill or panic the process:
/// `PipeSafeWriter` swallows the resulting BrokenPipe write/flush errors and
/// the binary still exits through its normal error path (exit 1 for the
/// missing source file), exactly the head/tail-closed-early scenario the
/// writer exists for.
#[test]
fn early_closed_stderr_pipe_does_not_kill_the_process() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_s3util"))
        .args([
            "cp",
            "-vvv",
            "--disable-color-tracing",
            "/nonexistent/source/file/for/tracing/brokenpipe.bin",
            "s3://nonexistent-bucket-for-tracing-brokenpipe/key",
        ])
        .env_remove("RUST_LOG")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn s3util binary");

    // Read a single byte so tracing is known to be initialized and writing,
    // then drop the read end — every later write/flush of stderr hits
    // BrokenPipe inside PipeSafeWriter.
    let mut stderr = child.stderr.take().expect("stderr must be piped");
    let mut first_byte = [0u8; 1];
    let _ = stderr.read(&mut first_byte);
    drop(stderr);

    let status = child.wait().expect("failed to wait for s3util");
    assert_eq!(
        status.code(),
        Some(1),
        "process must exit 1 via its own error path, not die on SIGPIPE or panic"
    );
}
