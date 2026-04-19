//! Process-level test: invalid args that survive clap's own parsing but fail
//! `Config::try_from` must be re-raised through clap's error machinery so the
//! user sees the validation message on stderr and the process exits non-zero.
//!
//! Covers `src/bin/s3util/main.rs` lines 25-30 — the `Err(error_message)` arm
//! of `match Config::try_from(cp_args)`.
//!
//! Doesn't require AWS: the error fires before any S3 call.

use std::process::{Command, Stdio};

#[test]
fn both_local_paths_exit_non_zero_with_validation_message_on_stderr() {
    let bin = env!("CARGO_BIN_EXE_s3util");

    // Two local paths are valid per clap's per-arg value_parser (check_storage_path)
    // but rejected by Config::try_from's check_both_local guard. That error is
    // re-wrapped as clap::ErrorKind::ValueValidation and printed by .exit().
    let output = Command::new(bin)
        .args(["cp", "/tmp/s3util_e2e_src", "/tmp/s3util_e2e_dst"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        !output.status.success(),
        "both-local-paths invocation must exit non-zero.\n\
         status: {:?}\n--- stderr ---\n{stderr}\n--- stdout ---\n{stdout}",
        output.status.code()
    );

    assert!(
        stderr.contains("source and target cannot both be local paths"),
        "expected the check_both_local validation message on stderr.\n\
         --- stderr ---\n{stderr}"
    );
}
