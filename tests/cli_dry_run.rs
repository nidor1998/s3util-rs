//! Process-level CLI tests for the `--dry-run` flag.
//!
//! These spawn the s3util binary as a subprocess and assert that
//! `--dry-run` short-circuits *before* any AWS call, exits 0, and emits
//! a `[dry-run]` log line. Because the API call is skipped, no AWS
//! credentials or network access are required — the bucket name need
//! only be syntactically valid.

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

const FAKE_BUCKET: &str = "s3://nonexistent-bucket-for-s3util-dry-run-tests";

// ---------- cp / mv (CommonTransferArgs path) ----------

#[test]
fn cp_dry_run_exits_zero_and_logs_message() {
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"test content").unwrap();
    let target = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["cp", "--dry-run", src.path().to_str().unwrap(), &target]));
    assert!(ok, "cp --dry-run must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run]"),
        "missing [dry-run] in stderr: {stderr}"
    );
    assert!(
        stderr.contains("would copy"),
        "missing 'would copy' message: {stderr}"
    );
}

#[test]
fn cp_dry_run_at_default_verbosity_shows_message_via_level_bump() {
    // Default verbosity is Warn; without the --dry-run level bump, the info-
    // level [dry-run] line would be filtered out. This asserts the bump.
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"x").unwrap();
    let target = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, _code) =
        run(s3util().args(["cp", "--dry-run", src.path().to_str().unwrap(), &target]));
    assert!(ok);
    assert!(
        stderr.contains("[dry-run]"),
        "default-verbosity dry-run must show the line via level bump: {stderr}"
    );
}

#[test]
fn cp_dry_run_with_q_still_logs_via_level_bump() {
    // -q lowers Warn to Error; without the bump, info-level [dry-run] would
    // be invisible. The bump forces level to >= info regardless of -q.
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"x").unwrap();
    let target = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "cp",
        "--dry-run",
        "-q",
        src.path().to_str().unwrap(),
        &target,
    ]));
    assert!(ok);
    assert!(
        stderr.contains("[dry-run]"),
        "level bump must keep [dry-run] visible at -q: {stderr}"
    );
}

#[test]
fn cp_dry_run_transfer_completed_message_is_prefixed() {
    // The indicator emits a `Transfer completed.` summary line after the
    // copy phase finishes — under --dry-run that summary must also be
    // marked, matching the prefix on the per-direction "would copy" line.
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"x").unwrap();
    let target = format!("{FAKE_BUCKET}/key");
    let (_ok, _stdout, stderr, _code) =
        run(s3util().args(["cp", "--dry-run", src.path().to_str().unwrap(), &target]));
    assert!(
        stderr.contains("[dry-run] Transfer completed."),
        "Transfer-completed summary must be prefixed under --dry-run; \
         stderr: {stderr}"
    );
}

#[test]
fn cp_dry_run_with_qqq_still_logs_via_level_bump() {
    // -qqq normally silences tracing entirely; --dry-run overrides that.
    let src = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(src.path(), b"x").unwrap();
    let target = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "cp",
        "--dry-run",
        "-qqq",
        src.path().to_str().unwrap(),
        &target,
    ]));
    assert!(ok);
    assert!(
        stderr.contains("[dry-run]"),
        "dry-run must override -qqq silencing: {stderr}"
    );
}

#[test]
fn mv_dry_run_exits_zero_and_logs_messages() {
    // S3-to-S3 mv avoids needing a real local file. Both copy and source-
    // delete short-circuit, so the test sees both [dry-run] log lines.
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "mv",
        "--dry-run",
        &format!("{FAKE_BUCKET}/src"),
        &format!("{FAKE_BUCKET}/dst"),
    ]));
    assert!(ok, "mv --dry-run must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(stderr.contains("[dry-run]"));
    assert!(
        stderr.contains("would copy") && stderr.contains("would delete source"),
        "mv must log both copy and source-delete dry-run lines: {stderr}"
    );
}

// ---------- cp --skip-existing under --dry-run ----------

#[test]
fn cp_dry_run_skip_existing_with_existing_local_target_logs_skip() {
    // Existing local target → target_exists returns true → skip branch
    // logs "[dry-run] would skip: target exists." and exits 0. Use an S3
    // source so check_both_local doesn't reject the args; the skip branch
    // returns before any AWS traffic is initiated.
    let dst = tempfile::NamedTempFile::new().unwrap();
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "cp",
        "--dry-run",
        "--skip-existing",
        &format!("{FAKE_BUCKET}/key"),
        dst.path().to_str().unwrap(),
    ]));
    assert!(
        ok,
        "cp --dry-run --skip-existing must exit 0; stderr={stderr}"
    );
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run] would skip"),
        "missing '[dry-run] would skip' in stderr: {stderr}"
    );
}

#[test]
fn cp_dry_run_skip_existing_with_missing_local_target_logs_would_copy() {
    // Missing local target → target_exists returns false → skip branch
    // does not fire → existing dry-run "would copy object." line is logged.
    // Use an S3 source for the same reason as above; the dry-run path
    // short-circuits before any actual SDK request is sent.
    let dir = tempfile::tempdir().unwrap();
    let dst = dir.path().join("missing-target.dat");
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "cp",
        "--dry-run",
        "--skip-existing",
        &format!("{FAKE_BUCKET}/key"),
        dst.to_str().unwrap(),
    ]));
    assert!(
        ok,
        "cp --dry-run --skip-existing must exit 0; stderr={stderr}"
    );
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run]"),
        "missing [dry-run] marker in stderr: {stderr}"
    );
    assert!(
        stderr.contains("would copy object."),
        "missing 'would copy object.' in stderr: {stderr}"
    );
}

// ---------- cp dry-run via stdio (StdioToS3 / S3ToStdio) ----------

#[test]
fn cp_dry_run_stdio_to_s3_exits_zero_and_logs_message() {
    // Stdio source ("-") with S3 target ⇒ StdioToS3 direction. Under
    // --dry-run, the would-copy line must fire and no AWS call is made.
    let target = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, code) = run(s3util().args(["cp", "--dry-run", "-", &target]));
    assert!(ok, "cp --dry-run from stdio must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run]"),
        "missing [dry-run] marker: {stderr}"
    );
    assert!(
        stderr.contains("would copy object."),
        "stdio→s3 dry-run must log 'would copy object.': {stderr}"
    );
}

#[test]
fn cp_dry_run_s3_to_stdio_exits_zero_and_logs_message() {
    // S3 source with stdio target ("-") ⇒ S3ToStdio direction. Under
    // --dry-run, no GetObject is issued.
    let source = format!("{FAKE_BUCKET}/key");
    let (ok, _stdout, stderr, code) = run(s3util().args(["cp", "--dry-run", &source, "-"]));
    assert!(ok, "cp --dry-run to stdio must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run]"),
        "missing [dry-run] marker: {stderr}"
    );
    assert!(
        stderr.contains("would copy object."),
        "s3→stdio dry-run must log 'would copy object.': {stderr}"
    );
}

// ---------- rm (CommonClientArgs path) ----------

#[test]
fn rm_dry_run_exits_zero_and_logs_message() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["rm", "--dry-run", &format!("{FAKE_BUCKET}/key")]));
    assert!(ok, "rm --dry-run must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(stderr.contains("[dry-run]"));
    assert!(stderr.contains("would delete object"));
}

// ---------- create-bucket (special: 2 API calls) ----------

#[test]
fn create_bucket_dry_run_exits_zero_and_logs_message() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["create-bucket", "--dry-run", FAKE_BUCKET]));
    assert!(ok, "create-bucket --dry-run must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(stderr.contains("[dry-run]"));
    assert!(stderr.contains("would create bucket"));
}

#[test]
fn create_bucket_dry_run_with_tagging_logs_both_lines() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "create-bucket",
        "--dry-run",
        "--tagging",
        "env=test&team=sre",
        FAKE_BUCKET,
    ]));
    assert!(ok);
    assert!(
        stderr.contains("would create bucket"),
        "missing 'would create bucket': {stderr}"
    );
    assert!(
        stderr.contains("would put bucket tagging"),
        "missing 'would put bucket tagging': {stderr}"
    );
}

// ---------- put-* (representative: put-bucket-cors with JSON config) ----------

#[test]
fn put_bucket_cors_dry_run_exits_zero_and_logs_message() {
    let cfg = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(cfg.path(), r#"{"CORSRules":[]}"#).unwrap();
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-cors",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert!(ok, "put-bucket-cors --dry-run must exit 0; stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(stderr.contains("[dry-run]"));
    assert!(stderr.contains("would put bucket CORS"));
}

#[test]
fn put_bucket_cors_dry_run_validates_json_before_short_circuit() {
    // Per spec: pre-flight (including JSON parsing) runs to completion. A
    // malformed JSON config must surface the parse error even under
    // --dry-run, not silently succeed.
    let cfg = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(cfg.path(), b"not valid json {").unwrap();
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "put-bucket-cors",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert!(!ok, "malformed JSON must still fail under --dry-run");
    assert!(
        stderr.to_lowercase().contains("json") || stderr.to_lowercase().contains("parse"),
        "expected JSON parse error; got: {stderr}"
    );
}

// ---------- delete-* (representative: delete-bucket-cors) ----------

#[test]
fn delete_bucket_cors_dry_run_exits_zero_and_logs_message() {
    let (ok, _stdout, stderr, code) =
        run(s3util().args(["delete-bucket-cors", "--dry-run", FAKE_BUCKET]));
    assert!(ok, "stderr={stderr}");
    assert_eq!(code, Some(0));
    assert!(stderr.contains("[dry-run]"));
    assert!(stderr.contains("would delete bucket CORS"));
}

// ---------- coverage smoke tests for every remaining mutating command ----------
//
// One test per command, each shaped to exercise the dry-run short-circuit
// (the `if args.dry_run { ... }` block). Required positional arguments are
// supplied with a syntactically valid value; commands taking a JSON config
// file get the minimum body each parser accepts (verified empirically).
// All tests assert exit 0 and `[dry-run]` in stderr — a regression in any
// run_X function would surface here without needing AWS access.

const FAKE_OBJECT: &str = "s3://nonexistent-bucket-for-s3util-dry-run-tests/key";

fn write_json(body: &str) -> tempfile::NamedTempFile {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(tmp.path(), body).unwrap();
    tmp
}

fn assert_dry_run(stderr: &str, ok: bool, code: Option<i32>, phrase: &str) {
    assert!(ok, "stderr: {stderr}");
    assert_eq!(code, Some(0));
    assert!(
        stderr.contains("[dry-run]"),
        "missing [dry-run] in stderr: {stderr}"
    );
    assert!(
        stderr.contains(phrase),
        "stderr missing '{phrase}': {stderr}"
    );
}

// delete-* family (8 commands beyond delete-bucket-cors)

#[test]
fn delete_bucket_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args(["delete-bucket", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete bucket");
}

#[test]
fn delete_bucket_encryption_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-bucket-encryption", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete bucket encryption");
}

#[test]
fn delete_bucket_lifecycle_configuration_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args([
        "delete-bucket-lifecycle-configuration",
        "--dry-run",
        FAKE_BUCKET,
    ]));
    assert_dry_run(&stderr, ok, code, "would delete bucket lifecycle");
}

#[test]
fn delete_bucket_policy_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-bucket-policy", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete bucket policy");
}

#[test]
fn delete_bucket_tagging_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-bucket-tagging", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete bucket tagging");
}

#[test]
fn delete_bucket_website_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-bucket-website", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete bucket website");
}

#[test]
fn delete_object_tagging_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-object-tagging", "--dry-run", FAKE_OBJECT]));
    assert_dry_run(&stderr, ok, code, "would delete object tagging");
}

#[test]
fn delete_object_tagging_dry_run_with_version_id_includes_version_field() {
    // Exercises the `version_id = %args.source_version_id...` field of the
    // dry-run info!() — distinct from the no-version-id smoke above.
    let (ok, _, stderr, code) = run(s3util().args([
        "delete-object-tagging",
        "--dry-run",
        "--source-version-id",
        "abc123",
        FAKE_OBJECT,
    ]));
    assert_dry_run(&stderr, ok, code, "would delete object tagging");
    assert!(
        stderr.contains("abc123"),
        "version_id should appear in dry-run log: {stderr}"
    );
}

#[test]
fn delete_public_access_block_dry_run_smoke() {
    let (ok, _, stderr, code) =
        run(s3util().args(["delete-public-access-block", "--dry-run", FAKE_BUCKET]));
    assert_dry_run(&stderr, ok, code, "would delete public access block");
}

// put-* family (10 commands beyond put-bucket-cors)

#[test]
fn put_bucket_encryption_dry_run_smoke() {
    let cfg = write_json(r#"{"Rules":[]}"#);
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-encryption",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket encryption");
}

#[test]
fn put_bucket_lifecycle_configuration_dry_run_smoke() {
    let cfg = write_json(r#"{"Rules":[]}"#);
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket lifecycle");
}

#[test]
fn put_bucket_logging_dry_run_smoke() {
    let cfg = write_json("{}");
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-logging",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket logging");
}

#[test]
fn put_bucket_notification_configuration_dry_run_smoke() {
    let cfg = write_json("{}");
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-notification-configuration",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket notification");
}

#[test]
fn put_bucket_policy_dry_run_smoke() {
    // put-bucket-policy passes the body verbatim — no JSON validation in
    // s3util — so any UTF-8 file body is accepted at the parse stage.
    let cfg = write_json(r#"{"Version":"2012-10-17","Statement":[]}"#);
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-policy",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket policy");
}

#[test]
fn put_bucket_tagging_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-tagging",
        "--dry-run",
        FAKE_BUCKET,
        "--tagging",
        "env=test",
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket tagging");
}

#[test]
fn put_bucket_versioning_enabled_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-versioning",
        "--dry-run",
        "--enabled",
        FAKE_BUCKET,
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket versioning");
    assert!(stderr.contains("Enabled"), "stderr: {stderr}");
}

#[test]
fn put_bucket_versioning_suspended_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-versioning",
        "--dry-run",
        "--suspended",
        FAKE_BUCKET,
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket versioning");
    assert!(stderr.contains("Suspended"), "stderr: {stderr}");
}

#[test]
fn put_bucket_website_dry_run_smoke() {
    let cfg = write_json("{}");
    let (ok, _, stderr, code) = run(s3util().args([
        "put-bucket-website",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put bucket website");
}

#[test]
fn put_object_tagging_dry_run_smoke() {
    let (ok, _, stderr, code) = run(s3util().args([
        "put-object-tagging",
        "--dry-run",
        FAKE_OBJECT,
        "--tagging",
        "env=test",
    ]));
    assert_dry_run(&stderr, ok, code, "would put object tagging");
}

#[test]
fn put_object_tagging_dry_run_with_version_id_includes_version_field() {
    let (ok, _, stderr, code) = run(s3util().args([
        "put-object-tagging",
        "--dry-run",
        FAKE_OBJECT,
        "--tagging",
        "env=test",
        "--source-version-id",
        "ver1",
    ]));
    assert_dry_run(&stderr, ok, code, "would put object tagging");
    assert!(
        stderr.contains("ver1"),
        "version_id should appear in dry-run log: {stderr}"
    );
}

#[test]
fn put_public_access_block_dry_run_smoke() {
    let cfg = write_json("{}");
    let (ok, _, stderr, code) = run(s3util().args([
        "put-public-access-block",
        "--dry-run",
        FAKE_BUCKET,
        cfg.path().to_str().unwrap(),
    ]));
    assert_dry_run(&stderr, ok, code, "would put public access block");
}

// rm with --source-version-id covers the version_id field of the dry-run log
#[test]
fn rm_dry_run_with_version_id_includes_version_field() {
    let (ok, _, stderr, code) =
        run(s3util().args(["rm", "--dry-run", "--source-version-id", "v42", FAKE_OBJECT]));
    assert_dry_run(&stderr, ok, code, "would delete object");
    assert!(
        stderr.contains("v42"),
        "version_id must appear in dry-run log: {stderr}"
    );
}

// ---------- read-only commands must NOT expose --dry-run ----------

#[test]
fn get_bucket_cors_help_does_not_expose_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["get-bucket-cors", "--help"]));
    assert!(ok);
    assert!(
        !stdout.contains("--dry-run"),
        "get-* must not expose --dry-run; help: {stdout}"
    );
}

#[test]
fn head_bucket_help_does_not_expose_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["head-bucket", "--help"]));
    assert!(ok);
    assert!(
        !stdout.contains("--dry-run"),
        "head-* must not expose --dry-run; help: {stdout}"
    );
}

#[test]
fn head_object_help_does_not_expose_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["head-object", "--help"]));
    assert!(ok);
    assert!(!stdout.contains("--dry-run"));
}

// ---------- mutating commands DO expose --dry-run ----------

#[test]
fn cp_help_lists_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["cp", "--help"]));
    assert!(ok);
    assert!(stdout.contains("--dry-run"), "cp --help missing --dry-run");
}

#[test]
fn rm_help_lists_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["rm", "--help"]));
    assert!(ok);
    assert!(stdout.contains("--dry-run"));
}

#[test]
fn delete_bucket_help_lists_dry_run() {
    let (ok, stdout, _stderr, _code) = run(s3util().args(["delete-bucket", "--help"]));
    assert!(ok);
    assert!(stdout.contains("--dry-run"));
}
