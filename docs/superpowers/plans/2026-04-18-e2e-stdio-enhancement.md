# E2E Stdio Test Enhancement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add comprehensive E2E coverage (46 tests = 2 kept + 44 new) for stdio transfers — checksums, ETag, SSE, chunk-size, metadata — and make the transfer functions injectable so tests can drive stdin/stdout in-process.

**Architecture:** Refactor `stdio_to_s3::transfer` and `s3_to_stdio::transfer` to take `AsyncRead` / `AsyncWrite` parameters. Add two new test helpers (`cp_test_data_stdin_to_s3`, `cp_test_data_s3_to_stdout`) that feed `Cursor<Vec<u8>>` and capture output into `Vec<u8>`. New tests live in three new files (`e2e_stdio_integrity_check.rs`, `e2e_stdio_sse.rs`, `e2e_stdio_metadata.rs`) plus 2 smoke tests added to the existing `e2e_stdio.rs`. Existing 2 subprocess-based tests in `e2e_stdio.rs` remain unchanged to preserve real-CLI coverage.

**Tech Stack:** Rust, Tokio, AWS SDK for Rust (aws-sdk-s3), `cargo test` with `RUSTFLAGS="--cfg e2e_test"`, AWS credentials profile `s3sync-e2e-test`.

**Reference documents:**
- Spec: `docs/superpowers/specs/2026-04-18-e2e-stdio-enhancement-design.md`
- Assertion conventions: `docs/superpowers/specs/2026-04-17-e2e-test-quality-parity-design.md`
- SSE-KMS precedent: `tests/e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs:420-446`
- SSE-C precedent: `tests/e2e_integrity_check.rs:3613-3650`
- DSSE-KMS precedent: `tests/e2e_integrity_check.rs:3456-3497`
- S3→local checksum roundtrip precedent: `tests/e2e_integrity_check.rs:530-592`

**E2E test invocation:** `RUSTFLAGS="--cfg e2e_test" cargo test --test <file_stem> -- --nocapture` (requires AWS credentials under profile `s3sync-e2e-test` and the `ap-northeast-1` region). All tests use `#![cfg(e2e_test)]` at the file top.

**Commit policy:** Per `CLAUDE.md`, every commit must first pass `cargo fmt` and `cargo clippy --all-features`. Ask the human to review before committing.

---

## Task 1: Refactor `stdio_to_s3::transfer` to accept injectable reader

**Files:**
- Modify: `src/transfer/stdio_to_s3.rs`

- [ ] **Step 1: Update the function signature and body**

Replace the contents of `src/transfer/stdio_to_s3.rs` with:

```rust
use anyhow::{Context, Result};
use async_channel::Sender;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::DateTime;
use tokio::io::AsyncReadExt;
use tracing::info;

use crate::Config;
use crate::storage::Storage;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics};

/// Transfer data from an async reader (typically stdin) to an S3 object.
///
/// The reader is drained to memory so the total size is known before dispatch
/// to the upload manager — which decides single-part vs multipart based on
/// `multipart_threshold`. Tests inject `Cursor<Vec<u8>>`; the CLI binary
/// injects `tokio::io::stdin()`.
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    mut reader: impl tokio::io::AsyncRead + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let target_clone = dyn_clone::clone_box(&*target);

    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .context("failed to read from stdin")?;

    let source_size = buffer.len() as u64;
    let byte_stream = ByteStream::from(buffer);

    let get_object_output = GetObjectOutput::builder()
        .set_body(Some(byte_stream))
        .set_content_length(Some(source_size as i64))
        .set_content_type(config.content_type.clone())
        .set_last_modified(Some(DateTime::from_secs(chrono::Utc::now().timestamp())))
        .build();

    let tagging = if config.disable_tagging {
        None
    } else {
        config.tagging.clone()
    };

    let object_checksum = ObjectChecksum {
        key: target_key.to_string(),
        version_id: None,
        checksum_algorithm: config.additional_checksum_algorithm.clone(),
        checksum_type: None,
        object_parts: None,
        final_checksum: None,
    };

    let _put_object_output = target
        .put_object(
            target_key,
            target_clone,
            target_key,
            source_size,
            None,
            get_object_output,
            tagging,
            Some(object_checksum),
            None,
            None,
            None,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    info!(target_key = target_key, "stdin transfer completed.");

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}
```

- [ ] **Step 2: Verify compile (will fail — callers not updated yet)**

Run: `cargo build`
Expected: compile error at the two CLI call sites (`src/bin/s3util/cli/mod.rs:293` and `:323`). That's expected and fixed in Task 3.

- [ ] **Step 3: Do NOT commit yet.** Commit after Task 3.

---

## Task 2: Refactor `s3_to_stdio::transfer` to accept injectable writer

**Files:**
- Modify: `src/transfer/s3_to_stdio.rs`

- [ ] **Step 1: Update the function signature**

At `src/transfer/s3_to_stdio.rs:20-26`, change the signature to accept a writer parameter (new parameter in bold):

```rust
pub async fn transfer(
    config: &Config,
    source: Storage,
    source_key: &str,
    mut writer: impl tokio::io::AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()>
```

- [ ] **Step 2: Replace the `stdout()` binding inside the body**

At `src/transfer/s3_to_stdio.rs:96`, replace:

```rust
    let mut stdout = tokio::io::stdout();
```

with (delete the line — the incoming `writer` parameter replaces it):

```rust
    // stdout is now the injected `writer` parameter from the caller
```

Then rename every subsequent occurrence of `stdout` to `writer` within this function body. There are three:
- `src/transfer/s3_to_stdio.rs:121` — `stdout.write_all(&buf[..n])` → `writer.write_all(&buf[..n])`
- `src/transfer/s3_to_stdio.rs:175` — `stdout.flush()` → `writer.flush()`
- (any other occurrences — confirm via grep `\bstdout\b` on the file; should be exactly these two plus the now-deleted binding)

`AsyncWriteExt` is already imported via `use tokio::io::{AsyncReadExt, AsyncWriteExt};` at line 3 — no import change needed.

- [ ] **Step 3: Verify compile (still will fail at CLI call sites)**

Run: `cargo build`
Expected: compile errors only at the two CLI call sites in `src/bin/s3util/cli/mod.rs`.

- [ ] **Step 4: Do NOT commit yet.** Commit after Task 3.

---

## Task 3: Update CLI call sites to pass `tokio::io::stdin()` / `tokio::io::stdout()`

**Files:**
- Modify: `src/bin/s3util/cli/mod.rs:293, :323`

- [ ] **Step 1: Update `stdio_to_s3::transfer` caller**

At `src/bin/s3util/cli/mod.rs:293`, the call currently passes (config, target, target_key, cancellation_token, stats_sender). Add `tokio::io::stdin()` between `target_key` and `cancellation_token`:

```rust
s3util_rs::transfer::stdio_to_s3::transfer(
    &config,
    target,
    &target_key,
    tokio::io::stdin(),
    cancellation_token.clone(),
    stats_sender.clone(),
)
.await
```

- [ ] **Step 2: Update `s3_to_stdio::transfer` caller**

At `src/bin/s3util/cli/mod.rs:323`, add `tokio::io::stdout()` between `source_key` and `cancellation_token`:

```rust
s3util_rs::transfer::s3_to_stdio::transfer(
    &config,
    source,
    &source_key,
    tokio::io::stdout(),
    cancellation_token.clone(),
    stats_sender.clone(),
)
.await
```

- [ ] **Step 3: Verify compile**

Run: `cargo build`
Expected: clean build.

- [ ] **Step 4: Verify no regressions in non-e2e tests**

Run: `cargo test --lib`
Expected: all existing unit tests pass.

- [ ] **Step 5: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`
Expected: no changes from fmt, no clippy warnings.

- [ ] **Step 6: Commit**

Ask human to review, then:

```bash
git add src/transfer/stdio_to_s3.rs src/transfer/s3_to_stdio.rs src/bin/s3util/cli/mod.rs
git commit -m "refactor(transfer): inject stdin/stdout into stdio transfers

Replaces hardcoded tokio::io::stdin()/stdout() with AsyncRead/AsyncWrite
parameters so tests can drive stdio transfers in-process and capture
output. CLI callers pass tokio::io::stdin()/stdout() unchanged.
"
```

---

## Task 4: Add `generate_random_bytes` helper

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add the helper function inside `impl TestHelper`**

Add the following method alongside `create_random_data_file` (which is around `tests/common/mod.rs:701`):

```rust
/// Generate an in-memory byte buffer of `size` bytes using the same seed
/// file as `create_random_data_file`. Deterministic across runs so
/// checksum/ETag assertions remain stable.
pub fn generate_random_bytes(size: usize) -> Result<Vec<u8>> {
    let mut seed_file = File::open(RANDOM_DATA_SEED_FILE)?;
    let mut seed_data = vec![0u8; 1024];
    seed_file.read_exact(&mut seed_data)?;

    let mut data = Vec::with_capacity(size);
    while data.len() + seed_data.len() <= size {
        data.extend_from_slice(&seed_data);
    }
    let remaining = size - data.len();
    if remaining > 0 {
        data.extend_from_slice(&seed_data[..remaining]);
    }

    Ok(data)
}
```

- [ ] **Step 2: Verify compile**

Run: `RUSTFLAGS="--cfg e2e_test" cargo build --tests`
Expected: clean build (the helper is dead code for now; `#![allow(dead_code)]` at the top of `common/mod.rs` suppresses the warning).

- [ ] **Step 3: Do NOT commit yet.** Commit after Task 5 together.

---

## Task 5: Add `cp_test_data_stdin_to_s3` and `cp_test_data_s3_to_stdout` helpers

**Files:**
- Modify: `tests/common/mod.rs`

**Context:** The existing `cp_test_data()` has `TransferDirection::StdioToS3` and `TransferDirection::S3ToStdio` branches (around `tests/common/mod.rs:1168-1241`) that call the transfer functions directly with no stdin/stdout injection. These branches are unusable in-process. Replace them with `unreachable!()` guards (since the new helpers are the only sanctioned entry points for stdio) and add the two new methods.

- [ ] **Step 1: Mark the old stdio branches unreachable**

Inside `cp_test_data()`, replace the bodies of the `StdioToS3` and `S3ToStdio` branches (lines ~1168-1241) with:

```rust
TransferDirection::StdioToS3 => {
    panic!(
        "use cp_test_data_stdin_to_s3 for stdin→S3 transfers in tests"
    );
}
TransferDirection::S3ToStdio => {
    panic!(
        "use cp_test_data_s3_to_stdout for S3→stdout transfers in tests"
    );
}
```

- [ ] **Step 2: Add `cp_test_data_stdin_to_s3` method**

Append this method to the `impl TestHelper` block (near the existing `cp_test_data`):

```rust
/// Run a stdin→S3 transfer with the given stdin bytes. Returns collected stats.
///
/// Use this instead of `cp_test_data` when the source is `-` (stdin).
pub async fn cp_test_data_stdin_to_s3(
    &self,
    args: Vec<&str>,
    stdin_bytes: Vec<u8>,
) -> StatsCount {
    let cli = parse_from_args(args).unwrap();
    let Commands::Cp(cp_args) = cli.command;
    let config = Config::try_from(cp_args).unwrap();

    let cancellation_token = create_pipeline_cancellation_token();
    let (stats_sender, stats_receiver) = async_channel::unbounded();

    let target_request_payer = if config.target_request_payer {
        Some(RequestPayer::Requester)
    } else {
        None
    };

    let (_source_key, target_key) = Self::extract_keys_for_test(
        &config,
        &TransferDirection::StdioToS3,
    );
    let has_warning = Arc::new(AtomicBool::new(false));

    let empty_target = match &config.target {
        StoragePath::S3 { bucket, .. } => StoragePath::S3 {
            bucket: bucket.clone(),
            prefix: String::new(),
        },
        other => other.clone(),
    };
    let target = S3StorageFactory::create(
        config.clone(),
        empty_target,
        cancellation_token.clone(),
        stats_sender.clone(),
        config.target_client_config.clone(),
        target_request_payer,
        None,
        None,
        has_warning.clone(),
        None,
    )
    .await;

    let reader = std::io::Cursor::new(stdin_bytes);
    let result = s3util_rs::transfer::stdio_to_s3::transfer(
        &config,
        target,
        &target_key,
        reader,
        cancellation_token.clone(),
        stats_sender.clone(),
    )
    .await;

    stats_sender.close();

    let mut stats = StatsCount::default();
    while let Ok(sync_stats) = stats_receiver.try_recv() {
        match sync_stats {
            SyncStatistics::SyncComplete { .. } => stats.sync_complete += 1,
            SyncStatistics::SyncError { .. } => stats.sync_error += 1,
            SyncStatistics::SyncWarning { .. } => stats.sync_warning += 1,
            SyncStatistics::ETagVerified { .. } => stats.e_tag_verified += 1,
            SyncStatistics::ChecksumVerified { .. } => stats.checksum_verified += 1,
            _ => {}
        }
    }

    if result.is_err() {
        stats.sync_error += 1;
    }

    stats
}
```

- [ ] **Step 3: Add `cp_test_data_s3_to_stdout` method**

Append this method:

```rust
/// Run an S3→stdout transfer. Returns (stats, captured_stdout_bytes).
///
/// Use this instead of `cp_test_data` when the target is `-` (stdout).
pub async fn cp_test_data_s3_to_stdout(
    &self,
    args: Vec<&str>,
) -> (StatsCount, Vec<u8>) {
    let cli = parse_from_args(args).unwrap();
    let Commands::Cp(cp_args) = cli.command;
    let config = Config::try_from(cp_args).unwrap();

    let cancellation_token = create_pipeline_cancellation_token();
    let (stats_sender, stats_receiver) = async_channel::unbounded();

    let source_request_payer = if config.source_request_payer {
        Some(RequestPayer::Requester)
    } else {
        None
    };

    let (source_key, _target_key) = Self::extract_keys_for_test(
        &config,
        &TransferDirection::S3ToStdio,
    );
    let has_warning = Arc::new(AtomicBool::new(false));

    let empty_source = match &config.source {
        StoragePath::S3 { bucket, .. } => StoragePath::S3 {
            bucket: bucket.clone(),
            prefix: String::new(),
        },
        other => other.clone(),
    };
    let source = S3StorageFactory::create(
        config.clone(),
        empty_source,
        cancellation_token.clone(),
        stats_sender.clone(),
        config.source_client_config.clone(),
        source_request_payer,
        None,
        None,
        has_warning.clone(),
        None,
    )
    .await;

    let mut writer = Vec::<u8>::new();
    let result = s3util_rs::transfer::s3_to_stdio::transfer(
        &config,
        source,
        &source_key,
        &mut writer,
        cancellation_token.clone(),
        stats_sender.clone(),
    )
    .await;

    stats_sender.close();

    let mut stats = StatsCount::default();
    while let Ok(sync_stats) = stats_receiver.try_recv() {
        match sync_stats {
            SyncStatistics::SyncComplete { .. } => stats.sync_complete += 1,
            SyncStatistics::SyncError { .. } => stats.sync_error += 1,
            SyncStatistics::SyncWarning { .. } => stats.sync_warning += 1,
            SyncStatistics::ETagVerified { .. } => stats.e_tag_verified += 1,
            SyncStatistics::ChecksumVerified { .. } => stats.checksum_verified += 1,
            _ => {}
        }
    }

    if result.is_err() {
        stats.sync_error += 1;
    }

    (stats, writer)
}
```

- [ ] **Step 4: Verify compile under the e2e_test cfg**

Run: `RUSTFLAGS="--cfg e2e_test" cargo build --tests`
Expected: clean build.

- [ ] **Step 5: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 6: Commit**

Ask human to review, then:

```bash
git add tests/common/mod.rs
git commit -m "test(common): add stdio-specific cp_test_data helpers

Adds generate_random_bytes, cp_test_data_stdin_to_s3, and
cp_test_data_s3_to_stdout. The two stdio branches of cp_test_data
are replaced with explicit panics directing callers to the new
helpers — the original branches were unusable in-process because
they called transfer() with the hardcoded real stdin/stdout.
"
```

---

## Task 6: Add 2 in-process smoke tests to `tests/e2e_stdio.rs`

**Files:**
- Modify: `tests/e2e_stdio.rs`

- [ ] **Step 1: Add `stdin_to_s3_basic_smoke` test**

Append inside the `mod tests` block of `tests/e2e_stdio.rs` (after the existing `s3_to_stdout` test):

```rust
/// In-process smoke: verifies `cp_test_data_stdin_to_s3` works end-to-end
/// with StatsCount collection. Does not assert any checksum/ETag — that is
/// covered by `e2e_stdio_integrity_check.rs`.
#[tokio::test]
async fn stdin_to_s3_basic_smoke() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let target = format!("s3://{}/smoke.txt", bucket);
    let stdin_bytes = b"stdin smoke test".to_vec();

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes.clone(),
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert!(helper.is_object_exist(&bucket, "smoke.txt", None).await);

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 2: Add `s3_to_stdout_basic_smoke` test**

Append:

```rust
/// In-process smoke: verifies `cp_test_data_s3_to_stdout` captures stdout
/// and returns StatsCount.
#[tokio::test]
async fn s3_to_stdout_basic_smoke() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let test_content = b"stdout smoke test";
    helper
        .put_object(&bucket, "smoke.txt", test_content.to_vec())
        .await;

    let source = format!("s3://{}/smoke.txt", bucket);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &source,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stdout_bytes, test_content);

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 3: Run the two new smoke tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio stdin_to_s3_basic_smoke s3_to_stdout_basic_smoke -- --nocapture`
Expected: both pass. The existing `stdin_to_s3` and `s3_to_stdout` subprocess tests are NOT invoked by this filter.

- [ ] **Step 4: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_stdio.rs
git commit -m "test(stdio): add in-process smoke tests using new helpers"
```

---

## Task 7: Create `tests/e2e_stdio_integrity_check.rs` + stdin→S3 checksum tests (10 tests)

**Files:**
- Create: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Create the file skeleton**

Create `tests/e2e_stdio_integrity_check.rs`:

```rust
#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    // ---------------------------------------------------------------
    // stdin → S3 — additional-checksum tests (5 algorithms × single/multipart)
    // ---------------------------------------------------------------
}
```

- [ ] **Step 2: Add the first stdin→S3 checksum test (SHA256 single-part) as the pattern reference**

Inside the `mod tests` block:

```rust
#[tokio::test]
async fn stdin_to_s3_checksum_sha256_single_part() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let target = format!("s3://{}/checksum.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 1);

    let head = helper.head_object(&bucket, "checksum.dat", None).await;
    assert!(head.checksum_sha256().is_some());

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 3: Add the matching multipart test (9 MiB → 2-part)**

```rust
#[tokio::test]
async fn stdin_to_s3_checksum_sha256_multipart() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/checksum.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 1);

    let head = helper.head_object(&bucket, "checksum.dat", None).await;
    assert!(head.checksum_sha256().is_some());

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 4: Add the 8 remaining checksum tests by varying the algorithm**

For each `(algo_const, head_accessor)` pair in the list below, create a `_single_part` and `_multipart` test by copying the two tests above and replacing `"SHA256"` with `algo_const` and `head.checksum_sha256()` with `head.<head_accessor>()`:

| Test name family | `--additional-checksum-algorithm` value | `head` accessor |
|---|---|---|
| `stdin_to_s3_checksum_crc32_{single_part\|multipart}` | `"CRC32"` | `checksum_crc32()` |
| `stdin_to_s3_checksum_crc32c_{single_part\|multipart}` | `"CRC32C"` | `checksum_crc32_c()` |
| `stdin_to_s3_checksum_crc64nvme_{single_part\|multipart}` | `"CRC64NVME"` | `checksum_crc64_nvme()` |
| `stdin_to_s3_checksum_sha1_{single_part\|multipart}` | `"SHA1"` | `checksum_sha1()` |
| `stdin_to_s3_checksum_sha256_{single_part\|multipart}` | (already done above) | `checksum_sha256()` |

Each test's body is identical to the two above except for those two substitutions. Follow the existing naming convention in `e2e_integrity_check.rs:528-1016` as reference.

- [ ] **Step 5: Verify compile**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check --no-run`
Expected: clean compile.

- [ ] **Step 6: Run one small test to confirm the pattern works**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check stdin_to_s3_checksum_sha256_single_part -- --nocapture`
Expected: PASS (requires AWS credentials).

- [ ] **Step 7: Run all 10 stdin→S3 checksum tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check stdin_to_s3_checksum -- --nocapture`
Expected: all 10 pass.

- [ ] **Step 8: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 9: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add stdin→S3 additional-checksum coverage

Covers 5 algorithms (CRC32, CRC32C, CRC64NVME, SHA1, SHA256) at both
single-part (1 KiB) and multipart (9 MiB at default 8 MiB threshold)
sizes, for 10 tests total.
"
```

---

## Task 8: Add stdin→S3 ETag tests (2 tests)

**Files:**
- Modify: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Add section marker and two tests**

Append inside the `mod tests` block:

```rust
// ---------------------------------------------------------------
// stdin → S3 — ETag verification (automatic, no SSE, no checksum)
// ---------------------------------------------------------------

#[tokio::test]
async fn stdin_to_s3_e_tag_single_part() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let target = format!("s3://{}/etag.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 0);

    let head = helper.head_object(&bucket, "etag.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(!etag.contains('-'), "single-part ETag should not contain '-N' suffix, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}

#[tokio::test]
async fn stdin_to_s3_e_tag_multipart() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/etag.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 0);

    let head = helper.head_object(&bucket, "etag.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-2"), "multipart ETag should contain '-2' for 2 parts, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 2: Run both tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check stdin_to_s3_e_tag -- --nocapture`
Expected: both pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add stdin→S3 ETag single-part and multipart tests"
```

---

## Task 9: Add stdin→S3 chunk-size / threshold tests (4 tests)

**Files:**
- Modify: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Add section marker and 4 tests**

Append inside the `mod tests` block:

```rust
// ---------------------------------------------------------------
// stdin → S3 — chunk-size / multipart-threshold behavior
// ---------------------------------------------------------------

#[tokio::test]
async fn stdin_to_s3_threshold_below_default() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(7 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/size.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);

    let head = helper.head_object(&bucket, "size.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(!etag.contains('-'), "7 MiB < 8 MiB threshold → single-part ETag, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}

#[tokio::test]
async fn stdin_to_s3_threshold_above_default() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/size.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);

    let head = helper.head_object(&bucket, "size.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-2"), "9 MiB > 8 MiB threshold → 2-part multipart, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}

#[tokio::test]
async fn stdin_to_s3_chunk_size_5mb() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/size.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);

    let head = helper.head_object(&bucket, "size.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-4"), "16 MiB / 5 MiB → 4 parts expected, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}

#[tokio::test]
async fn stdin_to_s3_chunk_size_8mb() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let stdin_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
    let target = format!("s3://{}/size.dat", bucket);

    let stats = helper
        .cp_test_data_stdin_to_s3(
            vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "-",
                &target,
            ],
            stdin_bytes,
        )
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);

    let head = helper.head_object(&bucket, "size.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-2"), "16 MiB / 8 MiB → 2 parts expected, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
}
```

- [ ] **Step 2: Run all 4 tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check stdin_to_s3_threshold stdin_to_s3_chunk_size -- --nocapture`
Expected: all 4 pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add stdin→S3 chunk-size and threshold tests"
```

---

## Task 10: Add S3→stdout checksum tests (10 tests)

**Files:**
- Modify: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Add section marker and the SHA256 single-part pattern test**

Append inside the `mod tests` block:

```rust
// ---------------------------------------------------------------
// S3 → stdout — additional-checksum tests (5 algorithms × single/multipart source)
// ---------------------------------------------------------------

#[tokio::test]
async fn s3_to_stdout_checksum_sha256_single_part() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    // Upload source object with the checksum algorithm so it has a stored checksum.
    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/download.dat", bucket);
    let upload_stats = helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;
    assert_eq!(upload_stats.sync_complete, 1);

    // Now download via stdout with --enable-additional-checksum.
    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--enable-additional-checksum",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 1);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}
```

- [ ] **Step 2: Add the matching multipart source test**

```rust
#[tokio::test]
async fn s3_to_stdout_checksum_sha256_multipart() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/download.dat", bucket);
    let upload_stats = helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--additional-checksum-algorithm",
            "SHA256",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;
    assert_eq!(upload_stats.sync_complete, 1);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--enable-additional-checksum",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 1);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}
```

- [ ] **Step 3: Add the 8 remaining checksum tests by varying the algorithm**

For each `algo_const` value in `["CRC32", "CRC32C", "CRC64NVME", "SHA1"]`, create `_single_part` and `_multipart` tests by copying the two above and replacing `"SHA256"` with the new value. Function names: `s3_to_stdout_checksum_crc32_{single_part|multipart}`, `s3_to_stdout_checksum_crc32c_{single_part|multipart}`, etc. No `head.checksum_*()` accessor is needed for these tests — the assertion is on `stats.checksum_verified == 1` and on `stdout_bytes == src_bytes`.

- [ ] **Step 4: Run all 10 tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check s3_to_stdout_checksum -- --nocapture`
Expected: all 10 pass.

- [ ] **Step 5: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add S3→stdout additional-checksum coverage

Covers 5 algorithms × single-part + multipart source, 10 tests total.
Each uploads via cp_test_data(local→S3) then downloads via stdout
with --enable-additional-checksum.
"
```

---

## Task 11: Add S3→stdout ETag tests (2 tests)

**Files:**
- Modify: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Add section marker and two tests**

Append inside the `mod tests` block:

```rust
// ---------------------------------------------------------------
// S3 → stdout — ETag verification (automatic)
// ---------------------------------------------------------------

#[tokio::test]
async fn s3_to_stdout_e_tag_single_part() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    helper
        .put_object(&bucket, "etag.dat", src_bytes.clone())
        .await;
    let s3_path = format!("s3://{}/etag.dat", bucket);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 0);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
}

#[tokio::test]
async fn s3_to_stdout_e_tag_multipart() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    // Upload a multipart object via local→S3 so the source has an -N ETag.
    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/etag.dat", bucket);
    let upload_stats = helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;
    assert_eq!(upload_stats.sync_complete, 1);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stats.checksum_verified, 0);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}
```

- [ ] **Step 2: Run both tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check s3_to_stdout_e_tag -- --nocapture`
Expected: both pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add S3→stdout ETag single-part and multipart tests"
```

---

## Task 12: Add S3→stdout source-part-count tests (3 tests)

**Files:**
- Modify: `tests/e2e_stdio_integrity_check.rs`

- [ ] **Step 1: Add section marker and 3 tests**

Append inside the `mod tests` block:

```rust
// ---------------------------------------------------------------
// S3 → stdout — source multipart layout variations
// ---------------------------------------------------------------

#[tokio::test]
async fn s3_to_stdout_source_multipart_5mb_parts() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/source.dat", bucket);
    let upload_stats = helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--multipart-threshold",
            "5MiB",
            "--multipart-chunksize",
            "5MiB",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;
    assert_eq!(upload_stats.sync_complete, 1);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stdout_bytes, src_bytes);

    let head = helper.head_object(&bucket, "source.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-4"), "16 MiB / 5 MiB → 4 parts expected, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn s3_to_stdout_source_multipart_8mb_parts() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/source.dat", bucket);
    let upload_stats = helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;
    assert_eq!(upload_stats.sync_complete, 1);

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stdout_bytes, src_bytes);

    let head = helper.head_object(&bucket, "source.dat", None).await;
    let etag = head.e_tag().unwrap();
    assert!(etag.contains("-2"), "16 MiB / 8 MiB → 2 parts expected, got: {etag}");

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn s3_to_stdout_source_threshold_boundary() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();

    // 7 MiB — below default 8 MiB threshold → single-part source
    let small_bytes = TestHelper::generate_random_bytes(7 * 1024 * 1024).unwrap();
    let small_file = local_dir.join("small.dat");
    std::fs::write(&small_file, &small_bytes).unwrap();
    let small_path = format!("s3://{}/small.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            small_file.to_str().unwrap(),
            &small_path,
        ])
        .await;

    let (stats_small, out_small) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &small_path,
            "-",
        ])
        .await;
    assert_eq!(stats_small.sync_complete, 1);
    assert_eq!(stats_small.e_tag_verified, 1);
    assert_eq!(out_small, small_bytes);
    let head_small = helper.head_object(&bucket, "small.dat", None).await;
    assert!(
        !head_small.e_tag().unwrap().contains('-'),
        "7 MiB source → single-part ETag"
    );

    // 9 MiB — above default 8 MiB threshold → 2-part multipart source
    let big_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
    let big_file = local_dir.join("big.dat");
    std::fs::write(&big_file, &big_bytes).unwrap();
    let big_path = format!("s3://{}/big.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            big_file.to_str().unwrap(),
            &big_path,
        ])
        .await;

    let (stats_big, out_big) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &big_path,
            "-",
        ])
        .await;
    assert_eq!(stats_big.sync_complete, 1);
    assert_eq!(stats_big.e_tag_verified, 1);
    assert_eq!(out_big, big_bytes);
    let head_big = helper.head_object(&bucket, "big.dat", None).await;
    assert!(
        head_big.e_tag().unwrap().contains("-2"),
        "9 MiB source → 2-part multipart ETag"
    );

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}
```

- [ ] **Step 2: Run all 3 tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_integrity_check s3_to_stdout_source -- --nocapture`
Expected: all 3 pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_integrity_check.rs
git commit -m "test(stdio): add S3→stdout source multipart layout tests"
```

---

## Task 13: Create `tests/e2e_stdio_sse.rs` + stdin→S3 SSE tests (4 tests)

**Files:**
- Create: `tests/e2e_stdio_sse.rs`

- [ ] **Step 1: Create the file with skeleton and all 4 stdin→S3 SSE tests**

Create `tests/e2e_stdio_sse.rs`:

```rust
#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::ServerSideEncryption;
    use common::*;

    use super::*;

    // ---------------------------------------------------------------
    // stdin → S3 — all SSE modes
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_sse_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--sse",
                    "AES256",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1); // SSE-S3 ETag is still MD5-based
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::Aes256
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--sse",
                    "aws:kms",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0); // KMS ETag not MD5-based
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--sse",
                    "aws:kms:dsse",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--target-sse-c",
                    "AES256",
                    "--target-sse-c-key",
                    TEST_SSE_C_KEY_1,
                    "--target-sse-c-key-md5",
                    TEST_SSE_C_KEY_1_MD5,
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        // SSE-C HeadObject requires the customer key; rather than round-trip,
        // just verify upload succeeded (object exists with SSE-C encryption).
        assert!(helper.is_object_exist(&bucket, "sse.dat", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
```

- [ ] **Step 2: Run all 4 tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_sse stdin_to_s3_ -- --nocapture`
Expected: all 4 pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_sse.rs
git commit -m "test(stdio): add stdin→S3 SSE tests (S3/KMS/DSSE-KMS/SSE-C)"
```

---

## Task 14: Add S3→stdout SSE tests (4 tests)

**Files:**
- Modify: `tests/e2e_stdio_sse.rs`

- [ ] **Step 1: Add section and 4 tests before the closing `}` of `mod tests`**

Append inside the `mod tests` block (still before the final `}`):

```rust
// ---------------------------------------------------------------
// S3 → stdout — all SSE modes
// ---------------------------------------------------------------

#[tokio::test]
async fn s3_to_stdout_sse_s3() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/sse.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--sse",
            "AES256",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 1);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn s3_to_stdout_sse_kms() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/sse.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--sse",
            "aws:kms",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 0); // KMS ETag not MD5-based
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn s3_to_stdout_dsse_kms() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper.create_bucket(&bucket, REGION).await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/sse.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--sse",
            "aws:kms:dsse",
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 0);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}

#[tokio::test]
async fn s3_to_stdout_sse_c() {
    TestHelper::init_dummy_tracing_subscriber();

    let helper = TestHelper::new().await;
    let bucket = TestHelper::generate_bucket_name();
    helper
        .create_bucket_with_sse_c_encryption(&bucket, REGION)
        .await;

    let local_dir = TestHelper::create_temp_dir();
    let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
    let upload_file = local_dir.join("upload.dat");
    std::fs::write(&upload_file, &src_bytes).unwrap();

    let s3_path = format!("s3://{}/sse.dat", bucket);
    helper
        .cp_test_data(vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-sse-c",
            "AES256",
            "--target-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--target-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            upload_file.to_str().unwrap(),
            &s3_path,
        ])
        .await;

    let (stats, stdout_bytes) = helper
        .cp_test_data_s3_to_stdout(vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3sync-e2e-test",
            "--source-sse-c",
            "AES256",
            "--source-sse-c-key",
            TEST_SSE_C_KEY_1,
            "--source-sse-c-key-md5",
            TEST_SSE_C_KEY_1_MD5,
            &s3_path,
            "-",
        ])
        .await;

    assert_eq!(stats.sync_complete, 1);
    assert_eq!(stats.sync_error, 0);
    assert_eq!(stats.sync_warning, 0);
    assert_eq!(stats.e_tag_verified, 0);
    assert_eq!(stats.checksum_verified, 0);
    assert_eq!(stdout_bytes, src_bytes);

    helper.delete_bucket_with_cascade(&bucket).await;
    let _ = std::fs::remove_dir_all(&local_dir);
}
```

- [ ] **Step 2: Run all 4 tests**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_sse s3_to_stdout -- --nocapture`
Expected: all 4 pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_sse.rs
git commit -m "test(stdio): add S3→stdout SSE tests (S3/KMS/DSSE-KMS/SSE-C)"
```

---

## Task 15: Create `tests/e2e_stdio_metadata.rs` with 3 tests

**Files:**
- Create: `tests/e2e_stdio_metadata.rs`

**Exact CLI flags used below** (verified against `tests/e2e_local_to_s3.rs:64-82, 1657-1708, 1966`):
`--content-type`, `--cache-control`, `--content-disposition`, `--content-encoding`, `--content-language`, `--expires`, `--website-redirect`, `--metadata`, `--tagging`, `--disable-tagging`.

The `verify_test_object_metadata` helper (in `tests/common/mod.rs`) asserts the full set of metadata and tags against the `TEST_*` constants already defined in `common/mod.rs:53-74`.

- [ ] **Step 1: Create the file**

Create `tests/e2e_stdio_metadata.rs`:

```rust
#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Upload from stdin with the full set of system and user metadata, then
    /// verify every field via `verify_test_object_metadata`.
    #[tokio::test]
    async fn stdin_to_s3_full_metadata_and_tags() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/full_meta.dat", bucket);

        // NOTE: adjust flag names to match the exact CLI syntax confirmed in Step 1.
        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--content-type",
                    TEST_CONTENT_TYPE,
                    "--cache-control",
                    TEST_CACHE_CONTROL,
                    "--content-disposition",
                    TEST_CONTENT_DISPOSITION,
                    "--content-encoding",
                    TEST_CONTENT_ENCODING,
                    "--content-language",
                    TEST_CONTENT_LANGUAGE,
                    "--expires",
                    TEST_EXPIRES,
                    "--website-redirect",
                    TEST_WEBSITE_REDIRECT,
                    "--metadata",
                    TEST_METADATA_STRING,
                    "--tagging",
                    TEST_TAGGING,
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        helper
            .verify_test_object_metadata(&bucket, "full_meta.dat")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Upload from stdin with a custom content-type and verify it round-trips.
    #[tokio::test]
    async fn stdin_to_s3_content_type_custom() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/ctype.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--content-type",
                    "application/json",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        let head = helper.head_object(&bucket, "ctype.dat", None).await;
        assert_eq!(head.content_type().unwrap(), "application/json");

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// `--disable-tagging` passed alongside `--tagging` should result in no
    /// tags on the uploaded object.
    #[tokio::test]
    async fn stdin_to_s3_disable_tagging() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/notag.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--tagging",
                    TEST_TAGGING,
                    "--disable-tagging",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        let tagging = helper.get_object_tagging(&bucket, "notag.dat", None).await;
        assert!(
            tagging.tag_set().is_empty(),
            "disable-tagging should strip tags, got: {:?}",
            tagging.tag_set()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
```

- [ ] **Step 2: Compile and run**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_metadata --no-run`
Expected: clean compile. If compile fails due to wrong flag name, fix per Step 1 research.

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio_metadata -- --nocapture`
Expected: all 3 pass.

- [ ] **Step 3: Verify formatting and lints**

Run: `cargo fmt && cargo clippy --all-features`

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_stdio_metadata.rs
git commit -m "test(stdio): add stdin→S3 metadata, content-type, disable-tagging tests"
```

---

## Task 16: Final verification

**Files:** none (verification only)

- [ ] **Step 1: Run the full new-test suite**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_stdio --test e2e_stdio_integrity_check --test e2e_stdio_sse --test e2e_stdio_metadata -- --nocapture`
Expected: 46 tests pass (2 existing subprocess + 2 smoke + 31 integrity + 8 SSE + 3 metadata = 46).

- [ ] **Step 2: Run the full e2e suite on author's sandbox**

Run: `RUSTFLAGS="--cfg e2e_test" cargo test -- --nocapture`
Expected: existing e2e tests still pass (ensures the transfer refactor did not regress).

- [ ] **Step 3: Final formatting and lint check**

Run: `cargo fmt && cargo clippy --all-features`
Expected: no changes from fmt, no clippy warnings.

- [ ] **Step 4: Human code review per CLAUDE.md**

Ask the human reviewer to look over the full branch diff. Address any feedback before merging.

---

## Summary

| Phase | Tasks | New Tests |
|---|---|---|
| 1. Library refactor | 1-3 | 0 |
| 2. Test helpers | 4-5 | 0 |
| 3. Smoke tests | 6 | 2 |
| 4. Integrity | 7-12 | 31 |
| 5. SSE | 13-14 | 8 |
| 6. Metadata | 15 | 3 |
| 7. Verification | 16 | 0 |
| **Total** | **16** | **44** (plus 2 kept) |
