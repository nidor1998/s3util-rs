# E2E Stdio Test Enhancement — Design

**Date:** 2026-04-18
**Status:** Approved (awaiting implementation plan)
**Scope:** Part A of a two-part ask. Part B (`--version-id` basic-pattern coverage across all directions) is deliberately excluded and will be designed in a separate spec.

## Context

`tests/e2e_stdio.rs` currently contains two tests that spawn `cargo run` subprocesses to verify stdin→S3 and S3→stdout transfers. They assert only object existence / stdout content, not checksum, ETag, SSE, chunk-size, or metadata behavior. The e2e-test-parity series (`2026-04-18-e2e-test-parity-phase2b` and `2026-04-17-e2e-test-quality-parity`) explicitly excluded stdio — this spec closes that gap.

Concretely, coverage is needed for:

- **stdin→S3**: all additional checksums; ETag validation (single-part and multipart); all SSE modes; chunk-size / threshold behavior; metadata, user metadata, tags; content-type.
- **S3→stdout**: all additional checksums; ETag validation (single-part and multipart); all SSE modes; source-object chunk-size variations.

## Non-Goals

- Tests for `--version-id` on stdio (or any direction) — deferred to the Part B spec.
- Server-side-copy tests involving stdio — unsupported by the CLI (already enforced in `args/mod.rs:80`).
- Negative tests for corrupted ETag / checksum — not part of the current request.
- Changes to assertion conventions established by the Phase 3 quality-parity spec — this design adopts them verbatim.

## Key Constraints Discovered During Design

1. **`cp_test_data()` cannot drive stdio in-process today.** The `StdioToS3` and `S3ToStdio` branches in `tests/common/mod.rs:1168-1241` call the transfer functions directly, which hardcode `tokio::io::stdin()` / `tokio::io::stdout()`. In an in-process test, stdin would hang and stdout would write to the real terminal. To enable `StatsCount`-based assertions (required by Phase 3 discipline), the transfer functions must accept injectable reader/writer parameters.

2. **Chunk-size behavior already exists for stdin.** `stdio_to_s3.rs` buffers all stdin into memory and passes the total size to `target.put_object(...)`. That path hits `UploadManager::upload` (`src/storage/s3/upload_manager.rs:155-165`), which checks `is_multipart_upload_required(source_total_size)` and dispatches to multipart or single-part. Tests should **verify** this existing behavior, not document an absence.

3. **SSE scope differs per direction.** stdin→S3 can set any of SSE-S3/KMS/DSSE-KMS/SSE-C on the target `put_object`. S3→stdout requires only the SSE-C client key on the source side; KMS/DSSE-KMS/S3 decrypt transparently.

## Design

### Section 1 — File structure & naming

```
tests/
├── e2e_stdio.rs                     (existing — keep 2 subprocess tests; add 2 in-process smoke tests)
├── e2e_stdio_integrity_check.rs     (NEW — 31 tests: checksums + ETag + chunk-size, both directions)
├── e2e_stdio_sse.rs                 (NEW — 8 tests: SSE-S3/KMS/DSSE-KMS/SSE-C, both directions)
└── e2e_stdio_metadata.rs            (NEW — 3 tests: stdin→S3 metadata / tags / content-type)
```

Mirrors the feature-split precedent already used by `e2e_multipart_integrity_check_*.rs`. Metadata is stdin→S3-only because S3→stdout has no metadata destination. Naming convention inside each file follows `e2e_integrity_check.rs`:

- `stdin_to_s3_<feature>_<variant>` — e.g., `stdin_to_s3_checksum_sha256_multipart`
- `s3_to_stdout_<feature>_<variant>` — e.g., `s3_to_stdout_sse_c`

### Section 2 — Library & test-helper changes

**`src/transfer/stdio_to_s3.rs`** — add a reader parameter:

```rust
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    reader: impl tokio::io::AsyncRead + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()>
```

Replace the internal `tokio::io::stdin()` with the parameter.

**`src/transfer/s3_to_stdio.rs`** — add a writer parameter:

```rust
pub async fn transfer(
    config: &Config,
    source: Storage,
    source_key: &str,
    writer: impl tokio::io::AsyncWrite + Unpin + Send,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()>
```

Replace the internal `tokio::io::stdout()` with the parameter.

**`src/bin/s3util/cli/mod.rs:293, :323`** — pass `tokio::io::stdin()` / `tokio::io::stdout()` at the two CLI call sites. Preserves current binary behavior.

**`tests/common/mod.rs`** — add two new public methods that internally call the refactored transfer functions with `Cursor<Vec<u8>>` / `Vec<u8>` in place of stdin/stdout:

- `cp_test_data_stdin_to_s3(&self, args: Vec<&str>, stdin_bytes: Vec<u8>) -> StatsCount`
- `cp_test_data_s3_to_stdout(&self, args: Vec<&str>) -> (StatsCount, Vec<u8>)`

The existing stdio branches of `cp_test_data()` are removed (they were unusable). Non-stdio callers of `cp_test_data()` are unaffected.

**`tests/common/mod.rs` — new helper**: `generate_random_bytes(size: usize) -> Vec<u8>` — seeded RNG for reproducible multipart-size buffers. Mirrors `create_random_data_file` but returns an in-memory buffer.

**No changes needed to**: `verify_test_object_metadata`, `verify_e_tag`, `head_object`, `get_object_bytes`, `create_bucket_with_sse_c_encryption`, `TEST_SSE_C_KEY_*` constants.

### Section 3 — Coverage matrix

Default `multipart_threshold` and `multipart_chunksize` are both 8 MiB (`src/config/mod.rs:153-154`).

#### `tests/e2e_stdio.rs` (existing — 2 kept + 2 new smoke)

| Test | Purpose |
|---|---|
| `stdin_to_s3` (existing) | Subprocess pipe; real CLI-stdin end-to-end |
| `s3_to_stdout` (existing) | Subprocess pipe; real CLI-stdout end-to-end |
| `stdin_to_s3_basic_smoke` (NEW) | In-process via `cp_test_data_stdin_to_s3`; confirms helper works |
| `s3_to_stdout_basic_smoke` (NEW) | In-process via `cp_test_data_s3_to_stdout`; confirms captured-stdout + StatsCount |

#### `tests/e2e_stdio_integrity_check.rs` (31 tests)

**Checksums — stdin→S3** (10 = 5 algorithms × {single-part, multipart}):

- `stdin_to_s3_checksum_{crc32|crc32c|crc64nvme|sha1|sha256}_{single_part|multipart}`
- Single-part: 1 KiB input → single `put_object`.
- Multipart: 9 MiB input at default 8 MiB threshold → 2-part multipart upload.
- Assert: `sync_complete == 1`, `sync_error == 0`, `sync_warning == 0`, `checksum_verified == 1`, and `head_object().checksum_<algo>().is_some()`.

**ETag — stdin→S3** (2):

- `stdin_to_s3_e_tag_{single_part|multipart}` — no checksum algo, no SSE (ETag verification is automatic on upload). Assert `e_tag_verified == 1`; for the multipart variant, also assert ETag has a `-N` suffix via `head_object()`.

**Chunk size / threshold — stdin→S3** (4):

- `stdin_to_s3_threshold_below_default` — 7 MiB input, default 8 MiB threshold → single-part ETag.
- `stdin_to_s3_threshold_above_default` — 9 MiB input, default threshold → 2-part ETag.
- `stdin_to_s3_chunk_size_5mb` — 16 MiB input, `--multipart-chunksize 5MiB --multipart-threshold 5MiB` → 4-part ETag.
- `stdin_to_s3_chunk_size_8mb` — 16 MiB input, default 8 MiB → 2-part ETag.

**Checksums — S3→stdout** (10 = 5 algorithms × {single-part source, multipart source}):

- `s3_to_stdout_checksum_{crc32|crc32c|crc64nvme|sha1|sha256}_{single_part|multipart}`
- Source object pre-uploaded via `cp_test_data(local→S3)` with `--additional-checksum-algorithm <ALGO>`.
- Download CLI passes `--enable-additional-checksum` (auto-detects algorithm from source response — matches convention in `e2e_integrity_check.rs::s3_to_local_checksum_*_roundtrip` tests).
- Assert: `sync_complete == 1`, `sync_error == 0`, `sync_warning == 0`, `checksum_verified == 1`, captured stdout bytes match source content.

**ETag — S3→stdout** (2):

- `s3_to_stdout_e_tag_{single_part|multipart}` — no checksum flag, no SSE. ETag verification is automatic on download. Assert `e_tag_verified == 1`.

**Source-part-count variations — S3→stdout** (3):

- `s3_to_stdout_source_multipart_5mb_parts` — source uploaded with 5 MiB parts (16 MiB → 4 parts).
- `s3_to_stdout_source_multipart_8mb_parts` — source uploaded with default 8 MiB parts (16 MiB → 2 parts).
- `s3_to_stdout_source_threshold_boundary` — source uploaded with size exactly at the threshold boundary (7 MiB single-part, then 9 MiB multipart) in two sub-assertions within one test.

#### `tests/e2e_stdio_sse.rs` (8 tests)

**stdin→S3** (4):

- `stdin_to_s3_sse_s3` — `--sse AES256`; standard bucket. Assert: `head_object().server_side_encryption() == Aes256`, `e_tag_verified == 1` (ETag is MD5-based under SSE-S3).
- `stdin_to_s3_sse_kms` — `--sse aws:kms` (AWS-managed default key, no key ARN); standard bucket. Assert: `head_object().server_side_encryption() == AwsKms`, `e_tag_verified == 0` (KMS ETag is not MD5-based), `checksum_verified == 0` (no checksum algo specified).
- `stdin_to_s3_dsse_kms` — `--sse aws:kms:dsse`; standard bucket. Assert: `head_object().server_side_encryption() == AwsKmsDsse`, `e_tag_verified == 0`, `checksum_verified == 0`.
- `stdin_to_s3_sse_c` — `--target-sse-c AES256 --target-sse-c-key <TEST_SSE_C_KEY_1> --target-sse-c-key-md5 <TEST_SSE_C_KEY_1_MD5>`; bucket via `create_bucket_with_sse_c_encryption`. Assert: `head_object().sse_customer_algorithm() == "AES256"`, `e_tag_verified == 0`, `checksum_verified == 0`.

**S3→stdout** (4):

- `s3_to_stdout_sse_s3` — source encrypted with `--sse AES256`; download succeeds; `e_tag_verified == 1`; stdout bytes match source.
- `s3_to_stdout_sse_kms` — source encrypted with `--sse aws:kms`; download succeeds; `e_tag_verified == 0`; stdout bytes match source. No client key needed (decryption transparent).
- `s3_to_stdout_dsse_kms` — source encrypted with `--sse aws:kms:dsse`; download succeeds; `e_tag_verified == 0`; stdout bytes match source.
- `s3_to_stdout_sse_c` — source encrypted with SSE-C; download passes `--source-sse-c`, `--source-sse-c-key`, `--source-sse-c-key-md5`; `e_tag_verified == 0`, `checksum_verified == 0`; stdout bytes match source. Bucket via `create_bucket_with_sse_c_encryption`.

#### `tests/e2e_stdio_metadata.rs` (3 tests, stdin→S3 only)

- `stdin_to_s3_full_metadata_and_tags` — all metadata flags + `--tags`; assert via `verify_test_object_metadata()`.
- `stdin_to_s3_content_type_custom` — `--content-type application/json`; assert `head_object().content_type() == "application/json"`.
- `stdin_to_s3_disable_tagging` — `--disable-tagging` alongside `--tags`; assert empty tag set via `get_object_tagging()`.

**Total: 46 tests (2 kept + 44 new).**

### Section 4 — Assertion discipline, data, and bucket setup

**Every new test asserts:**

- `stats.sync_complete == 1`
- `stats.sync_error == 0`
- `stats.sync_warning == 0`

Plus feature-specific assertions listed per-test in Section 3.

**Data generation:**

- Small (1 KiB) / multipart (9 MiB, 16 MiB) buffers via new `generate_random_bytes(size)`. Seeded for reproducibility so ETag/checksum assertions are deterministic.

**Source objects for S3→stdout:**

- Single-part: `helper.put_object(...)` with pre-generated bytes.
- Multipart: `cp_test_data(local→S3)` with explicit `--multipart-chunksize` / `--multipart-threshold` flags — reuses the existing transfer pipeline rather than reimplementing multipart upload.

**Bucket setup:**

- Non-SSE-C tests: `helper.create_bucket(&bucket, REGION)`.
- SSE-C tests: `helper.create_bucket_with_sse_c_encryption(&bucket, REGION)`.
- SSE-KMS / DSSE-KMS: standard bucket; `--sse aws:kms` and `--sse aws:kms:dsse` are passed without `--sse-kms-key-id`, which uses the AWS-managed default key — matches the existing pattern in `e2e_multipart_integrity_check_*_5mb_chunk.rs`.

**SSE-specific assertion rules** (consolidated — applied consistently across Sections 3 SSE tests):

| Mode | `e_tag_verified` | `checksum_verified` (when no checksum algo set) |
|---|---|---|
| SSE-S3 (`AES256`) | 1 (ETag is MD5-based) | 0 |
| SSE-KMS (`aws:kms`) | 0 (ETag not MD5-based) | 0 |
| DSSE-KMS (`aws:kms:dsse`) | 0 | 0 |
| SSE-C | 0 | 0 (checksum disabled under SSE-C) |

The checksum and ETag test families in Section 3 use **no SSE**, so they assert `checksum_verified == 1` / `e_tag_verified == 1` as listed.

**Cleanup / isolation:**

- Each test generates a unique bucket name via `generate_bucket_name` and calls `delete_bucket_with_cascade` at the end.
- `#[cfg(e2e_test)]` at file top and `#[tokio::test]` on each test, matching existing convention.
- `TestHelper::init_dummy_tracing_subscriber()` at the top of each test body.

### Section 5 — Build sequence

The implementation plan will execute in this order. Each phase is independently verifiable by `cargo build`, `cargo fmt`, `cargo clippy --all-features`.

**Phase 1 — Library refactor** (enables in-process testing)

1. Add `reader: impl AsyncRead + Unpin + Send` parameter to `src/transfer/stdio_to_s3.rs::transfer`; replace `tokio::io::stdin()` with the parameter.
2. Add `writer: impl AsyncWrite + Unpin + Send` parameter to `src/transfer/s3_to_stdio.rs::transfer`; replace `tokio::io::stdout()` with the parameter.
3. Update `src/bin/s3util/cli/mod.rs:293, :323` to pass `tokio::io::stdin()` / `tokio::io::stdout()`.
4. Verify: `cargo build`, `cargo fmt`, `cargo clippy --all-features`, non-e2e tests pass.

**Phase 2 — Test-helper additions**

5. Add `cp_test_data_stdin_to_s3(args, stdin_bytes)` and `cp_test_data_s3_to_stdout(args)` to `tests/common/mod.rs`; remove the unusable stdio branches from the original `cp_test_data`.
6. Add `generate_random_bytes(size)` helper.
7. Verify: `cargo build --tests`, existing tests compile.

**Phase 3 — Smoke tests**

8. Add `stdin_to_s3_basic_smoke` and `s3_to_stdout_basic_smoke` to `tests/e2e_stdio.rs`.
9. Run them to confirm the new helpers work end-to-end before expanding coverage.

**Phase 4 — Integrity tests**

10. Create `tests/e2e_stdio_integrity_check.rs` with the 31 checksum / ETag / chunk-size tests.
11. Verify: `cargo fmt`, `cargo clippy --all-features`, run a subset in e2e.

**Phase 5 — SSE tests**

12. Create `tests/e2e_stdio_sse.rs` with the 8 SSE tests.

**Phase 6 — Metadata tests**

13. Create `tests/e2e_stdio_metadata.rs` with the 3 metadata tests.

**Phase 7 — Final verification**

14. Run full e2e suite on the author's AWS sandbox.
15. `cargo fmt` and `cargo clippy --all-features` pass.
16. Human code review per `CLAUDE.md`.

## Summary

This design adds **46 tests** (2 kept + 44 new) across 4 test files and makes a **small library refactor** (2 function signatures, 2 CLI call sites) to enable `StatsCount`-based assertions on stdio transfers. Coverage maps 1:1 to the original request bullets: all additional checksums, ETag validation (single-part and multipart), all SSE modes, chunk-size / threshold behavior, metadata / user-metadata / tags, and content-type. `--version-id` coverage is a separate spec.
