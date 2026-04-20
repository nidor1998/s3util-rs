# S3→Local Parallel Downloads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enable parallel ranged downloads in `s3_to_local::transfer` by wiring it through the existing `first_chunk` helper and local storage's `put_object_multipart`, matching the pattern used by `local_to_s3` and `s3_to_s3`.

**Architecture:** Rewrite `src/transfer/s3_to_local.rs` to call `source.head_object()` first (for full object size + composite checksum), compute a first-chunk range via `first_chunk::get_first_chunk_range()`, issue a ranged `get_object()` for the first chunk, and pass the resulting `GetObjectOutput` (with `content_range=Some(…)`) into `target.put_object()`. The local storage already dispatches to `put_object_multipart` on `content_range.is_some()` and spawns parallel ranged-GET tasks bounded by `parallel_upload_semaphore` (sized by `max_parallel_uploads`). Add a small `detect_additional_checksum_with_head_object` helper to mirror the existing `detect_additional_checksum` for `GetObjectOutput`.

**Tech Stack:** Rust (edition 2024), Tokio, `aws-sdk-s3`, `anyhow`, `tracing`.

---

## File Structure

**Files modified (2):**

- `src/types/mod.rs` — add `detect_additional_checksum_with_head_object` (public function) and its four unit tests. One clear responsibility: inspect a `HeadObjectOutput` for S3 checksum fields and report the algorithm + value.
- `src/transfer/s3_to_local.rs` — rewrite `transfer()` body. The file's responsibility (move one S3 object to one local file) is unchanged; only the internal sequencing changes.

**Files NOT touched:**

- `src/transfer/first_chunk.rs` — already supports the S3-source → local-target path
- `src/storage/local/mod.rs` — `put_object_multipart` already parallelizes ranged downloads
- `src/config/` — no new fields, no new CLI flags
- Any other transfer module

Spec: `docs/superpowers/specs/2026-04-19-s3-to-local-parallel-downloads-design.md`

---

## Task 1: Commit the design spec

**Files:**
- New: `docs/superpowers/specs/2026-04-19-s3-to-local-parallel-downloads-design.md` (already written during brainstorming, not yet committed)

- [ ] **Step 1: Confirm the spec file is present and untracked**

Run: `git status docs/superpowers/specs/2026-04-19-s3-to-local-parallel-downloads-design.md`
Expected: shows as untracked (`??`).

- [ ] **Step 2: Ask user to review the spec, then commit**

Per `CLAUDE.md`: "Always ask a person to review the code before committing it." Pause and request review. After approval:

```bash
git add docs/superpowers/specs/2026-04-19-s3-to-local-parallel-downloads-design.md
git commit -m "docs: spec for s3_to_local parallel ranged downloads"
```

---

## Task 2: Add `detect_additional_checksum_with_head_object` helper

**Files:**
- Modify: `src/types/mod.rs` (add function after line 192, add tests after line 492)

- [ ] **Step 1: Write the four failing tests**

Append these tests inside the existing `#[cfg(test)] mod tests { ... }` block in `src/types/mod.rs`, directly after the existing `detect_additional_checksum_returns_crc64nvme_when_only_one_present` test (around line 492):

```rust
    #[test]
    fn detect_additional_checksum_with_head_object_returns_none_when_no_checksum_present() {
        let head = HeadObjectOutput::builder().build();
        assert!(detect_additional_checksum_with_head_object(&head).is_none());
    }

    #[test]
    fn detect_additional_checksum_with_head_object_returns_sha256_when_present() {
        let head = HeadObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .build();
        let (algo, value) = detect_additional_checksum_with_head_object(&head).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Sha256));
        assert_eq!(value, "sha256-value");
    }

    #[test]
    fn detect_additional_checksum_with_head_object_prefers_explicit_over_auto_added_crc64nvme() {
        // S3 may auto-add CRC64NVME alongside an explicitly chosen algorithm.
        // Per the function's documented contract, the explicit choice wins.
        let head = HeadObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .checksum_crc64_nvme("crc64-value")
            .build();
        let (algo, value) = detect_additional_checksum_with_head_object(&head).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Sha256));
        assert_eq!(value, "sha256-value");
    }

    #[test]
    fn detect_additional_checksum_with_head_object_returns_crc64nvme_when_only_one_present() {
        let head = HeadObjectOutput::builder()
            .checksum_crc64_nvme("crc64-value")
            .build();
        let (algo, value) = detect_additional_checksum_with_head_object(&head).unwrap();
        assert!(matches!(algo, ChecksumAlgorithm::Crc64Nvme));
        assert_eq!(value, "crc64-value");
    }
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --all-features --lib -- types::tests::detect_additional_checksum_with_head_object`
Expected: four failures with "cannot find function `detect_additional_checksum_with_head_object` in this scope".

- [ ] **Step 3: Add the helper function**

Insert this function in `src/types/mod.rs` immediately after the closing `}` of `detect_additional_checksum` (at line 192, before `pub fn get_additional_checksum`):

```rust
/// Detect which checksum algorithm the source object uses by inspecting the HeadObjectOutput fields.
/// Returns the algorithm and its value if found.
///
/// Priority favors explicitly user-chosen algorithms (SHA256/SHA1/CRC32/CRC32C) over CRC64NVME,
/// which S3 often auto-adds to objects uploaded with a different explicit algorithm. This ensures
/// a multipart object uploaded with `--additional-checksum-algorithm SHA256` is verified with
/// SHA256, not with the auto-added full-object CRC64NVME.
pub fn detect_additional_checksum_with_head_object(
    head_object_output: &HeadObjectOutput,
) -> Option<(ChecksumAlgorithm, String)> {
    if let Some(v) = head_object_output.checksum_sha256() {
        return Some((ChecksumAlgorithm::Sha256, v.to_string()));
    }
    if let Some(v) = head_object_output.checksum_sha1() {
        return Some((ChecksumAlgorithm::Sha1, v.to_string()));
    }
    if let Some(v) = head_object_output.checksum_crc32_c() {
        return Some((ChecksumAlgorithm::Crc32C, v.to_string()));
    }
    if let Some(v) = head_object_output.checksum_crc32() {
        return Some((ChecksumAlgorithm::Crc32, v.to_string()));
    }
    if let Some(v) = head_object_output.checksum_crc64_nvme() {
        return Some((ChecksumAlgorithm::Crc64Nvme, v.to_string()));
    }
    None
}
```

(`HeadObjectOutput` is already imported on line 8.)

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test --all-features --lib -- types::tests::detect_additional_checksum_with_head_object`
Expected: 4 passed.

- [ ] **Step 5: Format and lint**

Run: `cargo fmt --all --check`
Expected: no output (exit 0).

Run: `cargo clippy --all-features -- -D warnings`
Expected: no warnings, exit 0.

- [ ] **Step 6: Ask user to review, then commit**

Per `CLAUDE.md`: "Always ask a person to review the code before committing it." Pause and request review of the change. After approval:

```bash
git add src/types/mod.rs
git commit -m "feat: add detect_additional_checksum_with_head_object helper

Mirrors detect_additional_checksum for HeadObjectOutput. s3_to_local
needs this to detect the source's composite checksum algorithm from
a HEAD response, because a ranged GET against a composite-multipart
object does not carry the root composite checksum."
```

---

## Task 3: Rewrite `s3_to_local::transfer` to use first-chunk + parallel multipart path

**Files:**
- Modify: `src/transfer/s3_to_local.rs` (full rewrite of `transfer()`)

**Why no pre-written unit test:** `transfer()` orchestrates `Storage` trait calls that require S3 interaction. It is covered end-to-end by the e2e suites (see Task 5). The helper in Task 2 has direct unit tests.

- [ ] **Step 1: Replace the contents of `src/transfer/s3_to_local.rs`**

Overwrite the entire file with:

```rust
use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{debug, info};

use crate::Config;
use crate::storage::Storage;
use crate::transfer::first_chunk;
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, detect_additional_checksum_with_head_object};

/// Transfer an S3 object to the local filesystem.
///
/// Uses first-chunk optimization: for objects at or above `multipart_threshold`,
/// only the first chunk is fetched initially via a range request. Local storage's
/// `put_object_multipart` then fetches remaining chunks in parallel (bounded by
/// `max_parallel_uploads`) and writes them to the destination file at their
/// offsets. For smaller objects, a single non-ranged `get_object` is issued.
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    source_key: &str,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // HEAD the source to learn the full object size and the composite/final
    // checksum. A ranged GET against a composite-multipart object does not
    // return the root composite checksum; HEAD does.
    let head_object_output = source
        .head_object(
            source_key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context(format!(
            "failed to get source object metadata: {source_key}"
        ))?;

    let source_size = head_object_output.content_length().unwrap_or(0);

    // Auto-detect checksum algorithm from HEAD. `additional_checksum_algorithm`
    // is rejected at CLI validation when the target is local, so only the mode
    // path applies.
    let (detected_algorithm, source_additional_checksum) =
        if config.additional_checksum_mode.is_some() {
            detect_additional_checksum_with_head_object(&head_object_output)
                .map(|(a, c)| (Some(a), Some(c)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    // Compute the first-chunk range. Returns None for objects below
    // `multipart_threshold` (or below 5 MiB absolute minimum) — in that case
    // we perform a single full-object GET just like before.
    let range = first_chunk::get_first_chunk_range(
        &*source,
        config,
        source_size,
        source_key,
        config.version_id.clone(),
    )
    .await?;

    debug!(
        key = source_key,
        size = source_size,
        range = range.as_deref(),
        "first chunk range for the object",
    );

    let get_object_output = source
        .get_object(
            source_key,
            config.version_id.clone(),
            config.additional_checksum_mode.clone(),
            range.clone(),
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context(format!("failed to download source object: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    // Build object checksum. `build_object_checksum` branches on
    // content_range being populated to fetch the full part-size manifest when
    // needed for composite checksum verification. Pass `source_key` (the S3
    // key) so `get_object_parts_attributes` looks up parts on the source.
    let checksum_algorithms: Option<Vec<_>> =
        detected_algorithm.as_ref().map(|a| vec![a.clone()]);
    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        source_key,
        &get_object_output,
        checksum_algorithms.as_deref(),
        source_additional_checksum.clone(),
    )
    .await?;

    // Local storage dispatches on `content_range.is_some()`: when true, it
    // routes into `put_object_multipart`, which spawns parallel ranged GETs
    // against the source for the remaining chunks and writes each at the
    // correct offset in a temp file. `source_size` is the full object size
    // (not the first-chunk size) — required by the multipart write loop.
    let _put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
            get_object_output,
            None, // local storage ignores tagging
            object_checksum,
            None,
        )
        .await
        .context(format!("failed to write to target file: {target_key}"))?;

    info!(
        source_key = source_key,
        target_key = target_key,
        size = source_size,
        "transfer completed."
    );

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}
```

- [ ] **Step 2: Verify the crate builds**

Run: `cargo build --all-features`
Expected: successful build, no errors, no warnings.

- [ ] **Step 3: Run the full unit test suite**

Run: `cargo test --all-features --lib`
Expected: all tests pass (including the 4 new ones from Task 2).

- [ ] **Step 4: Format and lint**

Run: `cargo fmt --all --check`
Expected: no output (exit 0).

Run: `cargo clippy --all-features -- -D warnings`
Expected: no warnings, exit 0.

- [ ] **Step 5: Verify e2e test code still compiles under the `e2e_test` cfg**

Per `CLAUDE.md`, do not run e2e tests (they hit real AWS). Only verify they compile:

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features`
Expected: successful check, no errors.

- [ ] **Step 6: Ask user to review, then commit**

Per `CLAUDE.md`: "Always ask a person to review the code before committing it." Pause and request review of the change. After approval:

```bash
git add src/transfer/s3_to_local.rs
git commit -m "feat: parallelize S3→local downloads via ranged GETs

s3_to_local previously streamed the full object through a single
non-ranged GET. Wire it through the existing first-chunk helper and
local storage's put_object_multipart — the same parallel-download
infrastructure used by local_to_s3 and s3_to_s3 for their multipart
uploads. Objects at or above multipart_threshold are now fetched in
parallel chunks bounded by max_parallel_uploads; smaller objects
continue to use a single GET.

No new config, no new traits, no new CLI flags."
```

---

## Task 4: Final pre-e2e verification (implementer)

**Files:** none (verification only)

- [ ] **Step 1: Run the full pre-commit check matrix**

All of these must pass; any failure blocks the task.

```bash
cargo fmt --all --check
cargo clippy --all-features -- -D warnings
cargo test --all-features
RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features
```

Expected: each command exits 0 with no warnings.

---

## Task 5: User-run e2e validation

- [ ] **Step 1: Ask the user to run the relevant e2e suites**

Per `CLAUDE.md`, e2e tests are for the user to run. Inform the user that the following suites are the ones most relevant to validating this change, and that all must pass:

- `tests/e2e_s3_to_local.rs`
- `tests/e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_16mb_file_8mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_30mb_file_8mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs`
- `tests/e2e_multipart_integrity_check_edge_case.rs`
- `tests/e2e_roundtrip_checksum.rs`
- `tests/e2e_roundtrip_multipart_etag.rs`
- `tests/e2e_roundtrip_s3_to_s3.rs`
- `tests/e2e_roundtrip_local_to_s3.rs`
- `tests/e2e_cancel_test.rs`
- `tests/e2e_special_characters.rs`

Suggested user command:

```bash
RUSTFLAGS="--cfg e2e_test" cargo test --all-features --test e2e_s3_to_local -- --test-threads=1 --nocapture
```

(And the equivalents for each file above, or simply run the whole e2e suite.)

Expected: all pass. If any fail, investigate — most likely culprits are checksum detection mismatches or a first-chunk range off-by-one.

---

## Notes for the implementer

- **Do NOT run e2e tests yourself** — `CLAUDE.md` forbids it. Use `cargo check` under the `e2e_test` cfg to validate compilation only.
- **Commit boundaries**: each implementation task (1, 2, 3) produces one commit. Don't batch Task 2 (helper) and Task 3 (rewrite) into one commit — the helper commit should land first so the rewrite is a narrow diff.
- **Small objects continue to work unchanged**: `get_first_chunk_range` returns `None` when `source_size < multipart_threshold` (or < 5 MiB floor). The resulting non-ranged `get_object` produces a `GetObjectOutput` with `content_range=None`, which `put_object` routes to `put_object_single_part` — the exact path the current code takes.
- **Order matters**: `head_object` first, then `get_first_chunk_range` (which may also call `head_object_first_part` under `--auto-chunksize`), then ranged `get_object`, then `validate_content_range`. Do not reorder these.
- **`version_id` must be threaded through all three S3 calls** (`head_object`, `get_first_chunk_range`, `get_object`). The existing per-chunk tasks inside `put_object_multipart` already handle version_id internally.
