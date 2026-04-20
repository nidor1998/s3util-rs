# Parallel Ranged Downloads in `s3_to_local`

## Problem

`src/transfer/s3_to_local.rs` currently issues a single `source.get_object()` call with `range=None` for every transfer. For large objects this streams the entire body through a single HTTP connection, leaving `max_parallel_uploads` and the `parallel_upload_semaphore` unused and falling well short of the throughput that `s3sync` achieves for the same workload.

The sibling transfer modules (`local_to_s3`, `s3_to_s3`) already fetch only the first chunk via a range request and let the target's multipart path pull remaining chunks in parallel. `s3_to_local` is the only transfer that skipped that pattern.

## Approach

Wire `s3_to_local.rs` into the existing first-chunk + parallel-multipart infrastructure, mirroring `local_to_s3.rs` step-for-step. The local storage's `put_object_multipart` (in `src/storage/local/mod.rs`) already performs parallel ranged `get_object` calls against the source, gated by the existing `parallel_upload_semaphore` (sized by `max_parallel_uploads`). We just need to trigger that path by supplying a `GetObjectOutput` whose `content_range` is set — which happens automatically when we pass a first-chunk `range` into `source.get_object()`.

Scope is intentionally minimal:

- No new CLI flag; reuse the existing `--max-parallel-uploads` knob (its help text already says "uploads/downloads").
- No changes to `first_chunk.rs`, `storage/local/mod.rs`, or config.
- One small helper added: `detect_additional_checksum_with_head_object`, mirroring the existing `detect_additional_checksum` for `GetObjectOutput`.

## Architecture

### Before

```
head_object                     (skipped)
get_object(range=None)          full object bytes streamed in one response
put_object → put_object_single_part
```

### After

```
head_object(no range)           → source_size + composite/final checksum
get_first_chunk_range(size)     → Some("bytes=0-N") when size ≥ multipart_threshold
                                  None when size < multipart_threshold  (identical to today)
get_object(range=Some(…))       → first chunk only
validate_content_range
put_object → put_object_multipart → FuturesUnordered of spawned tasks;
                                    each task does get_object(range="bytes=M-K")
                                    and writes at offset M;
                                    bounded by parallel_upload_semaphore
                                    (size = max_parallel_uploads)
```

For objects under the threshold, `get_first_chunk_range` returns `None` and the existing single-part path is used — behavior is byte-for-byte identical to today.

## Changes

### 1. `src/transfer/s3_to_local.rs` (rewrite)

The new `transfer()` body follows the same shape as `local_to_s3::transfer()`:

```rust
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

    // 1. HEAD for full size + composite checksum (no range).
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

    // 2. Detect checksum algorithm + value from HEAD (not GET).
    //    A ranged GET against a composite-multipart object does not carry the
    //    root composite checksum; HEAD does.
    let (detected_algorithm, source_additional_checksum) =
        if config.additional_checksum_mode.is_some() {
            detect_additional_checksum_with_head_object(&head_object_output)
                .map(|(a, c)| (Some(a), Some(c)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

    // 3. First-chunk range (None for small objects — preserves current behavior).
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

    // 4. GET (ranged when size ≥ multipart_threshold).
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

    // 5. Build object_checksum as before (now with content_range populated when ranged).
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

    // 6. put_object: local storage dispatches on content_range.is_some() and
    //    routes into put_object_multipart, which spawns parallel ranged GETs.
    let _put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
            get_object_output,
            None,
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

### 2. `src/types/mod.rs` (new helper)

Added directly below `detect_additional_checksum`:

```rust
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

Priority order matches `detect_additional_checksum` exactly — the same rationale applies (prefer an explicitly-chosen algorithm over the auto-added CRC64NVME that S3 can inject alongside it).

Unit tests are added next to the existing `detect_additional_checksum_*` tests:

- `detect_additional_checksum_with_head_object_returns_none_when_no_checksum_present`
- `detect_additional_checksum_with_head_object_returns_sha256_when_present`
- `detect_additional_checksum_with_head_object_prefers_explicit_over_auto_added_crc64nvme`
- `detect_additional_checksum_with_head_object_returns_crc64nvme_when_only_one_present`

## What reuses existing code verbatim

- `first_chunk::get_first_chunk_range` — already handles the S3-source → local-target branch (the existing code path that was previously never exercised by `s3_to_local`).
- `first_chunk::validate_content_range`.
- `first_chunk::build_object_checksum` — already branches correctly when `content_range` is populated, populating `object_parts` via `get_object_parts_attributes` for composite verification.
- `LocalStorage::put_object_multipart` in `src/storage/local/mod.rs` — already performs parallel ranged GETs via `FuturesUnordered`, writes each chunk at its offset via `seek(SeekFrom::Start(offset))`, verifies composite ETag / checksum after all tasks complete, and drives cancellation via `self.cancellation_token`.
- `parallel_upload_semaphore` — the same semaphore that bounds `local_to_s3` upload parallelism.

## What does NOT change

- `first_chunk.rs` — no changes.
- `src/storage/local/mod.rs` — no changes; its `put_object_multipart` already implements parallel ranged downloads.
- `src/config/` — no new fields, no new CLI args.
- `StorageTrait` — no new methods.
- Transfer modules other than `s3_to_local.rs` — unchanged.
- `s3_to_local::transfer` signature — unchanged.

## Edge cases

**Small objects (< 5 MiB or < `multipart_threshold`):** `get_first_chunk_range` returns `None`. `get_object` is called with `range=None`. `put_object` dispatches to `put_object_single_part`. Byte-for-byte identical to current behavior.

**Versioned objects:** `config.version_id` is threaded into `head_object`, `get_object`, and — via `get_first_chunk_range` — into `head_object_first_part` when `--auto-chunksize` is in effect. The parallel ranged-GET tasks inside `put_object_multipart` reuse the same `source_version_id` (already plumbed there for `local_to_s3`).

**SSE-C:** `head_object`, `get_object`, and the per-chunk GETs inside `put_object_multipart` all receive `source_sse_c`, `source_sse_c_key`, and `source_sse_c_key_md5`. Already plumbed.

**`--auto-chunksize`:** `get_first_chunk_range` calls `head_object_first_part` to learn the source's first-part size. `build_object_checksum → get_object_parts_if_necessary` fetches the full part-size manifest so `verify_local_file` can reconstruct the composite checksum / ETag over the written file using the source's part boundaries.

**Cancellation:**
- Two explicit `if cancellation_token.is_cancelled() { return Ok(()); }` guards, matching today's shape: one at function entry, one after the first `get_object` returns (this is also the guard before `put_object` is invoked).
- Inside `put_object_multipart`, each per-chunk task checks the token during streaming, and the spawn loop checks it before each `acquire_owned()` on the semaphore. Already wired.

**Failure modes:**
- `head_object` failure → `Err` with context `"failed to get source object metadata: {source_key}"`.
- `get_object` (first chunk) failure → `Err` with existing context `"failed to download source object: {source_key}"`.
- `validate_content_range` mismatch → `Err` bubbled up.
- Any per-chunk ranged-GET failure inside `put_object_multipart` → propagates via `upload_parts_join_handles.next().await`; the temp file is discarded (never persisted).
- Partial-write invariant: `put_object_multipart` persists the temp file only after *all* spawned tasks complete successfully AND `total_upload_size == source_size`.

## Testing

### New unit tests

Four tests for `detect_additional_checksum_with_head_object` in `src/types/mod.rs`, mirroring the existing `detect_additional_checksum_*` tests.

### Existing tests that already cover the behavior

No new e2e files are required. The change is a routing flip in `s3_to_local.rs`; these suites already exercise the flow and will automatically route through the new parallel path for objects at or above the default 8 MiB `multipart_threshold`:

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

Per `CLAUDE.md`, the user runs e2e tests; during implementation we verify compilation only:

```
RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features
```

### Pre-commit verification

- `cargo fmt --all --check`
- `cargo clippy --all-features -- -D warnings`
- `cargo test --all-features`
- `RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features`

## Dependencies (already in s3util-rs)

- `first_chunk::get_first_chunk_range` — `src/transfer/first_chunk.rs`
- `first_chunk::validate_content_range` — `src/transfer/first_chunk.rs`
- `first_chunk::build_object_checksum` — `src/transfer/first_chunk.rs`
- `LocalStorage::put_object_multipart` — `src/storage/local/mod.rs`
- `detect_additional_checksum` (existing) — `src/types/mod.rs` (reference for the new helper)
- `parallel_upload_semaphore` — `src/config/mod.rs::ClientConfig`
- `max_parallel_uploads` — `src/config/mod.rs::Config` (already a CLI flag, help text already mentions downloads)
