# Streaming Multipart Upload for stdin Source

## Problem

`src/transfer/stdio_to_s3.rs::transfer` drains the full stdin reader with `read_to_end` before handing the buffer to `target.put_object()`. For very large inputs this holds the entire payload in memory, which is a significant OOM risk.

When the input fits below `multipart_threshold`, buffering is acceptable — the single-part upload needs the size up front anyway. When the input exceeds `multipart_threshold`, the upload will be multipart, and there is no structural reason to buffer the whole stream: multipart parts can be uploaded as chunks are read.

## Approach

Probe up to `multipart_threshold` bytes from the reader, then branch:

- If the reader hit EOF before reaching the threshold, fall into the existing in-memory single-part path. Behavior is byte-for-byte identical to today.
- If the probe reaches the threshold, switch to a new streaming multipart path that reads chunks from the reader on demand and uploads them with bounded parallelism.

Memory in the streaming path is bounded by `multipart_chunksize × parallel_upload_semaphore_permits` — one chunk buffer per in-flight upload task, capped by the same semaphore that already governs `local_to_s3` multipart uploads.

The upload manager is extended with a new `upload_stream` entry point that accepts a `Box<dyn AsyncRead>` and an unknown total size. All per-part MD5, additional-checksum, and tagging logic is reused.

## Architecture

### Probe-then-switch flow in `stdio_to_s3::transfer`

```
stdin reader
    │
    ▼
probe_up_to(reader, multipart_threshold) → Vec<u8>
    │
    ├── len < threshold  (EOF first)
    │       │
    │       ▼
    │   transfer_buffered(config, target, key, buffer, ...)
    │       │
    │       ▼
    │   target.put_object(...)  ── existing single-part path, unchanged
    │
    └── len == threshold  (more data possible)
            │
            ▼
        Cursor::new(buffer).chain(reader) → chained AsyncRead
            │
            ▼
        target.put_object_stream(key, chained, tagging, object_checksum, ...)
            │
            ▼
        UploadManager::upload_stream(bucket, key, chained)
            │
            ▼
        CreateMultipartUpload → upload_parts_stream → CompleteMultipartUpload
```

### Memory bound

Streaming path holds at most:

- One `Vec<u8>` of size `chunksize` per spawned upload_part task.
- Number of simultaneously-spawned tasks is capped by `config.target_client_config.parallel_upload_semaphore`.

Total peak ≈ `multipart_chunksize × parallel_upload_semaphore_permits`, independent of total input size.

## Changes

### `src/transfer/stdio_to_s3.rs`

Refactor `transfer` into three pieces:

1. **`probe_up_to`** — helper that reads up to `limit` bytes using `reader.take(limit as u64).read_to_end(&mut buf)`. Returns the filled `Vec<u8>`. A return where `buf.len() < limit` means the reader hit EOF; `buf.len() == limit` means the limit was reached and there may be more data.

2. **`transfer_buffered`** — the current body of `transfer` extracted into a helper that accepts an already-probed `Vec<u8>` instead of calling `read_to_end` itself. All existing logic preserved: `compute_source_checksum`, synthetic `GetObjectOutput` construction, tagging, `target.put_object` call. No behavioral change vs the current code for inputs below `multipart_threshold`.

3. **Streaming branch** — when the probe returns a full buffer:
   - Construct `chained = Box::new(Cursor::new(buffer).chain(reader))`.
   - Build the `ObjectChecksum` skeleton (same shape as today's lines 103–110: `checksum_algorithm` set, others `None`).
   - Call `target.put_object_stream(target_key, chained, tagging, Some(object_checksum_skeleton), None)`.
   - On success, emit `SyncStatistics::SyncComplete` to `stats_sender`.

### `src/storage/mod.rs` (`StorageTrait`)

Add one new method with a default impl that errors:

```rust
async fn put_object_stream(
    &self,
    key: &str,
    reader: Box<dyn AsyncRead + Send + Unpin>,
    tagging: Option<String>,
    object_checksum: Option<ObjectChecksum>,
    if_none_match: Option<String>,
) -> Result<PutObjectOutput> {
    Err(anyhow!("put_object_stream not supported on this storage"))
}
```

`put_object` and all other trait methods are unchanged.

### `src/storage/s3/mod.rs`

Implement `put_object_stream` for `S3Storage`. It builds an `UploadManager` with:

- `source_total_size: None`
- `source_additional_checksum: None`
- `object_parts: None`

…then calls the new `UploadManager::upload_stream(bucket, key, reader)`. Returns the `PutObjectOutput` produced by the completion path.

### `src/storage/local/mod.rs`

No change. `LocalStorage` gets the default `put_object_stream` impl (returns error). stdin-to-local is not a valid transfer mode in s3util-rs cp.

### `src/storage/s3/upload_manager.rs`

**Field type change:**

- `source_total_size: u64` → `source_total_size: Option<u64>`
- `UploadManager::new` accepts `Option<u64>`. Existing call sites (`local_to_s3`, `s3_to_s3`, and the single-part stdin path) pass `Some(n)`. Streaming stdin passes `None`.

**Sites that read `source_total_size`** (lines 146, 165, 392, 587, 614, 641, 922, 930, 1280, 1288, 1324, 1462, 1463, 1472, 1516):

- `upload()` entry point (lines 146, 165): guarded by `if let Some(n) = self.source_total_size` since it runs only for the known-size path. Unchanged semantics.
- Validation and preallocation sites (392, 922, 930, 1280, 1288, 1324, 1462, 1463, 1472, 1516): inside code paths that are only reached via `upload()`, which is only called when size is known. These can `.expect("known size in upload() path")` or be left as `.unwrap()` — justified by the invariant that `upload_stream()` never reaches these sites.

**New entry point: `upload_stream`**:

```rust
pub async fn upload_stream(
    &mut self,
    bucket: &str,
    key: &str,
    reader: Box<dyn AsyncRead + Send + Unpin>,
) -> Result<PutObjectOutput> {
    // Always multipart (size unknown, probe already confirmed >= threshold).
    let create_multipart_upload_output =
        self.create_multipart_upload(bucket, key).await?;
    let upload_id = create_multipart_upload_output.upload_id().unwrap();

    let completed_parts = self
        .upload_parts_stream(bucket, key, upload_id, reader)
        .await
        .context("upload_parts_stream() failed.");

    match completed_parts {
        Ok(parts) => {
            self.complete_multipart_upload_stream(bucket, key, upload_id, parts).await
        }
        Err(e) => {
            let _ = self.abort_multipart_upload(bucket, key, upload_id).await;
            Err(e)
        }
    }
}
```

`create_multipart_upload` and `abort_multipart_upload` helpers already exist in the file and are reused unchanged. `complete_multipart_upload_stream` is a small wrapper around the existing completion logic that uses the accumulated total size instead of `self.source_total_size.unwrap()`.

**New helper: `upload_parts_stream`** — derived from `upload_parts`'s `full_body_available` branch with these differences:

| `upload_parts` (today) | `upload_parts_stream` (new) |
|---|---|
| `for offset in (0..source_total_size).step_by(chunksize)` | `loop` until a chunksize-bounded read returns < chunksize bytes |
| Reads from `get_object_output_first_chunk.body` | Reads from the passed-in `reader` |
| Always full_body_available path | Always streaming — no content_range branch |
| Part 1 data is special-cased from body | Part 1 data is just the first chunksize bytes read — no special case |
| `source_total_size` drives per-part chunksize | Read `min(chunksize, remaining)` — "remaining" determined by whether the read filled the buffer |

Read pattern per iteration:

```rust
let mut buffer = Vec::<u8>::with_capacity(config_chunksize);
buffer.resize_with(config_chunksize, Default::default);
let n = read_exact_or_eof(&mut reader, &mut buffer).await?;
if n == 0 {
    break; // EOF on a clean boundary
}
buffer.truncate(n);
let is_last = n < config_chunksize;

// update additional checksum synchronously on &buffer, then finalize() — see checksum section below.
// spawn upload task as today (with the same semaphore acquire); buffer moves into ByteStream.
// accumulate total_uploaded_size += n.

if is_last { break; }
part_number += 1;
```

`read_exact_or_eof` reads until the buffer is full or EOF, returning the number of bytes read. (Built from `AsyncReadExt::read` in a loop.)

**Checksum integration**:

- Reads from `reader` happen sequentially on the main task inside `upload_parts_stream`, already in part-number order. No ordered channel or mutex is needed — the checksum state lives as a plain local `Option<AdditionalChecksum>` in `upload_parts_stream`.
- Within each loop iteration, before moving `buffer` into the spawned upload_part task:
  1. `checksum.update(&buffer)` — incorporate this part's bytes.
  2. `checksum.finalize()` — close out this part's chunk in the composite computation.
  3. Spawn the upload_part task (moves `buffer` into `ByteStream`).
- After the spawn loop exits and all spawned tasks have joined, `checksum.finalize_all()` yields the source-side additional checksum string.
- That string is stored on `self` (e.g. into `self.source_additional_checksum`) before the completion/verify tail runs, so the existing `validate_checksum()` call sees it exactly as today.
- Cost: one `update`/`finalize` pair synchronously on the main task per part (microseconds vs. the millisecond-scale upload_part network call). No extra memory: the update reads `&buffer` before it's moved.

**Verification invariants preserved**:

- Per-part MD5: computed per chunk inside each spawned task via `md5::compute(&buffer)` (identical to existing code at line 773). Sent with `Content-MD5`. S3 rejects bad parts server-side.
- Composite ETag check: the source-ETag comparison in `verify_e_tag` is a no-op for stdin today (no prior ETag) and remains a no-op.
- Additional checksum: source-side computed incrementally as above; target-side checksum pulled from `CompleteMultipartUpload` response; `validate_checksum` compares them — identical final check to today.

## Data flow (streaming multipart)

```
stdin
  │
  ▼
[probe returns Vec<u8> of threshold bytes]
  │
  ▼
Cursor<Vec<u8>>.chain(stdin)   ── single AsyncRead
  │
  ▼
UploadManager::upload_stream
  │
  ▼
CreateMultipartUpload
  │
  ▼
upload_parts_stream loop (main task):
  │
  ├── read_exact_or_eof(reader, &mut buffer[chunksize])
  │     │
  │     ▼
  │   additional_checksum.update(&buffer)   ── synchronous, in part order
  │   additional_checksum.finalize()         ── per-part boundary
  │     │
  │     ▼
  │   spawn upload_part task (semaphore-bounded), moving buffer:
  │     • md5::compute(&buffer)  ── per-part MD5, sent via Content-MD5
  │     • client.upload_part(...)
  │     • append CompletedPart to shared vec
  │
  └── loop until read returns < chunksize
  │
  ▼
join all spawned tasks
  │
  ▼
additional_checksum.finalize_all() → source_additional_checksum
  │
  ▼
CompleteMultipartUpload
  │
  ▼
validate_checksum(source_additional_checksum, target_checksum)
  │
  ▼
PutObjectOutput returned to stdio_to_s3
```

## Configuration

No new CLI flags or config fields. Streaming behavior is implicit: driven by `multipart_threshold` — which already exists and already controls single-part vs multipart decisions everywhere else.

## Edge cases

- **Exactly `multipart_threshold` bytes, no more.** Probe returns `buffer.len() == threshold`, streaming path runs, first read from the chained reader yields `threshold` bytes (from the buffered `Cursor`) filling the first part, second read returns 0 (EOF) — completes with one part. Valid MPU.
- **`multipart_threshold` < `multipart_chunksize`.** Probe buffers `multipart_threshold` bytes. First chunksize read pulls those bytes plus additional bytes from stdin until chunksize (or EOF). Normal streaming flow.
- **Zero bytes on stdin.** Probe returns empty `Vec`, goes to single-part path, uploads empty object. (Existing behavior.)
- **Read error mid-stream.** `read_exact_or_eof` propagates `io::Error`. `upload_stream` catches it, calls `abort_multipart_upload`, returns the error.
- **Cancellation mid-stream.** Existing cancellation check is preserved at the top of each iteration of the streaming loop. On cancellation: break loop, call `abort_multipart_upload`.

## What is NOT changed

- `put_object` trait method signature.
- `local_to_s3.rs`, `s3_to_s3.rs`, `s3_to_local.rs`, `s3_to_stdio.rs` transfer modules.
- `LocalStorage` implementation.
- CLI flags and config schema.
- The single-part stdin path — byte-for-byte identical to today for inputs below `multipart_threshold`.
- `upload_parts`, `upload_with_auto_chunksize`, `upload_parts_with_auto_chunksize` — these paths still require known size and are not reachable from streaming.

## Testing

### Unit tests in `src/transfer/stdio_to_s3.rs`

| Test | Reader | Expected path | Verifies |
|---|---|---|---|
| `probe_up_to_helper` | `Cursor` with 100 bytes | — | helper returns exactly `min(limit, available)` bytes |
| `probe_eof_before_threshold` | `Cursor` with `threshold - 1` bytes | single-part (`transfer_buffered`) | probe returns `< threshold`; single-part path invoked |
| `probe_hits_threshold_exactly` | `Cursor` with exactly `threshold` bytes | streaming MPU, 1 part | probe returns `== threshold`; streaming path completes with one part |
| `probe_hits_threshold_with_more_data` | `Cursor` with `threshold * 3` bytes | streaming MPU, multiple parts | streaming path reads remaining data from chain, part count is `ceil(total/chunksize)` |

These tests need a mock implementing `StorageTrait::put_object_stream`. If the existing test infrastructure doesn't already mock `StorageTrait`, add a minimal test double.

### Unit tests for checksum invariant

For each algorithm in `{Sha256, Sha1, Crc32, Crc32C, Crc64Nvme}`:

| Test | Verifies |
|---|---|
| `streaming_checksum_matches_buffered_<algo>` | Feeding N bytes (where N spans multiple chunks) through the streaming incremental checksum path produces the same string as `compute_source_checksum` called on the full `Vec<u8>` |

This is the critical correctness invariant.

### Unit tests for `upload_parts_stream`

| Test | Reader | Verifies |
|---|---|---|
| `stream_empty_is_handled` | 0-byte reader (edge case: should not be reached from stdio_to_s3 because probe returns empty → single-part; include the test for defence in depth) | returns an error if reached with 0 bytes |
| `stream_single_chunk` | exactly `chunksize` bytes | 1 part uploaded; part order correct |
| `stream_partial_last_chunk` | `chunksize + chunksize/2` bytes | 2 parts: full + partial; part order correct |
| `stream_multiple_full_chunks` | `chunksize * 4` bytes | 4 parts; part_numbers `[1,2,3,4]` despite parallel spawning |

Requires S3 client mocking at the `upload_part`/`create_multipart_upload`/`complete_multipart_upload` level. Reuse existing test infrastructure if present.

### E2E tests (user runs, per CLAUDE.md)

Extensions to `tests/e2e_stdio_integrity_check.rs`:

- Pipe a payload of size `multipart_threshold + 1` through stdin; verify uploaded object integrity (size + additional checksum) against the original.
- Pipe a large payload (e.g. `multipart_threshold * 10`) — success alone validates streaming doesn't OOM.
- Repeat per checksum algorithm configured via CLI.

All existing e2e tests in `tests/e2e_stdio_*.rs` must continue to pass (no behavioral regression for inputs below threshold, no regression for inputs above threshold that were working with the in-memory path).

### Compile verification (Claude runs)

- `cargo fmt`
- `cargo clippy --all-features`
- `RUSTFLAGS="--cfg e2e_test" cargo clippy` (verifies e2e test code compiles)
- `cargo test --lib` (unit tests)

### Manual verification guidance

After implementation, the user can confirm memory bounding by:

- Running a large-payload e2e scenario with `/usr/bin/time -l` (macOS) or `/usr/bin/time -v` (Linux) while piping `multipart_chunksize × 100` bytes through stdin.
- Observed RSS should stay within `multipart_chunksize × parallel_upload_semaphore_permits × small_constant`, not grow with total input size.

## Dependencies (already in s3util-rs)

- `tokio::io::AsyncRead`, `AsyncReadExt::take`, `AsyncReadExt::chain`
- `std::io::Cursor` (for buffered bytes)
- `AdditionalChecksum` with `update`/`finalize`/`finalize_all` — `src/storage/checksum/mod.rs`
- `aws_sdk_s3::primitives::ByteStream::from(Vec<u8>)` (in per-part upload task)
- `UploadManager` helpers: `create_multipart_upload`, `abort_multipart_upload`, existing parts of the complete/verify tail
