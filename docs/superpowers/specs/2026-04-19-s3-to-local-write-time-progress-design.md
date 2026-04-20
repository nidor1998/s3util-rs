# S3→Local write-time progress emission

Date: 2026-04-19
Scope: S3→local download path only

## Motivation

When downloading from S3 to a local drive, the progress bar currently ticks on
bytes read from the HTTP body (via `AsyncReadWithCallback`), not on bytes
written to disk. Two symptoms fall out of this:

1. **Inaccuracy.** "100%" means "network receive complete," not "file on disk
   is complete." For large multipart downloads, the file can still be flushing
   after the bar says done.
2. **Visible lag.** In `put_object_multipart` each part reads its full chunk
   (default 8 MiB) into memory before `write_all`. So the bar jumps up during
   HTTP read, then stalls while parallel writes finish.

Goal: tie `SyncBytes` emission to successful `write_all` calls on the local
file for S3→local transfers, so bar position tracks durable progress.

## Non-goals

- Upload paths (local→S3, s3→s3, stdio→s3) keep emitting from the read wrapper.
  Those paths have no file-write step on the target local disk; emitting at
  HTTP-read is already the right boundary for them.
- `s3_to_stdio` keeps its existing explicit emit after `stdout.write_all`
  (`src/transfer/s3_to_stdio.rs:119`). Already correct.
- No change to the indicator (`src/bin/s3util/cli/indicator.rs`) or the
  `SyncStatistics::SyncBytes` type. Consumer side is unchanged.
- No change to checksum verification or bandwidth limiting.

## Design

### Silenceable reader

Make `AsyncReadWithCallback::stats_sender` optional:

```rust
stats_sender: Option<Sender<SyncStatistics>>
```

In `poll_read`, guard the existing `send_blocking(SyncBytes(n))` on `Some`.
Bandwidth limiting and the checksum/object-checksum paths stay unchanged —
only the stats emission is gated.

`convert_to_buf_byte_stream_with_callback` in `src/storage/s3/mod.rs`
propagates the `Option` through to the wrapper.

Every existing caller must update from `stats_sender` to `Some(stats_sender)`.
The `Option` change is deliberately breaking so the compiler catches every
call site.

### Write-site emission (S3→local only)

`src/storage/local/mod.rs` has three read-wrapper construction sites on the
download path. All three switch to `None` for `stats_sender`:

- Singlepart `put_object` (`convert_to_buf_byte_stream_with_callback` around
  line 395).
- Multipart `put_object_multipart` first-chunk reader (around line 531).
- Multipart `put_object_multipart` per-part reader inside the spawned task
  (around line 658).

Each successful `file.write_all(...)` on the download path then emits
`SyncBytes` for the exact bytes it wrote:

```rust
let _ = stats_sender.send(SyncStatistics::SyncBytes(n as u64)).await;
```

This matches the existing pattern at `src/transfer/s3_to_stdio.rs:119` (async
`.send(...).await` with the error swallowed). Call sites:

- Singlepart path (`src/storage/local/mod.rs:418`): emit `buffer_len` after
  each per-chunk `write_all` inside the `fill_buf` loop. Granularity ≈ 8 KiB
  (BufReader default); essentially unchanged from today's granularity but now
  gated on the write succeeding.
- Multipart path (`src/storage/local/mod.rs:723`): emit `chunk_whole_data_size`
  after `write_all` inside each spawned part task. Granularity = one
  `multipart_chunksize` per part (default 8 MiB). Covers both part 1 (first
  chunk read into memory then written by the same task) and parts ≥ 2.

Parallel part writes emit concurrently into the shared stats channel; the
indicator already sums across all sources, so this is fine.

### No double counting

Since the read wrapper on the download path is silenced via `None`, the only
emissions for an S3→local transfer come from the write sites. The sum of
`SyncBytes` received on the channel equals the object size, matching the
new invariant the tests assert.

## Alternatives considered

- **Symmetric `AsyncWriteWithCallback`.** Wrap the local file writer and emit
  from `poll_write`. Cleaner abstraction, but larger diff, new type, and with
  `write_all` already being one call per chunk on both paths there's no
  granularity benefit over explicit per-`write_all` emission. Rejected.
- **Post-write hook inside the existing reader.** The reader has no clean way
  to know when bytes it produced are durably written; no reasonable hook
  point. Rejected.

## Error handling

`.send(...).await` failure on a closed channel is ignored via `let _ = ...`,
matching every existing `SyncBytes` emission call site in the project. The
transfer does not fail because the indicator has shut down.

## Tests

### `src/types/async_callback.rs`

- Update both existing tests (`callback_test`,
  `callback_test_with_bandwidth_limiter`) to pass `Some(stats_sender)`.
- Add a new test constructing the wrapper with `stats_sender = None`, read a
  test file fully, assert `buffer.len() == TEST_DATA_SIZE`. The separately-held
  stats receiver (created but never wired to the wrapper) must be empty after
  the read, locking in the silenced-reader contract.

### Local-storage download paths

For both the singlepart and multipart download tests already present in
`src/storage/local/`:

- Assert that the total `SyncBytes` accumulated across the stats channel
  equals the downloaded object size. This is the new invariant the write-site
  emission guarantees and directly verifies no double-counting and no
  missing emissions.

No new e2e tests (e2e is user-run; see `CLAUDE.md`).

## Risk

- **Missed call site for `Some` wrap.** Compile error at every existing
  `AsyncReadWithCallback::new` / `convert_to_buf_byte_stream_with_callback`
  call. Forces a visit to each.
- **Double counting.** Prevented by `None` on the three download-path read
  wrappers plus the new write-site emit. Asserted by the total-bytes test.
- **Cancellation.** The multipart path already checks cancellation at chunk
  boundaries; emission after `write_all` does not change cancellation
  semantics.
- **Parallel emission.** The stats channel is already shared across
  concurrent emitters (e.g., upload manager uses `send_blocking` from
  multiple tasks). No new concern.
