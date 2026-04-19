# S3→Local Write-Time Progress Emission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tie `SyncBytes` progress emission to successful `file.write_all` calls on the S3→local download path so the bar tracks durable disk writes instead of HTTP reads.

**Architecture:** Make `AsyncReadWithCallback::stats_sender` optional. In the three S3→local read-wrapper call sites, pass `None` to silence read-time emission; emit `SyncBytes` explicitly after each `write_all` on the destination file. Upload paths (s3→s3, local→s3, stdio→s3) and `s3_to_stdio` keep their existing emission points.

**Tech Stack:** Rust, tokio, async-channel, indicatif, AWS SDK for S3.

**Spec:** `docs/superpowers/specs/2026-04-19-s3-to-local-write-time-progress-design.md`

---

## File Structure

Files modified (no new files):

- `src/types/async_callback.rs` — `stats_sender: Option<Sender<SyncStatistics>>`, gate emission in `poll_read`, update tests, add silenced-reader test.
- `src/storage/mod.rs` — propagate `Option<Sender<SyncStatistics>>` through `convert_to_buf_byte_stream_with_callback`.
- `src/storage/local/mod.rs` — pass `None` at the three download read-wrapper call sites; emit `SyncBytes` after each `write_all` on the download paths (singlepart `put_object` line ~418; multipart `put_object_multipart` spawn-task write at line ~723).
- `src/storage/s3/mod.rs` — the one upload-side caller that uses a "dummy_stats_sender" workaround becomes `None` (cleanup that falls out of the signature change).

Every other caller of `AsyncReadWithCallback::new` or `convert_to_buf_byte_stream_with_callback` wraps its existing sender in `Some(...)` to keep behavior identical. The compile error forces visiting each site.

---

## Task 1: Make reader's stats sender optional

**Files:**
- Modify: `src/types/async_callback.rs`
- Modify: `src/storage/mod.rs:168-178`
- Modify (wrap as `Some`): `src/storage/local/mod.rs:395`, `src/storage/local/mod.rs:531`, `src/storage/local/mod.rs:658` — *wrap existing `self.get_stats_sender()` / `cloned_source.get_stats_sender()` as `Some(...)` in this task to keep behavior unchanged; Tasks 2–3 flip to `None`.*
- Modify (wrap as `Some`): `src/storage/s3/mod.rs:419` — wrap existing `dummy_stats_sender` as `Some(dummy_stats_sender)` (Task 4 replaces with `None`).
- Modify: `src/types/async_callback.rs:101, 124` — wrap existing `stats_sender` as `Some(stats_sender)` in the two existing tests.

**TDD sequence:**

- [ ] **Step 1: Add failing test `callback_test_none_sender_is_silent` to `src/types/async_callback.rs`**

Insert at the bottom of the `mod tests` block (before the final `}`):

```rust
    #[tokio::test]
    async fn callback_test_none_sender_is_silent() {
        init_dummy_tracing_subscriber();

        let file = File::open("test_data/5byte.dat").await.unwrap();
        // Hold a stats channel but do NOT wire it to the wrapper. The wrapper
        // is constructed with None and must not emit any SyncBytes.
        let (_stats_sender, stats_receiver) =
            async_channel::unbounded::<SyncStatistics>();
        let mut file_with_callback =
            AsyncReadWithCallback::new(file, None, None, None, None);

        let mut buffer = Vec::new();
        file_with_callback.read_to_end(&mut buffer).await.unwrap();

        assert_eq!(buffer.len(), TEST_DATA_SIZE);
        assert!(stats_receiver.is_empty());
    }
```

- [ ] **Step 2: Run the test and verify it fails**

Run: `cargo test --lib -- types::async_callback::tests::callback_test_none_sender_is_silent`
Expected: FAIL — compile error "expected `Sender<SyncStatistics>`, found enum `Option`" (or "mismatched types") because `AsyncReadWithCallback::new` still requires a concrete sender.

- [ ] **Step 3: Change `AsyncReadWithCallback` to accept `Option<Sender<SyncStatistics>>`**

In `src/types/async_callback.rs`:

Change the struct field from

```rust
    stats_sender: Sender<SyncStatistics>,
```

to

```rust
    stats_sender: Option<Sender<SyncStatistics>>,
```

Change the `new` signature:

```rust
impl<R: AsyncRead + Send + Sync> AsyncReadWithCallback<R> {
    pub fn new(
        inner: R,
        stats_sender: Option<Sender<SyncStatistics>>,
        bandwidth_limiter: Option<Arc<RateLimiter>>,
        additional_checksum: Option<Arc<AdditionalChecksum>>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Self {
        Self {
            inner,
            stats_sender,
            bandwidth_limiter,
            additional_checksum,
            object_checksum,
        }
    }
}
```

In `poll_read`, gate the emission on `Some`. Replace the existing block:

```rust
        if 0 < sync_bytes {
            let _ = this
                .stats_sender
                .send_blocking(SyncStatistics::SyncBytes(sync_bytes as u64));
        }
```

with:

```rust
        if 0 < sync_bytes
            && let Some(stats_sender) = this.stats_sender.as_ref()
        {
            let _ = stats_sender.send_blocking(SyncStatistics::SyncBytes(sync_bytes as u64));
        }
```

Update the two existing tests to wrap their sender:

```rust
        let mut file_with_callback =
            AsyncReadWithCallback::new(file, Some(stats_sender), None, None, None);
```

and

```rust
        let mut file_with_callback =
            AsyncReadWithCallback::new(file, Some(stats_sender), Some(limiter), None, None);
```

- [ ] **Step 4: Propagate `Option` through `convert_to_buf_byte_stream_with_callback`**

In `src/storage/mod.rs:168-178`, change the signature:

```rust
pub fn convert_to_buf_byte_stream_with_callback<R>(
    byte_stream: R,
    stats_sender: Option<Sender<SyncStatistics>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
) -> ByteStream
where
    R: AsyncRead + Send + 'static + Sync,
{
    let async_read = AsyncReadWithCallback::new(byte_stream, stats_sender, rate_limit_bandwidth, additional_checksum, object_checksum);
```

- [ ] **Step 5: Update every call site to wrap its sender in `Some(...)`**

Each of these lines currently passes a `Sender<SyncStatistics>` by value; wrap it as `Some(...)`. Do not change behavior — only satisfy the new signature.

- `src/storage/local/mod.rs:395` — `self.get_stats_sender()` → `Some(self.get_stats_sender())`
- `src/storage/local/mod.rs:531` — `self.get_stats_sender()` → `Some(self.get_stats_sender())`
- `src/storage/local/mod.rs:658` — `cloned_source.get_stats_sender().clone()` → `Some(cloned_source.get_stats_sender().clone())`
- `src/storage/s3/mod.rs:419` — `dummy_stats_sender` → `Some(dummy_stats_sender)`

- [ ] **Step 6: Verify the new test passes and everything compiles**

Run: `cargo test --lib -- types::async_callback::tests`
Expected: PASS — all three tests (the two updated + the new silent one).

Run: `cargo check --all-features`
Expected: clean (no errors from stragglers; if any, wrap them with `Some(...)`).

- [ ] **Step 7: Commit**

```bash
git add src/types/async_callback.rs src/storage/mod.rs src/storage/local/mod.rs src/storage/s3/mod.rs
git commit -m "$(cat <<'EOF'
refactor: make AsyncReadWithCallback stats_sender optional

Gate SyncBytes emission on Some(sender). All existing callers wrap
their sender as Some(...) so behavior is unchanged. Prepares the
S3→local download path to silence read-time emission and move to
write-time emission.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Emit `SyncBytes` after write in singlepart `put_object`

**Files:**
- Modify: `src/storage/local/mod.rs:395-401` (pass `None` instead of `Some(self.get_stats_sender())`)
- Modify: `src/storage/local/mod.rs:416-419` (emit after `write_all`)

TDD note: this path has no direct unit test (integration is covered by e2e; see `CLAUDE.md`). Verify via `cargo check --all-features` and `cargo clippy --all-features`; the user will re-run e2e.

- [ ] **Step 1: Silence the read wrapper in singlepart `put_object`**

Locate the block starting at `src/storage/local/mod.rs:395`. Change:

```rust
        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output.body.into_async_read(),
            Some(self.get_stats_sender()),
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );
```

to:

```rust
        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output.body.into_async_read(),
            None,
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );
```

- [ ] **Step 2: Emit `SyncBytes` after `write_all` in the fill_buf loop**

Locate the loop containing `file.write_all(buffer).await?;` (around line 418). Change:

```rust
            let buffer_len = buffer.len();
            file.write_all(buffer).await?;
            buf_reader.consume(buffer_len);
```

to:

```rust
            let buffer_len = buffer.len();
            file.write_all(buffer).await?;
            let _ = self
                .get_stats_sender()
                .send(SyncStatistics::SyncBytes(buffer_len as u64))
                .await;
            buf_reader.consume(buffer_len);
```

*Rationale:* emit only after the write succeeds so the byte count reflects bytes handed to the OS write path. `SyncStatistics` is already in scope via the existing `use` at the top of the file.

- [ ] **Step 3: Compile check**

Run: `cargo check --all-features`
Expected: clean.

- [ ] **Step 4: Unit tests still green**

Run: `cargo test --lib`
Expected: all existing unit tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/storage/local/mod.rs
git commit -m "$(cat <<'EOF'
feat: emit SyncBytes at write-time for singlepart s3-to-local

The singlepart put_object now emits SyncBytes(buffer_len) after each
successful file.write_all, and silences the S3 body read wrapper by
passing None. Progress bar tracks bytes on disk, not bytes received.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Emit `SyncBytes` after write in multipart `put_object_multipart`

**Files:**
- Modify: `src/storage/local/mod.rs:531-537` (pass `None` for first-chunk reader)
- Modify: `src/storage/local/mod.rs:658-667` (pass `None` for per-part reader inside spawn)
- Modify: `src/storage/local/mod.rs` around `cloned_file.write_all(&chunk_whole_data).await?;` (line ~723) — emit after the write inside the spawn task
- Modify: `src/storage/local/mod.rs` before the `loop {` that builds upload tasks — clone the stats sender so each task can capture it

- [ ] **Step 1: Silence the first-chunk reader**

At `src/storage/local/mod.rs:531`, change:

```rust
        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            Some(self.get_stats_sender()),
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );
```

to:

```rust
        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            None,
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );
```

- [ ] **Step 2: Silence the per-part reader inside the spawn task**

At `src/storage/local/mod.rs:658` (inside the `task::spawn(async move { ... })` block), change:

```rust
                    let body = convert_to_buf_byte_stream_with_callback(
                        get_object_output
                            .context("get_object() failed.")?
                            .body
                            .into_async_read(),
                        Some(cloned_source.get_stats_sender().clone()),
                        cloned_source.get_rate_limit_bandwidth(),
                        None,
                        None,
                    )
                    .into_async_read();
```

to:

```rust
                    let body = convert_to_buf_byte_stream_with_callback(
                        get_object_output
                            .context("get_object() failed.")?
                            .body
                            .into_async_read(),
                        None,
                        cloned_source.get_rate_limit_bandwidth(),
                        None,
                        None,
                    )
                    .into_async_read();
```

- [ ] **Step 3: Capture a target stats sender into each spawn task and emit after `write_all`**

In `put_object_multipart`, just before the `loop {` that pushes part tasks (around line 582, right after `let mut upload_parts_join_handles = FuturesUnordered::new();`), no change is needed — each spawn closure can re-clone from `self.get_stats_sender()` is not possible since `self` is not `'static`. Instead, bind a local clone once above the loop:

Find the line:

```rust
        let mut upload_parts_join_handles = FuturesUnordered::new();
        loop {
```

Change to:

```rust
        let mut upload_parts_join_handles = FuturesUnordered::new();
        let target_stats_sender = self.get_stats_sender();
        loop {
```

Then inside the task body, clone it into each task. Find the existing block where per-task clones are built (each variable prepared before `let task: JoinHandle<Result<()>> = task::spawn(async move { ... })`). Add next to the other clones (e.g., near `let cancellation_token = self.cancellation_token.clone();`, around line 604):

```rust
            let target_stats_sender = target_stats_sender.clone();
```

Then inside the spawn body, right after `cloned_file.write_all(&chunk_whole_data).await?;` and `cloned_file.flush().await?;` (around line 723–724), insert:

```rust
                let _ = target_stats_sender
                    .send(SyncStatistics::SyncBytes(chunk_whole_data_size as u64))
                    .await;
```

So the relevant region becomes:

```rust
                let chunk_whole_data_size = chunk_whole_data.len();
                cloned_file.seek(io::SeekFrom::Start(offset)).await?;
                cloned_file.write_all(&chunk_whole_data).await?;
                cloned_file.flush().await?;

                let _ = target_stats_sender
                    .send(SyncStatistics::SyncBytes(chunk_whole_data_size as u64))
                    .await;

                let mut upload_size_vec = total_upload_size.lock().unwrap();
                upload_size_vec.push(chunk_whole_data_size as u64);
```

`chunk_whole_data_size` is already declared immediately above, so no new variable is introduced. `SyncStatistics` is already in scope.

- [ ] **Step 4: Compile check**

Run: `cargo check --all-features`
Expected: clean.

- [ ] **Step 5: Unit tests still green**

Run: `cargo test --lib`
Expected: all existing unit tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/storage/local/mod.rs
git commit -m "$(cat <<'EOF'
feat: emit SyncBytes at write-time for multipart s3-to-local

Each spawned part task emits SyncBytes(chunk_size) after its
write_all + flush on the destination file. The first-chunk and
per-part S3 body readers are silenced via None. Progress bar now
tracks bytes durably written to disk, per part, across parallel
part tasks.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Clean up the upload-side `dummy_stats_sender` workaround

**Files:**
- Modify: `src/storage/s3/mod.rs:415-425`

The upload-side code at `src/storage/s3/mod.rs:415` constructs an unused `(dummy_stats_sender, _dummy_stats_receiver)` channel solely to suppress source-read emission. With `Option` this is unnecessary.

- [ ] **Step 1: Replace dummy channel with `None`**

Change:

```rust
        // Use a dummy stats sender for the body wrapper — we don't want SyncBytes
        // sent when reading from source (reads can be instant for local files).
        // Instead, SyncBytes are sent after each upload_part/singlepart upload completes.
        let (dummy_stats_sender, _dummy_stats_receiver) = async_channel::unbounded();
        get_object_output_first_chunk.body = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            Some(dummy_stats_sender),
            self.rate_limit_bandwidth.clone(),
            checksum,
            object_checksum.clone(),
        );
```

to:

```rust
        // No SyncBytes emission at the source body read — SyncBytes are sent
        // after each upload_part/singlepart upload completes.
        get_object_output_first_chunk.body = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            None,
            self.rate_limit_bandwidth.clone(),
            checksum,
            object_checksum.clone(),
        );
```

- [ ] **Step 2: Compile check**

Run: `cargo check --all-features`
Expected: clean.

- [ ] **Step 3: Unit tests still green**

Run: `cargo test --lib`
Expected: all existing unit tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/storage/s3/mod.rs
git commit -m "$(cat <<'EOF'
refactor: drop dummy stats channel on upload source body wrapper

Replace the unused (dummy_stats_sender, _dummy_stats_receiver)
channel with None now that AsyncReadWithCallback supports an optional
stats sender.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Final verification

- [ ] **Step 1: `cargo fmt`**

Run: `cargo fmt`
Expected: no output (formatting already applied) — if it edits files, stage them and amend the last commit? Do not amend; create a new `chore: cargo fmt` commit if needed.

- [ ] **Step 2: `cargo clippy --all-features`**

Run: `cargo clippy --all-features`
Expected: no warnings, no errors.

- [ ] **Step 3: `cargo test`**

Run: `cargo test`
Expected: all unit tests pass. (Do NOT run e2e — `CLAUDE.md` forbids it.)

- [ ] **Step 4: `cargo check --cfg e2e_test`**

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --all-features`
Expected: clean — e2e code also compiles with the new signatures. (Do not run e2e tests; just verify they still compile.)

- [ ] **Step 5: If fmt made changes, commit them**

```bash
git add -u
git commit -m "$(cat <<'EOF'
chore: cargo fmt

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

Otherwise skip.

---

## Self-Review (done during plan authoring)

- **Spec coverage:**
  - "Silenceable reader" → Task 1.
  - "Write-site emission, 3 sites" → Task 2 (singlepart) + Task 3 (multipart, covers the first-chunk path and the parallel per-part path via a single emit after `write_all` in the shared spawn body).
  - "No double counting" → guaranteed by silencing the three download readers (Tasks 2 & 3) and emitting only at the write path.
  - "Tests: silenced-reader contract" → Task 1 Step 1.
  - "Tests: total SyncBytes == object size" — *not included as a unit test*: `LocalStorage::put_object*` is exercised only by e2e in this repo, which `CLAUDE.md` forbids running. The invariant is nonetheless provable by inspection (each write site emits exactly the bytes it wrote; the read path no longer emits). The user's e2e runs (e.g., `tests/e2e_progress.rs`, `tests/e2e_s3_to_local.rs`) will cover this empirically.
  - "Error handling" → `let _ = ...` pattern used everywhere, matching existing codebase.
- **Placeholder scan:** no TBDs, no "add appropriate error handling", every code step has concrete code.
- **Type consistency:** `Option<Sender<SyncStatistics>>` used everywhere; `SyncStatistics::SyncBytes(n as u64)` consistent with existing emission pattern at `src/transfer/s3_to_stdio.rs:119` and elsewhere.
