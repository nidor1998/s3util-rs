# Streaming Multipart Upload for stdin Source — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current drain-to-memory-then-upload strategy in `stdio_to_s3::transfer` with a probe-then-switch design: fully buffer inputs smaller than `multipart_threshold` (unchanged), but stream chunks through a new EOF-driven multipart path for anything larger.

**Architecture:** `UploadManager` gains a new entry point `upload_stream` that reads from an `AsyncRead` (instead of a pre-fetched first chunk) and uploads parts until EOF, with bounded parallelism via the existing `parallel_upload_semaphore`. A new `StorageTrait::put_object_stream` method routes stdin's streaming path to S3 without polluting the existing `put_object` signature.

**Tech Stack:** Rust, tokio (AsyncRead/AsyncReadExt::take+chain), aws-sdk-s3 v1.x, anyhow, tracing

**Spec:** `docs/superpowers/specs/2026-04-18-stdio-streaming-multipart-design.md`

---

## File Structure

- **Modify:** `src/storage/s3/upload_manager.rs` — `source_total_size` becomes `Option<u64>`; add helpers `read_exact_or_eof`, `upload_parts_stream`, `upload_stream`
- **Modify:** `src/storage/mod.rs` — add `put_object_stream` trait method with default error impl
- **Modify:** `src/storage/s3/mod.rs` — implement `put_object_stream` for `S3Storage`
- **Modify:** `src/transfer/stdio_to_s3.rs` — add `probe_up_to` helper; extract `transfer_buffered`; add streaming dispatch
- **Unchanged:** `src/storage/local/mod.rs` (inherits default trait impl), `src/transfer/{local_to_s3,s3_to_s3,s3_to_local,s3_to_stdio}.rs`

---

### Task 1: Refactor `UploadManager::source_total_size` to `Option<u64>`

Preparatory refactor. No new behavior. All existing call sites pass `Some(n)`; all read sites unwrap with a panic message that makes the invariant explicit ("streaming entry point never reaches this site").

**Files:**
- Modify: `src/storage/s3/upload_manager.rs` (field type, `new` signature, ~15 read sites)
- Modify: `src/storage/s3/mod.rs` (caller of `UploadManager::new`)

- [ ] **Step 1: Change the field and constructor**

In `src/storage/s3/upload_manager.rs`:

Change field declaration (around line 76):

```rust
    source_total_size: Option<u64>,
```

Change `new()` parameter (around line 95) — update the function signature:

```rust
    pub fn new(
        client: Arc<Client>,
        config: Config,
        request_payer: Option<RequestPayer>,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        tagging: Option<String>,
        object_parts: Option<Vec<ObjectPart>>,
        express_onezone_storage: bool,
        source: Storage,
        source_key: String,
        source_total_size: Option<u64>,
        source_additional_checksum: Option<String>,
        if_none_match: Option<String>,
        has_warning: Arc<AtomicBool>,
    ) -> Self {
```

The body (`source_total_size,` on ~line 112) is already correct for the new type.

- [ ] **Step 2: Update the caller in `S3Storage::put_object`**

In `src/storage/s3/mod.rs`, find the `UploadManager::new(...)` call inside `put_object` and wrap the size: change the argument `source_size` to `Some(source_size)`.

- [ ] **Step 3: Update every read of `self.source_total_size` to unwrap**

In `src/storage/s3/upload_manager.rs`, for each read site listed below, change to `.expect("source_total_size is Some in non-streaming upload path")` (or `.unwrap()` with an inline comment explaining the invariant).

Sites (based on grep): lines 146, 165, 392, 587, 614, 641, 922, 930, 1280, 1288, 1324, 1462, 1463, 1472, 1516.

For each site, the expression changes from `self.source_total_size` to `self.source_total_size.expect("source_total_size is Some in non-streaming upload path")`. For arithmetic sites, store in a local variable first:

```rust
let source_total_size = self
    .source_total_size
    .expect("source_total_size is Some in non-streaming upload path");
```

…then use `source_total_size` in the rest of that scope.

- [ ] **Step 4: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors. No behavior change (all existing paths always set `source_total_size` to `Some`).

- [ ] **Step 5: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass (no behavioral change).

- [ ] **Step 6: Commit**

```bash
git add src/storage/s3/upload_manager.rs src/storage/s3/mod.rs
git commit -m "refactor: source_total_size is Option<u64> in UploadManager"
```

---

### Task 2: Add `read_exact_or_eof` helper in `upload_manager.rs` with TDD

This helper reads into a `&mut [u8]` slice, returning the actual number of bytes read (between 0 and `slice.len()`). Used by the streaming loop to detect EOF on chunk boundaries.

**Files:**
- Modify: `src/storage/s3/upload_manager.rs` (add function + `#[cfg(test)] mod tests` block at end of file if absent)

- [ ] **Step 1: Write failing tests**

Append to `src/storage/s3/upload_manager.rs` (or append to existing `#[cfg(test)] mod tests` block):

```rust
#[cfg(test)]
mod read_exact_or_eof_tests {
    use super::read_exact_or_eof;
    use std::io::Cursor;

    #[tokio::test]
    async fn reads_full_buffer_when_reader_has_more() {
        let mut reader = Cursor::new(vec![1u8; 100]);
        let mut buf = vec![0u8; 50];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 50);
        assert_eq!(buf, vec![1u8; 50]);
    }

    #[tokio::test]
    async fn reads_partial_when_reader_has_less() {
        let mut reader = Cursor::new(vec![7u8; 30]);
        let mut buf = vec![0u8; 50];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 30);
        assert_eq!(&buf[..30], &vec![7u8; 30][..]);
        assert_eq!(&buf[30..], &vec![0u8; 20][..]);
    }

    #[tokio::test]
    async fn reads_zero_when_reader_is_empty() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut buf = vec![0u8; 10];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn reads_exact_amount_when_reader_matches_buffer() {
        let mut reader = Cursor::new(vec![9u8; 64]);
        let mut buf = vec![0u8; 64];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 64);
        assert_eq!(buf, vec![9u8; 64]);
    }
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib read_exact_or_eof_tests -- --nocapture`
Expected: FAIL — `read_exact_or_eof` does not exist yet.

- [ ] **Step 3: Implement `read_exact_or_eof`**

Add the following function to `src/storage/s3/upload_manager.rs` (place it near the top of the `impl UploadManager` block's free functions, e.g., just above `impl UploadManager`):

```rust
/// Read bytes into `buf` until it is completely full or the reader signals EOF.
/// Returns the number of bytes actually read (0 <= n <= buf.len()).
///
/// `AsyncReadExt::read_exact` can't distinguish "EOF at a clean boundary" from
/// "EOF mid-read"; this helper treats any short read that terminates with
/// `read() -> Ok(0)` as a clean EOF and returns the accumulated count.
async fn read_exact_or_eof<R: tokio::io::AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<usize> {
    use tokio::io::AsyncReadExt;

    let mut total = 0;
    while total < buf.len() {
        let n = reader
            .read(&mut buf[total..])
            .await
            .context("read_exact_or_eof: reader returned an error")?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}
```

Ensure the imports at top of file include `tokio::io::AsyncReadExt` (it's already imported on line 22).

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test --lib read_exact_or_eof_tests -- --nocapture`
Expected: All 4 tests PASS.

- [ ] **Step 5: Run clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 6: Commit**

```bash
git add src/storage/s3/upload_manager.rs
git commit -m "feat: add read_exact_or_eof helper for streaming reads"
```

---

### Task 3: Add `probe_up_to` helper in `stdio_to_s3.rs` with TDD

This helper reads up to `limit` bytes from a reader into a fresh `Vec<u8>`. If the returned Vec's length is less than `limit`, the reader hit EOF; if equal, the limit was reached and more data may exist.

**Files:**
- Modify: `src/transfer/stdio_to_s3.rs` (add function + `#[cfg(test)] mod tests`)

- [ ] **Step 1: Write failing tests**

Append to `src/transfer/stdio_to_s3.rs`:

```rust
#[cfg(test)]
mod probe_tests {
    use super::probe_up_to;
    use std::io::Cursor;

    #[tokio::test]
    async fn returns_all_bytes_when_reader_smaller_than_limit() {
        let mut reader = Cursor::new(vec![1u8; 30]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 30);
        assert_eq!(buf, vec![1u8; 30]);
    }

    #[tokio::test]
    async fn returns_exactly_limit_bytes_when_reader_larger() {
        let mut reader = Cursor::new(vec![2u8; 200]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 100);
        assert_eq!(buf, vec![2u8; 100]);
    }

    #[tokio::test]
    async fn returns_limit_bytes_when_reader_exactly_limit() {
        let mut reader = Cursor::new(vec![3u8; 100]);
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert_eq!(buf.len(), 100);
        assert_eq!(buf, vec![3u8; 100]);
    }

    #[tokio::test]
    async fn returns_empty_for_empty_reader() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let buf = probe_up_to(&mut reader, 100).await.unwrap();
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn leaves_remaining_bytes_in_reader() {
        let data = vec![5u8; 50];
        let mut reader = Cursor::new(data);
        let _probed = probe_up_to(&mut reader, 20).await.unwrap();
        // Read the rest — should be 30 bytes left
        let mut rest = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut reader, &mut rest)
            .await
            .unwrap();
        assert_eq!(rest.len(), 30);
    }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

Run: `cargo test --lib probe_tests -- --nocapture`
Expected: FAIL — `probe_up_to` does not exist.

- [ ] **Step 3: Implement `probe_up_to`**

Add to `src/transfer/stdio_to_s3.rs` (place near the top of the file, below imports, above `pub async fn transfer`):

```rust
/// Read up to `limit` bytes from `reader` into a fresh `Vec<u8>`.
/// If the returned Vec's length is `< limit`, the reader reached EOF.
/// If `== limit`, the limit was reached and the reader may have more data.
async fn probe_up_to<R: tokio::io::AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    limit: usize,
) -> Result<Vec<u8>> {
    use tokio::io::AsyncReadExt;

    let mut buf = Vec::with_capacity(limit);
    (&mut *reader)
        .take(limit as u64)
        .read_to_end(&mut buf)
        .await
        .context("probe_up_to: failed to read from reader")?;
    Ok(buf)
}
```

- [ ] **Step 4: Run tests to confirm they pass**

Run: `cargo test --lib probe_tests -- --nocapture`
Expected: All 5 tests PASS.

- [ ] **Step 5: Run clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 6: Commit**

```bash
git add src/transfer/stdio_to_s3.rs
git commit -m "feat: add probe_up_to helper for stdin size discovery"
```

---

### Task 4: Pin the incremental-vs-batch `AdditionalChecksum` invariant with tests

The streaming path relies on the invariant that feeding N bytes as a sequence of `update(chunk)` + `finalize()` + eventual `finalize_all()` produces the same multipart-composite checksum as a single `compute_source_checksum` call on the full buffer. This task locks that invariant in as a test so future regressions in `AdditionalChecksum` are caught.

**Files:**
- Modify: `src/transfer/stdio_to_s3.rs` (append test module)

- [ ] **Step 1: Write the tests**

Append to `src/transfer/stdio_to_s3.rs`:

```rust
#[cfg(test)]
mod checksum_invariant_tests {
    use super::compute_source_checksum;
    use crate::storage::checksum::AdditionalChecksum;
    use aws_sdk_s3::types::ChecksumAlgorithm;

    fn all_algorithms() -> Vec<ChecksumAlgorithm> {
        vec![
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
            ChecksumAlgorithm::Crc64Nvme,
        ]
    }

    // Simulates what upload_parts_stream will do: chunked update/finalize,
    // then one finalize_all at EOF.
    fn streaming_checksum(
        buffer: &[u8],
        algorithm: ChecksumAlgorithm,
        multipart_chunksize: usize,
        full_object_checksum: bool,
    ) -> String {
        let mut c = AdditionalChecksum::new(algorithm, full_object_checksum);
        let mut offset = 0;
        while offset < buffer.len() {
            let end = std::cmp::min(offset + multipart_chunksize, buffer.len());
            c.update(&buffer[offset..end]);
            let _ = c.finalize();
            offset = end;
        }
        c.finalize_all()
    }

    #[test]
    fn streaming_matches_buffered_for_multipart_sizes() {
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0xABu8; chunksize * 4 + 17]; // 4 full chunks + partial

        for algo in all_algorithms() {
            let batched = compute_source_checksum(
                &buffer,
                algo.clone(),
                chunksize,
                threshold,
                false,
            );
            let streamed = streaming_checksum(&buffer, algo.clone(), chunksize, false);
            assert_eq!(
                batched, streamed,
                "algorithm {:?}: batched vs streamed checksum mismatch",
                algo
            );
        }
    }

    #[test]
    fn streaming_matches_buffered_for_exact_chunksize_multiples() {
        let chunksize = 1024usize;
        let threshold = 1024usize;
        let buffer = vec![0x5Au8; chunksize * 3];

        for algo in all_algorithms() {
            let batched = compute_source_checksum(
                &buffer,
                algo.clone(),
                chunksize,
                threshold,
                false,
            );
            let streamed = streaming_checksum(&buffer, algo.clone(), chunksize, false);
            assert_eq!(batched, streamed, "algorithm {:?}", algo);
        }
    }
}
```

- [ ] **Step 2: Run the tests**

Run: `cargo test --lib checksum_invariant_tests -- --nocapture`
Expected: All tests PASS (this is validating the existing `AdditionalChecksum` API — no new implementation).

If any test fails, the assumption that streaming produces the same composite checksum is wrong — stop and investigate before proceeding with Task 6.

- [ ] **Step 3: Commit**

```bash
git add src/transfer/stdio_to_s3.rs
git commit -m "test: pin streaming-vs-batched AdditionalChecksum invariant"
```

---

### Task 5: Add `put_object_stream` to `StorageTrait` with default error impl

**Files:**
- Modify: `src/storage/mod.rs` (add trait method with default body; add imports if needed)

- [ ] **Step 1: Add the new method to the trait**

In `src/storage/mod.rs`, inside the `pub trait StorageTrait: DynClone { ... }` block (the trait currently ends around line 153), add the following method declaration after `put_object_tagging` (around line 140):

```rust
    async fn put_object_stream(
        &self,
        _key: &str,
        _reader: Box<dyn AsyncRead + Send + Unpin>,
        _tagging: Option<String>,
        _object_checksum: Option<ObjectChecksum>,
        _if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        Err(anyhow!(
            "put_object_stream is not supported on this storage"
        ))
    }
```

Ensure `AsyncRead` and `anyhow!` are in scope at the top of the file — `AsyncRead` is already imported (line 25), and `anyhow` is already imported (line 1).

- [ ] **Step 2: Run clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors. `LocalStorage` inherits the default impl (returns error — stdin-to-local is never called).

- [ ] **Step 3: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass (no behavior change for any current caller).

- [ ] **Step 4: Commit**

```bash
git add src/storage/mod.rs
git commit -m "feat: add put_object_stream to StorageTrait with default error impl"
```

---

### Task 6: Add `UploadManager::upload_parts_stream`

The core of the streaming MPU. Mirrors the structure of the `full_body_available` branch of `upload_parts` but reads from a passed-in reader until EOF, computes the additional checksum incrementally, and uses accumulated size tracking.

**Files:**
- Modify: `src/storage/s3/upload_manager.rs` (append method inside `impl UploadManager`)

- [ ] **Step 1: Add the method skeleton**

Add the following method inside the `impl UploadManager` block (place it after the existing `upload_parts` method, around line 945):

```rust
    // skipcq: RS-R1000
    async fn upload_parts_stream(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> Result<(Vec<CompletedPart>, u64, Option<String>)> {
        use crate::storage::checksum::AdditionalChecksum;

        let shared_multipart_etags = Arc::new(Mutex::new(Vec::<MutipartEtags>::new()));
        let shared_upload_parts = Arc::new(Mutex::new(Vec::<CompletedPart>::new()));

        let config_chunksize = self.config.transfer_config.multipart_chunksize as usize;
        let mut total_uploaded_size: u64 = 0;
        let mut part_number: i32 = 1;

        let mut additional_checksum = self
            .config
            .additional_checksum_algorithm
            .clone()
            .map(|algo| AdditionalChecksum::new(algo, self.config.full_object_checksum));

        let mut upload_parts_join_handles = FuturesUnordered::new();

        loop {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }

            // Read up to config_chunksize bytes for this part.
            let mut buffer = vec![0u8; config_chunksize];
            let n = read_exact_or_eof(&mut reader, &mut buffer).await?;
            if n == 0 {
                // Clean EOF on a chunk boundary.
                break;
            }
            buffer.truncate(n);
            let is_last_chunk = n < config_chunksize;

            // Incremental additional checksum — synchronous, in part order.
            if let Some(c) = additional_checksum.as_mut() {
                c.update(&buffer);
                let _ = c.finalize();
            }

            total_uploaded_size += n as u64;

            // Capture per-task locals.
            let upload_parts = Arc::clone(&shared_upload_parts);
            let multipart_etags = Arc::clone(&shared_multipart_etags);
            let client = Arc::clone(&self.client);
            let target_bucket = bucket.to_string();
            let target_key = key.to_string();
            let target_upload_id = upload_id.to_string();
            let target_sse_c = self.config.target_sse_c.clone();
            let target_sse_c_key = self.config.target_sse_c_key.clone().key.clone();
            let target_sse_c_key_md5 = self.config.target_sse_c_key_md5.clone();
            let additional_checksum_algorithm = self.config.additional_checksum_algorithm.clone();
            let disable_payload_signing = self.config.disable_payload_signing;
            let disable_content_md5_header = self.config.disable_content_md5_header;
            let express_onezone_storage = self.express_onezone_storage;
            let request_payer = self.request_payer.clone();
            let stats_sender = self.stats_sender.clone();
            let current_part_number = part_number;
            let part_size = n as i64;

            let permit = self
                .config
                .clone()
                .target_client_config
                .unwrap()
                .parallel_upload_semaphore
                .acquire_owned()
                .await?;

            let task: JoinHandle<Result<()>> = task::spawn(async move {
                let _permit = permit;

                let md5_digest;
                let md5_digest_base64 =
                    if !express_onezone_storage && !disable_content_md5_header {
                        let md5_digest_raw = md5::compute(&buffer);
                        md5_digest = Some(md5_digest_raw);
                        Some(general_purpose::STANDARD.encode(md5_digest_raw.as_slice()))
                    } else {
                        md5_digest = None;
                        None
                    };

                // Compute per-part additional checksum for the upload_part call itself.
                let part_additional_checksum =
                    additional_checksum_algorithm.as_ref().map(|algo| {
                        let mut c = AdditionalChecksum::new(algo.clone(), false);
                        c.update(&buffer);
                        c.finalize()
                    });

                let body = ByteStream::from(buffer);

                debug!(
                    key = &target_key,
                    part_number = current_part_number,
                    size = part_size,
                    is_last = is_last_chunk,
                    "upload_part() start (streaming)"
                );

                let mut upload_part_builder = client
                    .upload_part()
                    .set_request_payer(request_payer.clone())
                    .bucket(&target_bucket)
                    .key(&target_key)
                    .upload_id(&target_upload_id)
                    .part_number(current_part_number)
                    .body(body)
                    .set_content_md5(md5_digest_base64)
                    .set_sse_customer_algorithm(target_sse_c.clone())
                    .set_sse_customer_key(target_sse_c_key.clone())
                    .set_sse_customer_key_md5(target_sse_c_key_md5.clone());

                if let Some(algo) = additional_checksum_algorithm.as_ref() {
                    upload_part_builder = upload_part_builder
                        .set_checksum_algorithm(Some(algo.clone()));
                    match algo {
                        ChecksumAlgorithm::Sha256 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_sha256(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Sha1 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_sha1(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc32 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc32(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc32C => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc32_c(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc64Nvme => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc64_nvme(part_additional_checksum.clone());
                        }
                        _ => {}
                    }
                }

                let upload_part_output = if disable_payload_signing {
                    upload_part_builder
                        .customize()
                        .disable_payload_signing()
                        .send()
                        .await
                        .context("aws_sdk_s3::client::Client upload_part() failed.")?
                } else {
                    upload_part_builder
                        .send()
                        .await
                        .context("aws_sdk_s3::client::Client upload_part() failed.")?
                };

                debug!(
                    key = &target_key,
                    part_number = current_part_number,
                    "upload_part() complete (streaming)"
                );
                trace!(key = &target_key, "{upload_part_output:?}");

                if md5_digest.is_some() {
                    let mut locked_etags = multipart_etags.lock().unwrap();
                    locked_etags.push(MutipartEtags {
                        digest: md5_digest.as_ref().unwrap().as_slice().to_vec(),
                        part_number: current_part_number,
                    });
                }

                let mut locked_upload_parts = upload_parts.lock().unwrap();
                locked_upload_parts.push(
                    CompletedPart::builder()
                        .e_tag(upload_part_output.e_tag().unwrap())
                        .set_checksum_sha256(
                            upload_part_output.checksum_sha256().map(|s| s.to_string()),
                        )
                        .set_checksum_sha1(
                            upload_part_output.checksum_sha1().map(|s| s.to_string()),
                        )
                        .set_checksum_crc32(
                            upload_part_output.checksum_crc32().map(|s| s.to_string()),
                        )
                        .set_checksum_crc32_c(
                            upload_part_output.checksum_crc32_c().map(|s| s.to_string()),
                        )
                        .set_checksum_crc64_nvme(
                            upload_part_output.checksum_crc64_nvme().map(|s| s.to_string()),
                        )
                        .part_number(current_part_number)
                        .build(),
                );

                let _ = stats_sender
                    .send(SyncStatistics::SyncBytes(part_size as u64))
                    .await;

                Ok(())
            });

            upload_parts_join_handles.push(task);
            part_number += 1;

            if is_last_chunk {
                break;
            }
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result
                .context("upload_parts_stream join failed")?
                .context("upload_parts_stream task failed")?;
        }

        // Sort parts by part_number since tasks finished in arbitrary order.
        let mut parts = shared_upload_parts.lock().unwrap().clone();
        parts.sort_by_key(|p| p.part_number.unwrap_or(0));

        // Collect per-part MD5s (in part order) for composite ETag computation later.
        {
            let mut etags = shared_multipart_etags.lock().unwrap();
            etags.sort_by_key(|e| e.part_number);
            for etag in etags.iter() {
                self.concatnated_md5_hash
                    .extend_from_slice(&etag.digest);
            }
        }

        let source_additional_checksum_string = additional_checksum.map(|mut c| c.finalize_all());

        Ok((parts, total_uploaded_size, source_additional_checksum_string))
    }
```

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 3: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass (no behavior change yet — method exists but has no callers).

- [ ] **Step 4: Commit**

```bash
git add src/storage/s3/upload_manager.rs
git commit -m "feat: add upload_parts_stream for EOF-driven streaming MPU"
```

---

### Task 7: Add `UploadManager::upload_stream` entry point

Creates the multipart upload, drives `upload_parts_stream`, then completes (or aborts on error). Includes the ETag and additional-checksum verification tail.

**Files:**
- Modify: `src/storage/s3/upload_manager.rs` (add method)

- [ ] **Step 1: Add the entry point method**

Add inside the `impl UploadManager` block (place it near `upload_parts_stream`):

```rust
    pub async fn upload_stream(
        &mut self,
        bucket: &str,
        key: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> Result<PutObjectOutput> {
        let checksum_type = if self.config.full_object_checksum {
            Some(ChecksumType::FullObject)
        } else {
            None
        };

        // Build minimal metadata from config. For stdin-sourced streams, there is
        // no source GetObjectOutput to pull metadata from; we rely purely on what
        // the user configured on the CLI.
        let create_output = self
            .client
            .create_multipart_upload()
            .set_request_payer(self.request_payer.clone())
            .set_storage_class(self.config.storage_class.clone())
            .bucket(bucket)
            .key(key)
            .set_metadata(self.config.metadata.clone())
            .set_tagging(self.tagging.clone())
            .set_content_type(self.config.content_type.clone())
            .set_server_side_encryption(self.config.sse.clone())
            .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_acl(self.config.canned_acl.clone())
            .set_checksum_algorithm(self.config.additional_checksum_algorithm.clone())
            .set_checksum_type(checksum_type.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client create_multipart_upload() failed.")?;

        let upload_id = create_output.upload_id().unwrap().to_string();

        let result = self
            .upload_parts_stream(bucket, key, &upload_id, reader)
            .await;

        let (parts, total_size, source_additional_checksum) = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = self.abort_multipart_upload(bucket, key, &upload_id).await;
                return Err(e);
            }
        };

        if parts.is_empty() {
            let _ = self.abort_multipart_upload(bucket, key, &upload_id).await;
            return Err(anyhow!(
                "upload_stream: no parts uploaded (empty stream reached streaming path)"
            ));
        }

        // Store the computed source checksum so validate_checksum picks it up.
        self.source_additional_checksum = source_additional_checksum.clone();

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        let complete_output = self
            .client
            .complete_multipart_upload()
            .set_request_payer(self.request_payer.clone())
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_multipart_upload)
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_checksum_type(checksum_type)
            .set_if_none_match(self.if_none_match.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client complete_multipart_upload() failed.")?;

        trace!(
            key = key,
            upload_id = upload_id,
            "{complete_output:?}"
        );

        // ETag verification — for stdin there's no source ETag to compare against,
        // but we compute the synthetic source ETag from the per-part MD5s (same
        // pattern as the local-source branch in upload_parts_and_complete).
        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && !self.config.disable_content_md5_header
        {
            let parts_count = ((total_size as f64)
                / (self.config.transfer_config.multipart_chunksize as f64))
                .ceil() as i64;
            let source_e_tag = Some(self.generate_e_tag_hash(parts_count));
            let target_sse = complete_output.server_side_encryption().cloned();
            let target_e_tag = complete_output.e_tag().map(|e| e.to_string());

            self.verify_e_tag(
                key,
                &None,
                false,
                &source_e_tag,
                &target_sse,
                &target_e_tag,
            )
            .await;
        }

        if !self.config.disable_additional_checksum_verify {
            let target_checksum = get_additional_checksum_from_multipart_upload_result(
                &complete_output,
                self.config.additional_checksum_algorithm.clone(),
            );

            self.validate_checksum(key, source_additional_checksum, target_checksum, &None, false)
                .await;
        }

        let _ = self
            .stats_sender
            .send(SyncStatistics::SyncBytes(total_size))
            .await;

        Ok(PutObjectOutput::builder()
            .e_tag(complete_output.e_tag().unwrap())
            .build())
    }
```

Note: `get_additional_checksum_from_multipart_upload_result` is already imported/in scope in this file (it's used at line 478). `generate_e_tag_hash` is already a method on `UploadManager`. `calculate_parts_count` is similarly available.

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 3: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/storage/s3/upload_manager.rs
git commit -m "feat: add UploadManager::upload_stream entry point"
```

---

### Task 8: Implement `S3Storage::put_object_stream`

Wire `StorageTrait::put_object_stream` on the `S3Storage` side to delegate to `UploadManager::upload_stream`.

**Files:**
- Modify: `src/storage/s3/mod.rs` (add trait method impl)

- [ ] **Step 1: Inspect how `S3Storage::put_object` builds the `UploadManager`**

Read the existing `put_object` impl at `src/storage/s3/mod.rs:358` to see exactly how it constructs `UploadManager`, the bucket/key extraction, cancellation, etc. The new `put_object_stream` mirrors that scaffolding, but calls `upload_stream` instead of `upload` and skips the `source`/`source_key`/`source_size` params.

- [ ] **Step 2: Add the impl**

Add a new method inside the `impl StorageTrait for S3Storage` block (place it just after `put_object`, around line 460). Replace the body with the following, adjusting bucket extraction and field accesses to match the style of the existing `put_object` impl in the file:

```rust
    async fn put_object_stream(
        &self,
        key: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        // Bucket extraction: follow the same pattern as put_object.
        let bucket = self.bucket_name();

        // For a streaming stdin source, there is no re-readable source Storage,
        // no source size, and no pre-computed source additional checksum — the
        // checksum is computed inside upload_stream as bytes flow through.
        let mut upload_manager = UploadManager::new(
            Arc::clone(&self.client),
            self.config.clone(),
            self.request_payer.clone(),
            self.cancellation_token.clone(),
            self.stats_sender.clone(),
            tagging,
            object_checksum.as_ref().and_then(|c| c.object_parts.clone()),
            self.is_express_onezone_storage(),
            // For streaming, there's no source Storage to clone; we can pass a
            // Box of `self` to satisfy the signature — UploadManager's streaming
            // path does not touch `source`.
            dyn_clone::clone_box(&*(self as &dyn StorageTrait)),
            key.to_string(),
            None, // source_total_size: None = streaming
            None, // source_additional_checksum: None = computed during stream
            if_none_match,
            self.has_warning.clone(),
        );

        upload_manager.upload_stream(&bucket, key, reader).await
    }
```

**Notes:**
- `bucket_name()` is assumed to be a helper on `S3Storage`. If not, extract the bucket the same way `put_object` does. (Consult the current `put_object` body.)
- The `source` parameter to `UploadManager::new` is never read by the streaming path; passing a clone of `self` satisfies the type.
- `object_checksum.as_ref().and_then(|c| c.object_parts.clone())` preserves the existing pattern of threading `object_parts` through to `UploadManager`.

- [ ] **Step 3: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 4: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/storage/s3/mod.rs
git commit -m "feat: implement put_object_stream for S3Storage"
```

---

### Task 9: Refactor `stdio_to_s3::transfer` — extract `transfer_buffered`

Split the existing body into a reusable helper that takes a pre-read `Vec<u8>`. Preserves exact current behavior (still calls `read_to_end` internally, for now).

**Files:**
- Modify: `src/transfer/stdio_to_s3.rs`

- [ ] **Step 1: Extract `transfer_buffered`**

Replace the current `pub async fn transfer` in `src/transfer/stdio_to_s3.rs` with the following two functions. The body of `transfer_buffered` is the current body of `transfer` starting from line 42 (`let source_size = buffer.len() as u64;`). `transfer` becomes a thin wrapper that reads all of stdin and delegates.

```rust
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

    let mut buffer = Vec::new();
    reader
        .read_to_end(&mut buffer)
        .await
        .context("failed to read from stdin")?;

    transfer_buffered(
        config,
        target,
        target_key,
        buffer,
        cancellation_token,
        stats_sender,
    )
    .await
}

async fn transfer_buffered(
    config: &Config,
    target: Storage,
    target_key: &str,
    buffer: Vec<u8>,
    _cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    let target_clone = dyn_clone::clone_box(&*target);

    let source_size = buffer.len() as u64;

    let source_additional_checksum =
        config
            .additional_checksum_algorithm
            .clone()
            .map(|algorithm| {
                compute_source_checksum(
                    &buffer,
                    algorithm,
                    config.transfer_config.multipart_chunksize as usize,
                    config.transfer_config.multipart_threshold as usize,
                    config.full_object_checksum,
                )
            });

    let (checksum_sha256, checksum_sha1, checksum_crc32, checksum_crc32_c, checksum_crc64_nvme) =
        match config.additional_checksum_algorithm.as_ref() {
            Some(ChecksumAlgorithm::Sha256) => {
                (source_additional_checksum.clone(), None, None, None, None)
            }
            Some(ChecksumAlgorithm::Sha1) => {
                (None, source_additional_checksum.clone(), None, None, None)
            }
            Some(ChecksumAlgorithm::Crc32) => {
                (None, None, source_additional_checksum.clone(), None, None)
            }
            Some(ChecksumAlgorithm::Crc32C) => {
                (None, None, None, source_additional_checksum.clone(), None)
            }
            Some(ChecksumAlgorithm::Crc64Nvme) => {
                (None, None, None, None, source_additional_checksum.clone())
            }
            _ => (None, None, None, None, None),
        };

    let byte_stream = ByteStream::from(buffer);

    let get_object_output = GetObjectOutput::builder()
        .set_body(Some(byte_stream))
        .set_content_length(Some(source_size as i64))
        .set_content_type(config.content_type.clone())
        .set_last_modified(Some(DateTime::from_secs(chrono::Utc::now().timestamp())))
        .set_checksum_sha256(checksum_sha256)
        .set_checksum_sha1(checksum_sha1)
        .set_checksum_crc32(checksum_crc32)
        .set_checksum_crc32_c(checksum_crc32_c)
        .set_checksum_crc64_nvme(checksum_crc64_nvme)
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
            source_additional_checksum,
            get_object_output,
            tagging,
            Some(object_checksum),
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

`compute_source_checksum` stays in the file as-is.

- [ ] **Step 2: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 3: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass (behavior unchanged — still `read_to_end` then in-memory upload).

- [ ] **Step 4: Commit**

```bash
git add src/transfer/stdio_to_s3.rs
git commit -m "refactor: extract transfer_buffered from stdio_to_s3::transfer"
```

---

### Task 10: Wire streaming dispatch into `stdio_to_s3::transfer`

Replace the unconditional `read_to_end` with `probe_up_to`. Dispatch based on whether the probe hit EOF.

**Files:**
- Modify: `src/transfer/stdio_to_s3.rs`

- [ ] **Step 1: Replace `transfer` with the dispatching version**

In `src/transfer/stdio_to_s3.rs`, replace the `pub async fn transfer` body (from Task 9) with:

```rust
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    mut reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let threshold = config.transfer_config.multipart_threshold as usize;
    let initial = probe_up_to(&mut reader, threshold).await?;

    if initial.len() < threshold {
        // Reader hit EOF before the threshold — stays in the in-memory path.
        return transfer_buffered(
            config,
            target,
            target_key,
            initial,
            cancellation_token,
            stats_sender,
        )
        .await;
    }

    // Threshold reached — stream the rest.
    transfer_streaming(
        config,
        target,
        target_key,
        initial,
        reader,
        cancellation_token,
        stats_sender,
    )
    .await
}

async fn transfer_streaming(
    config: &Config,
    target: Storage,
    target_key: &str,
    initial: Vec<u8>,
    reader: impl tokio::io::AsyncRead + Unpin + Send + 'static,
    _cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    use tokio::io::AsyncReadExt;

    // Chain the already-buffered bytes with the remaining reader.
    let chained: Box<dyn tokio::io::AsyncRead + Send + Unpin> =
        Box::new(std::io::Cursor::new(initial).chain(reader));

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
        .put_object_stream(target_key, chained, tagging, Some(object_checksum), None)
        .await
        .context(format!("failed to stream to target: {target_key}"))?;

    info!(
        target_key = target_key,
        "stdin streaming transfer completed."
    );

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}
```

**Note:** The `reader: impl AsyncRead + ... + 'static` bound is stricter than before (it now requires `'static`). This is needed because `Box::new(... .chain(reader))` produces a `'static` trait object. Callers must pass owned readers (e.g. `tokio::io::stdin()` or `Cursor<Vec<u8>>`), which is already the case.

- [ ] **Step 2: Update callers if needed**

In `src/bin/s3util/cli/mod.rs`, confirm that the caller passing `tokio::io::stdin()` to `stdio_to_s3::transfer` still typechecks. `tokio::io::stdin()` returns `Stdin`, which is `'static`. No change expected — but run clippy to confirm.

- [ ] **Step 3: Run cargo clippy**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 4: Run unit tests**

Run: `cargo test --lib`
Expected: All tests pass. The `probe_tests` and `checksum_invariant_tests` added in Tasks 3 and 4 should still pass; the newly-added dispatch has no direct unit test (validated via e2e).

- [ ] **Step 5: Commit**

```bash
git add src/transfer/stdio_to_s3.rs src/bin/s3util/cli/mod.rs
git commit -m "feat: wire streaming MPU dispatch into stdio_to_s3::transfer"
```

---

### Task 11: Final verification — fmt, clippy, e2e compile check

**Files:** all

- [ ] **Step 1: Run cargo fmt**

Run: `cargo fmt`
Expected: formatting applied cleanly.

- [ ] **Step 2: Run cargo clippy --all-features**

Run: `cargo clippy --all-features`
Expected: No warnings or errors.

- [ ] **Step 3: Verify e2e tests still compile**

Run: `RUSTFLAGS="--cfg e2e_test" cargo clippy`
Expected: No warnings or errors. (Per CLAUDE.md, we do **not** run e2e tests themselves; we only confirm compilation.)

- [ ] **Step 4: Run full unit test suite**

Run: `cargo test --lib --verbose`
Expected: All tests pass.

- [ ] **Step 5: Commit any formatting changes if needed**

If `cargo fmt` modified any files:

```bash
git add -A
git commit -m "style: apply cargo fmt"
```

If nothing changed, skip this step.

- [ ] **Step 6: Summary for user**

Report to the user:

- Changes committed since plan start (commit hashes list).
- Remind the user to run the full e2e suite manually to validate real-world behavior, especially:
  - `cargo test --test e2e_stdio_integrity_check --features ...` (and similar) under `RUSTFLAGS="--cfg e2e_test"`.
  - Consider exercising a `> multipart_threshold` payload through stdin and confirming memory stays bounded (e.g. via `/usr/bin/time -l` on macOS).

---

## Self-Review Notes

- **Spec coverage:** Every spec requirement maps to a task above — probe helper (Task 3), `transfer_buffered` extraction (Task 9), streaming dispatch (Task 10), `put_object_stream` trait + impl (Tasks 5, 8), `upload_parts_stream` with incremental checksum (Task 6), `upload_stream` entry point (Task 7), `source_total_size` → `Option<u64>` (Task 1), checksum invariant (Task 4), final verification (Task 11).
- **Placeholders:** none — every step that changes code contains the exact code.
- **Type consistency:** `source_total_size: Option<u64>` consistent across Tasks 1, 6, 7; `put_object_stream` signature consistent across Tasks 5, 8, 10; `read_exact_or_eof` signature consistent across Tasks 2, 6.
