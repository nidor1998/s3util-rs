# E2E Test Parity with s3sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ~122 E2E tests to bring s3util-rs to qualitative parity with s3sync's test coverage.

**Architecture:** Tests are added to existing files following the established pattern. Each test function creates its own bucket(s), runs `cp_test_data()` with specific CLI args, asserts stats counts, verifies ETag/checksum/content, then cleans up. File-local constants for SHA256/CRC64NVME are added to multipart test files, matching s3sync's names and values exactly.

**Tech Stack:** Rust, tokio, aws-sdk-s3, s3util-rs test harness (`TestHelper`, `StatsCount`)

**Reference files:**
- Spec: `docs/superpowers/specs/2026-04-17-e2e-test-parity-with-s3sync-design.md`
- s3sync tests: `s3sync/tests/` (reference implementation)
- Existing patterns: `tests/e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs` (representative)
- Test harness: `tests/common/mod.rs`

**Build & test commands:**
```bash
# Compile check only (no AWS needed)
RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features

# Run a single E2E test (requires AWS credentials)
RUST_MIN_STACK=20000000 RUSTFLAGS="--cfg e2e_test" cargo test --all-features test_name_here -- --nocapture --test-threads=1

# Lint
cargo fmt --all --check --verbose
cargo clippy --all-features
```

---

### Task 0: Add SSE-C bucket helper to common/mod.rs

**Files:**
- Modify: `tests/common/mod.rs`

- [ ] **Step 1: Add the `create_bucket_with_sse_c_encryption` method**

Add this method to the `impl TestHelper` block, after the existing `create_directory_bucket` method (around line 280):

```rust
    pub async fn create_bucket_with_sse_c_encryption(&self, bucket: &str, region: &str) {
        self.create_bucket(bucket, region).await;
        // Note: SSE-C is per-request encryption (key provided in each request header).
        // No PutBucketEncryption call needed — the --sse-c and --sse-c-key CLI args
        // handle this at the request level.
    }
```

> **Implementation note:** SSE-C is a per-request encryption mode. The bucket does not need special configuration. The `--sse-c` and `--sse-c-key` CLI args pass the encryption key with each S3 API call. If `PutBucketEncryption` is needed for the specific AWS account setup, add it here. For now this method is a wrapper that documents intent and can be extended later.

- [ ] **Step 2: Verify it compiles**

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features`
Expected: Compiles with no errors.

- [ ] **Step 3: Commit**

```bash
git add tests/common/mod.rs
git commit -m "feat: add create_bucket_with_sse_c_encryption test helper"
```

---

### Task 1: Add auto-chunksize test to 5mb_file_5mb_chunk

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs`

- [ ] **Step 1: Add file-local constants**

Add these constants inside the `mod tests` block, right after `use super::*;`:

```rust
    // Pre-computed values from s3sync (same seed file, same names)
    const SHA256_5M_PLUS_1_FILE_WHOLE: &str =
        "e3bfafb553570dea7233d3a6d6d5d3ac3cff422cd1b5b5f7af767f0778511ef9";
```

- [ ] **Step 2: Add the auto-chunksize test**

Append to the end of `mod tests`, before the closing `}`:

```rust
    /// Upload 5 MiB+1 with 5MiB chunks, then S3-to-S3 copy with --auto-chunksize.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_autochunk.bin", 5, 1)
                .unwrap();

        // Upload with 5MiB chunks
        let source_s3 = format!("s3://{}/5mb_plus1_autochunk.bin", bucket1);
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
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "5mb_plus1_autochunk.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        // S3-to-S3 with auto-chunksize
        let target_s3 = format!("s3://{}/5mb_plus1_autochunk.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "5mb_plus1_autochunk.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_5M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features`

- [ ] **Step 4: Run `cargo fmt` and `cargo clippy --all-features`**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs
git commit -m "feat: add 5mb auto-chunksize multipart integrity test"
```

---

### Task 2: Add auto-chunksize and KMS tests to 10mb_file_5mb_chunk

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs`

- [ ] **Step 1: Add file-local constants inside `mod tests`**

```rust
    // Pre-computed values from s3sync (same seed file, same names)
    const SHA256_10M_FILE_WHOLE: &str =
        "d5fc3f080e832d82161f9461291f87989b81a9e6281c33589d9563adefb46055";
    const SHA256_10M_PLUS_1_FILE_WHOLE: &str =
        "cbb719063c17ba48ec3925cc4ba8267addd8515b4f69e689da1dfc3a6683191a";
```

- [ ] **Step 2: Add 6 test functions**

Append these to `mod tests`:

```rust
    /// 10 MiB+1 with --auto-chunksize (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac.bin", 10, 1).unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac.bin", bucket1);
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
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);

        let target_s3 = format!("s3://{}/10mb_plus1_ac.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --auto-chunksize + SHA256.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac_sha.bin", 10, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac_sha.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/10mb_plus1_ac_sha.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac_sha.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --auto-chunksize + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac_crc64.bin", 10, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac_crc64.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/10mb_plus1_ac_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac_crc64.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --sse aws:kms (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_kms.bin", 10, 1).unwrap();

        let target = format!("s3://{}/10mb_plus1_kms.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --sse aws:kms + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_kms_crc64.bin", 10, 1)
                .unwrap();

        let target = format!("s3://{}/10mb_plus1_kms_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB-1 with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_5mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_minus1_5c_sha.bin", 10, -1)
                .unwrap();

        let target = format!("s3://{}/10mb_minus1_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_minus1_5c_sha.bin",
                ETAG_10M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 3: Verify it compiles**

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features`

- [ ] **Step 4: Run `cargo fmt` and `cargo clippy --all-features`**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs
git commit -m "feat: add 10mb auto-chunksize and KMS multipart integrity tests"
```

---

### Task 3: Add tests to 16mb_file_5mb_chunk

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs`

- [ ] **Step 1: Read the existing file to understand its current tests**

Read: `tests/e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs`

- [ ] **Step 2: Add file-local constants inside `mod tests`**

```rust
    const SHA256_16M_FILE_WHOLE: &str =
        "23bf32cdfd60784647663a160aee7c46ca7941173d48ad37db52713fda4562e1";
    const SHA256_16M_PLUS_1_FILE_WHOLE: &str =
        "0fbb2466d100013b3716965c89ac0c1375bba2c8f126e63ee6bc5ffff68ef33b";
    const SHA256_16M_MINUS_1_FILE_WHOLE: &str =
        "cf674acbd51c8c0e3c08ba06cb8b2bcfa871b2193399cca34d3915b8312f57cb";
```

- [ ] **Step 3: Add 8 test functions**

Follow the exact same patterns as Task 2, substituting:
- File size: 16 MB, chunk size: 5 MiB (`--multipart-threshold 5MiB --multipart-chunksize 5MiB`)
- Constants: `ETAG_16M_*_FILE_5M_CHUNK`, `SHA256_16M_*_FILE_WHOLE`
- File name prefix: `16mb_` in S3 keys

Tests to add:
1. `test_multipart_upload_16mb_plus_1_auto_chunksize` -- ETag only
2. `test_multipart_upload_16mb_plus_1_auto_chunksize_sha256`
3. `test_multipart_upload_16mb_plus_1_auto_chunksize_crc64nvme`
4. `test_multipart_upload_16mb_plus_1_kms` -- ETag only
5. `test_multipart_upload_16mb_plus_1_kms_crc64nvme`
6. `test_multipart_upload_16mb_minus_1_5mb_chunk_sha256`
7. `test_multipart_upload_16mb_minus_1_5mb_chunk_crc64nvme`
8. `test_multipart_upload_16mb_plus_1_kms_sha256` -- already exists? Check first. If not, add it.

> **For the implementer:** Read the existing file first. Some of these may already exist (e.g., `_kms_sha256`). Only add the ones that are missing. Each test follows the patterns in Task 2 exactly — just change file sizes, S3 key names, and constant names.

- [ ] **Step 4: Verify it compiles, run fmt/clippy**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs
git commit -m "feat: add 16mb/5mb auto-chunksize and KMS multipart integrity tests"
```

---

### Task 4: Add tests to 16mb_file_8mb_chunk

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_16mb_file_8mb_chunk.rs`

- [ ] **Step 1: Read the existing file**

- [ ] **Step 2: Add file-local constants**

Same `SHA256_16M_*` whole-file constants as Task 3.

- [ ] **Step 3: Add 8 test functions**

Same pattern as Task 3, but with `--multipart-chunksize 8MiB` and `ETAG_16M_*_FILE_8M_CHUNK` constants:
1. `test_multipart_upload_16mb_plus_1_8mb_auto_chunksize`
2. `test_multipart_upload_16mb_plus_1_8mb_auto_chunksize_sha256`
3. `test_multipart_upload_16mb_plus_1_8mb_auto_chunksize_crc64nvme`
4. `test_multipart_upload_16mb_plus_1_8mb_kms`
5. `test_multipart_upload_16mb_plus_1_8mb_kms_sha256`
6. `test_multipart_upload_16mb_plus_1_8mb_kms_crc64nvme`
7. `test_multipart_upload_16mb_minus_1_8mb_chunk_sha256`
8. `test_multipart_upload_16mb_minus_1_8mb_chunk_crc64nvme`

- [ ] **Step 4: Verify it compiles, run fmt/clippy**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_16mb_file_8mb_chunk.rs
git commit -m "feat: add 16mb/8mb auto-chunksize and KMS multipart integrity tests"
```

---

### Task 5: Add tests to 30mb_file_8mb_chunk

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_30mb_file_8mb_chunk.rs`

- [ ] **Step 1: Read the existing file**

- [ ] **Step 2: Add file-local constants**

```rust
    const SHA256_30M_FILE_WHOLE_HEX: &str =
        "05c1c771d4886e4cefdf0a4c0b907913fe2f829dd767418c94ea278b0b8bc3f9";
    const SHA256_30M_PLUS_1_FILE_WHOLE: &str =
        "4be88d40a77bbb954cad4715fca1f28a5fd7261bc34f9d9d7f4c6f5ea0dfb095";
    const SHA256_30M_MINUS_1_FILE_WHOLE: &str =
        "15ec020d762780610650cc065415691069c35ca2a400b7801f615114edc0737f";
```

> Note: `SHA256_30M_FILE_WHOLE_HEX` may already exist in `common/mod.rs` as a pub const. If so, use `common::SHA256_30M_FILE_WHOLE_HEX` instead of a local const.

- [ ] **Step 3: Add 8 test functions**

Same pattern with `--multipart-chunksize 8MiB` and `ETAG_30M_*_FILE_8M_CHUNK`:
1. `test_multipart_upload_30mb_plus_1_auto_chunksize`
2. `test_multipart_upload_30mb_plus_1_auto_chunksize_sha256`
3. `test_multipart_upload_30mb_plus_1_auto_chunksize_crc64nvme`
4. `test_multipart_upload_30mb_plus_1_kms`
5. `test_multipart_upload_30mb_plus_1_kms_sha256`
6. `test_multipart_upload_30mb_plus_1_kms_crc64nvme`
7. `test_multipart_upload_30mb_minus_1_8mb_chunk_sha256`
8. `test_multipart_upload_30mb_minus_1_8mb_chunk_crc64nvme`

- [ ] **Step 4: Verify it compiles, run fmt/clippy**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_30mb_file_8mb_chunk.rs
git commit -m "feat: add 30mb/8mb auto-chunksize and KMS multipart integrity tests"
```

---

### Task 6: Add edge case tests

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_edge_case.rs`

- [ ] **Step 1: Read the existing file**

- [ ] **Step 2: Add file-local constants from s3sync**

```rust
    const SHA256_8M_FILE_5M_CHUNK: &str =
        "EZAvWUpvGrpch+0S5qFJhcwxd6bw9HtocRRVc/FAwQA=-2";
    const SHA256_8M_FILE_NO_CHUNK: &str =
        "zV9Xxv/j9oUQSrpuxyaLqrh5BgMDS97IMCKLVy2ExaQ=";
    const CRC64NVME_8M_FILE_NO_CHUNK: &str = "io2hnVvxKgU=";
    const SHA256_30M_FILE_8M_CHUNK: &str =
        "5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4";
    const CRC64NVME_30M_FILE_8M_CHUNK: &str = "rrk4q4lsMS4=";
    const SHA256_30M_FILE_NO_CHUNK: &str =
        "BcHHcdSIbkzv3wpMC5B5E/4vgp3XZ0GMlOoniwuLw/k=";
    const CRC64NVME_30M_FILE_NO_CHUNK: &str = "rrk4q4lsMS4=";
```

- [ ] **Step 3: Add 6 test functions**

```rust
    /// 8MiB file with 7MiB threshold, 9MiB chunk (single chunk multipart).
    #[tokio::test]
    async fn test_multipart_upload_8mb_7mb_threshold_9mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_7t_9c.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_7t_9c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "7MiB",
                "--multipart-chunksize",
                "9MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_7t_9c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8MiB file with 9MiB threshold, 5MiB chunk (single put, below threshold).
    #[tokio::test]
    async fn test_upload_8mb_9mb_threshold_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_9t_5c.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_9t_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "9MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_9t_5c.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30MiB file with SHA256 checksum, 8MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_sha256.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb_sha256.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30MiB file with CRC64NVME checksum, 8MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_crc64.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb_crc64.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8MiB file with SHA256, no multipart (single put, 9MiB threshold).
    #[tokio::test]
    async fn test_upload_8mb_no_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_nc_sha.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_nc_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "9MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_nc_sha.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8MiB file with CRC64NVME, no multipart (9MiB threshold).
    #[tokio::test]
    async fn test_upload_8mb_no_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_nc_crc64.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_nc_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "9MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_nc_crc64.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 4: Verify it compiles, run fmt/clippy**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_edge_case.rs
git commit -m "feat: add edge case multipart integrity tests (threshold boundaries, 30mb, 8mb no-chunk)"
```

---

### Task 7: Add 8MiB boundary tests (SHA1/CRC32/CRC32C + auto-chunksize)

**Files:**
- Modify: `tests/e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs`

- [ ] **Step 1: Read the existing file**

- [ ] **Step 2: Add file-local constants from s3sync**

```rust
    const SHA256_8M_PLUS_1_FILE_WHOLE: &str =
        "e0a269be5fbff701eba9a07f82027f5a1e22bebc8df2f2027840a02184b84b3c";
    const SHA256_8M_MINUS_1_FILE_WHOLE: &str =
        "2c7ffa514126e20d9e4fce79b97ff739cb213852400dc1dab07a529da4ec3e44";
```

- [ ] **Step 3: Add 17 test functions (16 from Category 6 + 1 auto-chunksize from Category 1)**

Each test follows the same pattern. Here is one representative for each checksum algorithm, plus the auto-chunksize test:

```rust
    /// 8 MiB+1 with --auto-chunksize (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac.bin", 8, 1).unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);

        let target_s3 = format!("s3://{}/8mb_plus1_ac.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "8mb_plus1_ac.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_8M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB with SHA1 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_8mb_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_sha1.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_sha1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA1",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_sha1.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

The remaining 15 tests follow these exact patterns. For each of SHA1, CRC32, CRC32C:
- `test_multipart_upload_8mb_{algo}` -- exact 8MiB, `--additional-checksum-algorithm {ALGO}`
- `test_multipart_upload_8mb_plus_1_{algo}` -- 8MiB+1
- `test_multipart_upload_8mb_minus_1_{algo}` -- 8MiB-1
- `test_multipart_upload_8mb_plus_1_{algo}_auto_chunksize` -- plus_1 + `--auto-chunksize` (two-bucket pattern)
- `test_multipart_upload_8mb_plus_1_{algo}_kms` -- plus_1 + `--sse aws:kms`

Where `{algo}` is `sha1`/`crc32`/`crc32c` and `{ALGO}` is `SHA1`/`CRC32`/`CRC32C`.

> **For the implementer:** Write all 17 functions. Do NOT use macros or helper functions — each test must be a standalone `#[tokio::test] async fn`.

- [ ] **Step 4: Verify it compiles, run fmt/clippy**

- [ ] **Step 5: Commit**

```bash
git add tests/e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs
git commit -m "feat: add 8mb boundary SHA1/CRC32/CRC32C and auto-chunksize multipart tests"
```

---

### Task 8: Add negative integrity tests

**Files:**
- Modify: `tests/e2e_integrity_check.rs`

- [ ] **Step 1: Read the full existing file**

- [ ] **Step 2: Add 10 negative test functions**

Add a new section at the end of `mod tests`:

```rust
    // ---------------------------------------------------------------
    // Negative / no-verify tests
    // ---------------------------------------------------------------

    /// Multipart upload with --disable-multipart-verify: e_tag_verified should be 0.
    #[tokio::test]
    async fn local_to_s3_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "no_verify_mp.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/no_verify_mp.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-multipart-verify",
                local_dir.join("no_verify_mp.bin").to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// S3-to-Local multipart with --disable-multipart-verify.
    #[tokio::test]
    async fn s3_to_local_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_sized_object(&bucket, "dl_no_verify_mp.bin", 9 * 1024 * 1024)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("dl_no_verify_mp.bin");
        let source = format!("s3://{}/dl_no_verify_mp.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-multipart-verify",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// S3-to-S3 single-part: --disable-multipart-verify has no effect on single-part.
    #[tokio::test]
    async fn s3_to_s3_single_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "s2s_single_noverify.dat", vec![0u8; 1024])
            .await;

        let source = format!("s3://{}/s2s_single_noverify.dat", bucket1);
        let target = format!("s3://{}/s2s_single_noverify.dat", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-multipart-verify",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// S3-to-S3 multipart with --disable-multipart-verify.
    #[tokio::test]
    async fn s3_to_s3_multipart_no_verify_e_tag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "s2s_mp_noverify.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/s2s_mp_noverify.bin", bucket1);
        let target = format!("s3://{}/s2s_mp_noverify.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-multipart-verify",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Upload with 5MiB chunks, download with default (8MiB): ETag mismatch detected.
    #[tokio::test]
    async fn s3_to_local_multipart_e_tag_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "etag_ng.bin", 9 * 1024 * 1024);

        // Upload with 5MiB chunks
        let s3_path = format!("s3://{}/etag_ng.bin", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        // Download with default chunk size (8MiB) — ETag will NOT match
        let dl_file = local_dir.join("etag_ng_dl.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                dl_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        // ETag mismatch: verified count should be 0 (mismatch detected as warning)
        assert_eq!(stats.e_tag_verified, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Same as above but for S3-to-S3.
    #[tokio::test]
    async fn s3_to_s3_multipart_e_tag_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "s2s_ng.bin", 9 * 1024 * 1024);

        // Upload with 5MiB chunks
        let source_s3 = format!("s3://{}/s2s_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        // S3-to-S3 with default chunk size — ETag mismatch
        let target_s3 = format!("s3://{}/s2s_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload with 5MiB chunks, download with --auto-chunksize: ETag matches.
    #[tokio::test]
    async fn s3_to_local_multipart_e_tag_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "etag_auto.bin", 9 * 1024 * 1024);

        let s3_path = format!("s3://{}/etag_auto.bin", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let dl_file = local_dir.join("etag_auto_dl.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &s3_path,
                dl_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Same as above but for S3-to-S3.
    #[tokio::test]
    async fn s3_to_s3_multipart_e_tag_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "s2s_auto.bin", 9 * 1024 * 1024);

        let source_s3 = format!("s3://{}/s2s_auto.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/s2s_auto.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload with SHA256, copy with CRC32: checksum mismatch.
    #[tokio::test]
    async fn s3_to_s3_multipart_checksum_ng() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "cksum_ng.bin", 9 * 1024 * 1024);

        let source_s3 = format!("s3://{}/cksum_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                local_dir.join("cksum_ng.bin").to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/cksum_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        // Checksum algorithm mismatch — verification cannot match
        assert_eq!(stats.checksum_verified, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload with CRC32, copy expects SHA256: checksum mismatch.
    #[tokio::test]
    async fn s3_to_s3_multipart_checksum_ng_different_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "cksum_ng2.bin", 9 * 1024 * 1024);

        let source_s3 = format!("s3://{}/cksum_ng2.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                local_dir.join("cksum_ng2.bin").to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/cksum_ng2.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 3: Verify it compiles, run fmt/clippy**

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_integrity_check.rs
git commit -m "feat: add negative integrity tests (disable-verify, ETag mismatch, checksum mismatch)"
```

---

### Task 9: Add CRC64NVME integrity tests

**Files:**
- Modify: `tests/e2e_integrity_check.rs`

- [ ] **Step 1: Add 6 CRC64NVME test functions**

Add a new section after the negative tests:

```rust
    // ---------------------------------------------------------------
    // CRC64NVME checksum integrity per direction
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn local_to_s3_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc64.dat", b"crc64nvme integrity");

        let target = format!("s3://{}/crc64.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_single_crc64nvme_checksum_without_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc64_nomd5.dat", b"crc64nvme no md5");

        let target = format!("s3://{}/crc64_nomd5.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--disable-content-md5-header",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload with CRC64NVME first
        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "dl_crc64.dat", b"crc64nvme download");

        let s3_path = format!("s3://{}/dl_crc64.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        // Download
        let dl_file = local_dir.join("dl_crc64_out.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                dl_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_s3_single_crc64nvme_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "s2s_crc64.dat", b"crc64nvme s3 to s3");

        let source_s3 = format!("s3://{}/s2s_crc64.dat", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/s2s_crc64.dat", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc64nvme_checksum_ok() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "s2s_mp_crc64.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/s2s_mp_crc64.bin", bucket1);
        let target = format!("s3://{}/s2s_mp_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "s2s_mp_crc64.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_crc64nvme_checksum_auto() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "s2s_mp_crc64_ac.bin", 9 * 1024 * 1024);

        // Upload with 5MiB chunks + CRC64NVME
        let source_s3 = format!("s3://{}/s2s_mp_crc64_ac.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                local_dir.join("s2s_mp_crc64_ac.bin").to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/s2s_mp_crc64_ac.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "s2s_mp_crc64_ac.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 2: Verify it compiles, run fmt/clippy**

- [ ] **Step 3: Commit**

```bash
git add tests/e2e_integrity_check.rs
git commit -m "feat: add CRC64NVME checksum integrity tests per direction"
```

---

### Task 10: Add encryption-specific integrity tests

**Files:**
- Modify: `tests/e2e_integrity_check.rs`

- [ ] **Step 1: Add 18 encryption integrity tests**

> **For the implementer:** Add a new section at the end of `mod tests`. Each test follows the same pattern as the existing tests but adds `--sse aws:kms` / `--sse aws:kms:dsse` / `--sse-c --sse-c-key TEST_SSE_C_KEY_1` args and asserts encryption headers. There are 18 tests total: 3 encryption modes x 3 directions x 2 sizes (single/multipart).

The patterns are:
- **Single-part KMS:** Small file, `--sse aws:kms`, verify `ServerSideEncryption::AwsKms` header, `e_tag_verified == 1`
- **Single-part DSSE-KMS:** Same but `--sse aws:kms:dsse`, verify `ServerSideEncryption::AwsKmsDsse`
- **Single-part SSE-C:** Small file, `--sse-c --sse-c-key TEST_SSE_C_KEY_1`, `e_tag_verified == 1`
- **Multipart variants:** 9MiB file, same encryption args, verify SHA256 == `SHA256_9M_ZEROS`

Test function names:
1. `local_to_s3_sse_kms`
2. `s3_to_local_sse_kms`
3. `s3_to_s3_sse_kms`
4. `local_to_s3_dsse_kms`
5. `s3_to_local_dsse_kms`
6. `s3_to_s3_dsse_kms`
7. `local_to_s3_sse_c`
8. `s3_to_local_sse_c`
9. `s3_to_s3_sse_c`
10. `local_to_s3_multipart_sse_kms`
11. `s3_to_local_multipart_sse_kms`
12. `s3_to_s3_multipart_sse_kms`
13. `local_to_s3_multipart_dsse_kms`
14. `s3_to_local_multipart_dsse_kms`
15. `s3_to_s3_multipart_dsse_kms`
16. `local_to_s3_multipart_sse_c`
17. `s3_to_local_multipart_sse_c`
18. `s3_to_s3_multipart_sse_c`

> **For the implementer:** Write all 18 functions following the patterns shown in previous tasks. For KMS/DSSE tests, use the same patterns as `s3_to_s3_with_sse_kms` in `e2e_s3_to_s3.rs`. For SSE-C tests, use `--sse-c --sse-c-key` with the `TEST_SSE_C_KEY_1` constant. For `s3_to_local` SSE-C, the source must have been uploaded with SSE-C, and the download must pass `--source-sse-c --source-sse-c-key`.

- [ ] **Step 2: Verify it compiles, run fmt/clippy**

- [ ] **Step 3: Commit**

```bash
git add tests/e2e_integrity_check.rs
git commit -m "feat: add encryption-specific integrity tests (KMS, DSSE-KMS, SSE-C)"
```

---

### Task 11: Add SSE-C multipart tests

**Files:**
- Modify: `tests/e2e_local_to_s3.rs`
- Modify: `tests/e2e_s3_to_local.rs`
- Modify: `tests/e2e_s3_to_s3.rs`

- [ ] **Step 1: Read each file to find the right insertion point**

- [ ] **Step 2: Add 2 tests to `e2e_local_to_s3.rs`**

```rust
    #[tokio::test]
    async fn local_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "ssec_mp.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/ssec_mp.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse-c",
                "--sse-c-key",
                TEST_SSE_C_KEY_1,
                local_dir.join("ssec_mp.bin").to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 3: Add 1 test to `e2e_s3_to_local.rs`**

```rust
    #[tokio::test]
    async fn s3_to_local_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload with SSE-C
        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&local_dir, "dl_ssec_mp.bin", 9 * 1024 * 1024);

        let s3_path = format!("s3://{}/dl_ssec_mp.bin", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse-c",
                "--sse-c-key",
                TEST_SSE_C_KEY_1,
                local_dir.join("dl_ssec_mp.bin").to_str().unwrap(),
                &s3_path,
            ])
            .await;

        // Download with SSE-C
        let dl_file = local_dir.join("dl_ssec_mp_out.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                &s3_path,
                dl_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        TestHelper::verify_downloaded_file_sha256(dl_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

- [ ] **Step 4: Add 3 tests to `e2e_s3_to_s3.rs`**

Add `s3_to_s3_with_sse_c_multipart_upload`, `s3_to_s3_with_sse_c_server_side_copy`, and `s3_to_s3_with_sse_c_multipart_server_side_copy`. Each follows the same pattern: upload with SSE-C, then S3-to-S3 copy with `--source-sse-c` / `--sse-c` args.

- [ ] **Step 5: Verify it compiles, run fmt/clippy**

- [ ] **Step 6: Commit**

```bash
git add tests/e2e_local_to_s3.rs tests/e2e_s3_to_local.rs tests/e2e_s3_to_s3.rs
git commit -m "feat: add SSE-C multipart upload tests for all directions"
```

---

### Task 12: Add server-side copy advanced tests

**Files:**
- Modify: `tests/e2e_s3_to_s3.rs`

- [ ] **Step 1: Read the existing file to understand current tests and imports**

- [ ] **Step 2: Add 12 test functions**

Test functions to add:
1. `s3_to_s3_server_side_copy_all_metadata` -- Upload with all metadata fields, SSC, verify via `verify_test_object_metadata`
2. `s3_to_s3_server_side_copy_multipart_all_metadata` -- 9MiB variant
3. `s3_to_s3_with_tagging` -- `--tagging TEST_TAGGING`, verify via `get_object_tagging`
4. `s3_to_s3_server_side_copy_with_tagging` -- SSC + tagging
5. `s3_to_s3_server_side_copy_special_chars` -- Key `c++☃test`
6. `s3_to_s3_server_side_copy_special_chars_multipart` -- 9MiB + special chars key
7. `s3_to_s3_server_side_copy_multipart_with_metadata` -- SSC + 9MiB + metadata
8. `s3_to_s3_server_side_copy_multipart_auto_chunksize_metadata` -- SSC + 9MiB + auto-chunksize + metadata
9. `s3_to_s3_server_side_copy_with_website_redirect` -- `--website-redirect-location /redirect`
10. `s3_to_s3_server_side_copy_multipart_with_website_redirect` -- 9MiB + website redirect
11. `s3_to_s3_server_side_copy_multipart_full_object_crc32` -- SSC + `--full-object-checksum` + CRC32
12. `s3_to_s3_server_side_copy_multipart_full_object_crc32c` -- SSC + `--full-object-checksum` + CRC32C

> **For the implementer:** Follow the patterns from existing tests in `e2e_s3_to_s3.rs`. For all-metadata tests, pass all metadata CLI args (`--cache-control`, `--content-disposition`, `--content-encoding`, `--content-language`, `--content-type`, `--metadata`, `--tagging`, `--expires`) and verify with `verify_test_object_metadata`. For tagging tests, verify with `get_object_tagging` and compare tag maps. For special chars, use `c++☃test` as both S3 key and file name.

- [ ] **Step 3: Verify it compiles, run fmt/clippy**

- [ ] **Step 4: Commit**

```bash
git add tests/e2e_s3_to_s3.rs
git commit -m "feat: add server-side copy advanced tests (metadata, tagging, special chars, website redirect)"
```

---

## Verification

After all tasks are complete:

```bash
# Full compile check
RUSTFLAGS="--cfg e2e_test" cargo check --tests --all-features

# Format check
cargo fmt --all --check --verbose

# Clippy
cargo clippy --all-features

# Count new test functions
grep -c '#\[tokio::test\]' tests/e2e_*.rs
```

The total test count should increase by ~122 from the baseline.
