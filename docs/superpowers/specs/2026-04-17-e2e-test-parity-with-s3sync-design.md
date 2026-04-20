# E2E Test Parity with s3sync

## Goal

Bring s3util-rs E2E tests to qualitative parity with s3sync's E2E tests for all features that s3util-rs supports. Every assertion that s3sync makes must have an equivalent in s3util-rs, except for features that s3util-rs does not implement (sync/delete mode, dry-run, filtering, versioning, Lua callbacks, error/panic simulations, rate limiting, point-in-time snapshots, max-keys pagination, empty directory handling).

All constant names and pre-calculated hash values are copied verbatim from s3sync. Both repos use the same `test_data/random_data_seed` file to generate deterministic test data.

## Scope

~122 new test functions across 7 categories. No new test files -- all tests added to existing files.

### Category 1: Multipart Auto-Chunksize and KMS Variants (~54 tests)

Add missing auto-chunksize (`--auto-chunksize`) and KMS (`--sse aws:kms`) variants to each multipart integrity check file. s3sync tests every file-size/chunk-size combination with these options; s3util-rs currently only tests the base case.

**Constants to add:** File-local `const` (not `pub const`) in each multipart test file, matching s3sync's pattern. Constants include `SHA256_*_CHUNK` (base64 composite checksums with part count suffix), `CRC64NVME_*` (base64), and `SHA256_*_WHOLE` (hex). All values copied verbatim from s3sync.

**Files and test counts:**

- `e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs` (+1): `_plus_1_auto_chunksize`
- `e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs` (+1 base auto-chunksize, +16 in Category 6)
- `e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs` (+6): auto_chunksize and kms variants for base/sha256/crc64nvme
- `e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs` (+8): auto_chunksize and kms variants, plus minus_1 sha256/crc64nvme
- `e2e_multipart_integrity_check_16mb_file_8mb_chunk.rs` (+8): same pattern
- `e2e_multipart_integrity_check_30mb_file_8mb_chunk.rs` (+8): same pattern
- `e2e_multipart_integrity_check_edge_case.rs` (+6): threshold boundary tests (`8mb_7mb_threshold_5mb_chunk`, `8mb_7mb_threshold_9mb_chunk`, `8mb_9mb_threshold_5mb_chunk`), plus `30mb_sha256`, `30mb_crc64nvme`, `8mb_no_chunk_sha256`

**Each auto-chunksize test pattern:**
1. Create random data file from seed
2. Upload with specific `--multipart-chunksize`
3. Re-transfer with `--auto-chunksize`
4. Assert `sync_complete == 1`, `e_tag_verified == 1`
5. Verify content SHA256 against pre-calculated constant

**Each KMS test pattern:**
1. Create random data file from seed
2. Upload with `--sse aws:kms` and specific chunk size
3. Assert `sync_complete == 1`, `e_tag_verified == 1` or `checksum_verified == 1`
4. Verify server-side encryption header
5. Verify content SHA256

### Category 2: Negative Integrity Tests (~10 tests)

Add to `e2e_integrity_check.rs`. s3sync tests that verification correctly fails or is skipped in expected scenarios. s3util-rs only tests success paths.

**Tests:**

- `local_to_s3_multipart_no_verify_e_tag` -- `--disable-multipart-verify`, assert `e_tag_verified == 0`
- `s3_to_local_multipart_no_verify_e_tag` -- same for download
- `s3_to_s3_single_no_verify_e_tag` -- single-part unaffected by `--disable-multipart-verify`, assert `e_tag_verified == 1`
- `s3_to_s3_multipart_no_verify_e_tag` -- multipart with `--disable-multipart-verify`, assert `e_tag_verified == 0`
- `s3_to_local_multipart_e_tag_ng` -- upload with 5MiB chunks, download with 8MiB chunks (no `--auto-chunksize`), verify ETag mismatch detected
- `s3_to_s3_multipart_e_tag_ng` -- same for S3-to-S3
- `s3_to_local_multipart_e_tag_auto` -- upload with 5MiB chunks, download with `--auto-chunksize`, verify ETag matches
- `s3_to_s3_multipart_e_tag_auto` -- same for S3-to-S3
- `s3_to_s3_multipart_checksum_ng` -- upload with SHA256, copy with different checksum, verify mismatch
- `s3_to_s3_multipart_checksum_ng_different_checksum` -- upload with CRC32, copy expects SHA256, verify mismatch

### Category 3: CRC64NVME Checksum Integrity (~6 tests)

Add to `e2e_integrity_check.rs`. s3sync tests CRC64NVME for each direction, single-part and multipart. s3util-rs is missing per-direction single-part integrity tests.

**Tests:**

- `local_to_s3_single_crc64nvme_checksum` -- assert `checksum_verified == 1`
- `local_to_s3_single_crc64nvme_checksum_without_content_md5` -- `--disable-content-md5`, assert `checksum_verified == 1`
- `s3_to_local_single_crc64nvme_checksum` -- assert `checksum_verified == 1`
- `s3_to_s3_single_crc64nvme_checksum` -- assert `checksum_verified == 1`
- `s3_to_s3_multipart_crc64nvme_checksum_ok` -- verify checksum matches pre-calculated constant
- `s3_to_s3_multipart_crc64nvme_checksum_auto` -- with `--auto-chunksize`

### Category 4: SSE-C Multipart Tests (~6 tests)

Add to `e2e_local_to_s3.rs`, `e2e_s3_to_local.rs`, `e2e_s3_to_s3.rs`. Tests run normally (not `#[ignore]`) using `PutBucketEncryption` to configure the test bucket.

**Infrastructure:** New `TestHelper::create_bucket_with_sse_c_encryption(&self, bucket: &str, region: &str)` in `tests/common/mod.rs` that creates a bucket and calls `PutBucketEncryption` to configure SSE-C.

**Tests:**

- `local_to_s3_with_sse_c_multipart_upload` -- upload 9MiB with SSE-C key, verify content
- `s3_to_local_with_sse_c_multipart_upload` -- download multipart SSE-C object, verify SHA256
- `s3_to_s3_with_sse_c_multipart_upload` -- S3-to-S3 copy of multipart SSE-C object
- `s3_to_s3_with_sse_c_server_side_copy` -- server-side copy with SSE-C (single-part)
- `s3_to_s3_with_sse_c_multipart_server_side_copy` -- server-side copy with SSE-C (multipart)
- `s3_to_s3_server_side_copy_multipart_with_sse_c_auto_chunksize` -- SSE-C + auto-chunksize

### Category 5: Encryption-Specific Integrity (~18 tests)

Add to `e2e_integrity_check.rs`. 3 encryption modes (KMS, DSSE-KMS, SSE-C) x 3 directions x 2 sizes (single-part, multipart).

**Single-part (9):**

- `local_to_s3_sse_kms` / `s3_to_local_sse_kms` / `s3_to_s3_sse_kms`
- `local_to_s3_dsse_kms` / `s3_to_local_dsse_kms` / `s3_to_s3_dsse_kms`
- `local_to_s3_sse_c` / `s3_to_local_sse_c` / `s3_to_s3_sse_c`

Each: assert `sync_complete == 1`, `e_tag_verified == 1`, verify encryption header, verify content MD5/SHA256.

**Multipart (9):**

- `local_to_s3_multipart_sse_kms` / `s3_to_local_multipart_sse_kms` / `s3_to_s3_multipart_sse_kms`
- `local_to_s3_multipart_dsse_kms` / `s3_to_local_multipart_dsse_kms` / `s3_to_s3_multipart_dsse_kms`
- `local_to_s3_multipart_sse_c` / `s3_to_local_multipart_sse_c` / `s3_to_s3_multipart_sse_c`

Each: 9MiB file, assert `sync_complete == 1`, `e_tag_verified == 1`, verify encryption header, verify SHA256 against `SHA256_9M_ZEROS`.

### Category 6: 8MiB Boundary Tests (~16 tests)

Add to `e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs`. Complete test suites for SHA1, CRC32, CRC32C at the 8MiB boundary.

**SHA1 suite (5):**
- `test_multipart_upload_8mb_sha1` / `_plus_1_sha1` / `_minus_1_sha1`
- `test_multipart_upload_8mb_plus_1_sha1_auto_chunksize`
- `test_multipart_upload_8mb_plus_1_sha1_kms`

**CRC32 suite (5):**
- `test_multipart_upload_8mb_crc32` / `_plus_1_crc32` / `_minus_1_crc32`
- `test_multipart_upload_8mb_plus_1_crc32_auto_chunksize`
- `test_multipart_upload_8mb_plus_1_crc32_kms`

**CRC32C suite (5):**
- `test_multipart_upload_8mb_crc32c` / `_plus_1_crc32c` / `_minus_1_crc32c`
- `test_multipart_upload_8mb_plus_1_crc32c_auto_chunksize`
- `test_multipart_upload_8mb_plus_1_crc32c_kms`

**Base auto-chunksize (1):**
- `test_multipart_upload_8mb_plus_1_auto_chunksize`

### Category 7: Server-Side Copy Advanced (~12 tests)

Add to `e2e_s3_to_s3.rs`.

**Full metadata (2):**
- `s3_to_s3_server_side_copy_all_metadata` -- all 7+ metadata fields, verify with `verify_test_object_metadata`
- `s3_to_s3_server_side_copy_multipart_all_metadata` -- multipart variant

**Tagging (2):**
- `s3_to_s3_with_tagging` -- verify tags via `get_object_tagging`
- `s3_to_s3_server_side_copy_with_tagging`

**Special characters (2):**
- `s3_to_s3_server_side_copy_special_chars` -- key with `c++☃test`
- `s3_to_s3_server_side_copy_special_chars_multipart`

**Multipart + metadata (2):**
- `s3_to_s3_server_side_copy_multipart_with_metadata`
- `s3_to_s3_server_side_copy_multipart_auto_chunksize_metadata`

**Website redirect (2):**
- `s3_to_s3_server_side_copy_with_website_redirect`
- `s3_to_s3_server_side_copy_multipart_with_website_redirect`

**Full-object checksum + server-side copy (2):**
- `s3_to_s3_server_side_copy_multipart_full_object_crc32`
- `s3_to_s3_server_side_copy_multipart_full_object_crc32c`

## Infrastructure Changes

### `tests/common/mod.rs`

Add `TestHelper::create_bucket_with_sse_c_encryption`:
- Creates bucket with `create_bucket`
- Calls `PutBucketEncryption` to configure SSE-C
- Used by all SSE-C tests (Categories 4 and 5)

### File-Local Constants

Each multipart test file gets file-local `const` definitions for `SHA256_*_CHUNK`, `CRC64NVME_*`, and `SHA256_*_WHOLE` values. All values copied verbatim from s3sync's corresponding test files. Constants are private to each file (not `pub`), matching s3sync's structure.

## Out of Scope

- Sync/delete mode, dry-run, skip counting, mtime-based diff detection
- Filtering (include/exclude, metadata, tag, content-type)
- Versioning, Lua callbacks, event/filter/preprocess callbacks
- Error/panic simulations
- Rate limiting, point-in-time snapshots
- Max-keys pagination, empty directory handling
- Cancel tests
