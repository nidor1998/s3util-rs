# E2E Stats Assertion Coverage — Design

## Goal
Add missing `stats.sync_warning`, `stats.e_tag_verified`, and `stats.checksum_verified` assertions across e2e tests so that regressions in transfer-integrity verification can no longer pass silently.

## Scope
14 e2e test files where these assertions are missing or sparse. Integrity-check files (`e2e_integrity_check.rs`, `e2e_multipart_integrity_check_*.rs`, `e2e_stdio_integrity_check.rs`) are already dense and are left alone.

Files (in work order):

1. `e2e_progress.rs`, `e2e_stdio.rs`, `e2e_stdio_metadata.rs`, `e2e_special_characters.rs`
2. `e2e_roundtrip_stdio.rs`, `e2e_roundtrip_local_to_s3.rs`, `e2e_roundtrip_s3_to_s3.rs`, `e2e_roundtrip_express_one_zone.rs`, `e2e_roundtrip_checksum.rs`, `e2e_roundtrip_multipart_etag.rs`
3. `e2e_express_one_zone.rs`, `e2e_stdio_sse.rs`
4. `e2e_local_to_s3.rs`, `e2e_s3_to_local.rs`, `e2e_s3_to_s3.rs`

## Background
`StatsCount` (tests/common/mod.rs:137) aggregates `SyncStatistics` emissions from the production code. The three fields of interest are incremented by:

- `e_tag_verified` — `SyncStatistics::ETagVerified` emitted by `upload_manager.rs::validate_e_tag`, `local/mod.rs`, and `s3_to_stdio.rs` after a successful source-vs-target ETag compare. Skipped when `--disable-multipart-verify` or the source's ETag is unavailable.
- `checksum_verified` — `SyncStatistics::ChecksumVerified` emitted by `validate_checksum` only when `--additional-checksum-mode` is active and the source has an additional checksum that matches the target.
- `sync_warning` — `SyncStatistics::SyncWarning` emitted by the same validators when the source is remote S3 and a mismatch is ambiguous (e.g. chunksize differences). Real corruption of a local source becomes an error instead.

## Assertion rules

| Scenario | sync_warning | e_tag_verified | checksum_verified |
|---|---|---|---|
| Happy path, default (single or multipart, verify not disabled) | 0 | 1 | 0 |
| `--disable-multipart-verify` | 0 | 0 | 0 |
| `--additional-checksum-algorithm X` (local→S3, stdin→S3) | 0 | 1 | 1 |
| `--enable-additional-checksum` + `--additional-checksum-mode` (S3→local, S3→stdout) | 0 | 1 | 1 |
| S3→S3 with additional-checksum flag and source has checksum | 0 | 1 | 1 |
| Error case (`sync_error == 1`) | 0 | 0 | 0 |
| SSE-C / server-side-copy edge cases | per-test — flag rather than guess | | |

## Process per file
1. Walk each test that captures `stats` from `cp_test_data*`.
2. Insert the three asserts after the existing `sync_complete`/`sync_error` asserts.
3. Run `RUSTFLAGS="--cfg e2e_test" cargo check --tests` after each file to verify compilation.
4. Do not run the e2e tests; do not commit. The user commits.

## Risks
- Expected values may be wrong for edge cases (SSE-C, server-side-copy, conditional writes that fail pre-transfer). These will be flagged per test and left for user judgment rather than guessed.
- A subtle regression — if an assert encodes the wrong expectation, e2e runs will fail until the expectation is corrected. The user runs e2e tests locally, so feedback is fast.

## Out of scope
- Refactoring tests or common helpers.
- Adding new test scenarios.
- Changing production code.
