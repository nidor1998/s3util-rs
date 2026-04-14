# s3util cp — Design Specification

## Overview

`s3util cp` is a single-file copy tool for Amazon S3, equivalent to `aws s3 cp`. It reuses ~98% of the s3sync crate's storage layer via copy-and-adapt (same pattern as s3rm-rs). It does **not** use s3sync's pipeline mechanism — instead, it calls Storage methods directly for each transfer direction.

Stdio (`-`) is supported as source or target for piping.

## Usage

```
s3util cp [OPTIONS] [SOURCE] [TARGET]
```

- `SOURCE` / `TARGET`: `s3://<BUCKET>[/key]`, local file path, or `-` (stdio)
- Both cannot be `-`
- Both cannot be local paths
- Stdio disables parallel processing; validation is performed to the extent possible

## Architecture

### Approach

Copy-and-adapt from s3sync. Use Storage directly, no pipeline. The transfer module is the only truly new code (~2%).

### Future Extensibility

s3util is designed as a multi-subcommand CLI. Planned subcommands:

- `s3util cp` (this spec)
- `s3util head` (HeadObject)
- `s3util mv` (MoveObject — reuses transfer + delete_object)
- `s3util rm` (DeleteObject)
- `s3util mb` (MakeBucket)
- `s3util hb` (HeadBucket)
- `s3util rb` (RemoveBucket)

The structure supports adding new subcommands by:
1. Adding `XxxArgs` in `config/args/`
2. Extending `Commands` enum
3. Adding a runner in `cli/`

## Project Structure

```
s3util-rs/
├── Cargo.toml
├── src/
│   ├── lib.rs                    # Public API: re-exports config, storage, types, transfer
│   ├── bin/
│   │   └── s3util/
│   │       ├── main.rs           # CLI entry point, arg parsing, tracing setup
│   │       └── cli/
│   │           ├── mod.rs        # Shared: ctrl-c handler, progress display
│   │           └── cp.rs         # cp subcommand runner
│   ├── config/
│   │   ├── mod.rs                # Config struct (shared across subcommands)
│   │   └── args/
│   │       ├── mod.rs            # Cli, Commands enum
│   │       └── cp.rs             # CpArgs (clap derive)
│   ├── storage/                  # ~98% copy from s3sync (trimmed)
│   │   ├── mod.rs                # StorageTrait, Storage type, StorageFactory
│   │   ├── local/
│   │   │   └── mod.rs            # LocalStorage (single file I/O only, no listing)
│   │   └── s3/
│   │       ├── mod.rs            # S3Storage (no listing methods)
│   │       └── client_builder.rs # AWS SDK client builder (unchanged)
│   ├── transfer/                 # NEW — cp-specific logic
│   │   ├── mod.rs                # TransferDirection enum, dispatch function
│   │   ├── local_to_s3.rs        # File -> S3 (put_object / multipart upload)
│   │   ├── s3_to_local.rs        # S3 -> File (get_object -> write)
│   │   ├── s3_to_s3.rs           # S3 -> S3 (server-side copy or download+upload)
│   │   ├── stdio_to_s3.rs        # stdin -> S3 (streaming multipart, unknown size)
│   │   ├── s3_to_stdio.rs        # S3 -> stdout (get_object -> stdout)
│   │   └── progress.rs           # Progress bar wrapping byte streams
│   └── types/
│       ├── mod.rs                # Reused types from s3sync
│       ├── token.rs              # PipelineCancellationToken (copied from s3sync)
│       └── error.rs              # Error types
└── tests/
    ├── common/mod.rs             # Test helpers (adapted from s3sync)
    └── (e2e test files, see Testing section)
```

## Storage Layer (Copy-and-Adapt from s3sync)

### StorageTrait — Kept Methods

| Method | Purpose |
|---|---|
| `is_local_storage()` | Transfer direction detection |
| `is_express_onezone_storage()` | Express One Zone logic |
| `get_object()` | Download from S3 |
| `head_object()` | Get object metadata/size for progress bar |
| `head_object_first_part()` | Multipart verification |
| `get_object_parts()` | Multipart verification |
| `get_object_parts_attributes()` | Multipart verification |
| `put_object()` | Upload to S3 |
| `get_object_tagging()` | Tagging copy (`--sync-latest-tagging`) |
| `put_object_tagging()` | Set tagging (`--tagging`) |
| `delete_object()` | Kept for future `mv` subcommand |
| `get_client()` | Server-side copy |
| `generate_copy_source_key()` | Server-side copy |
| `get_stats_sender()` / `send_stats()` | Progress reporting |
| `get_rate_limit_bandwidth()` | Bandwidth limiting |
| `get_local_path()` | Local file path resolution |
| `set_warning()` | Warning state propagation |

### StorageTrait — Removed Methods

| Method | Reason |
|---|---|
| `list_objects()` | cp copies a single object, no directory listing |
| `list_object_versions()` | No version listing |
| `get_object_versions()` | No version listing (`--version-id` passes to `get_object`) |
| `delete_object_tagging()` | cp doesn't delete tagging |
| `is_versioning_enabled()` | No versioning mode |

### LocalStorage Trimming

- Remove `walkdir` directory traversal listing logic — cp reads/writes a single file
- Remove `listing_worker_semaphore` — no listing
- Remove `object_to_list` — no listing
- Remove `rate_limit_objects_per_sec` — single object

### S3Storage Trimming

- Remove listing methods (`list_objects`, `list_object_versions`)
- Remove `rate_limit_objects_per_sec` — single object

## Transfer Module

Five direction-specific functions. Each takes source, target, config, cancellation token, and stats sender.

### Transfer Direction Detection

```rust
enum TransferDirection {
    LocalToS3,
    S3ToLocal,
    S3ToS3,
    StdioToS3,
    S3ToStdio,
}
```

Determined by parsing source/target args: `s3://` prefix = S3, `-` = stdio, otherwise = local path.

### Per-Direction Logic

| Direction | Flow |
|---|---|
| `LocalToS3` | Read local file size. If below multipart threshold: single `put_object`. Otherwise: multipart upload. Progress bar wraps the read stream. |
| `S3ToLocal` | `head_object` for size. `get_object` and write body stream to file. Progress bar wraps the write. |
| `S3ToS3` | If `--server-side-copy`: `copy_object` via `get_client()`. Otherwise: `get_object` from source, `put_object` to target (download+upload). |
| `StdioToS3` | Size unknown. Always multipart upload. Read stdin in chunksize-sized buffers, upload each as a part. Progress bar shows bytes only (no percentage). |
| `S3ToStdio` | `head_object` for size. `get_object` and stream body to stdout. Progress bar shows bytes + percentage. |

### Multipart Verification

After upload, verify ETag or additional checksum against source (unless `--disable-multipart-verify` / `--disable-etag-verify`). Reuses s3sync's existing verification logic in Storage.

### Cancellation

Check `cancellation_token.is_cancelled()` between chunks. On cancellation during multipart upload, abort the multipart upload to clean up.

## Config & CLI Args

### Subcommand Structure

```rust
#[derive(Parser)]
#[command(name = "s3util", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Copy objects from/to S3
    Cp(CpArgs),
}
```

### CpArgs Options (Exact Spec)

**Positional arguments:**
- `[SOURCE]` — `s3://<BUCKET_NAME>[/prefix]` or local path or `-` (env: `SOURCE`)
- `[TARGET]` — `s3://<BUCKET_NAME>[/prefix]` or local path or `-` (env: `TARGET`)

**General:**
- `--show-progress` — show progress bar (env: `SHOW_NO_PROGRESS`)
- `--server-side-copy` — S3-to-S3 server-side copy only (env: `SERVER_SIDE_COPY`)

**AWS Configuration:**
- `--aws-config-file <FILE>` (env: `AWS_CONFIG_FILE`)
- `--aws-shared-credentials-file <FILE>` (env: `AWS_SHARED_CREDENTIALS_FILE`)
- `--source-profile <SOURCE_PROFILE>` (env: `SOURCE_PROFILE`)
- `--source-access-key <SOURCE_ACCESS_KEY>` (env: `SOURCE_ACCESS_KEY`)
- `--source-secret-access-key <SOURCE_SECRET_ACCESS_KEY>` (env: `SOURCE_SECRET_ACCESS_KEY`)
- `--source-session-token <SOURCE_SESSION_TOKEN>` (env: `SOURCE_SESSION_TOKEN`)
- `--target-profile <TARGET_PROFILE>` (env: `TARGET_PROFILE`)
- `--target-access-key <TARGET_ACCESS_KEY>` (env: `TARGET_ACCESS_KEY`)
- `--target-secret-access-key <TARGET_SECRET_ACCESS_KEY>` (env: `TARGET_SECRET_ACCESS_KEY`)
- `--target-session-token <TARGET_SESSION_TOKEN>` (env: `TARGET_SESSION_TOKEN`)

**Source Options:**
- `--source-region <SOURCE_REGION>` (env: `SOURCE_REGION`)
- `--source-endpoint-url <SOURCE_ENDPOINT_URL>` (env: `SOURCE_ENDPOINT_URL`)
- `--source-accelerate` (env: `SOURCE_ACCELERATE`)
- `--source-request-payer` (env: `SOURCE_REQUEST_PAYER`)
- `--source-force-path-style` (env: `SOURCE_FORCE_PATH_STYLE`)

**Target Options:**
- `--target-region <TARGET_REGION>` (env: `TARGET_REGION`)
- `--target-endpoint-url <TARGET_ENDPOINT_URL>` (env: `TARGET_ENDPOINT_URL`)
- `--target-accelerate` (env: `TARGET_ACCELERATE`)
- `--target-request-payer` (env: `TARGET_REQUEST_PAYER`)
- `--target-force-path-style` (env: `TARGET_FORCE_PATH_STYLE`)
- `--storage-class <STORAGE_CLASS>` (env: `STORAGE_CLASS`)

**Verification:**
- `--additional-checksum-algorithm <ADDITIONAL_CHECKSUM_ALGORITHM>` (env: `ADDITIONAL_CHECKSUM_ALGORITHM`)
- `--full-object-checksum` (env: `FULL_OBJECT_CHECKSUM`)
- `--enable-additional-checksum` (env: `ENABLE_ADDITIONAL_CHECKSUM`)
- `--disable-multipart-verify` (env: `DISABLE_MULTIPART_VERIFY`)
- `--disable-etag-verify` (env: `DISABLE_ETAG_VERIFY`)
- `--disable-additional-checksum-verify` (env: `DISABLE_ADDITIONAL_CHECKSUM_VERIFY`)

**Performance:**
- `--max-parallel-uploads <MAX_PARALLEL_UPLOADS>` (env: `MAX_PARALLEL_UPLOADS`, default: 16)

**Multipart Settings:**
- `--multipart-threshold <MULTIPART_THRESHOLD>` (env: `MULTIPART_THRESHOLD`, default: 8MiB)
- `--multipart-chunksize <MULTIPART_CHUNKSIZE>` (env: `MULTIPART_CHUNKSIZE`, default: 8MiB)
- `--auto-chunksize` (env: `AUTO_CHUNKSIZE`)

**Metadata/Headers:**
- `--cache-control <CACHE_CONTROL>` (env: `CACHE_CONTROL`)
- `--content-disposition <CONTENT_DISPOSITION>` (env: `CONTENT_DISPOSITION`)
- `--content-encoding <CONTENT_ENCODING>` (env: `CONTENT_ENCODING`)
- `--content-language <CONTENT_LANGUAGE>` (env: `CONTENT_LANGUAGE`)
- `--content-type <CONTENT_TYPE>` (env: `CONTENT_TYPE`)
- `--expires <EXPIRES>` (env: `EXPIRES`)
- `--metadata <METADATA>` (env: `METADATA`)
- `--website-redirect <WEBSITE_REDIRECT>` (env: `WEBSITE_REDIRECT`)
- `--no-sync-system-metadata` (env: `NO_SYNC_SYSTEM_METADATA`)
- `--no-sync-user-defined-metadata` (env: `NO_SYNC_USER_DEFINED_METADATA`)

**Tagging:**
- `--tagging <TAGGING>` (env: `TAGGING`)
- `--disable-tagging` (env: `DISABLE_TAGGING`)
- `--sync-latest-tagging` (env: `SYNC_LATEST_TAGGING`)

**Versioning:**
- `--version-id`

**Encryption:**
- `--sse <SSE>` (env: `SSE`)
- `--sse-kms-key-id <SSE_KMS_KEY_ID>` (env: `SSE_KMS_KEY_ID`)
- `--source-sse-c <SOURCE_SSE_C>` (env: `SOURCE_SSE_C`)
- `--source-sse-c-key <SOURCE_SSE_C_KEY>` (env: `SOURCE_SSE_C_KEY`)
- `--source-sse-c-key-md5 <SOURCE_SSE_C_KEY_MD5>` (env: `SOURCE_SSE_C_KEY_MD5`)
- `--target-sse-c <TARGET_SSE_C>` (env: `TARGET_SSE_C`)
- `--target-sse-c-key <TARGET_SSE_C_KEY>` (env: `TARGET_SSE_C_KEY`)
- `--target-sse-c-key-md5 <TARGET_SSE_C_KEY_MD5>` (env: `TARGET_SSE_C_KEY_MD5`)

**Tracing/Logging:**
- `--json-tracing` (env: `JSON_TRACING`)
- `--aws-sdk-tracing` (env: `AWS_SDK_TRACING`)
- `--span-events-tracing` (env: `SPAN_EVENTS_TRACING`)
- `--disable-color-tracing` (env: `DISABLE_COLOR_TRACING`)

**Retry Options:**
- `--aws-max-attempts <max_attempts>` (env: `AWS_MAX_ATTEMPTS`, default: 10)
- `--initial-backoff-milliseconds <initial_backoff>` (env: `INITIAL_BACKOFF_MILLISECONDS`, default: 100)
- `--force-retry-count <FORCE_RETRY_COUNT>` (env: `FORCE_RETRY_COUNT`, default: 5)
- `--force-retry-interval-milliseconds <force_retry_interval>` (env: `FORCE_RETRY_INTERVAL_MILLISECONDS`, default: 1000)

**Timeout Options:**
- `--operation-timeout-milliseconds <operation_timeout>` (env: `OPERATION_TIMEOUT_MILLISECONDS`)
- `--operation-attempt-timeout-milliseconds <operation_attempt_timeout>` (env: `OPERATION_ATTEMPT_TIMEOUT_MILLISECONDS`)
- `--connect-timeout-milliseconds <connect_timeout>` (env: `CONNECT_TIMEOUT_MILLISECONDS`)
- `--read-timeout-milliseconds <read_timeout>` (env: `READ_TIMEOUT_MILLISECONDS`)

**Advanced:**
- `--acl <ACL>` (env: `ACL`)
- `--no-guess-mime-type` (env: `NO_GUESS_MIME_TYPE`)
- `--put-last-modified-metadata` (env: `PUT_LAST_MODIFIED_METADATA`)
- `--auto-complete-shell <SHELL>` (env: `AUTO_COMPLETE_SHELL`)
- `--disable-stalled-stream-protection` (env: `DISABLE_STALLED_STREAM_PROTECTION`)
- `--disable-payload-signing` (env: `DISABLE_PAYLOAD_SIGNING`)
- `--disable-content-md5-header` (env: `DISABLE_CONTENT_MD5_HEADER`)
- `--disable-express-one-zone-additional-checksum` (env: `DISABLE_EXPRESS_ONE_ZONE_ADDITIONAL_CHECKSUM`)
- `--if-match` (env: `IF_MATCH`)
- `--copy-source-if-match` (env: `COPY_SOURCE_IF_MATCH`)
- `--if-none-match` (env: `IF_NONE_MATCH`)

**Verbosity:**
- `-v, --verbose...` — increase logging verbosity
- `-q, --quiet...` — decrease logging verbosity

### Config Validation (`Config::try_from(CpArgs)`)

- Source and target cannot both be `-`
- Source and target cannot both be local paths
- `--server-side-copy` requires both source and target to be S3
- `--version-id` requires source to be S3
- Stdio is incompatible with `--server-side-copy`

### Config — Fields Removed vs s3sync

| Removed Field | Reason |
|---|---|
| `worker_size` | No pipeline workers |
| `sync_with_delete` | cp doesn't delete |
| `dry_run` | cp doesn't have dry-run |
| `follow_symlinks` | Single file, not directory traversal |
| `head_each_target` | No target listing |
| `filter_config` (entire struct) | No filtering |
| `point_in_time` | No versioning mode |
| `enable_versioning` | No versioning mode |
| `max_keys` | No listing |
| `rate_limit_objects` | Single object |
| `max_parallel_listings` / `object_listing_queue_size` / `max_parallel_listing_max_depth` | No listing |
| `allow_parallel_listings_in_express_one_zone` | No listing |
| `delete_excluded` | No delete/filter |
| `max_delete` | No delete |
| `ignore_glacier_warnings` | Sync-specific |
| `report_sync_status` / `report_metadata_sync_status` / `report_tagging_sync_status` | Sync-specific reporting |
| All Lua callback fields | No Lua support |
| `event_manager` / `preprocess_manager` | No callbacks |
| `warn_as_error` | Sync-specific |
| All test simulation fields | Pipeline test scaffolding |

## Progress Display

Uses the `indicatif` crate.

| Scenario | Display |
|---|---|
| Size known | `[=====>    ] 45% 4.5MiB/10MiB 2.1MiB/s ETA 2s` |
| Size unknown (stdin) | Spinner + bytes transferred + rate (no percentage) |

- Enabled with `--show-progress`, disabled by default
- Progress bar wraps the byte stream — each chunk advances the bar
- Respects `--disable-color-tracing` for ANSI color control

## Results Report

Uses s3sync's indicator output format. Zero-count fields are omitted.

**Success with ETag + checksum:**
```
transferred 10 MiB | 3.3 MiB/sec,  transferred 1 objects | 0.3 objects/sec,  etag verified 1 objects,  checksum verified 1 objects
```

**Success with ETag only:**
```
transferred 10 MiB | 3.3 MiB/sec,  transferred 1 objects | 0.3 objects/sec,  etag verified 1 objects
```

**Verification failed:**
```
transferred 10 MiB | 3.3 MiB/sec,  transferred 1 objects | 0.3 objects/sec,  warning 1 objects
```

**Cancelled:**
```
transferred 4 MiB | 2.1 MiB/sec,  transferred 0 objects | 0.0 objects/sec,  warning 1 objects
```

## Ctrl-C Handling

Same as s3sync. Uses `PipelineCancellationToken` (wraps `tokio_util::sync::CancellationToken`).

- Ctrl-C triggers `cancellation_token.cancel()`
- Transfer functions check `cancellation_token.is_cancelled()` between chunks
- On cancellation during multipart upload, abort the multipart upload

## Error Handling & Exit Codes

Same exit codes as s3sync:

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | Error (transfer failed, verification failed, etc.) |
| 2 | Invalid arguments |
| 3 | Warning |

**Error scenarios:**

| Scenario | Behavior |
|---|---|
| Source not found (S3 404 / file not found) | Print error, exit 1 |
| Permission denied (S3 403 / filesystem) | Print error, exit 1 |
| Multipart upload fails mid-transfer | Abort multipart upload, print error, exit 1 |
| Verification fails (ETag/checksum mismatch) | Print warning + results report, exit 3 |
| Ctrl-C during transfer | Cancel via PipelineCancellationToken, abort multipart if in progress, print results report |
| Invalid args | Print usage hint, exit 2 |
| Network timeout / retry exhaustion | Follows s3sync's ForceRetryConfig, then error, exit 1 |

**Retry:** Reuses s3sync's `ForceRetryConfig` — retry on transient errors with configurable count and interval.

## Testing

### Unit Tests (in-module `#[cfg(test)]`)

- `config/args/` — argument parsing, validation (both `-` rejected, local-to-local rejected, `--server-side-copy` only with S3-to-S3, etc.)
- `transfer/mod.rs` — `TransferDirection` detection from source/target strings
- `config/mod.rs` — `Config::try_from(CpArgs)` validation

### E2E Tests (`#[cfg(e2e_test)]`, same pattern as s3sync)

**New tests (cp-specific):**
- `e2e_stdio.rs` — pipe through stdin/stdout, compare content
- `e2e_progress.rs` — `--show-progress` output validation

**Reused from s3sync (adapted for single-file semantics):**

Fully reusable:
- `e2e_integrity_check.rs` (66 tests) — ETag/checksum/encryption verification
- `e2e_multipart_integrity_check_10mb_file_5mb_chunk.rs` (15 tests)
- `e2e_multipart_integrity_check_16mb_file_5mb_chunk.rs` (15 tests)
- `e2e_multipart_integrity_check_16mb_file_8mb_chunk.rs` (15 tests)
- `e2e_multipart_integrity_check_30mb_file_8mb_chunk.rs` (15 tests)
- `e2e_multipart_integrity_check_5mb_file_5mb_chunk.rs` (15 tests)
- `e2e_multipart_integrity_check_8mb_file_8mb_chunk.rs` (35 tests)
- `e2e_multipart_integrity_check_edge_case.rs` (12 tests)

Partially reusable (sync-specific tests excluded):
- `e2e_local_to_s3.rs` — basic ops, multipart, checksums, encryption, metadata, storage class, ACL, payload signing, content-type (exclude: dry-run, delete, skip, filters, rate-limit-objects)
- `e2e_s3_to_local.rs` — basic download, multipart, checksums, encryption, integrity, metadata (exclude: delete, filters, empty dir traversal, max-keys listing)
- `e2e_s3_to_s3.rs` — basic copy, server-side copy, multipart, encryption, checksums, metadata, tagging, conditional uploads (exclude: delete, filters, dry-run, metadata/tag filtering)
- `e2e_express_one_zone.rs` — basic local-to-S3, S3-to-local, S3-to-S3, checksum variants (exclude: delete, parallel listing)
- `e2e_cancel_test.rs` — `cancel_put_object`, `cancel_put_object_auto_chunksize` (exclude: `cancel_list_*`, `cancel_sync_object_versions`, `cancel_sync_or_delete_object`)
- `e2e_sync_report.rs` — adapted for cp result format (exclude: metadata/tagging report)

Not reusable (pipeline/sync-specific):
- `error_simulations.rs` — pipeline stage error handling
- `panic_simulations.rs` — pipeline stage panic recovery
- `event_callback.rs` — no callback system
- `filter_callback.rs` — no filter callbacks
- `preprocess_callback.rs` — no preprocess callbacks
- `lua_callback.rs` — no Lua support
- `point_in_time_snapshot.rs` — no versioning mode
- `s3_to_s3_versioning.rs` — no versioning mode

**Estimated total: ~400+ reusable test cases.**
