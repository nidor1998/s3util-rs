# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2026-05-03

### Added

- `cp --skip-existing`: pre-flight check skips the transfer when the target object (S3 `HeadObject`) or local file already exists, exiting 0 without copying. Useful for re-runnable scripts that don't want to overwrite. Rejected in combination with `--if-none-match` (opposite intent) and with stdout target (no notion of "exists"). Target SSE-C credentials are honored on the HEAD so encrypted targets are still classifiable. Honored under `--dry-run` (the HEAD itself is read-only). Skip messages log at info level (visible with `-v`).
- `create-bucket --if-not-exists`: pre-flight `HeadBucket` skips the `CreateBucket` call when the bucket already exists, exiting 0. The `--tagging` step is intentionally not applied to a pre-existing bucket — this invocation didn't create it.
- Parallel download path for `s3://… → -` (S3 to stdout). With `--max-parallel-uploads > 1`, large objects are fetched via concurrent ranged `GetObject` requests while preserving exact byte ordering on the output stream. Previously, S3-to-stdout downloads were strictly serial.
- `cp --auto-chunksize` is now supported for the S3-to-stdout direction. Chunk boundaries align with the source's actual part sizes (read via `GetObjectAttributes`, falling back to per-part `HeadObject`), so the streamed bytes verify against the source's composite ETag exactly. The auto-chunksize path always uses the parallel pipeline.

### Changed

- README: clarified scope and operator responsibility. Resume of failed transfers, concurrency tuning beyond defaults (e.g., raising `--max-parallel-uploads` past safe limits for the host or the target's per-prefix capacity), and per-invocation API call minimization are explicitly out of scope. For workflows where API call count is the primary concern, `aws s3api` is recommended.
- README: added a memory warning for `--auto-chunksize` on client-side download paths (S3 → stdout, S3 → local, S3 → S3 without `--server-side-copy`). Peak memory ≈ the source's largest part size × `--max-parallel-uploads`. `--server-side-copy` sidesteps the issue (parts are copied via `UploadPartCopy` and never materialize locally).
- README: added a one-sentence sizing note on `--max-parallel-uploads` (host memory + target's per-prefix limits).
- README: added a top-of-file pointer asking new issues to be filed in the [s7cmd](https://github.com/nidor1998/s7cmd) umbrella repository so discussion across `s3sync` / `s3util-rs` / `s3rm-rs` / `s3ls-rs` stays in one place. Maintenance and releases continue here; existing issues are unaffected.

## [1.1.1] - 2026-05-02

### Fixed

- `--help` output for the `Tracing/Logging` flags (`--json-tracing`, `--aws-sdk-tracing`, `--span-events-tracing`, `--disable-color-tracing`) now shows a description for each flag, matching the wording used by `s3sync`. Previously these flags rendered with no description text.

## [1.1.0] - 2026-04-30

### Added

- `--dry-run` flag on every command that changes S3 state (`cp`, `mv`, `rm`, `create-bucket`, all `put-*`, and all `delete-*`). Preview an invocation safely: argument validation, JSON parsing, and SDK setup run as normal, an info-level `[dry-run]` log line describes what would have happened, and the binary exits 0 without making any AWS-side change. Read-only commands (`get-*`, `head-*`) deliberately do not accept this flag. Verbosity is forced to at least info while `--dry-run` is set so the message is visible at default verbosity.

### Changed

- README clarifies that `cp` supports objects up to Amazon S3's per-object size limit (currently 50 TB).

## [1.0.0] - 2026-04-29

### Added

- E2E tests asserting that every `put-bucket-*` subcommand which consumes JSON (`put-bucket-cors`, `put-bucket-encryption`, `put-bucket-lifecycle-configuration`, `put-bucket-logging`, `put-bucket-notification-configuration`, `put-bucket-website`, `put-public-access-block`, `put-bucket-policy`) rejects invalid JSON via both file and stdin with exit code 1.
- README link from the `s7cmd` mention to the [s7cmd repository](https://github.com/nidor1998/s7cmd).

### Changed

- Bumped version from 0.2.3 to 1.0.0.

## [0.2.3] - 2026-04-29

### Added

- Two-step CI release flow gated by a `create-release` job.
- Unit tests for the `parse_tagging_to_tags` helper.
- Unit tests covering rejection arms in `build_config_from_args`.

### Changed

- Refreshed crate description in metadata.
- Crate-level documentation now references the correct binary name.

## [0.2.2] - 2026-04-29

### Added

- Bucket-config CRUD subcommand suite: `get/put/delete-bucket-cors`, `get/put/delete-bucket-encryption`, `get/put/delete-bucket-lifecycle-configuration`, `get/put-bucket-logging`, `get/put-bucket-notification-configuration`, `get/put/delete-public-access-block`, `get/put/delete-bucket-website`.
- Categorized top-level `--help` output grouped by resource family.
- Dockerfile and `.dockerignore`.
- GitHub issue templates.
- Crates.io version and GitHub downloads badges in the README.
- Process-level CLI tests asserting exit code 2 on clap validation failures.

### Changed

- Bumped `clap_complete` and `serde_json`.
- README clarifies `cp`/`mv` scope, drops the MinIO mention, and normalizes stdin/stdout terminology.
- Removed the `docs/` directory and added it to `.gitignore`.

### Fixed

- Bare `s3util` invocation now shows the categorized help (matching `--help`).
- Tracing subscriber silently swallows `BrokenPipe` writes when output is piped to a consumer that closes early.
- Local multipart upload loop returns `Cancelled` instead of falling through `break` on cancellation.
- Dockerfile build context excludes `.git/` to keep image size small and reproducible.

## [0.2.1] - 2026-04-26

### Added

- Direct unit tests covering the mock `StorageTrait` implementations in `storage` / `transfer`.
- E2E tests for SSE-C `head-object` and object-not-found `get-object-tagging`.
- E2E test covering the SHA256 composite-checksum mismatch that arises from `s3-to-s3` chunksize divergence.

### Changed

- Consolidated the `create_large_file` helper used across storage tests.
- README's compat/parity Non-Goals collapsed into a single bullet.

## [0.2.0] - 2026-04-26

Initial release of the thin-wrapper subcommand suite.

### Added

- Object subcommands: `head-object`, `rm`, `get-object-tagging`, `put-object-tagging`, `delete-object-tagging`.
- Bucket subcommands: `head-bucket`, `create-bucket`, `delete-bucket`, `get/put/delete-bucket-tagging`, `get/put-bucket-versioning`, `get/put/delete-bucket-policy`.
- `create-bucket` supports S3 Express One Zone directory buckets (`<base>--<zone-id>--x-s3` names).
- `HeadError` type distinguishes `BucketNotFound` from subresource `NotFound` across `head-*` and `get-*` wrappers.
- `head-*` and `get-*` subcommands exit with code 4 when the target does not exist (separate from generic exit 1).
- Display-ordered `--help` puts `cp`/`mv`/`rm` first.
- Successful delete/put operations emit info-level logs (surface with `-v`).
- `get-bucket-versioning` emits both `MfaDelete` and the legacy `MFADelete` field.
- E2E coverage for create-bucket round-trip and AZ attributes for directory buckets, `--source-version-id` targeting on `head-object` / `object-tagging` / `rm`, and UTF-8 3-byte tag round-trip on object/bucket tagging.
- Renamed E2E IAM profile from `s3sync-e2e-test` to `s3util-e2e-test`.
- README rework: Non-Goals, declined multi-arg `cp`/`mv` form, exit-code 4 documentation, broader other-client list, `s7cmd` reference.

### Changed

- Thin-wrapper output aligned to `aws s3api` JSON shape.
- `parse_directory_bucket_zone` reuses the shared `EXPRESS_ONEZONE_STORAGE_SUFFIX` constant.

### Fixed

- `create-bucket` derives `LocationConstraint` from the SDK client's resolved region (honours `--target-region`, `AWS_REGION`, and the active profile's region).
- `head-object` error log distinguishes `BucketNotFound` from `NotFound`.
- `get-object-tagging` `NotFound` message clarified.
- Tracing/Logging heading casing aligned across subcommands.
