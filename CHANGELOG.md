# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
