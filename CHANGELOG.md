# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `put-object-annotation` subcommand: attaches a named annotation payload
  (file or stdin, 1 byte–1 MiB) to an S3 object via the `PutObjectAnnotation`
  API. Sends `Content-MD5` for transit integrity and an explicit CRC64NVME
  checksum, verifies the CRC64NVME returned by S3 against the locally computed
  value, and prints the response as JSON. Supports `--target-version-id`,
  `--target-request-payer`, and `--dry-run`.
- Add `get-object-annotation` subcommand to retrieve a named annotation payload
  from an S3 object (to a file or stdout), verifying the payload's integrity
  before it is written and, for file output, re-reading the saved file to
  recompute and re-verify its ETag / additional checksum from disk (like `cp`);
  a post-write mismatch leaves the file in place and exits 1. Supports
  `--target-version-id` and `--target-request-payer`.
- Add `list-object-annotations` subcommand to list an object's annotations as JSON.
- Add `delete-object-annotation` subcommand to delete a named annotation from an S3 object.
- `cp`/`mv`: `--enable-sync-object-annotations` copies the source object's
  annotations to the target after an S3 → S3 copy (the just-written target
  object has no annotations yet, so every source annotation is copied;
  synced in parallel under `--max-parallel-uploads`).
  `--disable-check-annotation-etag` skips the annotation ETag comparison
  when the source and target annotation lists are diffed. Ported from s3sync
  v1.59.0 (s3sync PR #243). Single-part `--server-side-copy` copies skip the
  manual sync because S3's `CopyObject` carries annotations; multipart
  server-side copies still sync manually. Annotation failures fail the
  transfer, so `mv` keeps the source object.

## [1.5.3] - 2026-06-27

Monthly update.

### Fixed

- S3 keys are now taken verbatim from `s3://` paths. Previously `.` and `..` segments were resolved away as if the key were a filesystem path (e.g. `cp /etc/hosts s3://bucket/..` uploaded to key `hosts`), and `%XX` sequences were percent-decoded. Keys are now stored exactly as written, matching the AWS CLI.
- Downloading to a bare filename in the current directory (e.g. `cp s3://bucket/key xyz`) no longer fails with `parent directory does not exist: ''`. Previously this required an explicit `./xyz`; the current directory is now used correctly when the target has no directory component.

### Changed

- aws-sdk-s3 `v1.133.0 -> v1.137.0`
- Updated other dependencies

## [1.5.2] - 2026-05-24

### Fixed

- `rename --source-if-match`, `--source-if-none-match`, `--target-if-match`, and `--target-if-none-match` now reject empty strings supplied via environment variables (`SOURCE_IF_MATCH`, `SOURCE_IF_NONE_MATCH`, `TARGET_IF_MATCH`, `TARGET_IF_NONE_MATCH`). Previously an empty variable was accepted and forwarded as an empty ETag to the S3 `RenameObject` API.

## [1.5.1] - 2026-05-24

### Fixed

- `rename --source-if-none-match` and `rename --target-if-none-match` now require an explicit `<ETAG>` value and forward it directly to the S3 `RenameObject` API (`IfSourceNoneMatch` / `IfDestinationNoneMatch`). Previously these flags were boolean switches that silently sent `*` regardless of the caller's intent, making it impossible to express a real ETag-based none-match condition.

## [1.5.0] - 2026-05-24

### Added

- `rename`: atomically rename an object within the same S3 Express One Zone directory bucket using the `RenameObject` API. Both source and target must be in the same bucket (name must end with `--x-s3`). Supports optional conditional checks: `--source-if-match <ETAG>`, `--source-if-none-match` (sends `*`), `--target-if-match <ETAG>`, and `--target-if-none-match` (sends `*`). Supports `--dry-run`. Exits 1 (error) when the source object or bucket is not found — consistent with an unexpected operation failure rather than a "not found" query result, so exit 4 is not used.

### Changed

- aws-sdk-s3 `v1.131.0 -> v1.133.0`
- Updated other dependencies

## [1.4.0] - 2026-05-07

### Added

- `presign`: generate a pre-signed URL for downloading an S3 object (`GetObject` only), matching `aws s3 presign`. `--expires-in <seconds>` controls the URL lifetime (default 3600, max 604800). The URL is signed locally from resolved credentials — no S3 API call is made — so presign succeeds even when the target bucket or key does not exist; the resulting fetch returns the appropriate 404 server-side. The signed URL is the only thing written to stdout.

## [1.3.0] - 2026-05-06

### Added

- New replication subcommands: `get-bucket-replication`, `put-bucket-replication`, `delete-bucket-replication`. Read, install, and remove a bucket's replication configuration (cross-region or same-region rules). The configuration JSON for `put-` matches the AWS-CLI input shape for `aws s3api put-bucket-replication`.
- New transfer-acceleration subcommands: `get-bucket-accelerate-configuration`, `put-bucket-accelerate-configuration`. Read and toggle (`Enabled` / `Suspended`) S3 Transfer Acceleration on a bucket.
- New requester-pays subcommands: `get-bucket-request-payment`, `put-bucket-request-payment`. Read and switch a bucket between owner-pays (default) and requester-pays billing.
- `get-bucket-policy-status`: report whether a bucket policy makes the bucket public, as `{"PolicyStatus": {"IsPublic": true|false}}`.
- `restore-object`: initiate a restore of an archived (S3 Glacier-class) object so it becomes readable for `--days N`. Retrieval tier selectable via `--tier <Standard|Bulk|Expedited>`; specific object versions selectable via `--source-version-id`. Honors `--dry-run`. Exits 4 (NotFound) when S3 reports `NoSuchBucket`, `NoSuchKey`, or `NoSuchVersion`, matching `head-object` and `get-object-tagging`; other failures still exit 1.

### Fixed

- `put-bucket-lifecycle-configuration`: rules that use object-size filters (`ObjectSizeGreaterThan`, `ObjectSizeLessThan` — at the top level of `Filter` or under `Filter.And`) and `NewerNoncurrentVersions` (under `NoncurrentVersionExpiration` and entries of `NoncurrentVersionTransitions`) are now applied to the bucket as written. In 1.2.0 these fields parsed without error but were silently ignored, so the bucket ended up configured as if you had not specified them.
- `put-bucket-encryption`: rules can now include `BlockedEncryptionTypes` (used to block SSE-C uploads on a bucket). In 1.2.0 the field was silently ignored.
- `put-bucket-lifecycle-configuration`: the `Date` field on `Expiration` and `Transitions` now accepts the ISO 8601 date-only form (`YYYY-MM-DD`, interpreted as midnight UTC), matching what AWS CLI v2 accepts. Previously only full RFC 3339 timestamps with a time component were accepted.
- `put-bucket-logging`: `TargetGrants` is now applied to the bucket as written. Previously the field parsed without error but was silently dropped, so the bucket ended up configured as if you had not specified it. AWS CLI v2 input that includes `TargetGrants` (canonical user / `AmazonCustomerByEmail` / `Group` URI grantees with `FULL_CONTROL` / `READ` / `WRITE` permission) now round-trips correctly.
- `get-bucket-lifecycle-configuration` output now includes `ObjectSizeGreaterThan` / `ObjectSizeLessThan` (under both `Filter` and `Filter.And`), `NewerNoncurrentVersions` (under `NoncurrentVersionExpiration` and each entry of `NoncurrentVersionTransitions`), and the top-level `TransitionDefaultMinimumObjectSize`. In 1.2.0 these were stripped from the output even when set on the bucket, so the JSON did not reflect the actual configuration.
- `get-bucket-encryption` output now includes `BlockedEncryptionTypes` per rule when configured.
- `get-bucket-logging` output now includes `TargetGrants` per `LoggingEnabled` when the bucket has them configured.
- `head-object` output now includes `ContentRange` when set (returned by S3 when the request specified a byte range).
- `head-object` output now includes `ChecksumSHA512`, `ChecksumMD5`, `ChecksumXXHASH64`, `ChecksumXXHASH3`, and `ChecksumXXHASH128` when S3 returns the corresponding `x-amz-checksum-*` response header. Previously these five checksums were stripped from the JSON output, so objects uploaded with one of those algorithms appeared to have no checksum.
- `head-object` output now emits `Expires` as an ISO 8601 timestamp (the parsed value of the `Expires` HTTP header) and a separate `ExpiresString` field containing the raw header value, matching `aws s3api head-object`. Previously the `Expires` key carried the raw HTTP-date string and `ExpiresString` was not emitted at all, so scripts expecting AWS-CLI-shape `Expires` saw an unparsed RFC 7231 string instead of an ISO 8601 timestamp.
- `get-bucket-replication` output now emits the `Time` container under `Destination.ReplicationTime` and the `EventThreshold` container under `Destination.Metrics` whenever S3 populates them, even if the inner `Minutes` field happens to be absent. Previously these wrapper objects were silently dropped together with the missing `Minutes`, hiding the fact that S3 had returned the surrounding RTC / replication-metrics block.

### Changed

- `get-bucket-versioning` output now emits only `MFADelete` (the casing AWS CLI v2 uses), not the additional legacy `MfaDelete` key. Scripts that read `MFADelete` are unaffected; scripts that read the duplicate `MfaDelete` key need to switch to `MFADelete`.

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
