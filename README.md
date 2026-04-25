# s3util

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.91.1-red)
[![codecov](https://codecov.io/gh/nidor1998/s3util-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3util-rs)

## Safe, verifiable single-object copy for Amazon S3

`s3util` is a single-object copy tool for Amazon S3 and S3-compatible object stores. It ports the transfer, verification, and multipart semantics of [s3sync](https://github.com/nidor1998/s3sync) into a compact CLI focused on interactive and scripted use, and is intended to become part of the future `s3cmd-rs` toolkit.

Today it implements the `cp`, `mv`, and `head-bucket` subcommands (documented in detail below), plus fifteen thin S3 API wrappers: `head-object`, `rm`, `create-bucket`, `delete-bucket`, `put-bucket-versioning`, `get-bucket-versioning`, `put-bucket-policy`, `get-bucket-policy`, `delete-bucket-policy`, `get-bucket-tagging`, `put-bucket-tagging`, `delete-bucket-tagging`, `get-object-tagging`, `put-object-tagging`, and `delete-object-tagging`. `cp` covers Local↔S3, S3↔S3, and stdin/stdout streaming; `mv` covers Local↔S3 and S3↔S3 (no stdio) and deletes the source after a successful, verified copy. Both share the same multipart pipeline — with parallel multipart uploads and downloads (`--max-parallel-uploads`, default `16`) — plus checksum verification and metadata handling. `head-bucket` issues a single S3 `HeadBucket` API call against the given bucket and prints the response as AWS-CLI-shape JSON (`BucketRegion`, `AccessPointAlias`, etc.). The thin wrappers each map to a single S3 API call, produce no output on success (or JSON where noted), and exit with a non-zero code on error. Run `s3util -h` for the current top-level subcommand list, and `s3util <subcommand> -h` for per-command options.

Currently in **preview**.

## Table of contents

<details>
<summary>Click to expand to view table of contents</summary>

- [Overview](#overview)
    * [Scope](#scope)
- [Features](#features)
    * [Verifiable transfers](#verifiable-transfers)
    * [Full multipart support](#full-multipart-support)
    * [All transfer directions](#all-transfer-directions)
    * [Server-side copy](#server-side-copy)
    * [Stdio streaming](#stdio-streaming)
    * [Express One Zone support](#express-one-zone-support)
    * [SSE and SSE-C](#sse-and-sse-c)
    * [Metadata and tagging preservation](#metadata-and-tagging-preservation)
    * [Rate limiting](#rate-limiting)
    * [Observability](#observability)
- [Requirements](#requirements)
- [Installation](#installation)
    * [Build from source](#build-from-source)
- [Usage](#usage)
    * [Upload a local file](#upload-a-local-file)
    * [Download to local](#download-to-local)
    * [S3 → S3 copy](#s3--s3-copy)
    * [Stdin → S3](#stdin--s3)
    * [S3 → Stdout](#s3--stdout)
    * [Move with mv](#move-with-mv)
    * [Additional checksum verification](#additional-checksum-verification)
    * [Multipart tuning](#multipart-tuning)
    * [Custom endpoint (S3-compatible stores)](#custom-endpoint-s3-compatible-stores)
    * [Specify credentials](#specify-credentials)
    * [Specify region](#specify-region)
- [Detailed information](#detailed-information)
    * [Path and target resolution](#path-and-target-resolution)
    * [ETag verification](#etag-verification)
    * [Additional checksum verification](#additional-checksum-verification-1)
    * [Auto chunksize](#auto-chunksize)
    * [Server-side copy detail](#server-side-copy-detail)
    * [Stdin/stdout handling](#stdinstdout-handling)
    * [Express One Zone detail](#express-one-zone-detail)
    * [S3 Permissions](#s3-permissions)
    * [CLI process exit codes](#cli-process-exit-codes)
- [Advanced options](#advanced-options)
    * [--max-parallel-uploads](#--max-parallel-uploads)
    * [--multipart-threshold / --multipart-chunksize](#--multipart-threshold----multipart-chunksize)
    * [--auto-chunksize](#--auto-chunksize)
    * [--additional-checksum-algorithm](#--additional-checksum-algorithm)
    * [--full-object-checksum](#--full-object-checksum)
    * [--disable-multipart-verify / --disable-etag-verify](#--disable-multipart-verify----disable-etag-verify)
    * [--server-side-copy](#--server-side-copy)
    * [--if-none-match](#--if-none-match)
    * [--source-no-sign-request](#--source-no-sign-request)
    * [--rate-limit-bandwidth](#--rate-limit-bandwidth)
    * [-v / -q](#-v---q)
    * [--aws-sdk-tracing](#--aws-sdk-tracing)
    * [--auto-complete-shell](#--auto-complete-shell)
    * [--help](#--help)
- [All command line options](#all-command-line-options)
- [CI/CD Integration](#cicd-integration)
- [About testing](#about-testing)
- [Fully AI-generated (human-verified) software](#fully-ai-generated-human-verified-software)
    * [Quality verification (by AI self-assessment)](#quality-verification-by-ai-self-assessment)
    * [AI assessment of safety and correctness (by Claude, Anthropic)](#ai-assessment-of-safety-and-correctness-by-claude-anthropic)
- [License](#license)

</details>

## Overview

`s3util` is a compact copy tool for Amazon S3, built as a companion to [s3sync](https://github.com/nidor1998/s3sync). Where `s3sync` is optimized for bulk, recursive synchronization, `s3util` is optimized for the single-object case: a single `cp` invocation that copies one object, verifies it, reports progress, and exits with a meaningful status code.

All transfer, verification, and multipart code is shared in spirit with `s3sync` — but the CLI surface is deliberately narrow and the binary is a single file with no recursive/directory mode.

### Scope

s3util is a single-object copy/move tool. It is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — examples include the AWS CLI (`aws s3 cp` / `aws s3 mv`) and `s5cmd`, but the same applies to any S3 transfer tool. Its command-line flags, transfer semantics, verification rules, and exit codes are designed around safe, verifiable single-object transfers — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with s3util unmodified. If you need recursive/bulk synchronization use [s3sync](https://github.com/nidor1998/s3sync); for any other S3 functionality or compatibility with a specific tool's flag set, use that tool.

### Thin S3 API wrappers

Beyond `cp`/`mv`, `s3util` ships a set of single-call wrappers that mirror `aws s3api` subcommands with a simpler, script-friendly interface:

| Subcommand               | What it does                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------|
| `head-object`            | Prints `HeadObject` response as JSON; supports `--source-version-id` and SSE-C reads          |
| `rm`                     | Deletes a single S3 object; silent on success; supports `--source-version-id`                 |
| `get-bucket-tagging`     | Prints bucket tags as JSON (`{"TagSet": [...]}`); exits 1 with error on `NoSuchTagSet`        |
| `put-bucket-tagging`     | Replaces all tags from `--tagging "k=v&k2=v2"`; silent on success                            |
| `delete-bucket-tagging`  | Removes all tags from a bucket; silent on success                                             |
| `get-object-tagging`     | Prints object tags as JSON (`{"TagSet": [...], "VersionId": "..."}`); supports `--source-version-id` |
| `put-object-tagging`     | Replaces all tags from `--tagging "k=v&k2=v2"`; silent; supports `--source-version-id`       |
| `delete-object-tagging`  | Removes all tags from an object; silent; supports `--source-version-id`                       |
| `create-bucket`          | Creates a bucket; region from `--target-region`; optional `--tagging`; exit 3 if tagging step fails after create |
| `delete-bucket`          | Deletes an empty bucket; silent on success                                                    |
| `put-bucket-versioning`  | Enables or suspends versioning (`--enabled` / `--suspended`, mutually exclusive); silent       |
| `get-bucket-versioning`  | Prints versioning state as JSON (`{"Status": "Enabled"}` or `{}`); silent on unset            |
| `put-bucket-policy`      | Sets bucket policy from a file path or `-` (stdin); body sent verbatim, no client-side validation; silent |
| `get-bucket-policy`      | Prints policy as JSON (`{"Policy": "<escaped-JSON-string>"}`, matching `aws s3api`)            |
| `delete-bucket-policy`   | Removes bucket policy; silent on success                                                      |

## Features

### Verifiable transfers

When the source is a local file or stdin, `s3util` precalculates the ETag and — if `--additional-checksum-algorithm` is set — the additional checksum, then compares them against the S3-reported values. A mismatch is treated as an **error** (the destination object is considered corrupted).

For S3→S3 transfers, mismatches remain **warnings** (exit code 3), because differing multipart chunk sizes between source and destination legitimately produce different composite ETags/checksums.

Supported algorithms:
- **ETag** (MD5 for single-part, multipart-composite hash for multipart uploads)
- **SHA256**, **SHA1**, **CRC32**, **CRC32C**, **CRC64NVME** via `--additional-checksum-algorithm`
- Full-object and composite variants via `--full-object-checksum`

Verification can be selectively disabled with `--disable-etag-verify`, `--disable-multipart-verify`, or `--disable-additional-checksum-verify` when working with S3-compatible stores that behave differently.

### Full multipart support

- Configurable threshold (`--multipart-threshold`, default `8MiB`) and chunk size (`--multipart-chunksize`, default `8MiB`).
- Parallel part uploads/downloads (`--max-parallel-uploads`, default `16`).
- `--auto-chunksize` matches the source multipart layout on S3→S3 copies so checksums line up end-to-end.
- In-flight multipart uploads are aborted cleanly on ctrl-c.

### All transfer directions

Transfer direction is inferred automatically from the source/target combination:

| Source        | Target        | Direction     |
|---------------|---------------|---------------|
| local path    | `s3://…`      | Local → S3    |
| `s3://…`      | local path    | S3 → Local    |
| `s3://…`      | `s3://…`      | S3 → S3       |
| `-` (stdin)   | `s3://…`      | Stdin → S3    |
| `s3://…`      | `-` (stdout)  | S3 → Stdout   |

### Server-side copy

`--server-side-copy` uses S3's `CopyObject` / `UploadPartCopy` for S3→S3 transfers within the same account/region, avoiding a round-trip through the client. `s3util` falls back to client-side copy when server-side is not possible (different endpoints, SSE-C translation, etc.).

### Stdio streaming

Pipe data directly through S3 without touching the local filesystem:

```bash
pg_dump mydb | s3util cp - s3://my-bucket/backups/mydb.sql
s3util cp s3://my-bucket/backups/mydb.sql - | psql mydb
```

Stdin uploads compute the ETag and additional checksum on the fly and verify against the S3-reported values.

### Express One Zone support

`s3util` supports [Amazon S3 Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) directory buckets (the `--x-s3` bucket-name suffix). Additional-checksum verification is handled carefully for Express One Zone — use `--disable-express-one-zone-additional-checksum` if the defaults are too strict for your workload.

### SSE and SSE-C

- Target-side: `--sse AES256` / `aws:kms` / `aws:kms:dsse`, with `--sse-kms-key-id` for KMS variants.
- Source and target SSE-C: separate key/algorithm/MD5 flags for reading an SSE-C source and writing an SSE-C target (including re-keying across a client-side copy).

### Metadata and tagging preservation

S3→S3 copies preserve both system metadata (Content-Type, Cache-Control, Expires, Content-Disposition, Content-Encoding, Content-Language, website-redirect) and user-defined metadata by default. Use `--no-sync-system-metadata` / `--no-sync-user-defined-metadata` to opt out, or override individual headers explicitly.

Object tags are preserved on S3→S3 by default. `--tagging "k=v&k2=v2"` overrides, `--disable-tagging` clears.

### Rate limiting

`--rate-limit-bandwidth <BYTES_PER_SEC>` caps throughput using a leaky-bucket algorithm. Accepts unit-suffixed values like `50MB`, `100MiB`, `1GB`.

### Observability

- Optional progress bar (`--show-progress`) using [indicatif](https://docs.rs/indicatif).
- Structured JSON tracing (`--json-tracing`) for log aggregation systems.
- AWS SDK tracing (`--aws-sdk-tracing`) for deep troubleshooting.
- Configurable verbosity (`-v`/`-vv`/`-vvv`, `-q`/`-qq`).

## Requirements

- x86_64 Linux (kernel 3.2 or later)
- ARM64 Linux (kernel 4.1 or later)
- Windows 11 (x86_64, aarch64)
- macOS 11.0 or later (aarch64, x86_64)

`s3util` is written in Rust and requires Rust **1.91.1 or later** to build from source.

AWS credentials are required. `s3util` supports all standard AWS credential mechanisms:
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- AWS credentials file (`~/.aws/credentials`)
- AWS config file (`~/.aws/config`) with profiles
- IAM instance roles (EC2, ECS, Lambda)
- SSO/federated authentication
- Explicit `--source-*` / `--target-*` flags

For more information, see [SDK authentication with AWS](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credentials.html).

## Installation

### Build from source

```bash
# Clone the repository
git clone https://github.com/nidor1998/s3util-rs.git
cd s3util-rs

# Build release binary
cargo build --release

# The binary is at ./target/release/s3util
```

Shell completions can be generated:

```bash
s3util cp --auto-complete-shell bash   > /etc/bash_completion.d/s3util
s3util cp --auto-complete-shell zsh    > "${fpath[1]}/_s3util"
s3util cp --auto-complete-shell fish   > ~/.config/fish/completions/s3util.fish
```

## Usage

```
s3util <COMMAND> [OPTIONS] <SOURCE> <TARGET>
```

Supported path forms for `<SOURCE>` / `<TARGET>`:

| Form             | Meaning                                          |
|------------------|--------------------------------------------------|
| `s3://bucket`    | Bucket with empty prefix                         |
| `s3://bucket/k`  | Specific key (or prefix ending in `/`)           |
| `/local/path`    | Local filesystem path                            |
| `-`              | Standard input (as source) or stdout (as target) |

Every long flag also reads from an uppercase-underscore environment variable of the same name (for example `--max-parallel-uploads` ↔ `MAX_PARALLEL_UPLOADS`).

### Upload a local file

```bash
s3util cp ./release.tar.gz s3://my-bucket/releases/
```

If the target ends in `/` (or is a bucket root), the source basename is appended to form the key. The resolved write path is printed on a `-> <path>` line before the transfer summary.

### Download to local

```bash
s3util cp s3://my-bucket/hosts ../
```

**The target parent directory must already exist.** `s3util` does not create missing directories — it returns an error asking you to create them first.

### S3 → S3 copy

Client-side (default):

```bash
s3util cp s3://src-bucket/key s3://dst-bucket/key
```

Server-side (same account/region, avoids round-tripping bytes through the client):

```bash
s3util cp --server-side-copy --auto-chunksize \
  s3://src-bucket/key s3://dst-bucket/key
```

### Stdin → S3

```bash
pg_dump mydb | s3util cp --additional-checksum-algorithm CRC64NVME \
  - s3://my-bucket/backups/mydb-$(date +%F).sql
```

With stdin as the source there is no basename, so the target key must be spelled out.

### S3 → Stdout

```bash
s3util cp s3://my-bucket/backups/mydb-2026-04-19.sql - | psql mydb
```

### Move with mv

`mv` runs the same copy pipeline as `cp` and then deletes the source on success. Transfer, verification, multipart, metadata, tagging, SSE, server-side copy, rate limiting, and progress all behave identically — only the post-copy step differs.

```bash
# S3 → S3 move
s3util mv s3://src-bucket/key s3://dst-bucket/key

# Upload then delete the local file
s3util mv ./release.tar.gz s3://my-bucket/releases/

# Download then delete the source S3 object
s3util mv s3://my-bucket/old-key ./local-copy
```

Differences from `cp`:

- **Stdin/stdout is not supported.** A `-` source or target is rejected at argument-parse time.
- **The source is deleted only after a successful, verified copy.** If the copy fails, is canceled (SIGINT), or produces a verification warning, the source is left untouched and the command exits with the matching non-zero code. See [mv command behavior](#mv-command-behavior) for the exact gating logic.
- **`--no-fail-on-verify-error`** (mv only) treats a verification warning as success and proceeds to delete the source. Use only when you understand why your S3↔S3 chunksize layout produces an expected mismatch.
- **`--source-version-id`** deletes the specific source version after the copy (rather than creating a delete marker on the latest version).

### Head a bucket

```bash
s3util head-bucket s3://my-bucket
```

Prints the `HeadBucket` response as AWS-CLI-shape JSON:

```json
{
  "BucketRegion": "us-east-1",
  "AccessPointAlias": false
}
```

### Head an object

```bash
s3util head-object s3://my-bucket/path/to/key
```

Prints the `HeadObject` response as JSON. Use `--source-version-id` to head a specific version. SSE-C-encrypted objects can be read by supplying `--source-sse-c AES256 --source-sse-c-key <base64-key>`.

To request and verify an additional checksum alongside the metadata:

```bash
s3util head-object --enable-additional-checksum s3://my-bucket/path/to/key
```

### Delete an object (rm)

```bash
s3util rm s3://my-bucket/path/to/key
```

Deletes a single S3 object. Silent on success. To delete a specific version:

```bash
s3util rm --source-version-id <version-id> s3://my-bucket/path/to/key
```

### Manage object tagging

Retrieve tags:

```bash
s3util get-object-tagging s3://my-bucket/path/to/key
```

```json
{
  "TagSet": [
    { "Key": "env", "Value": "prod" }
  ],
  "VersionId": "abc123"
}
```

Replace all tags (URL-encoded query-string format):

```bash
s3util put-object-tagging --tagging "env=prod&team=platform" s3://my-bucket/path/to/key
```

Remove all tags:

```bash
s3util delete-object-tagging s3://my-bucket/path/to/key
```

All three support `--source-version-id` to target a specific object version.

### Bucket tagging

Retrieve tags on a bucket:

```bash
s3util get-bucket-tagging s3://my-bucket
```

```json
{
  "TagSet": [
    { "Key": "env", "Value": "prod" }
  ]
}
```

If the bucket has no tags configured, S3 returns `NoSuchTagSet` and s3util exits 1 with an error message.

Replace all tags (URL-encoded query-string format):

```bash
s3util put-bucket-tagging --tagging "env=prod&team=platform" s3://my-bucket
```

Remove all tags:

```bash
s3util delete-bucket-tagging s3://my-bucket
```

### Manage a bucket (create / delete)

Create a bucket (LocationConstraint is inferred from `--target-region`; `us-east-1` is handled as the AWS default):

```bash
s3util create-bucket --target-region us-west-2 s3://my-new-bucket

# With initial tags
s3util create-bucket --target-region us-west-2 --tagging "project=myapp&env=prod" s3://my-new-bucket
```

If `CreateBucket` succeeds but the subsequent `PutBucketTagging` call fails, `s3util` exits with code 3 and prints a warning. The bucket will exist but untagged — there is no automatic rollback.

Delete an empty bucket:

```bash
s3util delete-bucket s3://my-empty-bucket
```

### Bucket versioning

Enable versioning:

```bash
s3util put-bucket-versioning --enabled s3://my-bucket
```

Suspend versioning:

```bash
s3util put-bucket-versioning --suspended s3://my-bucket
```

`--enabled` and `--suspended` are mutually exclusive. Retrieve the current state:

```bash
s3util get-bucket-versioning s3://my-bucket
```

```json
{ "Status": "Enabled" }
```

Returns `{}` if versioning has never been configured on the bucket.

### Bucket policy

Upload a policy from a file:

```bash
s3util put-bucket-policy s3://my-bucket policy.json
```

Or pipe from stdin:

```bash
cat policy.json | s3util put-bucket-policy s3://my-bucket -
```

The policy body is sent verbatim — no client-side validation. Retrieve the current policy:

```bash
s3util get-bucket-policy s3://my-bucket
```

```json
{ "Policy": "{\"Version\":\"2012-10-17\",\"Statement\":[...]}" }
```

Remove the policy:

```bash
s3util delete-bucket-policy s3://my-bucket
```

### Additional checksum verification

```bash
# Upload with SHA256 additional checksum
s3util cp --additional-checksum-algorithm SHA256 \
  ./release.tar.gz s3://my-bucket/releases/release.tar.gz

# Download with SHA256 verification (requires server-side checksum)
s3util cp --enable-additional-checksum --additional-checksum-algorithm SHA256 \
  s3://my-bucket/releases/release.tar.gz ./release.tar.gz
```

### Multipart tuning

```bash
# Force multipart at 64 MiB with 16 MiB chunks and 8 parallel workers
s3util cp \
  --multipart-threshold 64MiB \
  --multipart-chunksize 16MiB \
  --max-parallel-uploads 8 \
  ./big.bin s3://my-bucket/big.bin

# Match the source chunk layout on S3 → S3 copy
s3util cp --auto-chunksize s3://src-bucket/big.bin s3://dst-bucket/big.bin
```

### Custom endpoint (S3-compatible stores)

```bash
s3util cp \
  --target-endpoint-url https://minio.example.com:9000 \
  --target-force-path-style \
  ./file.bin s3://my-bucket/file.bin
```

### Specify credentials

```bash
s3util cp \
  --target-access-key YOUR_KEY \
  --target-secret-access-key YOUR_SECRET \
  ./file.bin s3://my-bucket/file.bin
```

### Specify region

```bash
s3util cp --target-region us-west-2 ./file.bin s3://my-bucket/file.bin
```

## Detailed information

### Path and target resolution

If the target is `s3://bucket`, `s3://bucket/dir/`, or a directory-style local path (an existing directory, or one ending in a path separator like `../`), the source basename is appended. The resolved write path is printed on a `-> <path>` line before the transfer summary.

With stdin as the source there is no basename, so the target key must be spelled out.

### ETag verification

For single-part objects, the S3-reported ETag is the MD5 of the object. `s3util` computes this on the upload side and compares; for downloads it compares the source's reported ETag against the bytes actually received. A mismatch is treated as an error for Local/Stdin→S3 and S3→Local, and as a warning for S3→S3 (where multipart layout differences legitimately change the composite ETag).

`--disable-etag-verify` turns off ETag verification entirely. `--disable-content-md5-header` additionally omits the `Content-MD5` header on single-part uploads.

### Additional checksum verification

When `--additional-checksum-algorithm` is set, S3 stores the chosen algorithm's checksum alongside the object. Supported: `SHA256`, `SHA1`, `CRC32`, `CRC32C`, `CRC64NVME`.

- `--full-object-checksum` forces the full-object variant (required for CRC64NVME; incompatible with SHA1/SHA256).
- `--enable-additional-checksum` on download tells S3 to return the additional checksum so `s3util` can verify it.
- `--disable-additional-checksum-verify` uploads the additional checksum but skips local verification.

### Auto chunksize

`--auto-chunksize` issues additional `HeadObject` calls to discover the source's multipart layout and then mirrors it on the destination. This keeps the S3→S3 composite ETag and additional-checksum values identical end-to-end, at the cost of one extra `HeadObject` per part.

### Server-side copy detail

`--server-side-copy` uses `CopyObject` (single-part) or `UploadPartCopy` (multipart). Server-side copy is only valid when both source and target endpoints can see each other in the same AWS region/account (with appropriate cross-account IAM). It is not compatible with stdin or local paths. SSE-C re-keying across a server-side copy is supported by supplying both `--source-sse-c-*` and `--target-sse-c-*` flags.

### Stdin/stdout handling

- **Stdin → S3** streams bytes into a multipart upload once the threshold is crossed; below the threshold, stdin is buffered into a temp file first so a single-part PUT with a correct `Content-Length` can be issued.
- **S3 → Stdout** streams bytes straight to stdout. ETag and any requested additional checksum are computed inline from the streamed bytes and verified against the S3-reported values — the same verification as `S3 → Local`. A mismatch is logged as a warning (exit 3), or as an error if the configured additional checksum is a full-object checksum.

### Express One Zone detail

Directory buckets (`--x-s3` suffix) are automatically detected. Some S3 features behave differently on Express One Zone (for example, default additional-checksum handling); `--disable-express-one-zone-additional-checksum` overrides `s3util`'s default if your bucket policy demands it.

### S3 Permissions

Required permissions depend on the transfer direction. "Source" and "target" below refer to the source and target S3 buckets; for Local↔S3 only the relevant side applies.

**Source bucket** (any `cp`/`mv` reading from S3):

- `s3:GetObject` — always. Covers `GetObject`, `HeadObject`, and `GetObjectAttributes`.
- `s3:GetObjectTagging` — when source tags are read. This is the default on S3→S3; suppressed by `--disable-tagging`.
- `s3:GetObjectVersion` — when `--source-version-id` is used.
- `s3:DeleteObject` — when running `mv` (the source is deleted on success).
- `s3:DeleteObjectVersion` — when running `mv` with `--source-version-id`.

**Target bucket** (any `cp`/`mv` writing to S3):

- `s3:PutObject` — always. Covers `PutObject`, `CreateMultipartUpload`, `UploadPart`, `CompleteMultipartUpload`, and on `--server-side-copy` also `CopyObject` / `UploadPartCopy`.
- `s3:AbortMultipartUpload` — always (used to clean up on errors and ctrl-c).
- `s3:PutObjectTagging` — when target tags are written. This is the default on S3→S3, and also applies when `--tagging` is set.
- `s3:PutObjectAcl` — when `--acl` is set.

**Express One Zone** (directory buckets, `--x-s3` suffix):

- `s3express:CreateSession` — on each directory bucket the SDK opens a session for. Required in addition to the standard `s3:*` actions above.

**KMS-backed SSE:**

- `kms:Decrypt` — when reading SSE-KMS-encrypted source objects.
- `kms:Encrypt` and `kms:GenerateDataKey` — when writing with `--sse aws:kms` or `--sse aws:kms:dsse`.

SSE-C (`--source-sse-c*` / `--target-sse-c*`) requires no additional IAM permissions — the encryption key is supplied client-side and S3 does not store it.

### CLI process exit codes

| Code | Meaning                                                                                                             |
|------|---------------------------------------------------------------------------------------------------------------------|
| 0    | Success                                                                                                             |
| 1    | Error — transfer failed or configuration rejected                                                                   |
| 2    | Argument-parsing error — emitted by clap when an argument is unknown, missing, or has an invalid value              |
| 3    | Warning — transfer completed but a non-fatal issue was logged (e.g. S3→S3 ETag mismatch explained by chunksize)     |
| 101  | Abnormal termination (internal panic)                                                                               |
| 130  | User cancellation via SIGINT/ctrl-c (standard Unix SIGINT convention, 128 + 2)                                      |

## Advanced options

### --max-parallel-uploads

Number of parallel part uploads/downloads during multipart transfers. Default: `16`.

### --multipart-threshold / --multipart-chunksize

Object size threshold for switching to multipart (`--multipart-threshold`, default `8MiB`) and the size of each part (`--multipart-chunksize`, default `8MiB`). Both accept unit-suffixed values (`MB`, `MiB`, `GB`, `GiB`).

### --auto-chunksize

Match source/target chunk layout automatically (extra `HeadObject` per part). Recommended for S3→S3 copies where you want identical composite ETags on both sides.

### --additional-checksum-algorithm

Additional checksum algorithm for upload: `SHA256`, `SHA1`, `CRC32`, `CRC32C`, `CRC64NVME`. Also used to select the algorithm to verify on download (combined with `--enable-additional-checksum`).

### --full-object-checksum

Use the full-object variant of the additional checksum instead of the composite variant. Required and forced for CRC64NVME; incompatible with SHA1/SHA256.

### --disable-multipart-verify / --disable-etag-verify

Skip ETag or additional-checksum verification for multipart uploads. Useful when targeting S3-compatible stores that compute ETags differently.

### --server-side-copy

Use S3 server-side copy for S3→S3 transfers. Requires both endpoints to support the relevant server-side copy operations.

### --if-none-match

Upload only if the target key does not already exist. This is an optimistic "create new object" primitive at the S3 level.

### --source-no-sign-request

Access public S3 buckets anonymously — skips the entire AWS credential chain (profile, env, IMDS, SSO) on the source side.

### --rate-limit-bandwidth

Maximum bytes per second for the transfer. Accepts unit suffixes like `MB`, `MiB`, `GB`, `GiB`.

### -v / -q

`s3util` uses [tracing-subscriber](https://docs.rs/tracing-subscriber) for tracing. More occurrences of `-v` increase verbosity (`-v`: `info`, `-vv`: `debug`, `-vvv`: `trace`). Use `-q`, `-qq` to reduce verbosity. Default: warning and error messages.

### --aws-sdk-tracing

Enable AWS SDK for Rust's internal tracing. Useful for diagnosing endpoint/signature issues.

### --auto-complete-shell

Generate shell completion scripts:

```bash
s3util cp --auto-complete-shell bash
s3util cp --auto-complete-shell zsh
s3util cp --auto-complete-shell fish
s3util cp --auto-complete-shell powershell
s3util cp --auto-complete-shell elvish
```

### --help

For the full option list, see `s3util cp --help`.

## All command line options

<details>
<summary>Click to expand to view all command line options</summary>

### General

| Option                  | Description |
|-------------------------|-------------|
| `-v`, `--verbose`       | Increase logging verbosity (repeatable). |
| `-q`, `--quiet`         | Decrease logging verbosity (repeatable). |
| `--show-progress`       | Show progress bar. |
| `--server-side-copy`    | Use S3 server-side copy (S3→S3 only, same region/endpoint). |

### AWS configuration

| Option                                  | Description |
|-----------------------------------------|-------------|
| `--aws-config-file <FILE>`              | Alternate AWS config file. |
| `--aws-shared-credentials-file <FILE>`  | Alternate AWS credentials file. |
| `--source-no-sign-request`              | Access public S3 buckets anonymously. |
| `--source-profile <NAME>`               | Source AWS profile. |
| `--source-access-key <KEY>`             | Source access key. |
| `--source-secret-access-key <KEY>`      | Source secret access key. |
| `--source-session-token <TOKEN>`        | Source session token. |
| `--target-profile <NAME>`               | Target AWS profile. |
| `--target-access-key <KEY>`             | Target access key. |
| `--target-secret-access-key <KEY>`      | Target secret access key. |
| `--target-session-token <TOKEN>`        | Target session token. |

### Source options

| Option                             | Description |
|------------------------------------|-------------|
| `--source-region <REGION>`         | Source region. |
| `--source-endpoint-url <URL>`      | Source endpoint URL (for S3-compatible stores). |
| `--source-accelerate`              | Use S3 Transfer Acceleration on the source bucket. |
| `--source-request-payer`           | Send `x-amz-request-payer: requester` on source reads. |
| `--source-force-path-style`        | Force path-style addressing for source endpoint. |
| `--source-version-id <ID>`         | Specific source object version (S3 source only). |

### Target options

| Option                             | Description |
|------------------------------------|-------------|
| `--target-region <REGION>`         | Target region. |
| `--target-endpoint-url <URL>`      | Target endpoint URL. |
| `--target-accelerate`              | Use S3 Transfer Acceleration on the target bucket. |
| `--target-request-payer`           | Send `x-amz-request-payer: requester` on target writes. |
| `--target-force-path-style`        | Force path-style addressing for target endpoint. |
| `--storage-class <CLASS>`          | Target storage class: `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONE-ZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, `GLACIER_IR`, `EXPRESS_ONEZONE`. |

### Verification

| Option                                   | Description |
|------------------------------------------|-------------|
| `--additional-checksum-algorithm <ALGO>` | `SHA256`, `SHA1`, `CRC32`, `CRC32C`, `CRC64NVME`. |
| `--full-object-checksum`                 | Use full-object checksum instead of composite. Required/forced for CRC64NVME; incompatible with SHA1/SHA256. |
| `--enable-additional-checksum`           | Request additional checksum on download (S3 source only). |
| `--disable-multipart-verify`             | Skip ETag/additional-checksum verification for multipart uploads. |
| `--disable-etag-verify`                  | Skip ETag verification entirely. |
| `--disable-additional-checksum-verify`   | Do not verify additional checksum (still uploads it to S3 if configured). |
| `--no-fail-on-verify-error` (mv only)    | Treat verification warnings as success: delete source and exit 0. |

### Performance

| Option                                   | Description |
|------------------------------------------|-------------|
| `--max-parallel-uploads <N>`             | Parallel multipart uploads/downloads. Default `16`. |
| `--rate-limit-bandwidth <BYTES_PER_SEC>` | Bandwidth cap. Accepts `MB`, `MiB`, `GB`, `GiB`. |

### Multipart settings

| Option                                   | Description |
|------------------------------------------|-------------|
| `--multipart-threshold <SIZE>`           | Object size threshold for multipart. Default `8MiB`. |
| `--multipart-chunksize <SIZE>`           | Multipart chunk size. Default `8MiB`. |
| `--auto-chunksize`                       | Match source/target chunk layout automatically. |

### Metadata / headers

| Option                                | Description |
|---------------------------------------|-------------|
| `--cache-control <V>`                 | `Cache-Control` header on the target object. |
| `--content-disposition <V>`           | `Content-Disposition` header. |
| `--content-encoding <V>`              | `Content-Encoding` header. |
| `--content-language <V>`              | `Content-Language` header. |
| `--content-type <V>`                  | `Content-Type` header. |
| `--expires <RFC3339>`                 | `Expires` header, e.g. `2026-12-01T00:00:00Z`. |
| `--metadata <k=v,k2=v2>`              | User-defined metadata entries. |
| `--website-redirect <URL>`            | `x-amz-website-redirect-location` header. |
| `--no-sync-system-metadata`           | Skip copying system metadata. |
| `--no-sync-user-defined-metadata`     | Skip copying user-defined metadata. |

### Tagging

| Option                  | Description |
|-------------------------|-------------|
| `--tagging <QUERY>`     | Target object tagging as URL-encoded query string, e.g. `k1=v1&k2=v2`. |
| `--disable-tagging`     | Do not copy source tagging. |

### Encryption

| Option                            | Description |
|-----------------------------------|-------------|
| `--sse <MODE>`                    | Target SSE mode: `AES256`, `aws:kms`, `aws:kms:dsse`. |
| `--sse-kms-key-id <KEY_ID>`       | KMS key for `aws:kms` / `aws:kms:dsse`. |
| `--source-sse-c <ALG>`            | Source SSE-C algorithm (`AES256`). |
| `--source-sse-c-key <KEY>`        | Source SSE-C key (base64-encoded 256-bit). |
| `--source-sse-c-key-md5 <MD5>`    | Base64 MD5 of `--source-sse-c-key`. |
| `--target-sse-c <ALG>`            | Target SSE-C algorithm (`AES256`). |
| `--target-sse-c-key <KEY>`        | Target SSE-C key. |
| `--target-sse-c-key-md5 <MD5>`    | Base64 MD5 of `--target-sse-c-key`. |

### Tracing / logging

| Option                      | Description |
|-----------------------------|-------------|
| `--json-tracing`            | Emit traces as JSON. |
| `--aws-sdk-tracing`         | Enable AWS SDK tracing. |
| `--span-events-tracing`     | Emit span events. |
| `--disable-color-tracing`   | Disable ANSI colors in trace output. |

### Retry and timeouts

| Option                                              | Description |
|-----------------------------------------------------|-------------|
| `--aws-max-attempts <N>`                            | Max retry attempts. Default `10`. |
| `--initial-backoff-milliseconds <MS>`               | Initial backoff for exponential-with-jitter retry. Default `100`. |
| `--operation-timeout-milliseconds <MS>`             | Per-operation timeout. |
| `--operation-attempt-timeout-milliseconds <MS>`     | Per-attempt timeout. |
| `--connect-timeout-milliseconds <MS>`               | TCP connect timeout. |
| `--read-timeout-milliseconds <MS>`                  | Read timeout. |

### Advanced

| Option                                                | Description |
|-------------------------------------------------------|-------------|
| `--acl <ACL>`                                         | Canned ACL: `private`, `public-read`, `public-read-write`, `authenticated-read`, `aws-exec-read`, `bucket-owner-read`, `bucket-owner-full-control`. |
| `--no-guess-mime-type`                                | Do not infer MIME type from local filename. |
| `--put-last-modified-metadata`                        | Store source last-modified in target metadata. |
| `--auto-complete-shell <SHELL>`                       | Emit shell completions and exit. `bash`, `fish`, `zsh`, `powershell`, `elvish`. |
| `--disable-stalled-stream-protection`                 | Disable AWS SDK stalled-stream detection. |
| `--disable-payload-signing`                           | Omit payload signing for uploads. |
| `--disable-content-md5-header`                        | Omit `Content-MD5` on uploads (also disables single-part ETag verify). |
| `--disable-express-one-zone-additional-checksum`      | Skip default additional-checksum verification for Express One Zone. |
| `--if-none-match`                                     | Upload only if target key does not already exist. |

All options can also be set via environment variables. The environment variable name matches the long option name in `SCREAMING_SNAKE_CASE` with hyphens converted to underscores (e.g. `--max-parallel-uploads` becomes `MAX_PARALLEL_UPLOADS`).

**Precedence:** CLI arguments > environment variables > defaults.

</details>

## CI/CD Integration

`s3util` is designed for automated pipelines.

### JSON logging

Emit structured JSON logs for log aggregation systems (Datadog, Splunk, CloudWatch, etc.):

```bash
s3util cp --json-tracing ./artifact.tar.gz s3://my-bucket/artifacts/
```

### Quiet mode

Suppress info-level output for cleaner CI logs:

```bash
s3util cp -q ./artifact.tar.gz s3://my-bucket/artifacts/
```

## About testing

**Supported target: Amazon S3 only.**

Support for S3-compatible storage is best-effort and may behave differently. `s3util` has been tested with Amazon S3 and Express One Zone directory buckets.

## Fully AI-generated (human-verified) software

No human wrote a single line of source code in this project. Every line of source code, every test, all documentation, CI/CD configuration, and this README were generated by AI using [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview) (Anthropic).

Human engineers authored the requirements, design specifications, and the s3sync reference architecture. They thoroughly reviewed and verified the design, all source code, and all tests. Features of the preview binary have been manually tested against live AWS S3. The development followed a spec-driven process: requirements and design documents were written first, and the AI generated code to match those specifications under continuous human oversight.

### Quality verification (by AI self-assessment)

| Metric                         | Value                                                         |
|--------------------------------|---------------------------------------------------------------|
| Production code                | ~16,900 lines of Rust (48 source files)                       |
| E2E integration tests          | ~540 tests across 31 test files (gated behind `e2e_test`)     |
| Unit tests                     | ~69 tests embedded in `src/`                                  |
| Code coverage (llvm-cov)       | 94.33% regions, 94.39% functions, 94.85% lines                |
| Static analysis (clippy)       | 0 warnings (`cargo clippy --all-features`)                    |
| Formatting                     | 0 diffs (`cargo fmt --check`)                                 |
| Code reuse from [s3sync](https://github.com/nidor1998/s3sync) | significant (transfer, verification, multipart engine)         |

The codebase is built through spec-driven development with human review at every step. Coverage and test counts reflect the preview state and will grow alongside additional subcommands (`rm`, …).

### AI assessment of safety and correctness (by Claude, Anthropic)

<details>
<summary>Click to expand the full AI assessment</summary>

> Assessment date: _to be filled in by the maintainer at release time_.
>
> Assessed version: s3util-rs preview.
>
> The assessment below is a template produced from a repository-wide read of the current preview codebase (`config`, `storage`, `transfer`, `types`, the `s3util` binary, and the E2E test suite under `tests/`). It will be replaced with a formal assessment at each tagged release.

**Is s3util designed to produce verifiable, non-corrupting single-object copies, and is it sufficiently tested?**

There are two distinct risks with a copy tool: (1) the operator makes a mistake (wrong target, wrong source version), and (2) a software bug causes silent data corruption during transfer. These require different safeguards.

#### Protection against user mistakes

`s3util`'s CLI surface is intentionally narrow. A single `cp` subcommand, explicit source and target, and no recursive mode mean there is very little room for a "whoops, I deleted/overwrote a whole tree" class of error.

Concrete safeguards:

1. **Single-object only.** Directory sources are rejected. A source URL ending in `/` is rejected. A source URL whose final segment is `.` or `..` is rejected at argument-parse time.
2. **Target-parent must exist.** On downloads, `s3util` does not create missing directories — it returns an error and asks the user to create them.
3. **Resolved target printed before transfer.** When the source basename is appended (e.g. target is a bucket root or directory), the resolved path is printed on a `-> <path>` line so the operator can catch a mistake before any bytes move.
4. **Exit code 3 for warnings.** Transfers that complete but report a non-fatal issue (e.g. an S3→S3 ETag mismatch explained by chunksize differences) exit 3 instead of 0, so CI and scripts can treat warnings as something worth looking at.
5. **`--if-none-match`** implements "create only" at the S3 level, preventing accidental overwrite of an existing object.
6. **ctrl-c is safe.** A SIGINT handler cancels any in-flight multipart upload (issuing `AbortMultipartUpload`) before exiting with code 130 (standard Unix SIGINT convention), so scripts can distinguish user cancellation from normal success.

Each safeguard is independently testable; several have direct coverage in `e2e_cancel_test.rs`, `e2e_exit_codes.rs`, and `cli_config_validation_error.rs`.

#### Protection against software bugs

The more serious concern is whether a bug in `s3util` itself could cause silent corruption — for example, a multipart assembly bug that writes parts out of order, a checksum comparison that accepts any value, or a stdin code path that reports success without fully reading stdin.

Architecture-level safeguards:

- **Verification runs after every upload.** When the source is a local file or stdin, the upload-side ETag and (optionally) additional checksum are compared against S3-reported values. A mismatch is a hard error — the destination object is considered corrupted.
- **S3→S3 warnings, not errors.** On S3→S3 transfers, a checksum mismatch becomes a warning (exit code 3) because differing multipart chunksizes between source and destination can legitimately produce different composite values. `--auto-chunksize` lets users match source chunks exactly when they want identical composite hashes.
- **Algorithmic diversity.** Additional-checksum algorithms span MD/CRC/SHA families (`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256`) with both composite and full-object variants. This is enough to detect silent corruption under every realistic fault model AWS S3 can produce.
- **Multipart cleanup.** Fatal errors and ctrl-c both abort the in-flight multipart upload, so orphaned multipart fragments don't accumulate and bill.
- **Single-part path keeps `Content-MD5`.** The default single-part upload path sends `Content-MD5`, which S3 independently verifies on the server side, providing an end-to-end integrity check that does not depend on `s3util`'s own code being bug-free.

E2E test verification against live AWS S3 covers, at minimum:

- Multipart integrity at multiple file/chunk size combinations (`e2e_multipart_integrity_check_5mb_file_5mb_chunk`, `_8mb_file_8mb_chunk`, `_10mb_file_5mb_chunk`, `_16mb_file_5mb_chunk`, `_16mb_file_8mb_chunk`, `_30mb_file_8mb_chunk`, `_edge_case`).
- Full roundtrip verification in every direction (`e2e_roundtrip_local_to_s3`, `e2e_roundtrip_s3_to_s3`, `e2e_roundtrip_multipart_etag`, `e2e_roundtrip_stdio`, `e2e_roundtrip_checksum`, `e2e_roundtrip_express_one_zone`).
- Stdin/stdout integrity (`e2e_stdio_integrity_check`, `e2e_stdio_metadata`, `e2e_stdio_sse`).
- Cancellation correctness (`e2e_cancel_test`): a cancelled multipart upload leaves no object behind.
- Exit code correctness (`e2e_exit_codes`): every exit code path is exercised.
- Special characters in keys (`e2e_special_characters`) and Express One Zone behavior (`e2e_express_one_zone`).
- Public-bucket access without signing (`e2e_source_no_sign_request`).

#### Known limitations

- **Preview status.** `cp` and `mv` are wired up; additional commands (`rm`, …) will arrive in later releases.
- **Best-effort S3-compatible support.** The code is exercised against Amazon S3 (including Express One Zone). Non-AWS S3-compatible stores may behave differently — `--disable-multipart-verify` / `--disable-etag-verify` / `--disable-additional-checksum-verify` / `--target-force-path-style` are provided for these cases.
- **Single-file, no recursion.** By design — users who need recursive semantics should use `s3sync`.

#### Overall assessment

`s3util` inherits its transfer and verification engine from `s3sync`, which has been battle-tested in production. The CLI is deliberately narrow and the failure modes are well-scoped. The E2E suite exercises the critical integrity paths (multipart composition, stdio streaming, cancellation, exit codes) against live AWS S3 — not mocks — with explicit before/after state assertions.

This does not guarantee the absence of bugs, but it does mean the most dangerous categories of incorrect behavior (silent corruption, missed multipart cleanup, wrong exit codes) are actively tested against real infrastructure at each release.

</details>

## License

This project is licensed under the Apache-2.0 License.
