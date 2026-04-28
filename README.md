# s3util

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.91.1-red)
[![codecov](https://codecov.io/gh/nidor1998/s3util-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3util-rs)

## Tools for managing Amazon S3 objects and buckets

`s3util` is a collection of tools for managing objects and buckets on Amazon S3 and S3-compatible object stores. It ports the transfer, verification, and multipart semantics of [s3sync](https://github.com/nidor1998/s3sync) into a compact CLI focused on interactive and scripted use, and is intended to become part of the future `s7cmd` toolkit.

## Table of contents

<details>
<summary>Click to expand to view table of contents</summary>

- [Overview](#overview)
    * [Scope](#scope)
    * [Non-Goals](#non-goals)
- [Features](#features)
    * [Verifiable transfers](#verifiable-transfers)
    * [Full multipart support](#full-multipart-support)
    * [All transfer directions](#all-transfer-directions)
    * [Server-side copy](#server-side-copy)
    * [stdin/stdout streaming](#stdinstdout-streaming)
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
    * [stdin → S3](#stdin--s3)
    * [S3 → stdout](#s3--stdout)
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
    * [stdin/stdout handling](#stdinstdout-handling)
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

`s3util` is a collection of tools for managing objects and buckets on Amazon S3, built as a companion to [s3sync](https://github.com/nidor1998/s3sync). Where `s3sync` is optimized for bulk, recursive synchronization, `s3util` is optimized for single-object transfers and direct S3 API operations: each invocation operates on one object or one bucket, verifies the result where applicable, and exits with a meaningful status code.

`s3util`'s `cp` and `mv` subcommands follow the same design principles as `s3sync` for transfer, verification, and multipart handling — but each subcommand has a deliberately narrow surface, and the binary is a single file with no recursive/directory mode.

For object transfers in particular, `s3util` emphasizes high reliability, high performance, and advanced functionality: end-to-end checksum verification (ETag plus SHA256/SHA1/CRC32/CRC32C/CRC64NVME, composite or full-object), parallel multipart uploads and downloads, server-side copy, SSE-KMS and SSE-C (including SSE-C re-keying across copies), stdin/stdout streaming, tag and metadata preservation, rate-limited bandwidth control, and Express One Zone support. See [Features](#features) for the full list.

### Scope

s3util is designed to cover **common single-object and bucket-management operations** — single-object transfers (`cp` / `mv`) and common bucket management (creation/deletion, tagging, versioning, policy, lifecycle, encryption, CORS, public-access-block, website, logging, notifications). For any S3 use case outside that scope, use a more comprehensive tool such as the [AWS CLI](https://aws.amazon.com/cli/) (`aws s3` / `aws s3api`); for recursive or bulk synchronization, use [s3sync](https://github.com/nidor1998/s3sync).

The `cp` and `mv` subcommands operate on one object at a time; the thin S3 API wrappers each issue a single S3 API call. s3util is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — including the AWS CLI (`aws s3`, `aws s3api`) and tools such as `s3cmd`, `s5cmd`, `rclone`, and `mc`. Its command-line flags, transfer semantics, verification rules, and exit codes are designed around safe, verifiable single-object transfers and explicit per-API operations — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with s3util unmodified.

### Non-Goals

The following are explicitly out of scope and will not be added, regardless of demand:

- Recursive or directory-mode transfers — use [s3sync](https://github.com/nidor1998/s3sync) instead.
- Glob or wildcard expansion in S3 keys. For pattern-based matching, use s3sync, which supports regular expressions.
- Multiple source or destination arguments to `cp` / `mv` (e.g. `s3util cp a.txt b.txt s3://bucket/dest/`). Each invocation transfers exactly one object.
- Compatibility with other S3 clients — neither in flag names and
  behavior, nor in feature coverage. The presence of a feature, flag,
  or output format in `aws s3`, `aws s3api`, `s3cmd`, `s5cmd`,
  `rclone`, `mc`, or any other S3 tool is not, by itself, a reason
  to add or change it in s3util. Each request is evaluated only
  against s3util's own scope and design principles. Use that other
  tool if you need its specific surface.
- A plugin or extension mechanism.

Issues and pull requests requesting any of the above will be closed.

### Subcommands

`s3util` provides the following subcommands. `cp` and `mv` perform single-object transfers using the full multipart and verification pipeline; the remaining subcommands are thin wrappers around individual S3 API calls with a simpler, script-friendly interface than `aws s3api`.

| Subcommand               | What it does                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------|
| `cp`                     | Copies a single object: Local↔S3, S3↔S3, or stdin/stdout streaming; full multipart + checksum verification |
| `mv`                     | Moves a single object: same as `cp` plus deletes the source after a successful, verified copy (no stdio) |
| `rm`                     | Deletes a single S3 object; silent on success; supports `--source-version-id`                 |
| `head-object`            | Prints `HeadObject` response as JSON; supports `--source-version-id` and SSE-C reads          |
| `put-object-tagging`     | Replaces all tags from `--tagging "k=v&k2=v2"`; silent; supports `--source-version-id`       |
| `get-object-tagging`     | Prints object tags as JSON (`{"TagSet": [...], "VersionId": "..."}`); supports `--source-version-id` |
| `delete-object-tagging`  | Removes all tags from an object; silent; supports `--source-version-id`                       |
| `create-bucket`          | Creates a bucket; LocationConstraint from the SDK client's resolved region (`--target-region`, `AWS_REGION`, or profile); optional `--tagging`; exit 3 if tagging step fails after create |
| `head-bucket`            | Prints `HeadBucket` response as JSON                                                          |
| `delete-bucket`          | Deletes an empty bucket; silent on success                                                    |
| `put-bucket-policy`      | Sets bucket policy from a file path or `-` (stdin); body sent verbatim, no client-side validation; silent |
| `get-bucket-policy`      | Prints policy as JSON (`{"Policy": "<escaped-JSON-string>"}`, matching `aws s3api`); `--policy-only` prints just the inner policy as pretty-printed JSON |
| `delete-bucket-policy`   | Removes bucket policy; silent on success                                                      |
| `put-bucket-tagging`     | Replaces all tags from `--tagging "k=v&k2=v2"`; silent on success                            |
| `get-bucket-tagging`     | Prints bucket tags as JSON (`{"TagSet": [...]}`); exits 4 on `NoSuchTagSet` / `NoSuchBucket`  |
| `delete-bucket-tagging`  | Removes all tags from a bucket; silent on success                                             |
| `put-bucket-versioning`  | Enables or suspends versioning (`--enabled` / `--suspended`, mutually exclusive); silent       |
| `get-bucket-versioning`  | Prints versioning state as JSON (`{"Status": "Enabled"}`); silent when never configured (matches AWS CLI) |
| `put-bucket-lifecycle-configuration`     | Sets lifecycle configuration from a JSON file path or `-` (stdin); silent on success |
| `get-bucket-lifecycle-configuration`     | Prints lifecycle configuration as JSON (`{"Rules": […]}` matching `aws s3api`); exits 4 if no lifecycle is set |
| `delete-bucket-lifecycle-configuration`  | Removes lifecycle configuration; silent on success                                  |
| `put-bucket-encryption`                  | Sets default encryption from a JSON file path or `-` (stdin); silent on success     |
| `get-bucket-encryption`                  | Prints default encryption as JSON; exits 4 if no explicit encryption is set         |
| `delete-bucket-encryption`               | Removes default encryption; silent on success                                       |
| `put-bucket-cors`                        | Sets CORS rules from a JSON file path or `-` (stdin); silent on success             |
| `get-bucket-cors`                        | Prints CORS rules as JSON (`{"CORSRules": […]}`); exits 4 if no CORS is set         |
| `delete-bucket-cors`                     | Removes CORS rules; silent on success                                               |
| `put-public-access-block`                | Sets public-access-block from a JSON file path or `-` (stdin); silent on success    |
| `get-public-access-block`                | Prints public-access-block as JSON; exits 4 if none set                             |
| `delete-public-access-block`             | Removes public-access-block; silent on success                                      |
| `put-bucket-website`                     | Sets the website configuration from a JSON file path or `-` (stdin); silent on success |
| `get-bucket-website`                     | Prints website configuration as JSON; exits 4 if no website is configured                  |
| `delete-bucket-website`                  | Removes the website configuration; silent on success                                       |
| `put-bucket-logging`                     | Sets bucket logging from a JSON file path or `-` (stdin); empty `{}` JSON disables logging; silent on success |
| `get-bucket-logging`                     | Prints bucket logging configuration as JSON; silent when no logging is configured (matches AWS CLI) |
| `put-bucket-notification-configuration`  | Sets notification configuration from a JSON file path or `-` (stdin); empty `{}` JSON disables all notifications; silent on success |
| `get-bucket-notification-configuration`  | Prints notification configuration as JSON; silent when no notifications are configured (matches AWS CLI) |

## Features

The features described below relate to the `cp` and `mv` subcommands. For details on other subcommands, refer to the help documentation (`s3util -h` / `s3util <command> -h`) and the Amazon S3 documentation.

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
| `-` (stdin)   | `s3://…`      | stdin → S3    |
| `s3://…`      | `-` (stdout)  | S3 → stdout   |

S3 → S3 transfers can span **different AWS accounts**, **different regions**, and **different S3-compatible storage providers** (e.g. AWS S3 → S3-compatible storage, or vice versa). The source and target are independently configured via the paired `--source-*` and `--target-*` credential, profile, region, and endpoint flags — they need not share a single S3 endpoint.

### Server-side copy

`--server-side-copy` uses S3's `CopyObject` / `UploadPartCopy` for S3→S3 transfers within the same account/region, avoiding a round-trip through the client. `s3util` falls back to client-side copy when server-side is not possible (different endpoints, SSE-C translation, etc.).

### stdin/stdout streaming

Pipe data directly through S3 without touching the local filesystem:

```bash
pg_dump mydb | s3util cp - s3://my-bucket/backups/mydb.sql
s3util cp s3://my-bucket/backups/mydb.sql - | psql mydb
```

stdin uploads compute the ETag and additional checksum on the fly and verify against the S3-reported values.

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
| `-`              | stdin or stdout                                  |

Every long flag also reads from an uppercase-underscore environment variable of the same name (for example `--max-parallel-uploads` ↔ `MAX_PARALLEL_UPLOADS`).

The examples below describe the `cp` and `mv` commands. For details on other commands (`head-bucket`, `head-object`, `rm`, the bucket-management wrappers, etc.), run `s3util -h` for the top-level subcommand list and `s3util <command> -h` for per-command options.

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

Client-side S3 → S3 copies can span different AWS accounts, different regions, and different S3-compatible providers — point the `--source-*` and `--target-*` flags at independent endpoints:

```bash
# Cross-account, cross-region (separate profiles, separate regions)
s3util cp \
  --source-profile prod --source-region us-east-1 \
  --target-profile dev  --target-region us-west-2 \
  s3://prod-bucket/key s3://dev-bucket/key

# AWS S3 → S3-compatible storage
s3util cp \
  --target-endpoint-url https://s3.example.com:9000 \
  --target-force-path-style \
  s3://aws-bucket/key s3://compat-bucket/key
```

`--server-side-copy` is incompatible with this case (it requires source and target to be reachable from a single S3 endpoint); cross-endpoint copies always run client-side.

### stdin → S3

```bash
pg_dump mydb | s3util cp --additional-checksum-algorithm CRC64NVME \
  - s3://my-bucket/backups/mydb-$(date +%F).sql
```

With stdin as the source there is no basename, so the target key must be spelled out.

### S3 → stdout

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

- **stdin/stdout is not supported.** A `-` source or target is rejected at argument-parse time.
- **The source is deleted only after a successful, verified copy.** If the copy fails, is canceled (SIGINT), or produces a verification warning, the source is left untouched and the command exits with the matching non-zero code.
- **`--no-fail-on-verify-error`** (mv only) treats a verification warning as success and proceeds to delete the source. Use only when you understand why your S3↔S3 chunksize layout produces an expected mismatch.
- **`--source-version-id`** deletes the specific source version after the copy (rather than creating a delete marker on the latest version).

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
  --target-endpoint-url https://s3.example.com:9000 \
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

For single-part objects, the S3-reported ETag is the MD5 of the object. `s3util` computes this on the upload side and compares; for downloads it compares the source's reported ETag against the bytes actually received. A mismatch is treated as an error for Local/stdin→S3 and S3→Local, and as a warning for S3→S3 (where multipart layout differences legitimately change the composite ETag).

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

### stdin/stdout handling

- **stdin → S3** streams bytes into a multipart upload once the threshold is crossed; below the threshold, stdin is buffered into a temp file first so a single-part PUT with a correct `Content-Length` can be issued.
- **S3 → stdout** streams bytes straight to stdout. ETag and any requested additional checksum are computed inline from the streamed bytes and verified against the S3-reported values — the same verification as `S3 → Local`. A mismatch is logged as a warning (exit 3), or as an error if the configured additional checksum is a full-object checksum.

### Express One Zone detail

Directory buckets (`--x-s3` suffix) are automatically detected. Some S3 features behave differently on Express One Zone (for example, default additional-checksum handling); `--disable-express-one-zone-additional-checksum` overrides `s3util`'s default if your bucket policy demands it.

`create-bucket` also accepts directory-bucket names. The zone ID is parsed from the name (`<base>--<zone-id>--x-s3`) and the appropriate `Location`/`Bucket` configuration is sent. The zone type is inferred from the zone-ID shape — one hyphen is treated as an Availability Zone (e.g. `apne1-az4`), two or more as a Local Zone (e.g. `usw2-lax1-az1`). The active region (`--target-region` / `AWS_REGION` / profile) must match the zone's region; otherwise S3 will reject the request.

### S3 Permissions

The permissions below cover the `cp` and `mv` subcommands. Other subcommands have their own requirements; refer to the [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-iam-policies.html) for the full set.

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
| 4    | Not found — `head-bucket` / `head-object` (404 NoSuchBucket / NoSuchKey / NoSuchVersion); `get-object-tagging` / `get-bucket-policy` / `get-bucket-tagging` / `get-bucket-lifecycle-configuration` / `get-bucket-encryption` / `get-bucket-cors` / `get-public-access-block` / `get-bucket-website` when the addressed resource is missing (incl. NoSuchBucketPolicy / NoSuchTagSet / NoSuchLifecycleConfiguration / ServerSideEncryptionConfigurationNotFoundError / NoSuchCORSConfiguration / NoSuchPublicAccessBlockConfiguration / NoSuchWebsiteConfiguration); `get-bucket-versioning` / `get-bucket-logging` / `get-bucket-notification-configuration` only on `NoSuchBucket` (S3 returns success with an empty body when the subresource is unconfigured for these three) |
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

With `-v`, subcommands that are otherwise silent on success (`rm`, `create-bucket`, `delete-bucket`, the `put-*` and `delete-*` bucket/object subcommands) emit a structured info-level event to stderr describing what was changed (e.g. `Object deleted. bucket=… key=… version_id=…`). `get-bucket-versioning`, `get-bucket-logging`, and `get-bucket-notification-configuration` likewise log `Bucket … not configured.` when the bucket has no such configuration (each prints nothing on stdout in that case, matching `aws s3api`, since the underlying S3 API returns success with an empty body for these three).

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

Human engineers authored the requirements, design specifications, and the s3sync reference architecture. They thoroughly reviewed and verified the design, all source code, and all tests. Features of the binary have been manually tested against live AWS S3. The development followed a spec-driven process: requirements and design documents were written first, and the AI generated code to match those specifications under continuous human oversight.

### Quality verification (by AI self-assessment)

| Metric                         | Value                                                         |
|--------------------------------|---------------------------------------------------------------|
| Production code                | ~34,000 lines of Rust (130 source files)                      |
| E2E integration tests          | ~620 tests across 44 test files (gated behind `e2e_test`)     |
| Unit tests                     | ~870 tests embedded in `src/`                                 |
| Code coverage (llvm-cov)       | 94.32% regions, 88.99% functions, 94.85% lines                |
| Static analysis (clippy)       | 0 warnings (`cargo clippy --all-features`)                    |
| Formatting                     | 0 diffs (`cargo fmt --check`)                                 |
| Code reuse from [s3sync](https://github.com/nidor1998/s3sync) | significant (transfer, verification, multipart engine)         |

The codebase is built through spec-driven development with human review at every step. Coverage and test counts will continue to grow alongside future subcommands and refinements.

### AI assessment of safety and correctness (by Claude, Anthropic)

<details>
<summary>Click to expand the full AI assessment</summary>

> Assessment date: 2026-04-26.
>
> Assessed version: 0.2.0 (branch `v0.2.0`, commit `01c5ae2`).
>
> **Snapshot caveat:** v0.2.1+ added five additional bucket-configuration families following the same thin-wrapper pattern (`bucket-lifecycle-configuration`, `bucket-encryption`, `bucket-cors`, `public-access-block`, `bucket-website`) and v0.2.2 added two more (`bucket-logging`, `bucket-notification-configuration`). The new commands reuse the v0.2.0 conventions for argument shape, exit codes, JSON output, and `HeadError` mapping. A full re-assessment is pending; the safety claims below remain accurate for the v0.2.0 wrappers and apply by construction to the v0.2.1+ additions, but specific enumerations of subcommand counts and per-command details are frozen at the v0.2.0 snapshot.
>
> Scope: a repository-wide read of `config`, `storage`, `transfer`, `output`, `types`, the `s3util` binary, the integration suites under `tests/cli_*.rs` and `tests/e2e_*.rs`, the `Cargo.toml`/`Cargo.lock` dependency graph, the `cargo-deny` configuration, and the GitHub Actions CI workflows. The assessment is independent of the maintainer's "Quality verification" table above; coverage and test-count figures are not re-stated here.

**Is s3util designed so that each subcommand performs the operation the operator intended, with no silent data corruption and no silent loss of state, and is it sufficiently tested?**

v0.2.0 broadens s3util from a single-object copy tool into a collection of single-resource subcommands: the verifying transfer pipeline (`cp`, `mv`) and seventeen thin wrappers around individual S3 API calls (`rm`, `head-object`, `head-bucket`, `create-bucket`, `delete-bucket`, the bucket/object tagging family, the bucket-policy family, and the bucket-versioning pair). The risks therefore split into three categories: (1) the operator targets the wrong resource, (2) a bug in s3util corrupts data on a transfer, and (3) a thin wrapper succeeds at the wrong scope or leaves the bucket in a partial state. Each requires different safeguards.

#### Protection against user mistakes

`s3util`'s CLI surface is intentionally narrow. Every subcommand operates on exactly one resource (one object, one bucket, or one bucket subresource), sources and targets are explicit, and there is no recursive mode anywhere in the binary — leaving very little room for a "whoops, I deleted/overwrote a whole tree" class of error.

Concrete safeguards on the transfer subcommands (`cp`, `mv`):

1. **Single-object only.** Directory sources are rejected. A source URL ending in `/` is rejected. A source URL whose final segment is `.` or `..` is rejected at argument-parse time.
2. **Target-parent must exist.** On downloads, `s3util` does not create missing directories — it returns an error and asks the user to create them.
3. **Resolved target printed before transfer.** When the source basename is appended (e.g. target is a bucket root or directory), the resolved path is printed on a `-> <path>` line so the operator can catch a mistake before any bytes move.
4. **Exit code 3 for warnings.** Transfers that complete but report a non-fatal issue (e.g. an S3→S3 ETag mismatch explained by chunksize differences) exit 3 instead of 0, so CI and scripts can treat warnings as something worth looking at.
5. **`--if-none-match`** implements "create only" at the S3 level, preventing accidental overwrite of an existing object.
6. **ctrl-c is safe.** A SIGINT handler cancels any in-flight multipart upload (issuing `AbortMultipartUpload`) before exiting with code 130 (standard Unix SIGINT convention), so scripts can distinguish user cancellation from normal success.

Concrete safeguards on the thin S3 API wrappers:

7. **Argument shape is enforced per subcommand.** `rm` and `head-object` require `s3://<BUCKET>/<KEY>` and reject bucket-only paths; the `*-bucket-*` subcommands require `s3://<BUCKET>` and reject paths with a key. Mismatches are caught at parse time and exit 1 (validation), not after the SDK round-trip.
8. **Mutually exclusive intent flags.** `put-bucket-versioning` requires exactly one of `--enabled` / `--suspended`, enforced by `clap`'s argument groups; the operator cannot accidentally suspend versioning by omitting the intent flag.
9. **Read-side NotFound is a distinct exit code.** `head-bucket`, `head-object`, `get-object-tagging`, `get-bucket-policy`, `get-bucket-tagging`, `get-bucket-versioning`, `get-bucket-lifecycle-configuration`, `get-bucket-encryption`, `get-bucket-cors`, `get-public-access-block`, `get-bucket-website`, `get-bucket-logging`, and `get-bucket-notification-configuration` map S3's `NoSuchBucket`/`NoSuchKey`/`NoSuchVersion`/`NoSuchBucketPolicy`/`NoSuchTagSet`/`NoSuchLifecycleConfiguration`/`ServerSideEncryptionConfigurationNotFoundError`/`NoSuchCORSConfiguration`/`NoSuchPublicAccessBlockConfiguration`/`NoSuchWebsiteConfiguration` to exit code 4, distinct from generic error code 1. Scripts can distinguish "bucket missing" from "auth failed" without parsing stderr.
10. **Per-subresource error-code allowlists are pinned by tests.** `src/storage/s3/api.rs` defines `GET_*_NOT_FOUND_CODES` constants and asserts their exact contents in unit tests, so a typo or accidental edit (e.g. quietly demoting `AccessDenied` to NotFound, or expanding the allowlist) shows up as a unit-test failure rather than at e2e time.
11. **`NoSuchBucket` is always classified before subresource codes.** `classify_not_found` routes `NoSuchBucket` to `HeadError::BucketNotFound` (and a "bucket does not exist" message) before consulting the subresource list, so a missing bucket is never reported as a missing tag/policy. Pinned by test.

The honest gap on the destructive thin wrappers: `rm`, `delete-bucket`, `delete-bucket-policy`, `delete-bucket-tagging`, `delete-object-tagging`, `delete-bucket-lifecycle-configuration`, `delete-bucket-encryption`, `delete-bucket-cors`, `delete-public-access-block`, and `delete-bucket-website` execute silently on success and offer no `--dry-run`, no `--force`/confirmation flag, and no preview of the bucket state. This matches `aws s3api`'s philosophy — and the single-resource scope means a typo can damage at most one resource — but it does mean that "wrong bucket" or "wrong key" mistakes are not caught before the network call. Operators relying on this surface should script around it (e.g. a `head-*` precheck) rather than expect interactive guardrails.

The relevant validation paths are exercised by `tests/cli_arg_validation.rs`, `tests/cli_config_validation_error.rs`, the per-subcommand `tests/cli_<name>.rs` files (which run the binary end-to-end without network access), and — for live behaviour — `tests/e2e_exit_codes.rs`, `tests/e2e_cancel_test.rs`, `tests/e2e_head_bucket.rs`, `tests/e2e_head_object.rs`, `tests/e2e_create_delete_bucket.rs`, `tests/e2e_rm.rs`, `tests/e2e_bucket_policy.rs`, `tests/e2e_bucket_tagging.rs`, `tests/e2e_bucket_versioning.rs`, and `tests/e2e_object_tagging.rs`.

#### Protection against software bugs (transfer engine)

The transfer engine — shared in spirit with the production-tested `s3sync` — is the part of s3util whose bugs would cause the most serious damage: silent corruption, an upload that reports success without fully reading stdin, a multipart assembly that writes parts out of order, or a checksum comparator that accepts any value. The architectural safeguards here are unchanged from v0.1:

- **Verification runs after every upload.** When the source is a local file or stdin, the upload-side ETag and (optionally) additional checksum are compared against S3-reported values. A mismatch is a hard error — the destination object is considered corrupted.
- **S3→S3 warnings, not errors.** On S3→S3 transfers, a checksum mismatch becomes a warning (exit code 3) because differing multipart chunksizes between source and destination can legitimately produce different composite values. `--auto-chunksize` lets users match source chunks exactly when they want identical composite hashes.
- **Algorithmic diversity.** Additional-checksum algorithms span MD/CRC/SHA families (`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256`) with both composite and full-object variants. This is enough to detect silent corruption under every realistic fault model AWS S3 can produce.
- **Multipart cleanup.** Fatal errors and ctrl-c both abort the in-flight multipart upload (`UploadManager::abort_multipart_upload` is invoked from every error/cancel path in `src/storage/s3/upload_manager.rs`), so orphaned multipart fragments don't accumulate and bill.
- **Single-part path keeps `Content-MD5`.** The default single-part upload path sends `Content-MD5`, which S3 independently verifies on the server side, providing an end-to-end integrity check that does not depend on `s3util`'s own code being bug-free.

E2E test verification against live AWS S3 covers, at minimum:

- Multipart integrity at multiple file/chunk size combinations (`e2e_multipart_integrity_check_5mb_file_5mb_chunk`, `_8mb_file_8mb_chunk`, `_10mb_file_5mb_chunk`, `_16mb_file_5mb_chunk`, `_16mb_file_8mb_chunk`, `_30mb_file_8mb_chunk`, `_edge_case`).
- Full roundtrip verification in every direction (`e2e_roundtrip_local_to_s3`, `e2e_roundtrip_s3_to_s3`, `e2e_roundtrip_multipart_etag`, `e2e_roundtrip_stdio`, `e2e_roundtrip_checksum`, `e2e_roundtrip_express_one_zone`).
- stdin/stdout integrity (`e2e_stdio_integrity_check`, `e2e_stdio_metadata`, `e2e_stdio_sse`).
- Cancellation correctness (`e2e_cancel_test`): a cancelled multipart upload leaves no object behind.
- Exit code correctness (`e2e_exit_codes`): every exit code path is exercised.
- Special characters in keys (`e2e_special_characters`) and Express One Zone behavior (`e2e_express_one_zone`).
- Public-bucket access without signing (`e2e_source_no_sign_request`).

#### Protection against software bugs (thin S3 API wrappers)

The thin wrappers are structurally simple: each is a single async function in `src/storage/s3/api.rs` that builds an SDK request, awaits it, and maps the response (or error) into the runtime in `src/bin/s3util/cli/<name>.rs`. The dangerous classes of bug here are different from the transfer engine:

- **Mis-routed errors.** A `NoSuchBucket` reported as a "tag set not found" would mislead an operator into creating a tag set on a non-existent bucket (succeeds, then fails). The shared `HeadError` taxonomy and `classify_not_found` helper, together with the pinned-allowlist tests in `src/storage/s3/api.rs`, make this class of mis-routing a unit-test failure rather than a production surprise.
- **Silent partial state.** `create-bucket --tagging` is two API calls (`CreateBucket` then `PutBucketTagging`); if the second fails the bucket exists untagged. The runtime explicitly logs a warning and returns exit code 3, naming the partial state and the recovery path ("Retry tagging or delete the bucket manually"). It does **not** roll the bucket back automatically — that is a deliberate choice, and the only multi-step thin wrapper in v0.2.0.
- **JSON shape drift.** Output for `head-*` and `get-*` subcommands is hand-serialised in `src/output/json.rs` (the SDK types do not implement `Serialize` and the SDK field shape does not match `aws s3api --output json`). Each serializer omits absent fields rather than emitting `null`, double-encodes `Policy` to match `aws s3api`, and is covered by per-field unit tests in the same file. A regression in field naming or omission semantics shows up as a unit-test failure.
- **stdin handling for `put-bucket-policy`.** The policy body is read with synchronous `std::io::Read::read_to_string` and forwarded verbatim — s3util performs no client-side validation. S3 rejects malformed policies with `400 MalformedPolicy`, so a bad body cannot silently apply, but operators should be aware that the file path variant has no in-process size cap (S3's own ~20KB policy limit is the effective bound).

Cancellation: only the transfer subcommands install the SIGINT handler. The thin wrappers do not — each is a single SDK call, so ctrl-c terminates the process and the in-flight HTTP request is aborted at the connection layer. There is no in-flight multi-step state to clean up except the documented `create-bucket --tagging` window described above.

#### Cross-cutting concerns

- **Credential handling.** `AccessKeys`, `SseKmsKeyId`, and `SseCustomerKey` derive `Zeroize` + `ZeroizeOnDrop` and have hand-written `Debug` impls that print `** redacted **` for the secret fields. The `trace_config_summary` helper in `bin/s3util/main.rs` deliberately enumerates non-sensitive fields rather than `{:?}`-printing the whole `Config`, so a future field addition cannot silently leak via tracing.
- **TLS and crypto stack.** `Cargo.toml` opts out of `aws-sdk-s3`'s default features specifically to drop the legacy `rustls 0.21` alias (vulnerable `rustls-webpki 0.101.x`, RUSTSEC-2026-0098) and re-enables the modern `default-https-client` feature. `Cargo.lock` confirms `rustls 0.23.x`, `rustls-webpki 0.103.x`, and `ring 0.17.x` are the resolved versions; `openssl-sys` is in the `cargo-deny` ban list.
- **Supply chain enforcement.** `cargo-deny check` runs in CI on every push and PR (`.github/workflows/cargo-deny.yml` plus the `cargo_deny` job in `ci.yml`); `advisories.ignore = []`, so any new RUSTSEC advisory fails the build until reviewed. The project pins specific crate versions rather than wildcards, and the license allowlist is restricted to standard permissive licenses.
- **CI matrix.** `ci.yml` builds and unit-tests on Linux x86_64/aarch64 (gnu and musl), Windows x86_64/aarch64, and macOS aarch64 with stable Rust on every push; `cargo fmt --all --check` and `cargo clippy -- -D warnings` are required gates. E2E tests are gated behind `--cfg e2e_test` and run only by the maintainer against live AWS — not by CI — which is the right tradeoff (they would otherwise need credentials in CI and would create real billable resources).

#### Known limitations

- **Best-effort S3-compatible support.** The code is exercised against Amazon S3 (including Express One Zone). Non-AWS S3-compatible stores may behave differently — `--disable-multipart-verify` / `--disable-etag-verify` / `--disable-additional-checksum-verify` / `--target-force-path-style` are provided for these cases. The thin wrappers depend on S3 returning the documented error codes (`NoSuchBucketPolicy`, `NoSuchTagSet`, etc.); a compatible store that returns a different code will fall through to exit code 1 ("Other") rather than the dedicated NotFound exit 4.
- **Single-resource, no recursion.** By design — users who need recursive semantics should use `s3sync`. `rm` deletes one key, `delete-bucket` requires the bucket to be empty (S3 returns `409 BucketNotEmpty` otherwise, which surfaces as exit code 1).
- **No interactive guard on destructive thin wrappers.** As noted above, `rm` and the `delete-*` family act immediately. A `--dry-run`/`--yes` pair is not implemented in v0.2.0.
- **`create-bucket --tagging` is not transactional.** Documented and surfaced as exit code 3 with an explicit recovery hint, but operators must still act on the warning.
- **`put-bucket-policy` performs no client-side schema validation.** By design — the body is forwarded verbatim and S3 is the authority on policy validity.
- **Verification module contains a small number of `panic!()` invariant assertions** (e.g. `panic!("object_parts is empty")` in `src/storage/e_tag_verify.rs` and `src/storage/additional_checksum_verify.rs`). These guard caller-side preconditions and should never fire in normal operation; they would be cleaner as `Result::Err`, but their reachability is not currently demonstrated by any test.

#### Overall assessment

The transfer engine in v0.2.0 is unchanged from the production-tested foundation it inherits from `s3sync`: the multipart-composition, checksum-verification, cancellation, and stdio paths are all covered by E2E tests that run against live AWS S3 with explicit before/after state assertions. The most dangerous categories of behaviour (silent corruption, missed multipart cleanup, wrong exit codes) are actively tested against real infrastructure rather than mocks.

The new thin S3 API wrappers introduced in v0.2.0 are deliberately structured to reduce their own bug surface: each is one SDK call, error classification is centralised in a small set of helpers with pinned-allowlist tests, and JSON serialisation is hand-written with per-field unit coverage. The honest weak spot is the lack of interactive guard rails on the destructive subcommands (`rm`, `delete-bucket`, the `delete-*` family) — there is no `--dry-run` or confirmation prompt, and operators relying on these surfaces in scripts should add their own preflight checks. The single-resource scope keeps the worst-case blast radius bounded.

This assessment does not guarantee the absence of bugs. It does mean that the categories of incorrect behaviour with the highest blast radius — silent data corruption on transfer, mis-routed NotFound errors on read, and silent partial state on multi-step writes — are either prevented by design or made loud (exit 3 / exit 4 / a tracing warning) rather than silent.

</details>

## License

This project is licensed under the Apache-2.0 License.
