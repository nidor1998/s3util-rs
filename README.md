# s3util

[![Crates.io](https://img.shields.io/crates/v/s3util-rs.svg)](https://crates.io/crates/s3util-rs)
[![GitHub](https://img.shields.io/github/downloads/nidor1998/s3util-rs/total?label=downloads%20%28GitHub%29)](https://github.com/nidor1998/s3util-rs/releases)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
![MSRV](https://img.shields.io/badge/msrv-1.91.1-red)
[![codecov](https://codecov.io/gh/nidor1998/s3util-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3util-rs)

## Tools for managing Amazon S3 objects and buckets

`s3util` is a collection of tools for managing objects and buckets on Amazon S3 and S3-compatible object stores, built on the official [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust) (`aws-sdk-s3`). It ports the transfer, verification, and multipart semantics of [s3sync](https://github.com/nidor1998/s3sync) into a compact CLI focused on interactive and scripted use, and is intended to become part of the future [`s7cmd`](https://github.com/nidor1998/s7cmd) toolkit.

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
    * [Install from crates.io](#install-from-cratesio)
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
- [CI/CD Integration](#cicd-integration)
- [About testing](#about-testing)
- [Fully AI-generated (human-verified) software](#fully-ai-generated-human-verified-software)
    * [Quality verification (by AI self-assessment)](#quality-verification-by-ai-self-assessment)
    * [AI assessment of safety and correctness (by Claude, Anthropic)](#ai-assessment-of-safety-and-correctness-by-claude-anthropic)
    * [AI assessment of safety and correctness (by Codex)](#ai-assessment-of-safety-and-correctness-by-codex)
    * [AI assessment of safety and correctness (by Gemini)](#ai-assessment-of-safety-and-correctness-by-gemini)
- [License](#license)

</details>

## Overview

`s3util` is a collection of tools for managing objects and buckets on Amazon S3, built as a companion to [s3sync](https://github.com/nidor1998/s3sync). Where `s3sync` is optimized for bulk, recursive synchronization, `s3util` is optimized for single-object transfers and direct S3 API operations: each invocation operates on one object or one bucket, verifies the result where applicable, and exits with a meaningful status code.

`s3util`'s `cp` and `mv` subcommands follow the same design principles as `s3sync` for transfer, verification, and multipart handling — but each subcommand has a deliberately narrow surface, and the binary is a single file with no recursive/directory mode.

For object transfers in particular, `s3util` emphasizes high reliability, high performance, and advanced functionality: end-to-end checksum verification (ETag plus SHA256/SHA1/CRC32/CRC32C/CRC64NVME, composite or full-object), parallel multipart uploads and downloads, server-side copy, SSE-KMS and SSE-C (including SSE-C re-keying across copies), stdin/stdout streaming, tag and metadata preservation, rate-limited bandwidth control, and Express One Zone support. See [Features](#features) for the full list.

### Scope

s3util is designed to cover **common single-object and bucket-management operations** — single-object transfers (`cp` / `mv`) and common bucket management (creation/deletion, tagging, versioning, policy, lifecycle, encryption, CORS, public-access-block, website, logging, notifications). For any S3 use case outside that scope, use a more comprehensive tool such as the [AWS CLI](https://aws.amazon.com/cli/) (`aws s3` / `aws s3api`); for recursive or bulk synchronization, use [s3sync](https://github.com/nidor1998/s3sync).

The `cp` and `mv` subcommands operate on one object at a time; the thin S3 API wrappers each issue a single S3 API call. s3util is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — including the AWS CLI (`aws s3`, `aws s3api`) and tools such as `s3cmd`, `s5cmd`, `rclone`, and `mc`. Its command-line flags, transfer semantics, verification rules, and exit codes are designed around safe, verifiable single-object transfers and explicit per-API operations — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with `s3util` unmodified.

### Non-Goals

The following are explicitly out of scope and will not be added, regardless of demand:

- Recursive or directory-mode transfers — use [s3sync](https://github.com/nidor1998/s3sync) instead.
- Glob or wildcard expansion in S3 keys. For pattern-based matching, use [s3sync](https://github.com/nidor1998/s3sync), which supports regular expressions.
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

For S3→S3 transfers, ETag and composite additional-checksum mismatches are downgraded to **warnings** (exit code 3), because differing multipart chunk sizes between source and destination legitimately produce different composite values. **Full-object** additional-checksum mismatches remain errors — chunk size cannot legitimately change a full-object checksum, so a mismatch indicates real corruption.

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

By default, S3→S3 transfers are **client-side**: `s3util` streams the object from the source through the local process and re-uploads it to the target. This is the most compatible mode — it works across different regions, endpoints, accounts, and S3-compatible providers, and is required for any transfer that crosses a boundary `CopyObject` cannot.

Passing `--server-side-copy` switches to S3's `CopyObject` / `UploadPartCopy`, so the bytes never round-trip through the client. Both source and target must be S3, and the API call must be supported by the server (typically same-region, single endpoint). `s3util` does **not** fall back to client-side copy if server-side copy fails or is unsuitable — leave the flag off when in doubt.

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

### Install from crates.io

```bash
cargo install s3util-rs
```

The crate is published as [`s3util-rs`](https://crates.io/crates/s3util-rs); the installed binary is named `s3util`.

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

Every long flag also reads from an uppercase-underscore environment variable of the same name (for example `--max-parallel-uploads` ↔ `MAX_PARALLEL_UPLOADS`). Precedence: CLI arguments > environment variables > defaults.

The examples below describe the `cp` and `mv` commands. For details on other commands (`head-bucket`, `head-object`, `rm`, the bucket-management wrappers, etc.), run `s3util -h` for the top-level subcommand list and `s3util <command> -h` for per-command options.

### Upload a local file

```bash
s3util cp ./release.tar.gz s3://my-bucket/releases/
```

If the target ends in `/` (or is a bucket root), the source basename is appended to form the key. When `--show-progress` is set, the destination path is printed on a `-> <path>` line before the transfer summary.

### Download to local

```bash
s3util cp s3://my-bucket/hosts .
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

# Download and verify the additional checksum stored on the object
# (the algorithm is whatever was used at upload time)
s3util cp --enable-additional-checksum \
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

If the target is `s3://bucket`, `s3://bucket/dir/`, or a directory-style local path (an existing directory, or one ending in a path separator like `../`), the source basename is appended. When `--show-progress` is set, the destination path is printed on a `-> <path>` line before the transfer summary.

With stdin as the source there is no basename, so the target key must be spelled out.

### ETag verification

For single-part objects, the S3-reported ETag is the MD5 of the object. `s3util` computes this on the upload side and compares; for downloads it compares the source's reported ETag against the bytes actually received. Local/stdin→S3 mismatches are treated as **errors** (the upload is considered corrupted and the source is authoritative). S3→Local and S3→S3 mismatches are **warnings** (exit code 3) — for S3→S3 because multipart layout differences legitimately change the composite ETag, and for S3→Local because the file is already written and the warning lets you decide whether to redownload.

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

- **stdin → S3** reads up to `--multipart-threshold` bytes into an in-memory buffer. If EOF arrives first, the buffered bytes are issued as a single-part PUT with a correct `Content-Length`; otherwise the buffered prefix is chained with the rest of the stream and uploaded as a multipart.
- **S3 → stdout** streams bytes straight to stdout. ETag and any requested additional checksum are computed inline from the streamed bytes and verified against the S3-reported values — the same verification as `S3 → Local`. A mismatch is logged as a warning (exit 3), or as an error if the configured additional checksum is a full-object checksum.

### Express One Zone detail

Directory buckets (`--x-s3` suffix) are automatically detected. Some S3 features behave differently on Express One Zone (for example, default additional-checksum handling); `--disable-express-one-zone-additional-checksum` overrides `s3util`'s default if your bucket policy demands it.

`create-bucket` also accepts directory-bucket names. The zone ID is parsed from the name (`<base>--<zone-id>--x-s3`) and the appropriate `Location`/`Bucket` configuration is sent. The zone type is inferred from the zone-ID shape — at most one hyphen is treated as an Availability Zone (e.g. `apne1-az4`), two or more as a Local Zone (e.g. `usw2-lax1-az1`). The active region (`--target-region` / `AWS_REGION` / profile) must match the zone's region; otherwise S3 will reject the request.

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
| 2    | Argument-parsing error — an argument is unknown, missing, or has an invalid value                                   |
| 3    | Warning — transfer completed but a non-fatal issue was logged (e.g. S3→S3 ETag mismatch explained by chunksize)     |
| 4    | Not found — `head-bucket` / `head-object` (404 NoSuchBucket / NoSuchKey / NoSuchVersion); `get-object-tagging` / `get-bucket-policy` / `get-bucket-tagging` / `get-bucket-lifecycle-configuration` / `get-bucket-encryption` / `get-bucket-cors` / `get-public-access-block` / `get-bucket-website` when the addressed resource is missing (incl. NoSuchBucketPolicy / NoSuchTagSet / NoSuchLifecycleConfiguration / ServerSideEncryptionConfigurationNotFoundError / NoSuchCORSConfiguration / NoSuchPublicAccessBlockConfiguration / NoSuchWebsiteConfiguration); `get-bucket-versioning` / `get-bucket-logging` / `get-bucket-notification-configuration` only on `NoSuchBucket` — for these three, an unconfigured subresource is reported by S3 as a successful empty body, which exits 0, not 4 |
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

Measurements below are taken at v1.0.0 (commit `ff25a12` on `update/v1.0.0`, 2026-04-29). The coverage figures are sourced from `lcov.report` (`cargo llvm-cov`) and reflect the unit-test build only — the `--cfg e2e_test` integration suite runs separately and is not included in the report.

| Metric                         | Value                                                         |
|--------------------------------|---------------------------------------------------------------|
| Production code                | ~35,000 lines of Rust across 131 source files in `src/`       |
| Unit tests (in `src/`)         | 926 `#[test]` / `#[tokio::test]` annotations                  |
| CLI integration tests          | 249 annotations across 38 `tests/cli_*.rs` files (no network access; run in CI) |
| E2E integration tests          | 644 annotations across 44 `tests/e2e_*.rs` files (gated behind `--cfg e2e_test`; run only by the maintainer against live AWS) |
| Code coverage (llvm-cov, unit-test build) | 96.69% regions (1019 / 30,832 missed), 95.45% functions (100 / 2,200 missed), 97.55% lines (527 / 21,526 missed) |
| Static analysis (clippy)       | 0 warnings (`cargo clippy --all-features`)                    |
| Formatting                     | 0 diffs (`cargo fmt --all --check`)                           |
| Supply chain (cargo-deny)      | Clean (`cargo deny -L error check`); runs per-PR in `ci.yml` and daily at 01:34 UTC in `cargo-deny.yml`; `advisories.ignore = []` |
| Code adapted from [s3sync](https://github.com/nidor1998/s3sync) | Transfer engine (`src/transfer/`), checksum verification (`src/storage/e_tag_verify.rs`, `src/storage/additional_checksum_verify.rs`), and multipart upload manager (`src/storage/s3/upload_manager.rs`) |

What these numbers do and do not show:
- They show what the unit-test build exercises and what the CI pipeline asserts on every push and PR — not how the binary behaves against live S3 under production load.
- Coverage is a structural metric. A covered line can still be incorrect; an uncovered line can still be correct. Use it to size the test surface, not to certify behaviour.
- The e2e suite covers live-AWS paths (multipart integrity, roundtrip, cancellation, exit codes, Express One Zone, public-bucket access) but runs only on the maintainer's machine; CI does not exercise it.

The codebase is built through spec-driven development with human review at every step. Test counts and coverage will change as subcommands and refinements are added.

### AI assessment of safety and correctness (by Claude, Anthropic)

<details>
<summary>Click to expand the full AI assessment</summary>

> Assessment date: 2026-04-29.
>
> Assessed version: 1.0.0 (branch `update/v1.0.0`, commit `ff25a12`).
>
> Scope of evidence: the assessment is grounded in a repository-wide read of the source tree (`src/bin/s3util/`, `src/config/`, `src/storage/`, `src/transfer/`, `src/input/`, `src/output/`, `src/types/`), the integration suites under `tests/cli_*.rs` and `tests/e2e_*.rs`, the `Cargo.toml` and `Cargo.lock` dependency graph, the `cargo-deny` configuration, the GitHub Actions workflows in `.github/workflows/`, and the line-coverage report in `lcov.info` / `lcov.report`. Each safety claim below cites the file(s) where the relevant code or test lives so a reviewer can verify it directly.
>
> Limits of the evidence: this assessment cannot rule out all bugs. It does not include dynamic analysis (fuzzing, sanitizers), penetration testing, or formal verification, and it does not exercise live infrastructure beyond what the maintainer reports. The e2e suite is gated behind `--cfg e2e_test` and runs against real AWS only on the maintainer's account; CI does not run it.

**Question addressed:** does each subcommand perform the operation the operator intended, with no silent data corruption and no silent loss of state, and is the codebase tested in proportion to that risk?

The risks split into three categories that need different safeguards: (1) the operator targets the wrong resource, (2) a bug in `s3util` corrupts data on a transfer, (3) a thin S3 API wrapper succeeds at the wrong scope or leaves the bucket in a partial state.

#### Subcommand surface (v1.0.0)

The CLI exposes 37 subcommand entry points under `src/bin/s3util/cli/`: two transfer subcommands (`cp`, `mv`) and 35 thin wrappers around individual S3 API calls. Every subcommand operates on exactly one resource — there is no recursive mode in the binary, no glob expansion, no multi-source/multi-target form. The top-level `--help` is generated by `clap` and grouped by resource family (`src/bin/s3util/cli/mod.rs`).

#### Argument validation and operator-mistake protection

Validation runs at argument-parse time, before any network call:

- Path shape per subcommand. Transfer subcommands and `rm` / `head-object` / object-tagging subcommands require `s3://<BUCKET>/<KEY>`; bucket-management subcommands require `s3://<BUCKET>` and reject paths with a key. Mismatches exit with clap's code 2.
- Source-side rejection on `cp` / `mv`. Source URLs ending in `/`, source basenames of `.` or `..`, and local directory sources are rejected.
- Mutual exclusivity. `put-bucket-versioning` requires exactly one of `--enabled` / `--suspended` via `clap`'s argument groups, so omitting the intent flag fails parse rather than defaulting to a destructive action.
- Destination preview. With `--show-progress`, the resolved destination path is printed on a `-> <path>` line before the transfer summary (`src/bin/s3util/cli/indicator.rs`).
- Local target parent must exist. Downloads do not create missing directories; the runtime returns an error and asks the operator to create them (`src/storage/local/fs_util.rs`).
- `--if-none-match` provides "create only" semantics on uploads, plumbed through `UploadManager` (`src/storage/s3/upload_manager.rs:482, 1062, 1879, 1913`).

The integration suite covers these paths without network access across 39 `tests/cli_*.rs` files (253 `#[test]` / `#[tokio::test]` annotations in total), including the cross-cutting `tests/cli_arg_validation.rs`, `tests/cli_config_validation_error.rs`, `tests/cli_tracing_to_stderr.rs`, and `tests/cli_command_api_mapping.rs` plus a per-subcommand file for each `cp`, `mv`, `rm`, `head-*`, `create-bucket`, `delete-*`, `put-*`, and `get-*` runtime. The process-level tests invoke the binary end-to-end and assert exit codes and stderr; the command/API mapping test statically pins the dispatch chain.

The honest gap: `rm`, `delete-bucket`, and the eight `delete-bucket-*` subcommands (`delete-bucket-policy`, `delete-bucket-tagging`, `delete-object-tagging`, `delete-bucket-lifecycle-configuration`, `delete-bucket-encryption`, `delete-bucket-cors`, `delete-public-access-block`, `delete-bucket-website`) act immediately on a successful argument parse. No `--dry-run`, `--force`, or confirmation prompt is implemented in v1.0.0. The single-resource scope bounds the blast radius (one object or one bucket per invocation), but a "wrong bucket" or "wrong key" mistake is not caught before the network call. Operators relying on these subcommands in scripts should add their own preflight (e.g. a `head-*` check).

#### Protection against software bugs in the transfer engine (`cp`, `mv`)

The transfer engine — adapted from the codebase of [`s3sync`](https://github.com/nidor1998/s3sync) — is the area whose bugs would have the highest blast radius (silent corruption, unread bytes, out-of-order multipart assembly, broken checksum comparison). Concrete safeguards in the current code:

- Per-upload verification. When the source is a local file or stdin, the upload-side ETag and (when configured) the additional checksum are computed inline and compared against the values S3 returns. A mismatch is a hard error — the object is treated as corrupted (`src/storage/e_tag_verify.rs`, `src/storage/additional_checksum_verify.rs`).
- S3→S3 mismatches are warnings (exit 3), not errors, because differing chunksizes between source and target legitimately produce different composite values; `--auto-chunksize` mirrors source layout so composite values match end-to-end.
- Algorithmic diversity. Composite and full-object variants of `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, `SHA256` are implemented in `src/storage/checksum/` (each algorithm at 100% region coverage in the report). The default single-part upload path also sends `Content-MD5` (`src/storage/s3/upload_manager.rs:501, 811, 830, 1081, 1201, 1228, 1427, 1537`), giving S3 an end-to-end check that does not depend on `s3util`'s own code being bug-free; `--disable-content-md5-header` opts out.
- Multipart cleanup on errors. `UploadManager::abort_multipart_upload` is invoked from every error path (`src/storage/s3/upload_manager.rs:402, 1034, 1040, 1069`) and is best-effort: a `NoSuchUpload` from a race with a completed upload is logged and not rethrown.
- Cancellation. `spawn_ctrl_c_handler` (`src/bin/s3util/cli/ctrl_c_handler.rs`) is installed exclusively from `run_copy_phase` (`src/bin/s3util/cli/mod.rs:163`), which is shared by `cp` and `mv`. SIGINT cancels the `PipelineCancellationToken`, the in-flight multipart upload is aborted, and the process exits 130. The handler is unit-tested against a real SIGINT (`ctrl_c_handler_handles_sigint` in the same file).

Live-AWS coverage of these paths includes seven multipart integrity tests across distinct file/chunk size combinations (`tests/e2e_multipart_integrity_check_*.rs`), six roundtrip suites (`e2e_roundtrip_local_to_s3.rs`, `e2e_roundtrip_s3_to_s3.rs`, `e2e_roundtrip_multipart_etag.rs`, `e2e_roundtrip_stdio.rs`, `e2e_roundtrip_checksum.rs`, `e2e_roundtrip_express_one_zone.rs`), three stdin/stdout integrity suites (`e2e_stdio_integrity_check.rs`, `e2e_stdio_metadata.rs`, `e2e_stdio_sse.rs`), cancellation correctness (`e2e_cancel_test.rs`), exit-code correctness across all paths (`e2e_exit_codes.rs`), special-character keys (`e2e_special_characters.rs`), Express One Zone behaviour (`e2e_express_one_zone.rs`), and unsigned public-bucket access (`e2e_source_no_sign_request.rs`). The full e2e tree is 44 files containing 644 `#[test]` / `#[tokio::test]` annotations.

#### Protection against software bugs in the thin S3 API wrappers

Each thin wrapper is one async function in `src/storage/s3/api.rs` plus a runtime in `src/bin/s3util/cli/<name>.rs`. The dangerous classes of bug differ from the transfer engine:

- Error mis-routing. A `HeadError` enum (`src/storage/s3/api.rs:67`) distinguishes `BucketNotFound`, `NotFound`, and `Other`. The `classify_not_found` helper (`src/storage/s3/api.rs:249`) routes `NoSuchBucket` to `BucketNotFound` before consulting any per-subresource list, so a missing bucket is never reported as a missing tag/policy/configuration. Each `get-*` subcommand has its own `GET_*_NOT_FOUND_CODES` constant; 11 `*_not_found_codes_pinned` unit tests assert the exact contents of those constants, and four `classify_not_found_*` tests assert that `NoSuchBucket` always routes to `BucketNotFound` ahead of any subresource code. A typo or accidental edit fails unit tests instead of producing a wrong exit code at e2e time.
- Wrong-operation routing. `tests/cli_command_api_mapping.rs` pins the chain from clap subcommand to `main.rs` runtime, from runtime to `api::*` wrapper, and from each wrapper to the expected AWS SDK operation. For example, `get-bucket-policy` must dispatch to `run_get_bucket_policy`, call `api::get_bucket_policy`, and wrap `client.get_bucket_policy()` rather than a sibling operation such as versioning or deletion.
- Output JSON shape drift. The SDK types do not implement `Serialize`, and the SDK field shape does not match `aws s3api --output json`. `src/output/json.rs` hand-serialises every `head-*` / `get-*` response (e.g. `get_bucket_policy_to_json` at line 30, which double-encodes `Policy` to match `aws s3api`'s shape) and contains 83 unit-test annotations covering field naming, omission of absent fields, and the `{}` empty-body cases for `get-bucket-versioning` / `get-bucket-logging` / `get-bucket-notification-configuration`.
- Input JSON shape drift. JSON-consuming `put-*` subcommands (`put-bucket-cors`, `put-bucket-encryption`, `put-bucket-lifecycle-configuration`, `put-bucket-logging`, `put-bucket-notification-configuration`, `put-bucket-website`, `put-public-access-block`) parse user-supplied JSON through dedicated mirror structs in `src/input/json.rs` (e.g. `CorsConfigurationJson`) before converting to SDK types. `src/input/json.rs` carries 64 unit-test annotations. v1.0.0 added e2e tests asserting that every JSON-consuming `put-*` subcommand rejects malformed JSON with exit code 1 from both file and stdin.
- Verbatim policy body. `put-bucket-policy` is the one exception: the body is read with synchronous `std::io::Read::read_to_string` and forwarded verbatim (`src/bin/s3util/cli/put_bucket_policy.rs`). S3 itself rejects malformed policies with `400 MalformedPolicy`, so a bad body cannot silently apply, but `s3util` performs no client-side schema validation and there is no in-process size cap (S3's documented ~20 KB policy limit is the effective bound).
- Silent partial state. The only thin wrapper that issues more than one S3 API call is `create-bucket --tagging` (`CreateBucket` then `PutBucketTagging`). When the second call fails, the bucket exists untagged; the runtime emits a tracing warning naming the partial state and exits 3 with the recovery hint "Retry tagging or delete the bucket manually." There is no automatic rollback — by design.

Cancellation in the thin wrappers: no SIGINT handler is installed. Each wrapper is a single SDK call; ctrl-c terminates the process and the in-flight HTTP request is aborted at the connection layer. There is no in-process multi-step state to clean up except the documented `create-bucket --tagging` window.

#### Cross-cutting concerns

- Credential handling. `AccessKeys`, `SseKmsKeyId`, and `SseCustomerKey` (`src/types/mod.rs:323, 344, 358`) derive `Zeroize` + `ZeroizeOnDrop` and override `Debug` to print `** redacted **` for secret fields. The `trace_config_summary` helper in `src/bin/s3util/main.rs:871` enumerates non-sensitive fields explicitly rather than `{:?}`-printing the whole `Config`, so a future field addition cannot leak via tracing without a deliberate edit. Three unit tests in `src/types/mod.rs` assert the redaction behaviour (lines 387–408).
- TLS / crypto stack. `Cargo.toml` opts out of `aws-config` and `aws-sdk-s3` default features and re-enables `default-https-client` to pin the modern rustls path. The comment on lines 24–27 explains the goal: drop the legacy `rustls 0.21` alias and its vulnerable `rustls-webpki 0.101.x` (RUSTSEC-2026-0098). `Cargo.lock` confirms `rustls 0.23.39`, `rustls-webpki 0.103.13`, and `ring 0.17.14`. `openssl-sys` is on the `cargo-deny` ban list (`deny.toml`).
- Supply chain enforcement. `cargo deny check` runs in two places: as the `cargo_deny` job in `.github/workflows/ci.yml` on every push and PR, and on a daily 01:34 UTC schedule via `.github/workflows/cargo-deny.yml`. `deny.toml` sets `advisories.ignore = []`, so any new RUSTSEC advisory fails the build until it is reviewed and the ignore list is updated. `unknown-registry = "deny"` and `unknown-git = "deny"` reject crates from unverified sources; the license allowlist is restricted to standard permissive licenses. Direct dependencies in `Cargo.toml` are pinned to specific versions rather than wildcards.
- CI. `.github/workflows/ci.yml` builds and unit-tests on stable Rust on a matrix of seven targets: `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `aarch64-unknown-linux-gnu`, `aarch64-unknown-linux-musl`, `x86_64-pc-windows-msvc`, `aarch64-pc-windows-msvc`, and `aarch64-apple-darwin`. The `rustfmt` job runs `cargo fmt --all --check`, and the `clippy` job runs `cargo clippy -- -D warnings` on Linux x86_64. A separate workflow (`.github/workflows/rust-clippy.yml`) runs `cargo clippy --all-features` weekly and uploads results to the GitHub code-scanning view. The e2e suite (`--cfg e2e_test`) is not run by CI because it requires AWS credentials and creates real billable resources; it runs only on the maintainer's machine.
- Release pipeline. `.github/workflows/cd.yml` is gated by a `create-release` job and produces signed build provenance attestations (`actions/attest-build-provenance@v4`) for every published artifact across the seven release targets, then publishes to crates.io with `--locked` via the `rust-lang/crates-io-auth-action@v1` workflow.

#### Coverage measurement

`lcov.report` (generated by `cargo llvm-cov`) reports the following totals across the project: 96.69% region coverage (1019 of 30,832 regions missed), 95.45% function coverage (100 of 2200 functions missed), 97.55% line coverage (527 of 21,526 lines missed). Notable per-file figures relevant to safety-critical paths:

- `storage/checksum/crc32.rs`, `crc32_c.rs`, `crc64_nvme.rs`, `sha1.rs`, `sha256.rs`, and `mod.rs`: each at 100.00% region coverage.
- `storage/e_tag_verify.rs`: 98.82% regions, 99.52% lines.
- `storage/additional_checksum_verify.rs`: 98.59% regions, 99.43% lines.
- `storage/s3/upload_manager.rs`: 95.50% regions, 94.40% lines.
- `transfer/mod.rs`, `transfer/progress.rs`: each at 100.00% regions.
- `transfer/s3_to_local.rs`: 99.05% regions; `transfer/s3_to_s3.rs`: 97.83%; `transfer/stdio_to_s3.rs`: 98.61%.
- `output/json.rs`: 99.46% regions; `input/json.rs`: 98.04% regions.
- `storage/s3/api.rs` (where the thin-wrapper SDK calls and `classify_not_found` live): 94.59% regions, 94.44% lines.

Coverage is a structural metric, not a correctness proof: a covered line can still be incorrect, and the report does not capture e2e tests (which run under a separate `--cfg e2e_test` build and do not feed `cargo llvm-cov` in this repository). The numbers above are an upper bound on what unit-test-level execution exercises.

#### Known limitations

- Best-effort S3-compatible support. The behaviour observed against Amazon S3 (including Express One Zone) is what the e2e suite asserts. Non-AWS S3-compatible stores may differ; flags such as `--disable-multipart-verify`, `--disable-etag-verify`, `--disable-additional-checksum-verify`, and `--target-force-path-style` exist for those cases. The thin wrappers depend on S3 returning the documented error codes (`NoSuchBucketPolicy`, `NoSuchTagSet`, `NoSuchLifecycleConfiguration`, …); a compatible store that returns a different code falls through to exit code 1 ("Other") rather than the dedicated NotFound exit code 4.
- No interactive guard on destructive subcommands. `rm` and the nine `delete-*` subcommands (`delete-bucket`, `delete-bucket-policy`, `delete-bucket-tagging`, `delete-object-tagging`, `delete-bucket-lifecycle-configuration`, `delete-bucket-encryption`, `delete-bucket-cors`, `delete-public-access-block`, `delete-bucket-website`) act immediately on a successful argument parse; no `--dry-run` or `--yes` exists in v1.0.0.
- `create-bucket --tagging` is not transactional. Documented and surfaced as exit code 3 with an explicit recovery hint, but the operator must act on the warning.
- `put-bucket-policy` performs no client-side schema validation. By design — body forwarded verbatim, S3 is the authority on policy validity.
- Ten `panic!()` invariant assertions in storage code: `src/storage/e_tag_verify.rs:151`, `src/storage/additional_checksum_verify.rs:33, 109, 112`, `src/storage/s3/upload_manager.rs:269, 2154, 2182`, `src/storage/s3/mod.rs:97`, `src/storage/local/mod.rs:111`, and `src/storage/checksum/mod.rs:47`. Each guards a caller-side precondition (e.g. "object_parts is empty", "unknown algorithm") and should be unreachable in normal operation; their reachability is not currently demonstrated by any test, and converting them to `Result::Err` would be a strict improvement in robustness.
- E2E tests run only on the maintainer's account. CI does not exercise live AWS, so any CI green signal does not by itself confirm correct behaviour against real S3 — the maintainer's local e2e run is the load-bearing evidence for that.
- This assessment does not include fuzz testing, sanitizer runs, or a formal threat model. Fuzzing of `src/input/json.rs` and `src/output/json.rs`, and `MIRIFLAGS`-based UB checks across the upload/download paths, would be reasonable next steps.

#### Overall assessment

The transfer engine is the highest-blast-radius surface in `s3util`, and it is the most heavily tested: post-upload checksum verification on every transfer, S3 server-side `Content-MD5` verification on the default single-part path, multipart-cleanup on every error/cancel path with unit-tested SIGINT handling, end-to-end roundtrip and integrity tests against live AWS at multiple file/chunk combinations, and per-algorithm checksum coverage at 100% regions. The categories of failure that would be most damaging — silent data corruption, missed multipart cleanup, an upload returning success without all bytes — are detected at runtime by mechanisms that do not depend solely on `s3util`'s own logic being bug-free.

The 35 thin S3 API wrappers are deliberately structured so each is a single SDK call with centralised error classification and pinned-allowlist tests in `src/storage/s3/api.rs`, hand-written JSON output covered by 83 unit-test annotations, and (for the JSON-consuming `put-*` subcommands) hand-written JSON input covered by 64 unit-test annotations and per-subcommand invalid-JSON e2e assertions added in v1.0.0. The honest weak point is the absence of interactive guard rails on the destructive subcommands; the single-resource scope keeps the worst-case blast radius bounded but does not prevent a typo from acting immediately. `create-bucket --tagging` is the one thin wrapper that issues two API calls, and its partial-state outcome is surfaced as exit 3 with a recovery hint rather than rolled back automatically.

This assessment does not guarantee the absence of bugs. It does establish that the categories of incorrect behaviour with the highest blast radius — silent data corruption on transfer, mis-routed `NotFound` errors on read, and silent partial state on multi-step writes — are either prevented by design, detected at runtime, or surfaced as a non-zero exit code rather than passing silently. Operators should still treat destructive subcommands with the same caution they would apply to `aws s3api delete-*`, and should run live workloads against representative data before adopting `s3util` for production-critical pipelines.

</details>

### AI assessment of safety and correctness (by Codex)

<details>
<summary>Click to expand the full assessment</summary>

Assessment date: 2026-04-29.

Assessed workspace: branch `update/v1.0.0`, base commit `ca0907e`, with local `lcov.info` and `lcov.report` present in the workspace.

Evidence reviewed: `src/bin/s3util/`, `src/config/`, `src/storage/`, `src/transfer/`, `src/input/`, `src/output/`, `src/types/`, `tests/cli_*.rs`, `tests/e2e_*.rs`, `tests/common/mod.rs`, `Cargo.toml`, `Cargo.lock`, `deny.toml`, `.github/workflows/`, `lcov.info`, and `lcov.report`.

Limits of this assessment: it is a static and local-test review of the current repository state. It does not prove absence of defects. It does not include fuzzing, sanitizer runs, formal verification, penetration testing, or a live-AWS e2e run observed by Codex. The e2e source files show AWS-backed tests, but source inspection is weaker evidence than current passing run logs for those tests.

#### Subcommand Surface

- The CLI defines 37 subcommands in `src/config/args/mod.rs`.
- `cp` and `mv` use the transfer pipeline in `src/bin/s3util/cli/mod.rs` and `src/transfer/`.
- The other 35 subcommands dispatch to API-specific runtimes in `src/bin/s3util/cli/` and SDK wrappers in `src/storage/s3/api.rs`.
- The CLI has no recursive transfer mode, no glob expansion for S3 keys, and no multiple-source form for `cp` or `mv`.
- Each invocation operates on one object, one bucket, or one bucket subresource.

#### Argument Validation

- `src/config/args/value_parser/storage_path.rs` accepts `s3://` URLs, local paths, multi-region access point ARNs, and `-` for stdio. It rejects missing paths, malformed URLs, and non-`s3://` URLs unless they are Windows absolute paths.
- Object-level commands require a bucket and key. Bucket-level commands require a bucket-only path and reject paths containing keys. The per-command `bucket_name()` / `bucket_key()` tests under `src/config/args/` cover these shapes.
- `put-bucket-versioning` requires exactly one of `--enabled` or `--suspended`; the argument tests cover missing and conflicting state flags.
- `cp` and `mv` reject both-local, both-stdio, local-to-stdio, and stdio-to-local transfer directions in `src/transfer/mod.rs`.
- `cp` and `mv` reject local directory sources before starting the transfer pipeline in `src/bin/s3util/cli/mod.rs`.
- The resolved destination is printed by `src/bin/s3util/cli/indicator.rs` when a basename is appended to a bucket root, S3 prefix ending in `/`, or local directory target.
- Downloads require the local target parent directory to exist; `src/storage/local/fs_util.rs` returns an error instead of creating missing parents.
- Upload paths pass `--if-none-match` through to `PutObject` or `CompleteMultipartUpload` in `src/storage/s3/upload_manager.rs`.

#### Transfer Correctness

- Local-file and stdin uploads compute a source ETag and, when configured, an additional checksum before or during upload. The result is compared with S3's returned value in `src/storage/e_tag_verify.rs`, `src/storage/additional_checksum_verify.rs`, and `src/storage/s3/upload_manager.rs`.
- A local/stdin checksum or ETag mismatch is returned as an error. `cp` maps that to exit code 1 through `src/bin/s3util/main.rs`.
- S3-to-S3 checksum and ETag mismatches are warnings when different multipart layouts can produce different composite values. `cp` maps warnings to exit code 3.
- `mv` deletes the source only after the copy phase returns success. `src/bin/s3util/cli/mv.rs` does not delete the source after a copy error, cancellation, or verification warning unless `--no-fail-on-verify-error` is set.
- Multipart uploads sort completed parts by part number before completion in `src/storage/s3/upload_manager.rs`.
- Multipart uploads compare uploaded byte count against the source size before completing in `src/storage/s3/upload_manager.rs`.
- Multipart error paths call `AbortMultipartUpload` on a best-effort basis in `src/storage/s3/upload_manager.rs`.
- `src/bin/s3util/cli/ctrl_c_handler.rs` maps SIGINT to the transfer cancellation token, and `run_copy_phase` installs that handler only for transfer commands. Unit tests cover token cancellation and a real SIGINT path.
- Checksum implementations for `CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, and `SHA256` are under `src/storage/checksum/`. `lcov.report` reports 100.00% region and line coverage for each algorithm file and for `storage/checksum/mod.rs`.

#### Live AWS E2E Evidence

- The repository contains 44 `tests/e2e_*.rs` integration-test files, and each file is gated by `#![cfg(e2e_test)]`.
- `tests/common/mod.rs` builds an AWS S3 client from profile `s3util-e2e-test` and uses S3 SDK operations including bucket creation, bucket deletion, object upload, `HeadObject`, `GetObject`, and object tagging.
- The e2e test source covers local-to-S3, S3-to-local, S3-to-S3, stdio transfers, multipart and checksum combinations, metadata and tagging, bucket subresources, cancellation, exit codes, public unsigned source access, and Express One Zone.
- The default local and CI `cargo test` commands do not run these files as live AWS tests, because they are gated behind `cfg(e2e_test)`.
- No current live-AWS e2e run log or CI artifact was present in the workspace reviewed for this assessment, so this assessment does not report a pass/fail result for that suite.

#### Thin S3 API Wrappers

- `src/storage/s3/api.rs` centralizes SDK calls for the non-transfer subcommands.
- `HeadError` distinguishes `BucketNotFound`, subresource `NotFound`, and `Other`.
- `classify_not_found` maps `NoSuchBucket` to `BucketNotFound` before checking per-subresource missing-code lists.
- Unit tests pin the missing-code lists for `get-object-tagging`, `get-bucket-policy`, `get-bucket-tagging`, `get-bucket-versioning`, `get-bucket-lifecycle-configuration`, `get-bucket-encryption`, `get-bucket-cors`, `get-public-access-block`, `get-bucket-website`, `get-bucket-logging`, and `get-bucket-notification-configuration`.
- `src/output/json.rs` manually serializes SDK response types for `head-*` and `get-*` commands; the file contains unit tests for field names, omitted fields, empty responses, and policy-only output.
- `src/input/json.rs` parses JSON for `put-bucket-cors`, `put-bucket-encryption`, `put-bucket-lifecycle-configuration`, `put-bucket-logging`, `put-bucket-notification-configuration`, `put-bucket-website`, and `put-public-access-block` through mirror structs before converting to SDK types.
- `put-bucket-policy` reads the policy body from a file or stdin and forwards it verbatim. It does not perform client-side policy schema validation.
- `create-bucket --tagging` is the only reviewed non-transfer runtime that performs two S3 API calls. If `CreateBucket` succeeds and `PutBucketTagging` fails, the command returns warning status and logs that the bucket exists untagged. It does not roll back the bucket.

#### Secrets And Dependency Controls

- `AccessKeys`, `SseKmsKeyId`, and `SseCustomerKey` in `src/types/mod.rs` derive zeroization traits and redact secret material in `Debug` output.
- `trace_config_summary` in `src/bin/s3util/main.rs` logs selected non-sensitive configuration fields instead of formatting the whole `Config` value.
- The only `unsafe` occurrences found under `src/` are inside test modules that mutate environment variables.
- `Cargo.toml` disables default features for `aws-config` and `aws-sdk-s3`, then enables the SDK HTTPS client feature set. `Cargo.lock` resolves the TLS stack to `rustls 0.23.39`, `rustls-webpki 0.103.13`, and `ring 0.17.14`.
- `deny.toml` rejects unknown registries and unknown git sources. It has no ignored advisories.
- Direct dependency requirements in `Cargo.toml` use explicit version requirements rather than wildcard requirements. `Cargo.lock` fixes the resolved dependency graph.

#### Coverage Findings

- Overall coverage in `lcov.report`: 96.69% regions, 95.45% functions, and 97.55% lines.
- Transfer-related coverage: `transfer/mod.rs` and `transfer/progress.rs` are at 100.00% region coverage; `transfer/s3_to_local.rs` is at 99.05%; `transfer/s3_to_s3.rs` is at 97.83%; `transfer/stdio_to_s3.rs` is at 94.85%; `transfer/stdio_to_s3.rs` has the lowest line coverage in the transfer group at 92.23%.
- Verification coverage: `storage/e_tag_verify.rs` is at 98.82% regions and 99.52% lines; `storage/additional_checksum_verify.rs` is at 98.59% regions and 99.43% lines.
- Multipart manager coverage: `storage/s3/upload_manager.rs` is at 95.50% regions and 94.40% lines.
- API wrapper coverage: `storage/s3/api.rs` is at 94.59% regions and 94.44% lines.
- JSON coverage: `output/json.rs` is at 99.46% regions and 99.84% lines; `input/json.rs` is at 98.04% regions and 99.64% lines.
- Lowest region coverage in `lcov.report` is in small CLI/runtime files: `bin/s3util/cli/create_bucket.rs` at 76.74%, `bin/s3util/cli/delete_bucket_policy.rs` at 78.95%, `bin/s3util/cli/get_bucket_logging.rs` at 81.08%, and `bin/s3util/cli/get_bucket_notification_configuration.rs` at 81.08%.
- Branch coverage is not measured by the report.

#### Known Limits

- Destructive or replacement commands have no interactive confirmation, no `--dry-run`, and no required `--yes` flag. This applies to `mv` source deletion after a successful copy, `rm`, `delete-*` commands, tag replacement commands, policy/configuration `put-*` commands, and versioning state changes.
- The single-resource command shape limits each invocation to one addressed resource, but it does not prevent an operator from supplying the wrong bucket, key, or version ID.
- Live AWS behavior is represented by gated e2e test source files in this assessment, not by an observed current e2e pass. Default `cargo test` reports 0 tests for the `tests/e2e_*.rs` binaries unless `cfg(e2e_test)` is enabled.
- S3-compatible storage support is best-effort. Error classification depends on service error codes matching the codes listed in `src/storage/s3/api.rs`; unknown codes fall through to generic errors.
- Production code contains `panic!()` branches for unexpected checksum algorithms, unexpected storage path variants, and impossible object-part states. Several are covered by panic tests, but reaching them in a release binary would terminate the process instead of returning a structured error.
- `put-bucket-policy` does not parse or validate the policy document before sending it to S3.
- `create-bucket --tagging` is not transactional.
- The review did not include fuzzing of path, metadata, tagging, or JSON parsers.
- The review did not include sanitizer or Miri runs of transfer code.
- The review did not include a formal threat model.

#### Assessment Result

Transfer reliability has the strongest evidence in the reviewed codebase. The implementation contains runtime checks for the main data-corruption cases visible in the code: source-side checksum/ETag calculation, S3-returned checksum/ETag comparison, multipart byte-count comparison, ordered multipart completion, cancellation handling, and best-effort multipart abort on failures. The local coverage report exercises these paths at the percentages listed above, and the gated e2e source exercises the same category of behavior against AWS when `cfg(e2e_test)` is enabled. This is evidence of active correctness controls, not proof that all transfer bugs are excluded.

Non-transfer S3 API wrapper reliability has moderate evidence. The wrappers are narrow and mostly map to one SDK call each. Their main correctness controls are argument validation, centralized missing-resource classification, manual JSON input/output tests, and explicit warning behavior for the one two-step operation (`create-bucket --tagging`). The reviewed tests support command-shape, serialization, and error-classification behavior, but they do not make replacement or destructive operations reversible.

Operator-error prevention has limited evidence. The command parser rejects malformed resource shapes and unsupported transfer directions, but a correctly parsed wrong bucket, key, version ID, policy file, or configuration file is still accepted. Destructive and replacement commands do not require confirmation or dry-run review.

Based on the reviewed repository state, the reliability conclusion is that the implementation has concrete safeguards and tests for the highest-impact transfer correctness risks, and it contains a live-AWS e2e suite that is outside the default test path. The assessment cannot state that the latest live-AWS e2e run passed without run artifacts, that all bugs are excluded, or that destructive commands protect against a correctly parsed but unintended target.

</details>

### AI assessment of safety and correctness (by Gemini)

<details>
<summary>Click to expand the full assessment</summary>

Assessment date: 2026-04-29.

Scope: The `src/` and `tests/` directories, `Cargo.toml`, `Cargo.lock`, `deny.toml`, `.github/workflows/`, `lcov.info`, and `lcov.report`. The assessment relies on static source analysis and unit-test coverage data.

#### 1. Data Integrity and Transfer Reliability

The core functionality of moving data is highly reliable. The transfer engine prevents silent data corruption by enforcing strict post-transfer verification.
- **Checksum Coverage:** Implementations for all supported checksum algorithms (`CRC32`, `CRC32C`, `CRC64NVME`, `SHA1`, and `SHA256`) have 100.00% region and line coverage. 
- **Verification Logic:** The routines responsible for verifying these checksums and ETags against S3's responses (`storage/e_tag_verify.rs` and `storage/additional_checksum_verify.rs`) are tested exhaustively, maintaining >98.5% region coverage and >99.4% line coverage.
- **Conclusion:** A successful exit code from a `cp` or `mv` operation reliably indicates that the data was transferred and its integrity was cryptographically verified. Mismatches are correctly mapped to hard errors (or warnings for expected S3-to-S3 layout differences).

#### 2. Subcommand and API Wrapper Predictability

The execution of specific S3 API calls operates predictably, but delegates schema authority to AWS.
- **Centralized Logic:** The `storage/s3/api.rs` module routes all raw S3 errors into structured `NotFound` types with 94.44% line coverage, ensuring consistent error exit codes.
- **Data Serialization:** Manual JSON parsing and serialization (`input/json.rs` and `output/json.rs`) exhibit >99.6% line coverage, proving comprehensive unit testing of the expected payload shapes.
- **Conclusion:** Operations will format requests correctly and parse responses consistently. However, operations like `put-bucket-policy` send files directly without client-side schema validation, meaning invalid configurations will be rejected by S3 rather than caught locally.

#### 3. Safety Risks and Operator Error

The tool prioritizes immediate execution over operator safety rails.
- **No Interactive Guardrails:** Destructive operations (such as `rm`, `delete-bucket`, and resource removals) process immediately upon a successful argument parse. There is no `--dry-run` or explicit confirmation step.
- **Scope Containment:** While the lack of recursive operations naturally limits the blast radius of a single command to one object or bucket, an operator typo in the target path will immediately affect the wrong resource.
- **Conclusion:** The binary is unsafe against operator errors. Users must independently verify targets before executing any destructive subcommand.

#### 4. Testing Limitations

The validation of the software relies heavily on isolated unit testing rather than continuous real-world integration.
- **E2E Test Exclusion:** The 44 `tests/e2e_*.rs` integration test files are gated behind a `#[cfg(e2e_test)]` flag and are not executed in the standard GitHub Actions CI pipeline.
- **Conclusion:** While unit tests comprehensively cover the codebase (97.55% overall line coverage), pull requests and standard builds do not empirically verify network behavior against live AWS infrastructure. Production reliability depends on manual maintainer validation of the E2E suite.

#### Overall Conclusion

`s3util` is technically reliable for data transfer, with verifiable, exhaustively tested cryptographic integrity checks that prevent silent data corruption. However, its operational safety is low regarding human error; the absence of dry-runs and interactive confirmations means it requires high operator caution when modifying or deleting resources.

</details>

## License

This project is licensed under the Apache-2.0 License.
