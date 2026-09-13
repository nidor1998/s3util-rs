# s3util

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![codecov](https://codecov.io/gh/nidor1998/s3util-rs/graph/badge.svg)](https://codecov.io/gh/nidor1998/s3util-rs)

> **Note on issues:** This project continues to be maintained, and binaries will keep being released. However, to consolidate discussion across the [s3sync](https://github.com/nidor1998/s3sync) / [s3util-rs](https://github.com/nidor1998/s3util-rs) / [s3rm-rs](https://github.com/nidor1998/s3rm-rs) / [s3ls-rs](https://github.com/nidor1998/s3ls-rs) family, **please file new issues in the [s7cmd](https://github.com/nidor1998/s7cmd) repository** instead of here. [s7cmd](https://github.com/nidor1998/s7cmd) bundles these tools as subcommands built on the same underlying code, so its behavior matches the standalone binaries and it can be used in their place. **Before opening an issue, please read the Scope and Non-Goals sections in the READMEs of [s7cmd](https://github.com/nidor1998/s7cmd) and each project ([s3sync](https://github.com/nidor1998/s3sync) / [s3util-rs](https://github.com/nidor1998/s3util-rs) / [s3rm-rs](https://github.com/nidor1998/s3rm-rs) / [s3ls-rs](https://github.com/nidor1998/s3ls-rs))** — requests outside the documented scope will generally be declined. Existing issues in this repository will continue to be handled as usual.

## Tools for managing Amazon S3 objects and buckets

`s3util` is a collection of tools for managing objects and buckets on Amazon S3, built on the official [AWS SDK for Rust](https://github.com/awslabs/aws-sdk-rust) (`aws-sdk-s3`). It ports the transfer, verification, and multipart semantics of [s3sync](https://github.com/nidor1998/s3sync) into a compact CLI focused on interactive and scripted use, and is intended to become part of the future [`s7cmd`](https://github.com/nidor1998/s7cmd) toolkit.

## Table of contents

<details>
<summary>Click to expand to view table of contents</summary>

- [Overview](#overview)
    * [API call volume](#api-call-volume)
- [Features](#features)
    * [Verifiable transfers](#verifiable-transfers)
    * [Full multipart support](#full-multipart-support)
    * [All transfer directions](#all-transfer-directions)
    * [Server-side copy](#server-side-copy)
    * [stdin/stdout streaming](#stdinstdout-streaming)
    * [Express One Zone support](#express-one-zone-support)
    * [SSE and SSE-C](#sse-and-sse-c)
    * [Metadata and tagging preservation](#metadata-and-tagging-preservation)
    * [Object annotation sync](#object-annotation-sync)
    * [Rate limiting](#rate-limiting)
    * [Observability](#observability)
    * [Dry-run](#dry-run)
- [Requirements](#requirements)
- [Installation](#installation)
    * [Install from crates.io](#install-from-cratesio)
- [Usage](#usage)
    * [Upload a local file](#upload-a-local-file)
    * [Download to local](#download-to-local)
    * [S3 → S3 copy](#s3--s3-copy)
    * [S3 → S3 copy with object annotations](#s3--s3-copy-with-object-annotations)
    * [stdin → S3](#stdin--s3)
    * [S3 → stdout](#s3--stdout)
    * [Move with mv](#move-with-mv)
    * [Rename within Express One Zone](#rename-within-express-one-zone)
    * [presign](#presign)
    * [put-object-annotation](#put-object-annotation)
    * [get-object-annotation](#get-object-annotation)
    * [list-object-annotations](#list-object-annotations)
    * [delete-object-annotation](#delete-object-annotation)
    * [Additional checksum verification](#additional-checksum-verification)
    * [Multipart tuning](#multipart-tuning)
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
    * [--dry-run](#--dry-run)
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
- [Security assumptions](#security-assumptions)
- [Scope](#scope)
- [Non-Goals](#non-goals)
- [License](#license)

</details>

## Overview

`s3util` is a collection of tools for managing objects and buckets on Amazon S3, built as a companion to [s3sync](https://github.com/nidor1998/s3sync). Where `s3sync` is optimized for bulk, recursive synchronization, `s3util` is optimized for single-object transfers and direct S3 API operations: each invocation operates on one object or one bucket, verifies the result where applicable, and exits with a meaningful status code.

`s3util`'s `cp` and `mv` subcommands follow the same design principles as `s3sync` for transfer, verification, and multipart handling — but each subcommand has a deliberately narrow surface, and the binary is a single file with no recursive/directory mode.

For object transfers in particular, `s3util` emphasizes high reliability, high performance, and advanced functionality: end-to-end checksum verification (ETag plus SHA256/SHA1/CRC32/CRC32C/CRC64NVME, composite or full-object), parallel multipart uploads and downloads, server-side copy, SSE-KMS and SSE-C (including SSE-C re-keying across copies), stdin/stdout streaming, tag and metadata preservation, rate-limited bandwidth control, and Express One Zone support. See [Features](#features) for the full list.


### API call volume

`cp` and `mv` are optimized for parallel transfer and end-to-end
verification, not for minimizing the number of S3 API calls per
invocation. A single transfer can issue several requests beyond the
obvious `GetObject` / `PutObject`:

- `HeadObject` upfront (to discover size and the source's stored
  ETag / additional checksum).
- `GetObjectAttributes` and/or per-part `HeadObject` calls when
  `--auto-chunksize` is set (one per source part, to align local
  chunking with the source's part layout).
- Up to `--max-parallel-uploads` concurrent ranged `GetObject`
  requests on the download side, or `UploadPart` / `UploadPartCopy`
  requests on the upload / server-side-copy side.
- `CreateMultipartUpload` + `CompleteMultipartUpload` (plus
  `AbortMultipartUpload` on cancellation) on multipart uploads.
- A second `HeadObject` after a ranged GET when the object is a
  composite-multipart source whose root composite checksum was
  needed for verification.

If keeping the API call count to a minimum is important to you (cost,
rate limits, or audit-log volume), `s3util` is not the right tool —
prefer the AWS CLI's `aws s3api put-object` / `get-object` for direct,
single-call transfers without the verification and parallelism
machinery.

### Subcommands

`s3util` provides the following subcommands. `cp` and `mv` perform single-object transfers using the full multipart and verification pipeline; the remaining subcommands are thin wrappers around individual S3 API calls with a simpler, script-friendly interface than `aws s3api`.

| Subcommand               | What it does                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------|
| `cp`                     | Copies a single object: Local↔S3, S3↔S3, or stdin/stdout streaming; full multipart + checksum verification |
| `mv`                     | Moves a single object: same as `cp` plus deletes the source after a successful, verified copy (no stdio) |
| `rename`                 | Atomically renames an object within the same S3 Express One Zone directory bucket using the `RenameObject` API; source and target must be in the same bucket (name must end with `--x-s3`); supports conditional checks (`--source-if-match <ETAG>`, `--source-if-none-match <ETAG>`, `--target-if-match <ETAG>`, `--target-if-none-match <ETAG>`) and `--dry-run`; exits 1 on source/bucket not found |
| `rm`                     | Deletes a single S3 object; silent on success; supports `--source-version-id`                 |
| `head-object`            | Prints `HeadObject` response as JSON; supports `--source-version-id` and SSE-C reads          |
| `put-object-tagging`     | Replaces all tags from `--tagging "k=v&k2=v2"`; silent; supports `--source-version-id`       |
| `get-object-tagging`     | Prints object tags as JSON (`{"TagSet": [...], "VersionId": "..."}`); supports `--source-version-id` |
| `delete-object-tagging`  | Removes all tags from an object; silent; supports `--source-version-id`                       |
| `create-bucket`          | Creates a bucket; LocationConstraint from the SDK client's resolved region (`--target-region`, `AWS_REGION`, or profile); optional `--tagging`; `--bucket-namespace account-regional` + `--create-bucket-configuration LocationConstraint=<region>` for account-level regional buckets; exit 3 if tagging step fails after create |
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
| `put-bucket-lifecycle-configuration`     | Sets lifecycle configuration from a JSON file path or `-` (stdin); `--transition-default-minimum-object-size` (`varies_by_storage_class` / `all_storage_classes_128K`) sets the small-object transition cutoff, which S3 accepts only as a request parameter, not in the JSON; silent on success |
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
| `put-bucket-replication`                 | Sets replication configuration from a JSON file path or `-` (stdin); silent on success |
| `get-bucket-replication`                 | Prints replication configuration as JSON; exits 4 if no replication rules are set |
| `delete-bucket-replication`              | Removes replication configuration; silent on success                                |
| `put-bucket-accelerate-configuration`    | Enables or suspends Transfer Acceleration (`--enabled` / `--suspended`, mutually exclusive); silent |
| `get-bucket-accelerate-configuration`    | Prints Transfer Acceleration state as JSON (`{"Status": "Enabled"}`); silent when never configured |
| `put-bucket-request-payment`             | Sets request-payment payer (`--requester` / `--bucket-owner`, mutually exclusive); silent |
| `get-bucket-request-payment`             | Prints request-payment configuration as JSON (`{"Payer": "BucketOwner"}`)          |
| `get-bucket-policy-status`               | Prints policy status as JSON (`{"PolicyStatus": {"IsPublic": …}}`); exits 4 if no policy is attached |
| `restore-object`                         | Restores an archived object; takes `--days` and `--tier` (Standard / Bulk / Expedited); exits 4 on `NoSuchBucket` / `NoSuchKey` / `NoSuchVersion` |
| `presign`                                | Generates a pre-signed `GetObject` URL (GET only) and prints it to stdout; signed locally with no S3 API call, so bucket/object existence is not verified; `--expires-in <SECONDS>` sets validity (default 3600, max 604800 = 7 days) |
| `put-object-annotation`                  | Attaches a named annotation payload (file or `-` stdin, 1 byte–1 MiB, UTF-8) to an S3 object via `PutObjectAnnotation`; sends `Content-MD5` for transit integrity and an explicit CRC64NVME checksum; verifies the CRC64NVME returned by S3; prints response as JSON; supports `--annotation-name`, `--annotation-payload`, `--target-version-id`, `--target-request-payer`, `--dry-run`; exits 4 on not-found, 1 on verification mismatch or other error |
| `get-object-annotation`                  | Retrieves a named annotation payload from an S3 object via `GetObjectAnnotation` and writes it to a file or stdout; when writing to a file, verifies payload integrity (ETag/MD5 for AES256-encrypted objects, plus any additional checksum it can recompute — an unsupported algorithm such as SHA512 is treated as an error; content-length mismatch is an error; if neither verification applies, logs warning but exits 0); writes file atomically (temp file + rename), then re-reads the saved file and recomputes its ETag/additional checksum from disk (like `cp`) — a post-write mismatch leaves the file in place and exits 1; supports `--annotation-name`, `--target-version-id`, `--target-request-payer`; exits 4 on not-found, 1 on verification mismatch or other error |
| `list-object-annotations`                | Lists annotations on an S3 object via `ListObjectAnnotations`; makes a single request (up to 1000 results, pagination is not followed); supports `--annotation-prefix`, `--target-version-id`; outputs AWS-CLI-shape JSON on stdout; exits 4 on not-found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`), 1 on other error |
| `delete-object-annotation`               | Removes a named annotation from an S3 object via `DeleteObjectAnnotation`; supports `--annotation-name` (required), `--target-version-id`, `--target-request-payer`, `--dry-run`; produces no output on success; exits 4 on not-found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`), 1 on other error |

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
- Supports objects up to Amazon S3's [per-object size limit](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingObjects.html) (currently 50 TB).

### All transfer directions

Transfer direction is inferred automatically from the source/target combination:

| Source        | Target        | Direction     |
|---------------|---------------|---------------|
| local path    | `s3://…`      | Local → S3    |
| `s3://…`      | local path    | S3 → Local    |
| `s3://…`      | `s3://…`      | S3 → S3       |
| `-` (stdin)   | `s3://…`      | stdin → S3    |
| `s3://…`      | `-` (stdout)  | S3 → stdout   |

S3 → S3 transfers can span **different AWS accounts** and **different regions**. The source and target are independently configured via the paired `--source-*` and `--target-*` credential, profile, region, and endpoint flags — they need not share a single S3 endpoint.

### Server-side copy

By default, S3→S3 transfers are **client-side**: `s3util` streams the object from the source through the local process and re-uploads it to the target. This is the most compatible mode — it works across different regions, endpoints, and accounts, and is required for any transfer that crosses a boundary `CopyObject` cannot.

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

### Object annotation sync

For S3 → S3 copies, `cp` and `mv` can carry an object's annotations along
with the object. Pass `--enable-sync-object-annotations` to copy the source
object's annotations to the target after the object itself is written.
Because the sync runs against the just-written target object, which has no
annotations yet, every annotation on the source object is copied.
Note that copying annotations requires additional API calls.
With `--dry-run`, each annotation that a real run would copy is displayed
(`[dry-run] would copy object annotation.`) using a read-only
`ListObjectAnnotations` call on the source object.

Annotations are synced in parallel (`--max-parallel-uploads`). With
`--server-side-copy`, Amazon S3 copies the annotations of single-part copies
entirely within Amazon S3; however, S3's `UploadPartCopy` API (used for
multipart server-side copies) does not copy annotations, so
`--enable-sync-object-annotations` is still needed to carry the annotations
of multipart objects.

Before copying, s3util compares the source and target annotation lists with
the same logic as s3sync (annotation ETags where possible, falling back to
the annotation's Last-Modified timestamp); `--disable-check-annotation-etag`
skips the ETag comparison (for example, when SSE-KMS encryption makes ETags
incomparable). Against a freshly written target object this comparison
classifies every source annotation as new, so the flag is provided for
s3sync compatibility.

If creating, updating, or deleting an annotation fails, the transfer is
treated as a failure; `mv` then leaves the source object in place.
Both storages must be S3, and S3 Express One Zone is not supported.

### Rate limiting

`--rate-limit-bandwidth <BYTES_PER_SEC>` caps throughput using a leaky-bucket algorithm. Accepts unit-suffixed values like `50MB`, `100MiB`, `1GB`.

### Observability

- Optional progress bar (`--show-progress`) using [indicatif](https://docs.rs/indicatif).
- Structured JSON tracing (`--json-tracing`) for log aggregation systems.
- AWS SDK tracing (`--aws-sdk-tracing`) for deep troubleshooting.
- Configurable verbosity (`-v`/`-vv`/`-vvv`, `-q`/`-qq`).

### Dry-run

`--dry-run` is supported on every mutating subcommand — `cp`, `mv`, `rename`, `rm`, `create-bucket`, `restore-object`, every `put-*`, and every `delete-*`. With `--dry-run`, `s3util` runs all preflight work (argument validation, JSON-config parsing, SDK client construction, transfer-pipeline setup), stops just before the destructive Web API call, and emits an info-level log line prefixed with `[dry-run]` (e.g. `[dry-run] would delete bucket.`, `[dry-run] would copy object.`, `[dry-run] Transfer completed.`). The exit status is `0` on success.

To make the message visible at the default `WarnLevel`, `--dry-run` automatically raises the verbosity floor to **info**. Levels you've explicitly raised (`-vv` for debug, `-vvv` for trace) are preserved unchanged; lower levels (`-q`, even full silence with `-qqq`) are still bumped to info so the line stays visible.

> **Important:** `--dry-run` performs only a **formal check**. It validates flags, parses config-file JSON, and confirms the binary can reach the point where the API call would have been issued. It does **not** verify any AWS-side state — it does not check whether the source object or bucket exists, whether your credentials are accepted, whether the target endpoint is reachable, or whether your IAM policy would have allowed the operation. A successful dry-run means "the request was well-formed and ready to send", not "the operation would have succeeded against AWS".

`get-*` and `head-*` subcommands are read-only and do **not** expose `--dry-run`.

## Requirements

- x86_64 Linux (kernel 3.2 or later)
- ARM64 Linux (kernel 4.1 or later)
- Windows 11 (x86_64, aarch64)
- macOS 11.0 or later (aarch64, x86_64)

`s3util` is written in Rust and requires Rust **1.94.1 or later** to build from source.

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

Client-side S3 → S3 copies can span different AWS accounts and different regions — point the `--source-*` and `--target-*` flags at independent endpoints:

```bash
# Cross-account, cross-region (separate profiles, separate regions)
s3util cp \
  --source-profile prod --source-region us-east-1 \
  --target-profile dev  --target-region us-west-2 \
  s3://prod-bucket/key s3://dev-bucket/key
```

`--server-side-copy` is incompatible with this case (it requires source and target to be reachable from a single S3 endpoint); cross-endpoint copies always run client-side.

### S3 → S3 copy with object annotations

```bash
# Copy an object together with its annotations
s3util cp --enable-sync-object-annotations s3://source-bucket/key s3://target-bucket/key

# Same, but don't use annotation ETags to detect changes
s3util cp --enable-sync-object-annotations --disable-check-annotation-etag \
  s3://source-bucket/key s3://target-bucket/key
```

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

### Rename within Express One Zone

`rename` uses the S3 `RenameObject` API to atomically move an object within the same [Express One Zone](https://aws.amazon.com/s3/storage-classes/express-one-zone/) directory bucket. Both source and target must be in the same bucket, and the bucket name must end with `--x-s3`.

```bash
# Basic rename
s3util rename s3://my-bucket--apne1-az4--x-s3/old-key s3://my-bucket--apne1-az4--x-s3/new-key
```

`rename` supports optional conditional checks to implement optimistic-concurrency semantics:

```bash
# Rename only if the source ETag matches (optimistic update)
s3util rename \
  --source-if-match '"d41d8cd98f00b204e9800998ecf8427e"' \
  s3://my-bucket--apne1-az4--x-s3/old-key s3://my-bucket--apne1-az4--x-s3/new-key

# Rename only if the source ETag does not match (proceed when source has changed)
s3util rename \
  --source-if-none-match '"d41d8cd98f00b204e9800998ecf8427e"' \
  s3://my-bucket--apne1-az4--x-s3/old-key s3://my-bucket--apne1-az4--x-s3/new-key

# Rename only if the destination ETag does not match (skip if destination already has this content)
s3util rename \
  --target-if-none-match '"d41d8cd98f00b204e9800998ecf8427e"' \
  s3://my-bucket--apne1-az4--x-s3/old-key s3://my-bucket--apne1-az4--x-s3/new-key

# Preview without performing the rename
s3util rename --dry-run \
  s3://my-bucket--apne1-az4--x-s3/old-key s3://my-bucket--apne1-az4--x-s3/new-key
```

Conditional check flags:

| Flag | Type | Effect |
|------|------|--------|
| `--source-if-match <ETAG>` | String | Proceed only if the source ETag matches the given value |
| `--source-if-none-match <ETAG>` | String | Proceed only if the source ETag does not match the given value |
| `--target-if-match <ETAG>` | String | Proceed only if the destination ETag matches the given value |
| `--target-if-none-match <ETAG>` | String | Proceed only if the destination ETag does not match the given value |

Exit codes: `0` on success, `1` on error (including source/bucket not found), `2` on argument-parse failure, `130` on SIGINT. Note that `rename` always exits `1` (not `4`) when the source object or bucket is not found — unlike `restore-object` and the `get-*` / `head-*` subcommands.

### presign

`presign` generates a [pre-signed URL](https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html) for a `GetObject` request and prints it to stdout. The URL grants temporary, credential-free read access to a single object: anyone holding it can download that object until it expires. The URL is signed locally from the resolved credentials — **no S3 API call is made** — so the bucket and key are not checked for existence, and signing succeeds even if the object does not exist.

```bash
# Pre-sign with the default 1-hour expiry (3600 seconds)
s3util presign s3://my-bucket/report.pdf

# Pre-sign with a custom expiry (seconds; max 604800 = 7 days)
s3util presign s3://my-bucket/report.pdf --expires-in 86400
```

Options:

| Flag | Type | Description |
|------|------|-------------|
| `--expires-in <SECONDS>` | Integer | Seconds until the URL expires. Default `3600`; must be greater than `0` and at most `604800` (7 days, S3's maximum for a SigV4 pre-signed URL) |

With `--target-request-payer`, `x-amz-request-payer` becomes part of the URL's signature (`X-Amz-SignedHeaders=host;x-amz-request-payer`), because S3 requires the header on every request to a Requester Pays bucket and SigV4 requires any `x-amz-*` header to be signed. Whoever uses the URL must therefore send that header as well — `curl -H 'x-amz-request-payer: requester' '<URL>'` — otherwise S3 rejects the request with `SignatureDoesNotMatch`. Without the flag, the URL is plain but a Requester Pays bucket answers `403`.

Common options (e.g. `--target-region`, `--target-access-key`, `--target-profile`) apply the same way as for other subcommands. `presign` covers `GetObject` only — it does not pre-sign uploads or any other operation. If the signing credentials are temporary (for example STS or SSO session credentials), the URL stops working once those credentials expire, even if `--expires-in` has not elapsed. Because no request is sent to S3, `presign` has no `--dry-run` and never returns the not-found exit code `4`: it exits `0` once the URL is printed, or `1` / `2` on a signing or argument error.

### put-object-annotation

`put-object-annotation` attaches a named annotation payload to an S3 object via the `PutObjectAnnotation` API. The payload is read from a file path, or from stdin when `--annotation-payload -` is given.

```bash
# Attach an annotation from a file
s3util put-object-annotation s3://my-bucket/my-object \
  --annotation-name my-annotation \
  --annotation-payload ./annotation.json

# Attach an annotation from stdin
echo '{"note": "approved"}' | s3util put-object-annotation s3://my-bucket/my-object \
  --annotation-name review \
  --annotation-payload -

# Target a specific object version
s3util put-object-annotation s3://my-bucket/my-object \
  --annotation-name my-annotation \
  --annotation-payload ./annotation.json \
  --target-version-id <VERSION_ID>

# Preview without making the API call
s3util put-object-annotation --dry-run s3://my-bucket/my-object \
  --annotation-name my-annotation \
  --annotation-payload ./annotation.json
```

Options:

| Flag | Type | Description |
|------|------|-------------|
| `--annotation-name <NAME>` | String | Name of the annotation to attach |
| `--annotation-payload <PATH\|->` | Path\|- | File to read the payload from; `-` reads from stdin |
| `--target-version-id <ID>` | String | Attach the annotation to a specific object version |
| `--target-request-payer` | Flag | Send `x-amz-request-payer: requester` on the API call |
| `--dry-run` | Flag | Log what would be sent and exit 0 without calling the API |

**Payload constraints:** The payload must be between 1 byte and 1 MiB (inclusive) and valid UTF-8; S3 enforces UTF-8 server-side and the size limit is enforced locally before any network call. Binary payloads must be Base64-encoded by the caller before passing them as the annotation payload.

**Verification:** `s3util` sends a `Content-MD5` header for transit integrity and an explicit CRC64NVME checksum on every `PutObjectAnnotation` call. After the call it compares the CRC64NVME value returned by S3 against the locally computed value; a mismatch is treated as an error (exit 1). The ETag is echoed in the JSON output but is not used for verification.

**SSE inheritance:** Annotation encryption inherits the parent object's server-side encryption. Objects with no SSE are annotated under SSE-S3. Objects encrypted with SSE-C cannot have annotations — `put-object-annotation` does not support SSE-C-encrypted objects.

**Output:** Prints the `PutObjectAnnotation` response as JSON on success. Fields present in the response are included; absent fields are omitted. Example fields: `Key`, `AnnotationName`, `ObjectVersionId`, `ETag`, `ChecksumCRC64NVME`, `ChecksumType`, `ServerSideEncryption`, `RequestCharged`.

Exit codes: `0` success; `4` bucket/object/version not found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`); `1` verification mismatch or other error.

### get-object-annotation

`get-object-annotation` retrieves a named annotation payload from an S3 object via the `GetObjectAnnotation` API and writes it to a file or stdout.

```bash
# Retrieve an annotation to a file
s3util get-object-annotation s3://my-bucket/my-object output.json \
  --annotation-name my-annotation

# Stream annotation payload to stdout
s3util get-object-annotation s3://my-bucket/my-object - \
  --annotation-name my-annotation

# Retrieve a specific object version's annotation
s3util get-object-annotation s3://my-bucket/my-object output.json \
  --annotation-name my-annotation \
  --target-version-id <VERSION_ID>
```

Options:

| Flag | Type | Description |
|------|------|-------------|
| `--annotation-name <NAME>` | String | Name of the annotation to retrieve |
| `--target-version-id <ID>` | String | Retrieve the annotation from a specific object version |
| `--target-request-payer` | Flag | Send `x-amz-request-payer: requester` on the API call |

**Output modes:** When `<OUTFILE>` is a file path, `s3util` writes the annotation payload to a temporary file, verifies it, and atomically renames it to the target location (overwriting any existing file). Only after verification passes does the file materialize on disk. When `<OUTFILE>` is `-`, the raw payload is written to stdout and JSON output is suppressed (integrity verification still runs).

**Verification:** `checksum-mode` is always `ENABLED`. The payload is integrity-checked as follows:
- If `content-length` is present and differs from the received byte count → error (exit 1).
- If the object is encrypted with `AES256`, the ETag (MD5) is verified against the payload.
- If S3 returns an additional checksum (e.g., `ChecksumCRC64NVME`), it is verified against the payload.
- If S3 returns a checksum whose algorithm `s3util` cannot recompute (e.g. `SHA512`, `MD5`, `XXHASH*`), it is treated as an integrity error (exit 1) rather than being skipped.
- If neither ETag nor additional checksum verification is applicable → a warning is logged but exit code remains 0.

**Output:** Prints the `GetObjectAnnotation` response as JSON on success (file mode only). Fields present in the response are included; absent fields are omitted. Example fields: `LastModified`, `ContentLength`, `ETag`, `ChecksumCRC64NVME`, `ChecksumType`, `ServerSideEncryption`, `ObjectVersionId`, `RequestCharged`.

Example JSON output:

```json
{
    "LastModified": "2026-06-18T23:04:08+00:00",
    "ContentLength": 678260,
    "ETag": "\"b373009fdd7e9a9a2b266c2044fb7948\"",
    "ChecksumCRC64NVME": "hdWdywUmntQ=",
    "ChecksumType": "FULL_OBJECT",
    "ServerSideEncryption": "AES256"
}
```

Exit codes: `0` success; `4` bucket/object/version not found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`); `1` verification mismatch or other error.

### list-object-annotations

`list-object-annotations` lists the annotations attached to an S3 object via the `ListObjectAnnotations` API and prints the result as JSON on stdout.

```bash
# List all annotations on an object
s3util list-object-annotations s3://my-bucket/my-object

# List annotations whose name starts with a given prefix
s3util list-object-annotations s3://my-bucket/my-object \
  --annotation-prefix my-prefix

# List annotations for a specific object version
s3util list-object-annotations s3://my-bucket/my-object \
  --target-version-id <VERSION_ID>
```

Options:

| Flag | Type | Description |
|------|------|-------------|
| `--annotation-prefix <VALUE>` | String | Only list annotations whose name starts with this prefix |
| `--target-version-id <VALUE>` | String | List annotations for a specific object version |

Common options (e.g. `--target-region`, `--target-access-key`, `--target-request-payer`, `--verbose`) apply the same way as for other subcommands.

**Behavior:** A single `ListObjectAnnotations` request is made with `max-annotation-results` fixed at 1000. Pagination (`NextContinuationToken`) is not followed. Output is written to stdout as AWS-CLI-shape JSON (there is no `<OUTFILE>` argument for this subcommand).

Example JSON output:

```json
{
    "Annotations": [
        {
            "AnnotationName": "myname",
            "LastModified": "2026-06-18T23:04:08+00:00",
            "ETag": "\"b373009fdd7e9a9a2b266c2044fb7948\"",
            "ChecksumAlgorithm": ["CRC64NVME"],
            "Size": 678260
        }
    ],
    "AnnotationCount": 1,
    "AnnotationPrefix": null,
    "Bucket": "data.cpp17.org",
    "Key": "hosts",
    "ObjectVersionId": null,
    "RequestCharged": null
}
```

Exit codes: `0` success; `4` bucket/object/version not found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`); `1` other error; `2` argument/usage error.

### delete-object-annotation

`delete-object-annotation` removes a named annotation from an S3 object via the `DeleteObjectAnnotation` API. On success, nothing is written to stdout.

```bash
# Delete an annotation by name
s3util delete-object-annotation s3://my-bucket/my-object \
  --annotation-name my-annotation

# Delete an annotation on a specific object version
s3util delete-object-annotation s3://my-bucket/my-object \
  --annotation-name my-annotation \
  --target-version-id <VERSION_ID>

# Preview what would be deleted without making any changes
s3util delete-object-annotation --dry-run s3://my-bucket/my-object \
  --annotation-name my-annotation
```

Options:

| Flag | Type | Description |
|------|------|-------------|
| `--annotation-name <VALUE>` | String (required) | Name of the annotation to delete |
| `--target-version-id <VALUE>` | String | Delete the annotation for a specific object version |
| `--dry-run` | Flag | Log what would be sent and exit 0 without calling the API |

Common options (e.g. `--target-region`, `--target-access-key`, `--target-request-payer`, `--verbose`) apply the same way as for other subcommands.

**Behavior:** A single `DeleteObjectAnnotation` request is made. Nothing is written to stdout on success.

Exit codes: `0` success; `4` object/bucket/version not found (`NoSuchBucket` / `NoSuchKey` / `NoSuchVersion`); `1` other error.

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

> ⚠️ **Memory warning for `--auto-chunksize` on client-side download paths.**
> On any path where `s3util` downloads parts itself instead of letting S3 copy them server-side — that is, **S3 → stdout**, **S3 → Local**, and **S3 → S3 *without* `--server-side-copy`** — `--auto-chunksize` instructs `s3util` to use the source object's actual per-part sizes as the download chunk size, instead of `--multipart-chunksize`. Each parallel worker allocates one chunk in memory while it downloads, so peak memory usage is approximately:
>
>     source's largest part size × --max-parallel-uploads
>
> If the source was uploaded with very large parts (for example, 1 GiB parts × 16 parallel workers ≈ 16 GiB), this can exhaust available memory. Either lower `--max-parallel-uploads` to bound the allocation, or omit `--auto-chunksize` and accept that the per-part composite ETag/checksum may not verify on the resulting download.
>
> `--server-side-copy` (S3 → S3 only) sidesteps this entirely: parts are copied server-to-server via `UploadPartCopy` and never materialize in `s3util`'s memory.

### Server-side copy detail

`--server-side-copy` uses `CopyObject` (single-part) or `UploadPartCopy` (multipart). Server-side copy is only valid when both source and target endpoints can see each other in the same AWS region/account (with appropriate cross-account IAM). It is not compatible with stdin or local paths. SSE-C re-keying across a server-side copy is supported by supplying both `--source-sse-c-*` and `--target-sse-c-*` flags.

### stdin/stdout handling

- **stdin → S3** reads up to `--multipart-threshold` bytes into an in-memory buffer. If EOF arrives first, the buffered bytes are issued as a single-part PUT with a correct `Content-Length`; otherwise the buffered prefix is chained with the rest of the stream and uploaded as a multipart.
- **S3 → stdout** streams bytes straight to stdout. ETag and any requested additional checksum are computed inline from the streamed bytes and verified against the S3-reported values — the same verification as `S3 → Local`. A mismatch is logged as a warning (exit 3), or as an error if the configured additional checksum is a full-object checksum. For large objects the download runs in parallel (up to `--max-parallel-uploads` concurrent ranged GETs); peak memory is roughly `--multipart-chunksize × --max-parallel-uploads`. With `--auto-chunksize` the chunk size becomes the source's actual per-part size — see the [Auto chunksize](#auto-chunksize) memory warning before using it on objects uploaded with very large parts.

### Express One Zone detail

Directory buckets (`--x-s3` suffix) are automatically detected. Some S3 features behave differently on Express One Zone (for example, default additional-checksum handling); `--disable-express-one-zone-additional-checksum` overrides `s3util`'s default if your bucket policy demands it.

`create-bucket` also accepts directory-bucket names. The zone ID is parsed from the name (`<base>--<zone-id>--x-s3`) and the appropriate `Location`/`Bucket` configuration is sent. The zone type is inferred from the zone-ID shape — at most one hyphen is treated as an Availability Zone (e.g. `apne1-az4`), two or more as a Local Zone (e.g. `usw2-lax1-az1`). The active region (`--target-region` / `AWS_REGION` / profile) must match the zone's region; otherwise S3 will reject the request.

### Account-level regional buckets

To create an account-level regional bucket, pass `--bucket-namespace account-regional` together with `--create-bucket-configuration LocationConstraint=<region>`. `account-regional` is the only accepted `--bucket-namespace` value, and `LocationConstraint=<region>` is the only accepted `--create-bucket-configuration` shorthand. The two options are used together — specifying one without the other is rejected. When both are given they are sent to `CreateBucket` verbatim, bypassing the region-derived and directory-bucket configuration described above; align the `LocationConstraint` region with the client's resolved region (`--target-region` / `AWS_REGION` / profile).

```bash
s3util create-bucket \
    --bucket-namespace account-regional \
    --create-bucket-configuration LocationConstraint=ap-northeast-1 \
    --target-region ap-northeast-1 \
    s3://mybucket2-11111111-ap-northeast-1-an
```

When `--bucket-namespace` is omitted, nothing new is sent and `create-bucket` behaves exactly as before.

### S3 Permissions

The permissions below cover the `cp` and `mv` subcommands. Other subcommands have their own requirements; refer to the [AWS documentation](https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-iam-policies.html) for the full set.

Required permissions depend on the transfer direction. "Source" and "target" below refer to the source and target S3 buckets; for Local↔S3 only the relevant side applies.

**Source bucket** (any `cp`/`mv` reading from S3):

- `s3:GetObject` — always. Covers `GetObject`, `HeadObject`, and `GetObjectAttributes`.
- `s3:GetObjectVersion` — when the source bucket has (or ever had) versioning enabled, or when `--source-version-id`
  is used. Reads are pinned to the version observed by the initial `HeadObject`, so the GETs carry a `versionId` and
  S3 authorizes them against this action; a policy granting only `s3:GetObject` gets `AccessDenied` on a versioned
  bucket.
- `s3:GetObjectTagging` — when source tags are read. This is the default on S3→S3; suppressed by `--disable-tagging`.
- `s3:GetObjectVersionTagging` — when the tag read addresses a pinned version (versioned source bucket or
  `--source-version-id`), same rule as `s3:GetObjectVersion`.
- `s3:DeleteObject` — when running `mv` (the source is deleted on success).
- `s3:DeleteObjectVersion` — when running `mv` from a versioned source bucket or with `--source-version-id`
  (`mv` deletes exactly the version it copied).

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
| 4    | Not found — `head-bucket` / `head-object` / `restore-object` / `put-object-annotation` / `get-object-annotation` / `list-object-annotations` / `delete-object-annotation` (404 NoSuchBucket / NoSuchKey / NoSuchVersion); `get-object-tagging` / `get-bucket-policy` / `get-bucket-policy-status` / `get-bucket-tagging` / `get-bucket-lifecycle-configuration` / `get-bucket-encryption` / `get-bucket-cors` / `get-public-access-block` / `get-bucket-website` / `get-bucket-replication` when the addressed resource is missing (incl. NoSuchBucketPolicy / NoSuchTagSet / NoSuchLifecycleConfiguration / ServerSideEncryptionConfigurationNotFoundError / NoSuchCORSConfiguration / NoSuchPublicAccessBlockConfiguration / NoSuchWebsiteConfiguration / ReplicationConfigurationNotFoundError); `get-bucket-versioning` / `get-bucket-logging` / `get-bucket-notification-configuration` / `get-bucket-accelerate-configuration` / `get-bucket-request-payment` only on `NoSuchBucket` — for these five, an unconfigured subresource is reported by S3 as a successful empty body (or, for request payment, a default value), which exits 0, not 4 |
| 101  | Abnormal termination (internal panic)                                                                               |
| 130  | User cancellation via SIGINT/ctrl-c (standard Unix SIGINT convention, 128 + 2)                                      |

## Advanced options

### --max-parallel-uploads

Number of parallel part uploads/downloads during multipart transfers. Default: `16`. The default is sized for typical hosts and S3's per-prefix request behavior; if you raise it, you must size it to your host (peak memory is roughly `--multipart-chunksize × --max-parallel-uploads`) and to the target service's per-prefix request and bandwidth limits. Tuning is the operator's responsibility.

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

### --dry-run

Skip the destructive S3 Web API call and emit a `[dry-run]`-prefixed info-level log line instead. Exit status is `0` on success. Available on every mutating subcommand (`cp`, `mv`, `rename`, `rm`, `create-bucket`, `restore-object`, all `put-*`, all `delete-*`). The verbosity floor is forced to info while `--dry-run` is set so the message stays visible at default verbosity. See [Dry-run](#dry-run) under Features for the full description and the important caveat that this is a formal check only — no AWS-side state is verified.

### -v / -q

`s3util` uses [tracing-subscriber](https://docs.rs/tracing-subscriber) for tracing. More occurrences of `-v` increase verbosity (`-v`: `info`, `-vv`: `debug`, `-vvv`: `trace`). Use `-q`, `-qq` to reduce verbosity. Default: warning and error messages.

With `-v`, subcommands that are otherwise silent on success (`rm`, `create-bucket`, `restore-object`, `delete-bucket`, the `put-*` and `delete-*` bucket/object subcommands (except `put-object-annotation`, which always prints JSON on success)) emit a structured info-level event to stderr describing what was changed (e.g. `Object deleted. bucket=… key=… version_id=…`). `get-bucket-versioning`, `get-bucket-logging`, `get-bucket-notification-configuration`, and `get-bucket-accelerate-configuration` likewise log `Bucket … not configured.` when the bucket has no such configuration (each prints nothing on stdout in that case, matching `aws s3api`, since the underlying S3 API returns success with an empty body for these four).

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

S3-compatible storage (MinIO, Wasabi, Cloudflare R2, Backblaze B2, Google Cloud Storage's S3 interop, and any other non-AWS implementation of the S3 API) is **not supported**. The code is provided as-is against such targets: it may work, it may not, and behaviour may change between releases without notice. Bug reports, feature requests, or compatibility fixes filed against non-AWS S3-compatible stores will not be accepted. Endpoint and path-style flags (`--target-endpoint-url`, `--target-force-path-style`, etc.) remain in the binary because they are also useful for AWS-internal scenarios (e.g. FIPS endpoints, VPC endpoints), but their presence is not an endorsement of S3-compatible-store usage.

`s3util` has been tested with Amazon S3, including Express One Zone directory buckets. `s3util` has many end-to-end tests and unit tests, and they run every time a new version is released. None of those tests run against non-AWS S3-compatible stores.

## Fully AI-generated (human-verified) software

Every line of source code, every test, all documentation, CI/CD configuration, and this README were generated by AI using [Claude Code](https://docs.anthropic.com/en/docs/claude-code/overview) (Anthropic).

Human engineers authored the requirements, design specifications, and the s3sync reference architecture. They thoroughly reviewed and verified the design, all source code, and all tests. Features of the binary have been manually tested against live AWS S3. The development followed a spec-driven process: requirements and design documents were written first, and the AI generated code to match those specifications under continuous human oversight.

### Quality verification (by AI self-assessment)

Measurements below are taken at v1.10.2 (commit `ba6aec2` on `main`, 2026-09-13). The coverage figures are sourced from `llvm-cov-report.txt` (`cargo llvm-cov`; `lcov.info` is the matching machine-readable LCOV artifact) and reflect a single combined run — `RUSTFLAGS="--cfg e2e_test" cargo llvm-cov --all-features` on the maintainer's machine, 2026-09-13 — so the unit tests, the CLI integration tests, and the live-AWS e2e suite are all included in the report.

| Metric                         | Value                                                         |
|--------------------------------|---------------------------------------------------------------|
| Production code                | ~59,900 lines of Rust across 166 source files in `src/`       |
| Unit tests (in `src/`)         | 1,611 `#[test]` / `#[tokio::test]` annotations                |
| CLI integration tests          | 485 annotations across 61 `tests/cli_*.rs` files (no network access; run in CI) |
| E2E integration tests          | 838 annotations across 58 `tests/e2e_*.rs` files (gated behind `--cfg e2e_test`; run only by the maintainer against live AWS) |
| Code coverage (llvm-cov, combined unit + CLI + e2e run) | 97.74% regions (1,201 / 53,100 missed), 96.15% functions (146 / 3,790 missed), 98.61% lines (516 / 37,189 missed) |
| Static analysis (clippy)       | 0 warnings (`cargo clippy --all-features`)                    |
| Formatting                     | 0 diffs (`cargo fmt --all --check`)                           |
| Supply chain (cargo-deny)      | Clean (`cargo deny -L error check`); runs per-PR in `ci.yml` and daily at 01:34 UTC in `cargo-deny.yml`; `advisories.ignore = []` |
| Code adapted from [s3sync](https://github.com/nidor1998/s3sync) | Transfer engine (`src/transfer/`), checksum verification (`src/storage/e_tag_verify.rs`, `src/storage/additional_checksum_verify.rs`), and multipart upload manager (`src/storage/s3/upload_manager.rs`) |

What these numbers do and do not show:
- They show what the combined test run exercises — including the live-AWS e2e suite — not how the binary behaves under production load over time. CI asserts only the non-e2e build (unit and CLI tests) on every push and PR.
- Coverage is a structural metric. A covered line can still be incorrect; an uncovered line can still be correct. Use it to size the test surface, not to certify behaviour.
- The e2e suite covers live-AWS paths (multipart integrity, roundtrip, cancellation, exit codes, Express One Zone, public-bucket access) but runs only on the maintainer's machine; CI does not exercise it, and reproducing the coverage figures above requires AWS credentials.

The codebase is built through spec-driven development with human review at every step. Test counts and coverage will change as subcommands and refinements are added.

### AI assessment of safety and correctness (by Claude, Anthropic)

<details>
<summary>Click to expand the full assessment</summary>

> Assessment date: 2026-09-13.
>
> Assessed version: 1.10.2 (branch `main`, commit `e8d169e`; the source tree is identical to commit `ba6aec2`, at which the coverage artifacts below were generated).
>
> Assessor: Claude (Anthropic), model Fable 5.1 (`claude-fable-5-1`), Effort: high.
>
> Method and scope of evidence: this assessment was produced from scratch. It does not carry forward, quote, or consult any earlier assessment (by Claude or by any other model); every claim below was re-derived from the code at this commit. All 166 Rust files under `src/` (59,895 lines, inline test modules included) were read in full — no file was sampled or skipped — together with the 61 process-level suites in `tests/cli_*.rs`, the 58 live-AWS suites in `tests/e2e_*.rs`, `tests/common/mod.rs`, `Cargo.toml`, `Cargo.lock`, `deny.toml`, the five GitHub Actions workflows, and the coverage artifacts `lcov.info` / `llvm-cov-report.txt` (one combined unit + CLI + `--cfg e2e_test` live-AWS run; the two artifacts were cross-checked against each other and the e2e provenance of the build was confirmed). In addition, `cargo fmt --all --check`, `cargo clippy --all-features` and `cargo deny -L error check` were re-run locally on the assessment date, and the debug binary was executed offline to confirm the exit-code behaviour cited in §6. Line numbers refer to commit `e8d169e`.
>
> Limits of the evidence: no fuzzing, sanitizer, Miri, formal-verification or penetration-testing work was done. The live-AWS suite runs only on the maintainer's account (CI does not run it, and `cargo test` in CI does not even compile it). Coverage numbers measure which lines the tests executed, not whether the assertions are strong. A from-scratch read of ~60k lines by one reviewer will miss things; the findings in §6 are what this pass found, not a proof that nothing else exists.

**Verdict in one paragraph.** At v1.10.2, `s3util` is a conservatively engineered single-object S3 tool whose dangerous paths — overwriting data, deleting data, emitting bytes that do not match what S3 holds, and replacing bucket-wide configuration — each fail closed by design and each have tests that would fail if the guard were removed. The one-object-per-invocation scope, mandatory HEAD-time version pinning, temp-file-then-rename downloads, hard-error-on-mismatch for local/stdin sources, and `deny_unknown_fields` on every bucket-configuration input are the load-bearing decisions. No critical or high-severity defect was found. What was found (§6) is a handful of low-severity ergonomic and consistency issues, plus behaviours that are correct but that an operator must know about. The tool is fit for scripted and CI use provided the operator monitors exit codes (including exit 3) and previews destructive commands with `--dry-run`; it is not a substitute for bucket versioning or backups.

**1. What the binary can do, and therefore what can go wrong.** The clap tree (`src/config/args/mod.rs`) defines 52 subcommands: 32 that mutate S3 state and 20 that only read. Every mutating subcommand exposes `--dry-run`, and the read-only ones deliberately do not (`tests/cli_dry_run.rs:808-899` pins both directions). There are no interactive confirmation prompts anywhere; `--dry-run` is the only preview mechanism, so an operator who does not use it gets exactly what they typed. The worst plausible outcomes are: (a) a transfer that silently produces bytes different from the source, (b) `mv` deleting a source whose copy did not actually succeed, (c) `rm`/`delete-bucket-*`/`delete-*` acting on the wrong resource, (d) a `put-bucket-*` command replacing a bucket-wide configuration with a truncated or misparsed one, and (e) a leaked credential or SSE-C key. Sections 2–5 examine each.

**2. Transfer integrity (the core of the tool).** The transfer engine (`src/transfer/`, `src/storage/`) is adapted from s3sync and keeps its verification model, with several additions specific to single-object semantics:

- *Every direction verifies.* ETag verification is on by default in all five directions (local→S3, S3→local, S3→S3, stdin→S3, S3→stdout); the ETag *shape* is chosen from the source's own ETag (single-part MD5 vs composite `-N`), not from a size heuristic (`src/transfer/s3_to_stdio.rs:252-373`, `1153-1167`). Additional checksums (CRC32, CRC32C, SHA-1, SHA-256, CRC64NVME; composite or `--full-object-checksum`) are verified end to end when enabled; Express One Zone buckets get CRC64NVME automatically. Content-MD5 is sent on single-part PUTs and on every part unless `--disable-content-md5-header` is set.
- *Mismatch severity is chosen by who can be wrong.* When the source is a local file or stdin, the expected ETag is computed locally, so any mismatch is a hard error (exit 1) — `src/storage/s3/upload_manager.rs:673-681`. When the source is remote, a mismatch that is explainable by a chunksize difference is a warning (exit 3) with an explicit hint (`upload_manager.rs:114-123`); a full-object checksum mismatch is always an error because chunking cannot explain it (`s3_to_stdio.rs:452-461`, `upload_manager.rs:2106-2213`). This is the right split: it never downgrades a definite corruption to a warning.
- *Downloads cannot leave a half-written file behind.* `s3_to_local` writes to a `NamedTempFile` in the destination directory and runs every check (ETag, checksum, byte count, and for multipart the per-chunk plan-vs-actual size check that detects a source modified mid-download) *before* `persist()` renames it into place (`src/storage/local/mod.rs:466-486`, `755-763`, `815-860`). A size-mismatched download is never persisted (unit test at `local/mod.rs:2564-2585`).
- *The version is pinned at HEAD.* `s3_to_local`, `s3_to_s3` and the parallel `s3_to_stdout` path capture `version_id` from the initial HEAD and pass it to every subsequent ranged GET, parts lookup, tagging fetch and annotation copy (`src/transfer/s3_to_local.rs:60-101`, `s3_to_s3.rs:77-122`, `s3_to_stdio.rs:687-690`). An overwrite of the source mid-transfer therefore yields one coherent version rather than interleaved bytes from two; on unversioned buckets the pin is absent and the ETag check remains the backstop.
- *Ranged reads are checked at the byte level.* Every chunk GET validates `Content-Range` against the requested range (`src/transfer/first_chunk.rs:97`), and the stdout path additionally rejects over-reads (including those that align exactly with a buffer boundary) and short reads (`s3_to_stdio.rs:594-639`). With `--auto-chunksize`, the chunk plan is built from the source's real part layout and the transfer refuses to run if the parts do not sum to `Content-Length` (silent-truncation guard, `s3_to_stdio.rs:813-819`) or if a multipart-shaped source has no parts metadata (OOM guard — it will not fall back to one whole-object buffer, `s3_to_stdio.rs:776-797`).
- *Uploads do not leak state.* Multipart uploads are aborted on failure and on cancellation (best-effort, logged if the abort itself fails — `upload_manager.rs:478-483`, `1136`); part count and byte total are checked before `CompleteMultipartUpload`; `--if-none-match` is applied to PutObject, CompleteMultipartUpload, CopyObject and both the buffered and streaming stdin paths (`src/transfer/stdio_to_s3.rs:108-120`, `214-230`). The live suite confirms no orphan MPU survives a SIGINT (`tests/e2e_cancel_test.rs:97`).
- *Hostile metadata does not panic.* Part-size arithmetic in `src/storage/e_tag_verify.rs:165-202` and `additional_checksum_verify.rs` treats negative or oversized part sizes as "UNKNOWN" (fail-closed) rather than wrapping.
- *Memory is bounded.* The serial stdout path streams full-object checksums and buffers at most one chunk for composite ones; this is asserted on the buffer itself, not inferred from the digest (`s3_to_stdio.rs:3674-3765`). The parallel path holds at most `max-parallel-uploads` chunks. The one deliberate exception is single-part uploads below the multipart threshold (≤5 GiB by S3's rule), which are buffered whole so that Content-MD5 can be computed.
- *Cancellation is honest.* Ctrl-C sets a flag that takes precedence over any error surfaced by the forced shutdown, so the exit is 130 rather than a misleading 1 (`src/bin/s3util/cli/mod.rs:528-572`); a chunk-fetch or stdout-write failure that cancels peer workers is reported as the root-cause error, not as a cancellation (`s3_to_stdio.rs:989-999`, `1045-1055`). Both are pinned by process-level tests against an in-process fake S3 that trickles or stalls bodies (`tests/cli_sigint_exit_code.rs`), with no AWS access required.

**3. `mv`, `rm`, `rename` and other destructive commands.**

- `mv` is a copy followed by a delete that runs only if four gates pass in order: not cancelled, transfer returned Ok, no verification warning (unless `--no-fail-on-verify-error`), and the cancellation token re-checked immediately before the delete (`src/bin/s3util/cli/mv.rs:93-155`). `--dry-run` skips the delete. A self-move (same bucket, same endpoint text, same resolved key, and no non-`null` `--source-version-id`) is rejected before any copy (`mv.rs:39-91`), so `mv` can never delete the object it just wrote. The live suite covers copy-404, delete-failure-after-copy, verify-mismatch and Ctrl-C mid-transfer, all of which must leave the source intact (`tests/e2e_mv.rs:266-744`).
- On a versioned bucket, `mv` deletes the *specific version* it copied (explicit `--source-version-id`, else the version captured at HEAD — `mv.rs:124`). That is a permanent removal of that version, not a delete marker, and it is what makes the operation safe against a concurrent overwrite; it is intentional and tested (`e2e_mv.rs:379-499`), but operators who expect delete-marker semantics should know.
- `rm` deletes one key (optionally one version); it refuses bucket-only or local-path targets. There is no recursive or prefix delete anywhere in the tool, which caps the blast radius of a typo at one object.
- `rename` (Express One Zone only) requires both keys in the same `--x-s3` bucket at parse time (exit 2) and forwards `--source-if-match` / `--target-if-none-match` conditions, so a rename onto an existing key can be made conditional (`tests/e2e_rename.rs:240-598`).
- `create-bucket --if-not-exists` probes with HeadBucket first; an unexpected probe error aborts with exit 1 rather than being misread as "does not exist" (`tests/cli_unreachable_endpoint.rs:351`).

**4. Bucket-configuration inputs.** The eight `put-bucket-*` commands that take a JSON document deserialise it through hand-written mirrors of the AWS-CLI skeleton (`src/input/json.rs`). All 57 mirror structs carry `#[serde(deny_unknown_fields)]`, so a misspelled key, a lower-cased key, or the *wrapped* shape emitted by the corresponding `get-*` command is rejected instead of being dropped — which matters because a dropped key would otherwise be applied as "field absent" and could, for example, disable every Public Access Block protection (`json.rs:1680-1758` tests exactly these cases). Lifecycle `Date` accepts the ISO 8601 spellings the AWS CLI accepts, normalises to UTC, still rejects nonsense dates, and cannot panic on non-ASCII input (`json.rs:342-421`, hostile-input test at `3113-3141`). Two semantics inherited from the AWS CLI are worth stating plainly: an empty `{}` for logging or notification *disables* that feature (documented on the types), and absent Public Access Block fields are sent as `false`, so a partial document loosens whatever it omits. Bucket policies are sent verbatim after a JSON-validity check.

**5. Credentials, secrets and supply chain.**

- Every credential and SSE-C option is env-backed with `hide_env_values = true` (`src/config/args/common.rs:142-146`, `common_client.rs:59-67`); a test walks the whole clap tree to assert no env-backed secret is ever echoed in help (`config/args/tests.rs:1889-1917`). SSE-C keys and static credentials live in `Zeroize`/`ZeroizeOnDrop` types (`src/types/mod.rs:415-450`). The startup config trace logs a fixed allow-list of non-sensitive fields and goes to stderr, never stdout (`tests/cli_tracing_to_stderr.rs`).
- `--source-no-sign-request` switches the SDK to `no_credentials()` and conflicts with every explicit credential flag at parse time (`src/storage/s3/client_builder.rs:101-102`). Stalled-stream protection is on by default.
- Dependencies: `aws-sdk-s3 1.143.0` with default features off and the modern rustls 0.23 HTTPS client re-enabled explicitly, which drops the legacy rustls 0.21 alias (`Cargo.toml:24-30`); `Cargo.lock` resolves `rustls 0.23.43`. `deny.toml` has `ignore = []`, denies `openssl-sys`, and restricts sources to crates.io; `cargo deny -L error check` runs in CI and nightly by cron, and passed locally on the assessment date. Release builds use `--locked`, attach build-provenance attestations, and publish to crates.io via OIDC trusted publishing rather than a long-lived token.
- Statically checked wiring: `tests/cli_command_api_mapping.rs` reads the source and asserts that each subcommand dispatches to exactly its own runtime and that each runtime calls only its own `api::*` wrapper — the "get-bucket-policy accidentally calls get_bucket_versioning" class of bug cannot regress silently. `tests/cli_request_payer_plumbing.rs` does the same for `--target-request-payer` forwarding.

**6. Findings.** None is critical or high. Listed in descending order of practical relevance.

1. *`--target-request-payer` is accepted by `delete-object-tagging` but has no effect there.* The S3 `DeleteObjectTagging` API has no request-payer parameter, so the wrapper correctly omits it (`src/storage/s3/api.rs:605-618`; `tests/cli_request_payer_plumbing.rs:135-148` documents the intent). But the flag is still advertised via `CommonClientArgs` (`src/config/args/delete_object_tagging.rs:27-28`), so a user of a Requester Pays bucket will see the flag accepted and then get a 403 with no hint why. Low severity; a doc note or a parse-time rejection would close it.
2. *Local files are overwritten without notice.* `s3_to_local` and `get-object-annotation --outfile` rename over any existing file (`src/storage/local/mod.rs:486`, `860`; `src/bin/s3util/cli/get_object_annotation.rs:307`). The opt-in guard is `--skip-existing` (HeadObject/`try_exists` probe, `cli/cp.rs:11`, `44-71`); there is no `--no-clobber` equivalent on the annotation command. Correct per the documented semantics, but the default is "clobber".
3. *Nine `put-*` config-file positionals are env-backed under generic names* — `POLICY`, `CORS_CONFIGURATION`, `LIFECYCLE_CONFIGURATION`, `BUCKET_LOGGING_STATUS`, `NOTIFICATION_CONFIGURATION`, `REPLICATION_CONFIGURATION`, `WEBSITE_CONFIGURATION`, `PUBLIC_ACCESS_BLOCK_CONFIGURATION`, `SERVER_SIDE_ENCRYPTION_CONFIGURATION` (e.g. `src/config/args/put_bucket_policy.rs:20`). A stray `POLICY=` in the environment silently supplies the policy file when the positional is omitted. Source/target positionals are correctly *not* env-backed, so this cannot redirect a command to another bucket; it can only supply an input document.
4. *Exit-code inconsistencies (behavioural, not safety).* (a) The thin-wrapper commands validate target shape after parsing, so `s3util rm /local/path`, `s3util delete-bucket s3://b/key` and `s3util head-object s3://b` exit 1, whereas the equivalent `cp`/`mv`/`rename` validation errors exit 2 (verified by running the binary offline; `src/bin/s3util/main.rs:42-92` vs `:56-57`). (b) `rename` of a missing key exits 1 (`src/bin/s3util/cli/rename.rs:54-56`, pinned by `tests/e2e_rename.rs:176`), whereas every other object-addressed command exits 4 on NoSuchKey. The README exit-code table is accurate about both, but scripts that branch on 4 for "not found" must special-case `rename`.
5. *`list-object-annotations` is a single request capped at 1,000 entries.* The CLI wrapper does not follow `NextContinuationToken` (`src/storage/s3/api.rs:556-600`), but the JSON output carries `IsTruncated` and the token explicitly (`src/output/json.rs:406-423`), so the truncation is visible rather than silent. The annotation *sync* path used by `cp --enable-sync-object-annotations` does paginate (`src/storage/s3/mod.rs:567-618`).
6. *Post-write verification failures leave state behind.* `put-object-annotation` verifies the CRC64NVME S3 returns and exits 1 on mismatch, but the annotation has already been written (`cli/put_object_annotation.rs:90`, and the message says so); `create-bucket --tagging` exits 3 and leaves an untagged bucket if `PutBucketTagging` fails after creation (`cli/create_bucket.rs:79-87`). Both are logged; neither is rolled back.
7. *`-qqq` suppresses the error message.* Errors are emitted through `tracing::error!` before mapping to exit 1 (`main.rs:56-57` and siblings), so at maximum quietness only the exit code survives. Consistent with the documented quiet mode, but a CI job that silences stderr loses the diagnostic.
8. *Doc nit.* `src/lib.rs:1` still describes the crate as the `s7cmd` binary.

Two claims that were checked and are **not** findings: HeadObject/HeadBucket map a 403 (no `s3:ListBucket`) to `Other` → exit 1 rather than to "not found" (`api.rs:277-288`, `629-640`), which is the correct, non-misleading behaviour; and `classify_not_found` routes `NoSuchBucket` ahead of every per-resource code (`api.rs:356-364`), with 20 of the 21 not-found code tables pinned by tests (the `rename` table is the exception).

**7. Test evidence and what it covers.** The repository carries 1,611 unit-test annotations in `src/`, 485 process-level CLI tests across 61 files, and 838 live-AWS tests across 58 files. The combined llvm-cov run (unit + CLI + e2e) reports 97.74 % of regions (1,201 of 53,100 missed), 96.15 % of functions (146 of 3,790 missed) and 98.61 % of lines (516 of 37,189 missed). The lowest-covered production files are all thin CLI wrappers whose remaining branches are S3 error arms that real AWS cannot be made to produce on demand (`delete_bucket_policy.rs` 82.6 %, `head_bucket.rs` 84.9 %, `put_bucket_policy.rs` 86.4 %, `rename.rs` 87.5 %); the transfer engine, upload manager, local storage, checksum and JSON modules are all above 96 %. Three test layers deserve mention because they close the gap left by an e2e suite that CI cannot run: `tests/cli_unreachable_endpoint.rs` drives every wrapper's unexpected-error arm against a refused connection; `tests/cli_stub_server.rs` serves crafted S3 error codes and annotation responses (KMS-encrypted payloads, ETag mismatches, oversized bodies) that AWS will not return on request; and `tests/cli_sigint_exit_code.rs` proves graceful exit 130 against a fake endpoint. The live suite is broad rather than shallow: every checksum algorithm × single/multipart × SSE-S3/KMS/DSSE/SSE-C × direction, threshold ±1 byte boundaries, 200 MiB random-data round trips through stdin and stdout, Express One Zone, Requester Pays, and dry-run no-side-effect checks for every mutating command.

**8. Static hygiene (re-verified on the assessment date).** `cargo fmt --all --check` clean; `cargo clippy --all-features` clean (CI runs `clippy -- -D warnings` plus a scheduled SARIF upload); `cargo deny -L error check` reports advisories, bans, licenses and sources all ok. CI builds and tests seven targets (x86_64/aarch64 × glibc/musl Linux, macOS aarch64, Windows x86_64/aarch64); e2e code is compiled by the maintainer under `--cfg e2e_test` but not by CI.

**What this assessment cannot establish.** That the code is bug-free; that S3-compatible stores other than Amazon S3 behave identically (the tool targets Amazon S3 and Express One Zone specifically); that future dependency advisories will be patched before they matter; or that the live suite would pass today on an account other than the maintainer's. It also cannot vouch for behaviour under `--disable-etag-verify`, `--disable-multipart-verify` or `--disable-additional-checksum-verify`: those flags exist for chunksize-mismatch and unusual-store situations and remove exactly the guarantees §2 describes.

In plain terms: at v1.10.2 the failure modes most likely to cause silent harm — corrupted transfers, partial writes that look successful, a source deleted before its copy was proven, wrong-resource operations, secrets in logs, and bucket configuration silently replaced by a misparsed document — each have a specific, citable safeguard with a test that would fail if the safeguard were removed. The residual risks found by this pass are ergonomic: default overwrite of local files, a few exit-code inconsistencies, one advertised-but-inert flag, and env-backed input-file positionals. Whether the binary belongs in a given workflow is an operator decision; the controls the design assumes are `--dry-run` before destructive commands, exit-code monitoring including exit 3, bucket versioning or backups for irreplaceable data, and least-privilege IAM.

</details>

### AI assessment of safety and correctness (by Codex)

<details>
<summary>Click to expand the full assessment</summary>

Assessed from scratch on **2026-09-13**, against source revision `16ffe39881ab48f459d6b548bc34d37e9c28d0d3`. **LLM: OpenAI Codex; Model: GPT-5 family (exact model identifier unavailable in this session); Effort: not exposed by this session.** No previous or other AI assessment was used as a reference.

The review covered the complete production implementation: CLI dispatch and every command, argument/configuration validation, credentials and client construction, JSON input/output, local and S3 storage, all transfer directions, multipart uploads/downloads, checksums, annotations, cancellation, progress, tracing, shared types, and `build.rs`. The repository inventory contains 166 Rust files under `src/` (including test code), 120 under `tests/`, and the build script. Test results and the supplied coverage reports supplement the source review; coverage was not used to select which implementation modules to assess.

#### Verification performed

- `cargo test --locked --offline --all-features --all-targets`: **2,089 passed, 0 failed, 0 ignored** on `aarch64-apple-darwin`, using Rust 1.98.1. The successful run allowed local test-server sockets; the initial sandboxed run could not bind them.
- `cargo clippy --locked --offline --all-features --all-targets -- -D warnings`: **passed**.
- Independently reproduced the metadata validation issue below with a dry-run command; it exited successfully without transferring data.
- Reconciled the existing `lcov.info` and `llvm-cov-report.txt`: their line and function totals agree. These reports were inspected, not regenerated in this assessment.

| Coverage measure | Covered / total | Coverage |
|---|---:|---:|
| Lines | 36,673 / 37,189 | 98.61% |
| Functions | 3,644 / 3,790 | 96.15% |
| Regions | 51,899 / 53,100 | 97.74% |
| Branches | No branch records | Not measured |

The reports include inline test code and do not establish production-only coverage or prove that the reports were generated from this exact revision. `--all-features` does not enable the separate `cfg(e2e_test)` cloud suite: those binaries ran zero tests. Live S3 behavior, other operating systems, and a fresh dependency-advisory audit were not validated by this run.

#### Safety and correctness assessment

The implementation has substantial defensive behavior. CLI validation rejects many incompatible transfer, encryption, credential, and checksum options. Downloads use temporary files before publishing the destination. Multipart paths check sizes and integrity, and parallel stdout delivery preserves ordering with bounded coordination. Versioned S3 transfers retain the captured source version. `mv` checks cancellation, transfer errors, and verification warnings before deleting the source, and rejects ordinary same-object moves. Secret wrapper types redact sensitive debug output and zeroize their owned values. No production `unsafe` block was found; this does not audit unsafe code inside dependencies.

The following limitations materially constrain the reliability conclusion:

- **Concurrent modification can make `mv` delete uncopied data.** The [move decision](src/bin/s3util/cli/mv.rs) deletes a local pathname or an unversioned S3 key after copying, without an identity/ETag precondition on deletion. A replacement written between the copy and deletion can therefore be removed. Captured S3 version IDs protect versioned transfers, but local/unversioned moves are not transactional. The self-move guard also compares endpoint strings, so different aliases for the same service can bypass that guard.
- **Verification failure does not guarantee an unchanged destination.** [Local verification](src/storage/local/mod.rs) classifies some ETag/composite-checksum mismatches as warnings and subsequently persists the temporary file. S3 verification can run after PUT or multipart completion has committed the object. Stdout verification necessarily follows emitted bytes. The default warning/error status and `mv` source-preservation gates matter, but they cannot retract committed data; `--no-fail-on-verify-error` explicitly weakens the move safeguard. Some unsupported or unavailable verification cases only log that verification was skipped, so exit zero does not universally mean integrity was independently verified.
- **Cancellation and multipart cleanup are best effort.** In [multipart upload handling](src/storage/s3/upload_manager.rs), early propagation of a worker failure drops remaining spawned-task handles without consistently aborting and joining every worker before aborting the upload. Requests can remain in flight; some stream cleanup errors are discarded. The [stdin probe](src/transfer/stdio_to_s3.rs) awaits input without selecting on cancellation, and other I/O waits can also delay shutdown. Prompt termination and complete cleanup are not guaranteed for stalled inputs or interrupted remote operations.
- **Memory use is configuration- and object-dependent.** Buffered transfers, stdin probing, checksum generation, and multipart workers allocate buffers based on object size, thresholds, or part size. Large allowed settings and concurrency can exhaust memory; there is no aggregate memory budget. Public library configuration and malformed service responses also encounter unchecked assumptions and `unwrap` paths that CLI validation alone cannot eliminate.
- **S3-only metadata validation is incomplete.** `CommonTransferArgs::check_metadata_conflict` in [argument validation](src/config/args/common.rs) omits `metadata`. Consequently, `s3util cp s3://example-bucket/key /tmp/s3util-codex-assessment.out --metadata key=value --dry-run --source-no-sign-request` succeeds, although the local storage implementation does not apply that metadata. The dry run confirms acceptance; the discarded option follows from the storage implementation.

Bucket configuration, tagging, and annotation operations also depend on service-side semantic validation and can make partial changes across multiple requests. Local JSON validation and dry runs do not prove remote acceptance or provide rollback.

The source and passing local checks support confidence in ordinary transfers with stable sources, appropriate resource settings, and careful handling of exit status. They do **not** establish transactional moves, universal integrity verification, bounded shutdown under stalled I/O, or defect-free behavior. The concurrent-delete and post-commit-verification limitations are more consequential than the high coverage percentage suggests.

</details>

### AI assessment of safety and correctness (by Gemini)

<details>
<summary>Click to expand the full assessment</summary>

- **LLM:** Gemini
- **Model:** Gemini 3.8 Flash
- **Effort:** High
- **Assessment Date:** 2026-09-13
- **Assessed Version:** 1.10.2 (branch `main`, commit `16ffe39`)

**Scope & Methodology:** This assessment was conducted from scratch for v1.10.2 without referencing previous AI evaluations. The audit evaluated the entire codebase without omission: all 166 Rust source files under `src/` (59,895 lines of code), 120 test files under `tests/` and `src/` (2,934 total test annotations), dependency definitions (`Cargo.toml`, `Cargo.lock`, `deny.toml`), CI/CD workflows (`.github/workflows/`), and coverage reports (`llvm-cov-report.txt` and `lcov.info` generated from a combined unit + CLI + `--cfg e2e_test` live-AWS run).

#### 1. Quantitative Quality & Test Metrics

- **Production Code Surface:** 59,895 lines of Rust across 166 source files in `src/`.
- **Test Corpus Volume:** 2,934 test annotations across 120 test files:
  - **Embedded Unit Tests:** 1,611 `#[test]` / `#[tokio::test]` annotations within `src/`.
  - **CLI Integration Tests:** 485 annotations across 61 files (`tests/cli_*.rs`), executing offline process invocations verifying argument parsing, clap validation, env binding, exit codes, stderr/stdout formatting, and pipe safety.
  - **Live-AWS E2E Tests:** 838 annotations across 58 files (`tests/e2e_*.rs`), gated behind `#![cfg(e2e_test)]` for live Amazon S3 validation.
- **Combined Code Coverage (`llvm-cov-report.txt`):**
  - **Line Coverage:** 98.61% (36,673 / 37,189 lines covered; 516 missed).
  - **Region Coverage:** 97.74% (51,899 / 53,100 regions covered; 1,201 missed).
  - **Function Coverage:** 96.15% (3,644 / 3,790 functions executed; 146 missed).
- **Critical Subsystem Coverage Highlights:**
  - Digest & Checksum implementations (`storage/checksum/*`): **100.00%** region, function, and line coverage across CRC32, CRC32C, CRC64NVME, SHA1, SHA256, and algorithm dispatcher modules.
  - Annotation helpers (`storage/annotation.rs`): **100.00%** region, function, and line coverage (235 regions, 23 functions, 145 lines).
  - Verification modules: `storage/e_tag_verify.rs` (98.76% region, 99.42% line), `storage/additional_checksum_verify.rs` (98.51% region, 99.26% line).
  - Transfer engine: `transfer/mod.rs` (100.00% region, line), `transfer/progress.rs` (100.00% region, line), `transfer/stdio_to_s3.rs` (99.35% region, 99.70% line), `transfer/s3_to_local.rs` (99.24% region, 99.60% line), `transfer/s3_to_s3.rs` (99.02% region, 99.70% line), `transfer/s3_to_stdio.rs` (98.55% region, 98.64% line), `transfer/local_to_s3.rs` (98.39% region, 99.13% line), `transfer/first_chunk.rs` (98.08% region, 98.81% line), `storage/s3/upload_manager.rs` (96.00% region, 95.39% line).
  - CLI, Pipe Safety & Signal Handling: `bin/s3util/pipe_safe.rs` (99.11% region, 98.68% line), `bin/s3util/cli/ctrl_c_handler.rs` (98.77% region, 98.06% line), `bin/s3util/cli/mv.rs` (97.83% region, 99.08% line), `bin/s3util/cli/cp.rs` (95.83% region, 98.28% line), `bin/s3util/main.rs` (95.90% region, 99.44% line).
  - API & Serde layers: `storage/s3/api.rs` (99.01% region, 99.70% line), `storage/s3/client_builder.rs` (99.08% region, 99.71% line), `input/json.rs` (98.41% region, 99.91% line), `output/json.rs` (99.27% region, 99.63% line), `storage/local/fs_util.rs` (97.87% region, 98.79% line), `storage/local/mod.rs` (96.93% region, 98.12% line), `types/mod.rs` (98.95% region, 100.00% line).
- **Static Analysis & Supply Chain Controls:** 0 compiler warnings, 0 clippy warnings (`cargo clippy --all-targets --all-features -- -D warnings`), 0 formatting diffs (`cargo fmt --all -- --check`), clean dependency check via `cargo-deny` with zero advisory ignores (`advisories.ignore = []`), no banned crates (`openssl-sys` explicitly banned).

#### 2. Data Integrity & Transfer Reliability Safeguards

- **Comprehensive Digest Computation:** The `UploadManager` precalculates full-object and per-part hashes across CRC32, CRC32C, CRC64NVME, SHA1, and SHA256. Single-part and multipart uploads calculate and send `Content-MD5` headers by default (`upload_manager.rs`), ensuring server-side validation during transit unless explicitly disabled via `--disable-content-md5-header`.
- **Mid-Transfer Version Pinning:** Mid-transfer overwrite protection is enforced on versioned buckets (`transfer/s3_to_s3.rs`, `transfer/s3_to_local.rs`). Initial `HeadObject` calls capture the source version ID, attaching it to all subsequent ranged GETs, copy operations, and checksum calls to prevent interleaved multi-version reads.
- **Atomic Local Downloads:** Downloads write to a `NamedTempFile` within the destination directory (`storage/local/fs_util.rs`, `storage/local/mod.rs`). Full-object integrity checks (ETag, additional checksums, byte totals) complete on the temporary file prior to persistence. Only upon verification success is the file atomically moved into place via `temp_file.persist()`. On any failure or signal cancellation, the temporary file is dropped and removed immediately.
- **Safe Part-Sizing Defense:** In multipart download verification (`storage/e_tag_verify.rs`, `storage/additional_checksum_verify.rs`), part sizes reported by remote endpoints are checked against remaining unread bytes prior to allocating memory, preventing out-of-memory denial-of-service from non-compliant or hostile endpoints.
- **Shape-Aware ETag Classification:** The transfer engine distinguishes genuine ETag corruptions from structural chunking mismatches (`upload_manager.rs:is_chunksize_related_e_tag_mismatch`), preventing false-positive corruption errors when copying objects uploaded via single PUT but transferred via multipart.
- **S3 Express OneZone Integration:** Native directory bucket support incorporates CRC64NVME default checksums, session token authentication, and atomic within-bucket server-side renames via `rename_object`.

#### 3. Operational Safety, Transactional Move & Scope Containment

- **Strict Resource Isolation:** All 52 subcommands operate on exactly one target resource per invocation (single object, bucket, or subresource). Multi-object wildcards, recursive traversals, and multi-source copies are prohibited by design.
- **Self-Move Prevention:** The `mv` command executes `check_not_self_move` (`bin/s3util/cli/mv.rs`), comparing normalized source and destination buckets, keys, and endpoints to prevent self-overwriting data loss, while allowing intentional version promotions.
- **Four-Gate Transactional Move:** Moving an object is guarded by a sequential four-gate verification barrier before source deletion is issued (`bin/s3util/cli/mv.rs:apply_mv_decision_tree`):
  1. Cancellation check: pipeline interruption immediately halts without deletion.
  2. Transfer error check: non-zero transfer exit codes abort before deletion.
  3. Integrity verification check: ETag or checksum warnings abort deletion unless explicitly overridden with `--no-fail-on-verify-error`.
  4. Defensive re-check: verifies cancellation status immediately prior to dispatching `DeleteObject`.
- **Choice-Enforcing ArgGroups:** Subcommands modifying bucket states (`put-bucket-versioning`, `put-bucket-accelerate-configuration`, `put-bucket-request-payment`) enforce explicit configuration choices via clap `ArgGroup` and validation checks, exiting with status 2 on omission.
- **Universal `--dry-run` Implementation:** All 32 mutating subcommands implement `--dry-run`. It executes full CLI validation, schema deserialization, elevates logging verbosity to `info`, logs planned actions, and safely terminates prior to network mutation.

#### 4. UNIX Process Ergonomics & Resiliency

- **Pipe-Safe Command Output:** Dedicated pipe safety handling (`bin/s3util/pipe_safe.rs`) intercepts stdout writes for all command reports, presigned URLs, and shell completion scripts. Downstream pipe closures (`EPIPE` / `BrokenPipe`) from consumers like `head`, `grep -q`, or pagers are caught and exit 0 cleanly rather than crashing with OS error 32. In contrast, object data downloads to stdout (`cp s3://... -`) strictly fail with exit code 1 on broken pipes to signal truncated data.
- **Signal Cancellation Precedence:** Interrupt signals (SIGINT/Ctrl-C) during transfers are captured by `ctrl_c_handler.rs` via an atomic flag (`CTRL_C_RECEIVED`), ensuring Ctrl-C takes precedence over subsequent shutdown or stalled-stream errors, guaranteeing shell exit code 130 and triggering best-effort `AbortMultipartUpload` cleanup.
- **Decoupled Positional Arguments:** Positional arguments (`source`, `target`) do not bind to ambient shell environment variables (`SOURCE`/`TARGET`), eliminating unintended argument overrides in script environments.
- **Error Formatting Consistency:** Trailing newlines are guaranteed across all CLI validation errors re-raised through clap.

#### 5. S3 API Schema & Deserialization Strictness

- **Strict Input Deserialization:** 55 input payload structures in `src/input/json.rs` enforce `#[serde(deny_unknown_fields)]`. Misspelled or unrecognized fields fail parsing immediately with clear diagnostic errors.
- **Flexible ISO 8601 Date Handling:** The lifecycle date parser (`src/input/json.rs:parse_iso8601`) normalizes RFC 3339, basic ISO 8601 (`YYYYMMDDTHHMMSSZ`), bare calendar dates, and non-standard numeric offsets (`+00:00`, `+0900`) to UTC, completely resolving previous round-trip parsing failures between GET output and PUT input.
- **AWS CLI v2 Compatible Output:** 20 hand-crafted serializers in `src/output/json.rs` match AWS CLI output specifications, including double-encoded policy strings and `{}` responses for unconfigured subresources.
- **Structured Error Classification:** `classify_not_found` and `classify_object_annotation_not_found` map HTTP 404, `NoSuchKey`, `NoSuchBucket`, and `NoSuchAnnotation` cleanly to exit code 4 (`NotFound`).

#### 6. Security Model, Secrets Management & Supply Chain

- **Cryptographic Secret Zeroization:** Sensitive parameters (`AccessKeys`, `SseKmsKeyId`, `SseCustomerKey`) implement `Zeroize` and `ZeroizeOnDrop` (`types/mod.rs`), wiping secrets from memory when dropped.
- **Secret Redaction & Privacy:** Custom `Debug` implementations render sensitive keys as `** redacted **`. CLI help definitions configure `hide_env_values`, preventing credential leaks when invoking `--help` in environments with exported secret variables.
- **Modern TLS Stack:** Modern TLS is enforced using `rustls 0.23` with the `aws-lc-rs` crypto backend; legacy `rustls 0.21` is explicitly excluded (`Cargo.toml`). `openssl-sys` is strictly banned in `deny.toml`.
- **Active Supply Chain Auditing:** Dependency audits via `cargo-deny` show 0 vulnerabilities and 0 advisories (transitive `h2` updated to remediate RUSTSEC-2026-0258). Release workflows enforce SLSA Build Level 3 provenance attestations and crates.io OIDC trusted publishing.

#### 7. Residual Verification Boundaries & Operational Limits

- **Non-durable Local Disk Writes:** Atomic local file persistence uses filesystem `rename(2)` (`temp_file.persist()`) without calling `fsync()` on the written file descriptor or containing directory; abrupt operating system failure or power loss before disk cache flush could risk loss of newly written data.
- **Sparse `put-public-access-block` JSON Deserialization:** Mirroring AWS CLI v2 semantics, omitting boolean fields in the JSON payload defaults them to `false`; supplying an empty JSON object `{}` disables all four public access block protections.
- **Download ETag Warning Classification:** Mismatched download ETags emit an exit code 3 warning while retaining the persisted file, whereas byte-count discrepancies and additional-checksum mismatches fail hard with exit code 1.
- **Offline vs Live Test Gating:** The 838 live-AWS integration tests require real AWS infrastructure (`#![cfg(e2e_test)]`), while standard CI validates unit and CLI suites offline.

#### Overall Conclusion

Is this software reliable? **Yes, conditionally.** At v1.10.2, `s3util` demonstrates high technical correctness, architectural discipline, and defensive robustness for Amazon S3 operations. The transfer subsystem and API wrappers are backed by comprehensive automated test coverage (98.61% line coverage across ~59.9k LOC), robust checksum verification, and resilient UNIX process handling. Operators must maintain awareness of the operational boundaries—specifically noting that download ETag mismatches yield exit code 3 warnings rather than hard failures, and ensuring that bucket versioning, `--dry-run` previews, and IAM least-privilege policies remain standard operational controls.

</details>

## Contributing

- Bug reports are welcome, but responses are not guaranteed.
- Since this project is considered functionally complete, I will not accept any feature requests.
- If you find this project useful, feel free to fork and modify it as you wish.

🔒 I consider this project “complete” and will maintain it only minimally going forward.
However, I intend to keep the AWS SDK for Rust and other dependencies up to date monthly.

**Issue and PR lifecycle**

To keep the tracker focused, an issue or PR with no activity for 30 days is labeled `stale` and closed 7 days later unless a new comment (or, for PRs, a new commit) is added. Items labeled `pinned` or `security` are exempt; PRs are also exempt from `pinned`. Closed items can always be reopened.

## Security assumptions

s3util is built on a fundamental security assumption: **both the object storage system and the specific bucket you
operate on must be trusted.**

Within this trust model, s3util implements the security measures you would reasonably expect of an S3 utility:
encrypted transport (TLS/HTTPS) for data in transit, end-to-end integrity verification (ETag, MD5, SHA256, and CRC
checksums), support for server-side encryption, and secure handling of credentials through the standard AWS credential
providers. These measures protect the confidentiality and integrity of your data against transport-level and accidental
threats.

However, s3util assumes that the storage endpoint is honest and non-adversarial — that it correctly implements the S3
API and returns the data, metadata, and checksum values it actually stores, without tampering. The integrity
verification features are **not** a defense against a malicious or compromised storage backend that deliberately returns
falsified data or forged checksums. Against such an adversarial endpoint, these guarantees do not hold.

Crucially, trust must extend to the **bucket**, not just the storage provider. Even when the object storage system
itself is fully trustworthy, a bucket can still be adversarial — for example, a bucket you do not control, a shared
bucket writable by others, or one whose objects, metadata, or checksums were crafted by an attacker. If you copy or read
*from* such a bucket, the data and metadata it serves are already untrusted at the source, and s3util's guarantees no
longer apply. A trusted storage provider hosting an untrusted bucket is, for the purposes of this security model, an
untrusted source.

Operating on an untrusted, compromised, or non-conformant endpoint or bucket is outside s3util's security model.
Selecting a trustworthy storage provider, and ensuring that every bucket you operate on is one you control or trust —
including its credentials, encryption, and access policies — remains your responsibility.

## Scope

s3util is designed to cover **common single-object and bucket-management operations** — single-object transfers (`cp` / `mv`), single-object restore from archive (`restore-object`), and common bucket management (creation/deletion, tagging, versioning, policy, policy status, lifecycle, encryption, CORS, public-access-block, website, logging, notifications, replication, Transfer Acceleration, request payment). For any S3 use case outside that scope, use a more comprehensive tool such as the [AWS CLI](https://aws.amazon.com/cli/) (`aws s3` / `aws s3api`); for recursive or bulk synchronization, use [s3sync](https://github.com/nidor1998/s3sync).

The `cp` and `mv` subcommands operate on one object at a time; the thin S3 API wrappers each issue a single S3 API call. s3util is **not** intended to be a drop-in replacement for, or behaviorally compatible with, any other S3 client — including the AWS CLI (`aws s3`, `aws s3api`) and tools such as `s3cmd`, `s5cmd`, `rclone`, and `mc`. Its command-line flags, transfer semantics, verification rules, and exit codes are designed around safe, verifiable single-object transfers and explicit per-API operations — not interoperability with another tool's interface. Output formats and flag names will not be adjusted to match any external tool, and scripts written against another S3 client should not be expected to work with `s3util` unmodified.

## Non-Goals

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
- Diagnosing or fixing performance degradation, resource exhaustion,
  or errors caused by raising concurrency settings
  (`--max-parallel-uploads` and similar tuning flags) above their
  defaults. The documentation explicitly notes that these values
  must be sized to the host and the target service; tuning them is
  the operator's responsibility. Reports of the form "I raised
  `--max-parallel-uploads` and it failed / slowed down / hit rate
  limits" will be closed.
- Resuming a failed or interrupted transfer. `s3util` does not
  provide a resume feature for `cp` / `mv`, including for large
  multipart uploads and downloads. There is no checkpoint file,
  no part-list reuse, and no partial-progress state persisted
  between invocations: if a transfer is interrupted (network
  error, ctrl-c, process kill, host shutdown, etc.), the next
  invocation re-transfers the whole object from the start.
  In-flight multipart uploads are best-effort aborted on ctrl-c
  and on error paths, but for crashes or other non-graceful
  exits, cleaning up any leftover incomplete multipart uploads
  (e.g. via a bucket lifecycle rule or
  `aws s3api abort-multipart-upload`) is the operator's
  responsibility.
- A plugin or extension mechanism.

Issues and pull requests requesting any of the above will be closed.

## License

This project is licensed under the Apache-2.0 License.
