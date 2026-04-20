# s3util-rs

S3 utility commands (`cp`, and eventually `mv`, `rm`, etc.) for Amazon S3 and
S3-compatible object stores. Ports the transfer, verification, and multipart
semantics of [s3sync](https://github.com/nidor1998/s3sync) into a single-file
CLI focused on interactive / scripted use.

Currently in **preview**. Only the `cp` subcommand is wired up in the binary.

## Requirements

- Rust 1.91.1 or later (see `rust-version` in `Cargo.toml`).
- An AWS-compatible credentials source: profile, environment variables,
  IMDS, or explicit `--{source,target}-{access-key,secret-access-key,session-token}` flags.

## Build

```
cargo build --release
# binary: target/release/s3util
```

Shell completions can be generated:

```
s3util cp --auto-complete-shell bash   > /etc/bash_completion.d/s3util
s3util cp --auto-complete-shell zsh    > "${fpath[1]}/_s3util"
s3util cp --auto-complete-shell fish   > ~/.config/fish/completions/s3util.fish
```

## Usage

```
s3util <COMMAND> [OPTIONS] <SOURCE> <TARGET>
```

Supported path forms for `<SOURCE>` / `<TARGET>`:

| Form             | Meaning                                     |
|------------------|---------------------------------------------|
| `s3://bucket`    | Bucket with empty prefix                    |
| `s3://bucket/k`  | Specific key (or prefix ending in `/`)      |
| `/local/path`    | Local filesystem path                       |
| `-`              | Standard input (as source) or stdout (as target) |

Transfer direction is inferred from the source/target combination. Supported
directions: Local→S3, S3→Local, S3→S3 (client-side or `--server-side-copy`),
Stdin→S3, S3→Stdout.

### Exit codes

| Code | Meaning                                                     |
|------|-------------------------------------------------------------|
| 0    | Success (also: user cancellation via SIGINT/ctrl-c)         |
| 1    | Error — transfer failed or configuration rejected           |
| 3    | Warning — transfer completed but a non-fatal issue was logged (e.g. S3→S3 ETag mismatch explained by a chunksize difference) |

## Behavior notes

- **Single-file `cp` only.** Directory sources are rejected. There is no
  recursive upload/download mode.
- **Target parent directory must already exist** when downloading to local.
  `s3util` will not create missing directories; it returns an error asking the
  user to create them first.
- **Source S3 URL hygiene.** A source S3 URL ending in `/` is rejected —
  `s3util cp` copies a single object, not a prefix (no recursive mode). A
  source S3 URL whose final path segment is `.` or `..` (e.g.
  `s3://bucket/foo/..`) is rejected at argument-parse time. Targets may
  contain `..` — a user-chosen target like `../` or `../backup/` is honored
  and resolved by the OS in the usual way.
- **Verification on upload.** When the source is a local file or stdin,
  s3util precalculates the ETag and (if requested) the additional checksum,
  then compares them against the S3-reported values. A mismatch is treated as
  an error (the destination object is considered corrupted). For S3→S3
  transfers, mismatches remain warnings because they can be explained by
  differing multipart chunksizes.
- **Resolved target key.** If the target is `s3://bucket`, `s3://bucket/dir/`,
  or a directory-style local path (an existing directory, or one ending in a
  path separator like `../`), the source basename is appended. The resolved
  write path is printed on a `-> <path>` line before the transfer summary.
  With stdin as the source there is no basename, so the target key must be
  spelled out.
- **ctrl-c.** Cancellation aborts any in-flight multipart upload and exits 0.

## `s3util cp`

Copy a single object.

### Arguments

| Positional | Meaning |
|------------|---------|
| `SOURCE`   | `s3://bucket[/key]`, local path, or `-` for stdin. `SOURCE` env var. |
| `TARGET`   | `s3://bucket[/key]`, local path, or `-` for stdout. `TARGET` env var. |

### Options

Every long flag also reads from the uppercase-underscore environment variable
of the same name (for example `--max-parallel-uploads` ↔ `MAX_PARALLEL_UPLOADS`).

#### General

| Option                  | Description |
|-------------------------|-------------|
| `-v`, `--verbose`       | Increase logging verbosity (repeatable). |
| `-q`, `--quiet`         | Decrease logging verbosity (repeatable). |
| `--show-progress`       | Show progress bar. |
| `--server-side-copy`    | Use S3 server-side copy (S3→S3 only, same region/endpoint). |

#### AWS configuration

| Option                                  | Description |
|-----------------------------------------|-------------|
| `--aws-config-file <FILE>`              | Alternate AWS config file. |
| `--aws-shared-credentials-file <FILE>`  | Alternate AWS credentials file. |
| `--source-profile <NAME>`               | Source AWS profile. |
| `--source-access-key <KEY>`             | Source access key. |
| `--source-secret-access-key <KEY>`      | Source secret access key. |
| `--source-session-token <TOKEN>`        | Source session token. |
| `--target-profile <NAME>`               | Target AWS profile. |
| `--target-access-key <KEY>`             | Target access key. |
| `--target-secret-access-key <KEY>`      | Target secret access key. |
| `--target-session-token <TOKEN>`        | Target session token. |

#### Source options

| Option                             | Description |
|------------------------------------|-------------|
| `--source-region <REGION>`         | Source region. |
| `--source-endpoint-url <URL>`      | Source endpoint URL (for S3-compatible stores). |
| `--source-accelerate`              | Use S3 Transfer Acceleration on the source bucket. |
| `--source-request-payer`           | Send `x-amz-request-payer: requester` on source reads. |
| `--source-force-path-style`        | Force path-style addressing for source endpoint. |

#### Target options

| Option                             | Description |
|------------------------------------|-------------|
| `--target-region <REGION>`         | Target region. |
| `--target-endpoint-url <URL>`      | Target endpoint URL. |
| `--target-accelerate`              | Use S3 Transfer Acceleration on the target bucket. |
| `--target-request-payer`           | Send `x-amz-request-payer: requester` on target writes. |
| `--target-force-path-style`        | Force path-style addressing for target endpoint. |
| `--storage-class <CLASS>`          | Target storage class: `STANDARD`, `REDUCED_REDUNDANCY`, `STANDARD_IA`, `ONE-ZONE_IA`, `INTELLIGENT_TIERING`, `GLACIER`, `DEEP_ARCHIVE`, `GLACIER_IR`, `EXPRESS_ONEZONE`. |

#### Verification

| Option                                   | Description |
|------------------------------------------|-------------|
| `--additional-checksum-algorithm <ALGO>` | Additional checksum algorithm for upload: `SHA256`, `SHA1`, `CRC32`, `CRC32C`, `CRC64NVME`. |
| `--full-object-checksum`                 | Use full-object checksum instead of composite. Required/forced for CRC64NVME; incompatible with SHA1/SHA256. |
| `--enable-additional-checksum`           | Request additional checksum on download (S3 source only). |
| `--disable-multipart-verify`             | Skip ETag/additional-checksum verification for multipart uploads. |
| `--disable-etag-verify`                  | Skip ETag verification entirely. |
| `--disable-additional-checksum-verify`   | Do not verify additional checksum (still uploads it to S3 if configured). |

#### Performance

| Option                                   | Description |
|------------------------------------------|-------------|
| `--max-parallel-uploads <N>`             | Parallel multipart uploads/downloads. Default `16`. |
| `--rate-limit-bandwidth <BYTES_PER_SEC>` | Bandwidth cap. Accepts `MB`, `MiB`, `GB`, `GiB`. |

#### Multipart settings

| Option                                   | Description |
|------------------------------------------|-------------|
| `--multipart-threshold <SIZE>`           | Object size threshold for multipart. Default `8MiB`. |
| `--multipart-chunksize <SIZE>`           | Multipart chunk size. Default `8MiB`. |
| `--auto-chunksize`                       | Match source/target chunk layout automatically (extra `HEAD` per part). |

#### Metadata / headers

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
| `--no-sync-system-metadata`           | Skip copying system metadata (content-*, cache-control, expires, website-redirect). |
| `--no-sync-user-defined-metadata`     | Skip copying user-defined metadata. |

#### Tagging

| Option                  | Description |
|-------------------------|-------------|
| `--tagging <QUERY>`     | Target object tagging as URL-encoded query string, e.g. `k1=v1&k2=v2`. |
| `--disable-tagging`     | Do not copy source tagging. |

#### Versioning

| Option                            | Description |
|-----------------------------------|-------------|
| `--source-version-id <ID>`        | Specific source object version (S3 source only). |

#### Encryption

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

#### Tracing / logging

| Option                      | Description |
|-----------------------------|-------------|
| `--json-tracing`            | Emit traces as JSON. |
| `--aws-sdk-tracing`         | Enable AWS SDK tracing. |
| `--span-events-tracing`     | Emit span events. |
| `--disable-color-tracing`   | Disable ANSI colors in trace output. |

#### Retry

| Option                                     | Description |
|--------------------------------------------|-------------|
| `--aws-max-attempts <N>`                   | Max retry attempts. Default `10`. |
| `--initial-backoff-milliseconds <MS>`      | Initial backoff for exponential-with-jitter retry. Default `100`. |

#### Timeouts

| Option                                              | Description |
|-----------------------------------------------------|-------------|
| `--operation-timeout-milliseconds <MS>`             | Per-operation timeout (default: none). |
| `--operation-attempt-timeout-milliseconds <MS>`     | Per-attempt timeout (default: none). |
| `--connect-timeout-milliseconds <MS>`               | TCP connect timeout (default: AWS SDK default). |
| `--read-timeout-milliseconds <MS>`                  | Read timeout (default: none). |

#### Advanced

| Option                                                | Description |
|-------------------------------------------------------|-------------|
| `--acl <ACL>`                                         | Canned ACL: `private`, `public-read`, `public-read-write`, `authenticated-read`, `aws-exec-read`, `bucket-owner-read`, `bucket-owner-full-control`. |
| `--no-guess-mime-type`                                | Do not infer MIME type from local filename. |
| `--put-last-modified-metadata`                        | Store source last-modified in target metadata. |
| `--auto-complete-shell <SHELL>`                       | Emit shell completions and exit. `bash`, `fish`, `zsh`, `powershell`, `elvish`. |
| `--disable-stalled-stream-protection`                 | Disable AWS SDK stalled-stream detection. |
| `--disable-payload-signing`                           | Omit payload signing for uploads. |
| `--disable-content-md5-header`                        | Omit `Content-MD5` on uploads (also disables single-part ETag verify). |
| `--disable-express-one-zone-additional-checksum`     | Skip default additional-checksum verification for Express One Zone. |
| `--if-none-match`                                     | Upload only if target key does not already exist (optimistic create). |

## Examples

```
# Upload a single local file
s3util cp ./release.tar.gz s3://my-bucket/releases/

# Download to the parent directory (source basename is appended → ../hosts)
s3util cp s3://my-bucket/hosts ../

# Download, with additional SHA256 verification
s3util cp --enable-additional-checksum --additional-checksum-algorithm SHA256 \
  s3://my-bucket/releases/release.tar.gz ./release.tar.gz

# S3 → S3 copy with server-side copy and auto chunksize
s3util cp --server-side-copy --auto-chunksize \
  s3://src-bucket/key s3://dst-bucket/key

# Pipe to stdin → S3
pg_dump mydb | s3util cp --additional-checksum-algorithm CRC64NVME \
  - s3://my-bucket/backups/mydb-$(date +%F).sql

# Pipe S3 → stdout
s3util cp s3://my-bucket/backups/mydb-2026-04-19.sql - | psql mydb
```

## Development

```
cargo test --all-features        # unit tests
cargo clippy --all-features      # lints
cargo fmt                        # format
```

End-to-end tests hit real AWS and are gated behind the `e2e_test` cfg:

```
RUSTFLAGS="--cfg e2e_test" cargo test --all-features
```

## License

Apache-2.0.
