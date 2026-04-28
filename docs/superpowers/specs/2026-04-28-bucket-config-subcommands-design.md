# Bucket-configuration subcommand expansion

Status: approved (brainstorming session, 2026-04-28)
Branch: TBD (post-`update/v0.2.2`)

## Overview

Add 12 new subcommands to `s3util`, each a thin wrapper over a single AWS S3 API call. The user-facing CLI shape mirrors the existing `bucket-policy` family exactly — positional `<TARGET> [<CONFIG_FILE|->]`, JSON output for read commands, silent on success for write commands, `HeadError`-mapped exit codes (0 / 1 / 2 / 4).

The new commands cover four bucket-configuration resource families, each with `get` / `put` / `delete`:

| Family | Subcommands | Get-NotFound S3 code |
|---|---|---|
| `bucket-lifecycle-configuration` | get / put / delete | `NoSuchLifecycleConfiguration` |
| `bucket-encryption` | get / put / delete | `ServerSideEncryptionConfigurationNotFoundError` |
| `bucket-cors` | get / put / delete | `NoSuchCORSConfiguration` |
| `public-access-block` | get / put / delete | `NoSuchPublicAccessBlockConfiguration` |

Naming notes:
- `delete-bucket-lifecycle-configuration` (symmetric CLI name) wraps the SDK's `DeleteBucketLifecycle` operation. AWS CLI uses the asymmetric `delete-bucket-lifecycle`; we choose symmetry for predictability with the get/put pair.
- `*-public-access-block` (no `bucket-` prefix) is the SDK's actual operation naming; preserved as-is.

## Goals

- One thin S3-API command per subcommand. No client-side validation beyond JSON shape parsing — S3 is the authority on what is a valid configuration.
- User-facing CLI identical to `bucket-policy`: read `JSON file` *or* stdin (`-`) for the put commands; print AWS-CLI v2 `--output json` shape for the get commands; silent for delete.
- AWS-CLI input-shape compatibility: users can copy-paste the output of `aws s3api put-* --generate-cli-skeleton input` directly. Top-level keys are PascalCase, matching the AWS CLI's input JSON convention.
- AWS-CLI output-shape compatibility: get commands emit the same JSON keys, casing, and omission semantics as `aws s3api get-* --output json`.
- Exit codes follow the existing convention:
    - `0` success
    - `1` generic error (auth, network, malformed config rejected by S3)
    - `2` clap argument-parsing error
    - `4` NotFound — bucket missing OR resource missing (subresource)
- `HeadError` triage on the get commands: `BucketNotFound` (`NoSuchBucket`) → exit 4, "bucket … not found"; `NotFound` (subresource code per family) → exit 4, "<resource> for … not found"; `Other(_)` → exit 1.

## Non-goals

- No client-side schema validation beyond JSON shape parsing. S3 returns specific 400 errors (`MalformedXML`, `InvalidArgument`, etc.) — we surface those as exit 1 with the SDK's error message, exactly as `put-bucket-policy` does for malformed policies.
- No coverage of obscure / rarely-used SDK fields. The mirror structs cover the commonly used surface (see "Input mirror struct field coverage" below). Missing fields can be added as follow-up PRs.
- No directory-bucket (Express One Zone) special-casing. S3 Express buckets do not support these subresources; S3 returns the appropriate error and we propagate it.
- No SIGINT cleanup. Single API call; no in-flight state to abort.
- No `--*-only` shortcut flag (analogue of `--policy-only`). The new resource responses are already structured JSON — there is nothing to "unwrap."

## Architectural deviation from the policy pattern

The bucket-policy SDK call accepts a raw string body:
```rust
client.put_bucket_policy().bucket(b).policy(&str).send()
```
So `run_put_bucket_policy` ships the file contents verbatim.

The four new put commands take **typed structs**, not strings:
```rust
client.put_bucket_lifecycle_configuration().bucket(b).lifecycle_configuration(BucketLifecycleConfiguration { ... }).send()
client.put_bucket_cors().bucket(b).cors_configuration(CorsConfiguration { ... }).send()
client.put_bucket_encryption().bucket(b).server_side_encryption_configuration(ServerSideEncryptionConfiguration { ... }).send()
client.put_public_access_block().bucket(b).public_access_block_configuration(PublicAccessBlockConfiguration { ... }).send()
```
The `aws-sdk-s3` input types are smithy-generated and do **not** derive `serde::Deserialize`. To preserve the user-facing "JSON file or stdin" CLI shape, we introduce a new module `src/input/json.rs` containing `Deserialize` mirror structs, with conversions to the SDK types.

This is the smallest deviation the SDK allows. Users see the same CLI as `put-bucket-policy`. Internally, the JSON is parsed into typed structs before being handed to the SDK.

Three alternatives considered and rejected:
1. **Bypass SDK builders, send raw bytes via the lower HTTP layer.** Possible but fragile (signing, headers, SDK upgrades).
2. **Use `serde_json::Value` and field-extract.** Loses compile-time validation; same code volume.
3. **Find an existing JSON-to-SDK-type bridge in `aws-smithy-*`.** None is exposed for input types.

## Module layout

```
src/
├── bin/s3util/cli/
│   ├── mod.rs                                              # +12 dispatch arms, +12 pub-uses
│   ├── get_bucket_lifecycle_configuration.rs               # NEW
│   ├── put_bucket_lifecycle_configuration.rs               # NEW
│   ├── delete_bucket_lifecycle_configuration.rs            # NEW
│   ├── get_bucket_encryption.rs                            # NEW
│   ├── put_bucket_encryption.rs                            # NEW
│   ├── delete_bucket_encryption.rs                         # NEW
│   ├── get_bucket_cors.rs                                  # NEW
│   ├── put_bucket_cors.rs                                  # NEW
│   ├── delete_bucket_cors.rs                               # NEW
│   ├── get_public_access_block.rs                          # NEW
│   ├── put_public_access_block.rs                          # NEW
│   └── delete_public_access_block.rs                       # NEW
├── bin/s3util/main.rs                                      # +12 match arms
├── config/args/
│   ├── mod.rs                                              # +12 Subcommand variants, +12 dispatch arms
│   ├── get_bucket_lifecycle_configuration.rs               # NEW
│   ├── put_bucket_lifecycle_configuration.rs               # NEW
│   ├── delete_bucket_lifecycle_configuration.rs            # NEW
│   ├── get_bucket_encryption.rs                            # NEW
│   ├── put_bucket_encryption.rs                            # NEW
│   ├── delete_bucket_encryption.rs                         # NEW
│   ├── get_bucket_cors.rs                                  # NEW
│   ├── put_bucket_cors.rs                                  # NEW
│   ├── delete_bucket_cors.rs                               # NEW
│   ├── get_public_access_block.rs                          # NEW
│   ├── put_public_access_block.rs                          # NEW
│   └── delete_public_access_block.rs                       # NEW
├── input/                                                  # NEW DIRECTORY
│   ├── mod.rs                                              # NEW — pub mod json;
│   └── json.rs                                             # NEW — Deserialize mirror structs + into_sdk()
├── lib.rs                                                  # + pub mod input;
├── output/json.rs                                          # + 4 *_to_json() functions + tests
└── storage/s3/api.rs                                       # + 12 wrapper functions, + 4 *_NOT_FOUND_CODES consts, + pinned tests

tests/
├── cli_get_bucket_lifecycle_configuration.rs               # NEW (one per command, 12 total)
├── cli_put_bucket_lifecycle_configuration.rs               # NEW
├── cli_delete_bucket_lifecycle_configuration.rs            # NEW
├── …                                                       # (8 more, same shape per family)
├── e2e_bucket_lifecycle_configuration.rs                   # NEW (one per family, 4 total)
├── e2e_bucket_encryption.rs                                # NEW
├── e2e_bucket_cors.rs                                      # NEW
└── e2e_public_access_block.rs                              # NEW
```

Total: 26 new source files (12 args + 12 cli + `input/mod.rs` + `input/json.rs`), 16 new test files (12 process-level + 4 E2E), and 6 modified files (`config/args/mod.rs`, `bin/s3util/cli/mod.rs`, `bin/s3util/main.rs`, `lib.rs`, `output/json.rs`, `storage/s3/api.rs`).

## Per-command runtime entries

Each runtime entry mirrors its bucket-policy counterpart byte-for-byte, with the `api::*` call swapped:

### Get commands (`src/bin/s3util/cli/get_*.rs`)

```rust
pub async fn run_get_bucket_<resource>(
    args: Get…Args,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let bucket = args.bucket_name().map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    match api::get_bucket_<resource>(&client, &bucket).await {
        Ok(out) => {
            let json = output::json::get_bucket_<resource>_to_json(&out);
            println!("{}", serde_json::to_string_pretty(&json)?);
            Ok(ExitStatus::Success)
        }
        Err(HeadError::BucketNotFound) => { tracing::error!("bucket s3://{bucket} not found"); Ok(ExitStatus::NotFound) }
        Err(HeadError::NotFound)        => { tracing::error!("<resource> for s3://{bucket} not found"); Ok(ExitStatus::NotFound) }
        Err(HeadError::Other(e))         => Err(e),
    }
}
```

### Put commands (`src/bin/s3util/cli/put_*.rs`)

```rust
pub async fn run_put_bucket_<resource>(
    args: Put…Args,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args.bucket_name().map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let config_arg = args.config.as_deref().ok_or_else(|| anyhow::anyhow!("config source required"))?;
    let body = if config_arg == "-" {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
        buf
    } else {
        std::fs::read_to_string(config_arg).with_context(|| format!("reading config from {config_arg}"))?
    };
    let parsed: <Resource>ConfigurationJson = serde_json::from_str(&body)
        .with_context(|| format!("parsing JSON from {config_arg}"))?;
    let client = client_config.create_client().await;
    api::put_bucket_<resource>(&client, &bucket, parsed.into_sdk()?).await?;
    info!(bucket = %bucket, "Bucket <resource> set.");
    Ok(())
}
```

### Delete commands (`src/bin/s3util/cli/delete_*.rs`)

```rust
pub async fn run_delete_bucket_<resource>(
    args: Delete…Args,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args.bucket_name().map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    api::delete_bucket_<resource>(&client, &bucket).await?;
    info!(bucket = %bucket, "Bucket <resource> deleted.");
    Ok(())
}
```

## Args structs

Identical pattern to `get_bucket_policy.rs` / `put_bucket_policy.rs` / `delete_bucket_policy.rs`, including:
- `target: Option<String>` (positional, `value_parser = check_storage_path`, `required_unless_present = "auto_complete_shell"`)
- `bucket_name() -> Result<String, String>` rejecting non-S3 paths and paths with key/prefix
- `auto_complete_shell()` delegating to `common.auto_complete_shell`
- `#[command(flatten)] pub common: CommonClientArgs`
- For put: a second positional named after the SDK parameter (e.g. `lifecycle_configuration: Option<String>` with `env = "LIFECYCLE_CONFIGURATION"`, `cors_configuration` / `CORS_CONFIGURATION`, `server_side_encryption_configuration` / `SERVER_SIDE_ENCRYPTION_CONFIGURATION`, `public_access_block_configuration` / `PUBLIC_ACCESS_BLOCK_CONFIGURATION`), required unless `auto_complete_shell`. Mirrors `put_bucket_policy.rs`'s `pub policy: Option<String>` / `env = "POLICY"` naming.

## API wrappers (`src/storage/s3/api.rs`)

```rust
pub async fn get_bucket_lifecycle_configuration(client: &Client, bucket: &str) -> Result<GetBucketLifecycleConfigurationOutput, HeadError> { … }
pub async fn put_bucket_lifecycle_configuration(client: &Client, bucket: &str, cfg: BucketLifecycleConfiguration) -> Result<PutBucketLifecycleConfigurationOutput> { … }
pub async fn delete_bucket_lifecycle_configuration(client: &Client, bucket: &str) -> Result<DeleteBucketLifecycleOutput> { … }   // SDK op is DeleteBucketLifecycle

pub async fn get_bucket_encryption(client: &Client, bucket: &str) -> Result<GetBucketEncryptionOutput, HeadError> { … }
pub async fn put_bucket_encryption(client: &Client, bucket: &str, cfg: ServerSideEncryptionConfiguration) -> Result<PutBucketEncryptionOutput> { … }
pub async fn delete_bucket_encryption(client: &Client, bucket: &str) -> Result<DeleteBucketEncryptionOutput> { … }

pub async fn get_bucket_cors(client: &Client, bucket: &str) -> Result<GetBucketCorsOutput, HeadError> { … }
pub async fn put_bucket_cors(client: &Client, bucket: &str, cfg: CorsConfiguration) -> Result<PutBucketCorsOutput> { … }
pub async fn delete_bucket_cors(client: &Client, bucket: &str) -> Result<DeleteBucketCorsOutput> { … }

pub async fn get_public_access_block(client: &Client, bucket: &str) -> Result<GetPublicAccessBlockOutput, HeadError> { … }
pub async fn put_public_access_block(client: &Client, bucket: &str, cfg: PublicAccessBlockConfiguration) -> Result<PutPublicAccessBlockOutput> { … }
pub async fn delete_public_access_block(client: &Client, bucket: &str) -> Result<DeletePublicAccessBlockOutput> { … }
```

Plus four new constants and pinned tests:
```rust
const GET_BUCKET_LIFECYCLE_CONFIGURATION_NOT_FOUND_CODES: &[&str] = &["NoSuchLifecycleConfiguration"];
const GET_BUCKET_ENCRYPTION_NOT_FOUND_CODES:               &[&str] = &["ServerSideEncryptionConfigurationNotFoundError"];
const GET_BUCKET_CORS_NOT_FOUND_CODES:                     &[&str] = &["NoSuchCORSConfiguration"];
const GET_PUBLIC_ACCESS_BLOCK_NOT_FOUND_CODES:             &[&str] = &["NoSuchPublicAccessBlockConfiguration"];
```

Get wrappers route via `classify_not_found(code, …)` (existing helper) — no new error-classification code paths.

## Output JSON serialisers (`src/output/json.rs`)

Four new functions:
```rust
pub fn get_bucket_lifecycle_configuration_to_json(out: &GetBucketLifecycleConfigurationOutput) -> Value
pub fn get_bucket_encryption_to_json(out: &GetBucketEncryptionOutput) -> Value
pub fn get_bucket_cors_to_json(out: &GetBucketCorsOutput) -> Value
pub fn get_public_access_block_to_json(out: &GetPublicAccessBlockOutput) -> Value
```

Each emits PascalCase keys matching `aws s3api get-* --output json`. Optional fields are omitted when absent (never `null`). Empty top-level outputs collapse to `{}`.

## Input JSON deserialisers (`src/input/json.rs`)

Mirror structs (each with `#[derive(Deserialize)]` and PascalCase field names matching the AWS CLI input shape):

```rust
pub struct LifecycleConfigurationJson    { pub Rules: Vec<LifecycleRuleJson> }
pub struct CorsConfigurationJson         { pub CORSRules: Vec<CorsRuleJson> }
pub struct ServerSideEncryptionConfigurationJson { pub Rules: Vec<ServerSideEncryptionRuleJson> }
pub struct PublicAccessBlockConfigurationJson    {
    pub BlockPublicAcls: Option<bool>,
    pub IgnorePublicAcls: Option<bool>,
    pub BlockPublicPolicy: Option<bool>,
    pub RestrictPublicBuckets: Option<bool>,
}
```

Each top-level struct exposes `into_sdk(self) -> Result<<SdkType>>` constructing the SDK builder.

### Input mirror struct field coverage (commonly used surface)

The mirror structs cover the commonly used fields of each resource. Unsupported fields are left out and noted here as deferred follow-ups, not bugs.

**Lifecycle (`LifecycleRuleJson`)**: `ID`, `Status` (`Enabled`/`Disabled`), `Prefix` (deprecated S3 field, kept for AWS-CLI shape parity), `Filter` (`Prefix`, `Tag`, `And` with `Prefix`/`Tags`), `Expiration` (`Date`, `Days`, `ExpiredObjectDeleteMarker`), `NoncurrentVersionExpiration` (`NoncurrentDays`), `Transitions` (`Date`, `Days`, `StorageClass`), `NoncurrentVersionTransitions` (`NoncurrentDays`, `StorageClass`), `AbortIncompleteMultipartUpload` (`DaysAfterInitiation`).

Deferred (callable as follow-up):
- `Filter::ObjectSizeGreaterThan` / `ObjectSizeLessThan`
- `Filter::And::ObjectSizeGreaterThan` / `ObjectSizeLessThan`
- `NoncurrentVersionExpiration::NewerNoncurrentVersions`
- `NoncurrentVersionTransitions::NewerNoncurrentVersions`

**CORS (`CorsRuleJson`)**: `ID`, `AllowedHeaders`, `AllowedMethods`, `AllowedOrigins`, `ExposeHeaders`, `MaxAgeSeconds`.

**Encryption (`ServerSideEncryptionRuleJson`)**: `ApplyServerSideEncryptionByDefault` (`SSEAlgorithm`, `KMSMasterKeyID`), `BucketKeyEnabled`.

**PublicAccessBlock**: all four boolean fields; absent → false in the SDK builder.

### Error semantics

- Invalid JSON → exit 1 with "parsing JSON from <path>: <serde_json error>".
- Required-field missing → exit 1 with "parsing JSON from <path>: missing field `Rules`" (or equivalent).
- Field value rejected by S3 (e.g. invalid `StorageClass`) → exit 1 with the SDK's wrapped error.

## CLI wiring

In `src/config/args/mod.rs`:
- 12 `pub mod` declarations
- 12 `pub use … Args` re-exports
- 12 new `Commands` variants (with `#[command(display_order = …)]` chosen to keep `--help` grouped: lifecycle 19–21, encryption 22–24, cors 25–27, PAB 28–30; current versioning is 17/18 so this slots in directly after)
- 12 new arms in `build_config_from_args` returning the standard "dispatched in main.rs" error

In `src/bin/s3util/cli/mod.rs`:
- 12 `pub mod` declarations
- 12 `pub use` re-exports

In `src/bin/s3util/main.rs`:
- 12 new match arms, identical structure to existing `Commands::PutBucketPolicy(args) => …` etc.

## Tests

### Args struct unit tests (in each `src/config/args/<name>.rs`, `#[cfg(test)] mod tests`)

Per command, equivalent to `get_bucket_policy.rs` / `put_bucket_policy.rs` / `delete_bucket_policy.rs`:
- `accepts_bucket_only_path`
- `accepts_bucket_with_trailing_slash`
- `rejects_path_with_key`
- `missing_positional_with_auto_complete_shell_is_ok`
- For put commands: `accepts_bucket_and_file_path`, `accepts_bucket_and_stdin_dash`, `missing_config_positional_errors`, `missing_both_positionals_errors`

### CLI runtime unit tests (in each `src/bin/s3util/cli/<name>.rs`, `#[cfg(test)] mod tests`)

Only when there is internal logic worth testing in isolation. The policy module had `render_policy_only` to test; the new modules don't have analogous helpers, so most CLI runtime modules will have no inline tests. The runtime is exercised by the process-level CLI tests and (live) E2E tests.

### Output JSON tests (in `src/output/json.rs`, `#[cfg(test)] mod tests`)

Per `*_to_json` function, equivalent quantity to `get_bucket_policy_to_json` (3 tests) and richer ones (`get_bucket_tagging_to_json`, 3 tests):
- typical-config → expected JSON shape
- empty/absent → `{}`
- pretty-printed PascalCase keys
- per-conditional-field round-trip (one test per optional field)

### Input JSON tests (in `src/input/json.rs`, `#[cfg(test)] mod tests`)

Per mirror struct:
- `parses_aws_cli_skeleton_shape` (canonical example produced by `aws s3api put-* --generate-cli-skeleton input`)
- `rejects_invalid_json`
- `missing_required_field_errors`
- `minimal_input_works` (smallest legal config)
- `into_sdk_preserves_*` (per important field — at least 4 tests per resource)

### `api.rs` tests

- `<get_op>_not_found_codes_pinned` for each new constant (exact-equality with the expected slice)

### Process-level CLI tests (`tests/cli_<name>.rs`, one file per command, 12 total)

Mirroring `tests/cli_get_bucket_policy.rs` / `cli_put_bucket_policy.rs` / `cli_delete_bucket_policy.rs`:
- `help_succeeds_and_lists_option_groups`
- `missing_target_exits_non_zero` (exit 2)
- `auto_complete_shell_short_circuits_without_target`
- `bucket_with_key_exits_1` (validation rejects key/prefix)
- `target_access_key_without_secret_exits_non_zero`
- For put commands: `missing_both_positionals_exits_2`, `missing_config_positional_exits_2`, `nonexistent_config_file_exits_1`
- For delete commands: `target_no_sign_request_conflicts_with_target_profile`

### E2E tests (`tests/e2e_bucket_<family>.rs`, 4 files)

Mirroring `tests/e2e_bucket_policy.rs`:
- `put_get_delete_get_round_trip_via_file` — create bucket, put from file, get and assert key present, delete, get-after-delete asserts exit 4
- `put_via_stdin_and_get_round_trip` — stdin path
- `get_<resource>_on_bucket_without_<resource>_exits_4`
- `put_<resource>_on_missing_bucket_exits_1`
- `get_<resource>_on_nonexistent_bucket_exits_4`

E2E tests use `--target-profile s3util-e2e-test` (from auto-memory). All E2E tests are user-run (per CLAUDE.md). Implementation verifies they compile via `RUSTFLAGS="--cfg e2e_test" cargo check --tests` before claiming completion.

## Output JSON shape examples

### `get-bucket-lifecycle-configuration`
```json
{
  "Rules": [
    {
      "ID": "ExpireOldObjects",
      "Status": "Enabled",
      "Filter": { "Prefix": "logs/" },
      "Expiration": { "Days": 365 }
    }
  ]
}
```

### `get-bucket-encryption`
```json
{
  "ServerSideEncryptionConfiguration": {
    "Rules": [
      {
        "ApplyServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" },
        "BucketKeyEnabled": true
      }
    ]
  }
}
```
(Top-level wrapper key `ServerSideEncryptionConfiguration` matches `aws s3api get-bucket-encryption` output exactly.)

### `get-bucket-cors`
```json
{
  "CORSRules": [
    {
      "AllowedMethods": ["GET", "HEAD"],
      "AllowedOrigins": ["*"],
      "AllowedHeaders": ["*"],
      "MaxAgeSeconds": 3000
    }
  ]
}
```

### `get-public-access-block`
```json
{
  "PublicAccessBlockConfiguration": {
    "BlockPublicAcls": true,
    "IgnorePublicAcls": true,
    "BlockPublicPolicy": true,
    "RestrictPublicBuckets": true
  }
}
```
(Top-level wrapper key `PublicAccessBlockConfiguration` matches the AWS CLI output.)

## Input JSON shape examples (PUT)

Identical to `aws s3api put-* --generate-cli-skeleton input` output. The user copies that JSON into a file and passes the path as the second positional, exactly as for `put-bucket-policy`.

## Open questions / follow-ups

- Lifecycle rule fields `ObjectSizeGreaterThan` / `ObjectSizeLessThan` are deferred. Add as a follow-up PR if requested.
- README updates (the "All command line options" table, and per-command sections) are part of the implementation, not deferred.
- README "Scope" already lists "common bucket management … policy" — broaden to "lifecycle, encryption, CORS, public-access-block" in the same edit.
- A future `--config-only` style flag (analogous to `--policy-only`) is NOT planned; the new resources don't have a wrapping layer to strip.

## Verification before commit

Per CLAUDE.md, before any commit:
1. `cargo fmt`
2. `cargo clippy --all-features`
3. `cargo test` (non-e2e)
4. `RUSTFLAGS="--cfg e2e_test" cargo check --tests` (e2e compile check; do not run)

Per auto-memory, the user does the commits — Claude pauses and asks before any `git commit`.
