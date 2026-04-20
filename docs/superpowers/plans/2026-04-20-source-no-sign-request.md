# `--source-no-sign-request` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `--source-no-sign-request` CLI flag to `s3util cp` that skips credential loading and SigV4 signing on the source S3 client, enabling reads from public buckets.

**Architecture:** Introduce a new `S3Credentials::NoSignRequest` variant in `src/types/mod.rs`. Map it to `aws_config::ConfigLoader::no_credentials()` in `src/storage/s3/client_builder.rs`. Add a boolean `source_no_sign_request` field on `CpArgs` with clap `conflicts_with_all` against the five incompatible flags (`source_profile`, `source_access_key`, `source_secret_access_key`, `source_session_token`, `source_request_payer`). Add `env` for `SOURCE_NO_SIGN_REQUEST`. Add a `validate_storage_config` guard rejecting the flag when the source is not S3.

**Tech Stack:** Rust, clap 4 (derive), `aws-config`, `aws-sdk-s3`.

---

## Spec

See `docs/superpowers/specs/2026-04-20-source-no-sign-request-design.md`.

## Files touched

- `src/types/mod.rs` — add `S3Credentials::NoSignRequest` variant.
- `src/storage/s3/client_builder.rs` — add match arm in `load_config_credential`, extend `build_region_provider` `matches!`, add two unit tests.
- `src/config/args/mod.rs` — add `source_no_sign_request` field to `CpArgs`, add branch to `build_client_configs` source credential ladder, add error constant, add `check_source_no_sign_request_conflict` method, wire into `validate_storage_config`.
- `src/config/args/tests.rs` — add unit tests for parsing, clap conflicts, validation, and env var.
- `README.md` — document the new flag under AWS configuration.
- `tests/e2e_s3_to_local.rs` (or a new e2e file) — optional e2e smoke against a public bucket (gated on `cfg(e2e_test)`, run by the user).

---

## Task 1: Add `S3Credentials::NoSignRequest` variant

**Files:**
- Modify: `src/types/mod.rs:312-317` (enum definition)
- Modify: `src/storage/s3/client_builder.rs:73-103` (exhaustive match will break)

- [ ] **Step 1: Add a failing unit test in `src/storage/s3/client_builder.rs`**

Add at the bottom of the existing `#[cfg(test)] mod tests` block (after `create_client_from_custom_profile_overriding_region`, before the `build_profile_files_*` tests around line 614):

```rust
#[tokio::test]
async fn create_client_with_no_sign_request_credential() {
    init_dummy_tracing_subscriber();

    let client_config = ClientConfig {
        client_config_location: ClientConfigLocation {
            aws_config_file: None,
            aws_shared_credentials_file: None,
        },
        credential: crate::types::S3Credentials::NoSignRequest,
        region: Some("my-region".to_string()),
        endpoint_url: Some("https://my.endpoint.local".to_string()),
        force_path_style: false,
        retry_config: crate::config::RetryConfig {
            aws_max_attempts: 10,
            initial_backoff_milliseconds: 100,
        },
        cli_timeout_config: crate::config::CLITimeoutConfig {
            operation_timeout_milliseconds: None,
            operation_attempt_timeout_milliseconds: None,
            connect_timeout_milliseconds: None,
            read_timeout_milliseconds: None,
        },
        disable_stalled_stream_protection: false,
        request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
        parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        accelerate: false,
        request_payer: None,
    };

    let client = client_config.create_client().await;
    assert_eq!(
        client.config().region().unwrap().to_string(),
        "my-region".to_string()
    );
}
```

- [ ] **Step 2: Run test — expect compile failure**

```
cargo test --lib storage::s3::client_builder::tests::create_client_with_no_sign_request_credential
```

Expected: compile error, `no variant named 'NoSignRequest' found for enum 'S3Credentials'`.

- [ ] **Step 3: Add the `NoSignRequest` variant**

Edit `src/types/mod.rs` lines 312-317:

```rust
#[derive(Debug, Clone)]
pub enum S3Credentials {
    Profile(String),
    Credentials { access_keys: AccessKeys },
    FromEnvironment,
    NoSignRequest,
}
```

- [ ] **Step 4: Run test — expect compile failure on non-exhaustive match**

```
cargo test --lib storage::s3::client_builder::tests::create_client_with_no_sign_request_credential
```

Expected: compile error, `non-exhaustive patterns: '&S3Credentials::NoSignRequest' not covered` in `load_config_credential`.

- [ ] **Step 5: Add the match arm in `load_config_credential`**

Edit `src/storage/s3/client_builder.rs`, the match at lines 73-103. Insert a new arm after `FromEnvironment`:

```rust
crate::types::S3Credentials::FromEnvironment => {}
crate::types::S3Credentials::NoSignRequest => {
    config_loader = config_loader.no_credentials();
}
```

- [ ] **Step 6: Run the test — expect pass**

```
cargo test --lib storage::s3::client_builder::tests::create_client_with_no_sign_request_credential
```

Expected: PASS.

- [ ] **Step 7: Verify full build & clippy**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

Expected: no errors, no new warnings.

- [ ] **Step 8: Commit**

```bash
git add src/types/mod.rs src/storage/s3/client_builder.rs
git commit -m "feat(types): add S3Credentials::NoSignRequest variant

Maps to aws_config::ConfigLoader::no_credentials() in the S3 client
builder, producing anonymous (unsigned) requests."
```

---

## Task 2: Treat `NoSignRequest` like `FromEnvironment` for region resolution

**Files:**
- Modify: `src/storage/s3/client_builder.rs:105-132` (`build_region_provider`)
- Test in: `src/storage/s3/client_builder.rs` (same `tests` module)

- [ ] **Step 1: Write a failing unit test**

Add to the same `tests` module (next to the Task 1 test):

```rust
#[tokio::test]
async fn no_sign_request_uses_default_region_chain_not_profile_files() {
    // NoSignRequest must not consult profile files for region resolution.
    // Point at a nonexistent config file; if the code consulted it, client
    // construction would fail. With NoSignRequest it should fall through
    // to the default region chain.
    init_dummy_tracing_subscriber();

    let client_config = ClientConfig {
        client_config_location: ClientConfigLocation {
            aws_config_file: Some("/definitely/does/not/exist/config".into()),
            aws_shared_credentials_file: Some("/definitely/does/not/exist/creds".into()),
        },
        credential: crate::types::S3Credentials::NoSignRequest,
        region: Some("us-east-1".to_string()),
        endpoint_url: Some("https://my.endpoint.local".to_string()),
        force_path_style: false,
        retry_config: crate::config::RetryConfig {
            aws_max_attempts: 10,
            initial_backoff_milliseconds: 100,
        },
        cli_timeout_config: crate::config::CLITimeoutConfig {
            operation_timeout_milliseconds: None,
            operation_attempt_timeout_milliseconds: None,
            connect_timeout_milliseconds: None,
            read_timeout_milliseconds: None,
        },
        disable_stalled_stream_protection: false,
        request_checksum_calculation: RequestChecksumCalculation::WhenRequired,
        parallel_upload_semaphore: Arc::new(Semaphore::new(1)),
        accelerate: false,
        request_payer: None,
    };

    let client = client_config.create_client().await;
    assert_eq!(
        client.config().region().unwrap().to_string(),
        "us-east-1".to_string(),
    );
}
```

- [ ] **Step 2: Run test — may already pass, but the `matches!` guard must be updated for semantic correctness**

```
cargo test --lib storage::s3::client_builder::tests::no_sign_request_uses_default_region_chain_not_profile_files
```

If it passes as-is, the assertion is still satisfied because `region` is explicit. The behavior difference surfaces only when `region` is `None` (profile-file lookup would happen). We update the `matches!` anyway to match the documented semantics — verified in the next sub-test.

- [ ] **Step 3: Update `build_region_provider` in `src/storage/s3/client_builder.rs:120-132`**

Replace the `matches!` block:

```rust
let provider_region = if matches!(
    &self.credential,
    crate::types::S3Credentials::FromEnvironment | crate::types::S3Credentials::NoSignRequest,
) {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_default_provider()
} else {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_else(builder.build())
};
```

- [ ] **Step 4: Run the test — expect pass**

```
cargo test --lib storage::s3::client_builder::tests::no_sign_request_uses_default_region_chain_not_profile_files
```

Expected: PASS.

- [ ] **Step 5: Verify full build & clippy**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

- [ ] **Step 6: Commit**

```bash
git add src/storage/s3/client_builder.rs
git commit -m "feat(s3): route NoSignRequest through default region chain

Mirrors FromEnvironment so no profile-file lookup is attempted when
credentials are intentionally absent."
```

---

## Task 3: Add `--source-no-sign-request` CLI flag + build-config branch

**Files:**
- Modify: `src/config/args/mod.rs` — add field on `CpArgs` (near line 181, right after `source_request_payer`), add branch in `build_client_configs` source credential ladder (near line 876).
- Test in: `src/config/args/tests.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/config/args/tests.rs`:

```rust
#[test]
fn source_no_sign_request_produces_no_sign_request_credential() {
    let config = build_config_from_args(args_with_extra(
        "s3://public-bucket/key",
        "/tmp/out",
        &["--source-no-sign-request"],
    ))
    .unwrap();

    let source_credential = config.source_client_config.unwrap().credential;
    assert!(
        matches!(source_credential, S3Credentials::NoSignRequest),
        "expected NoSignRequest, got {source_credential:?}"
    );
}
```

- [ ] **Step 2: Run test — expect parse error (unknown flag)**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_produces_no_sign_request_credential
```

Expected: test fails with clap "unexpected argument '--source-no-sign-request'".

- [ ] **Step 3: Add the field to `CpArgs`**

Edit `src/config/args/mod.rs`, insert directly after `source_request_payer` (the field at line 181). Conflict list intentionally empty for now — Task 4 adds conflicts and their tests.

```rust
/// Do not sign the request. If this argument is specified, credentials will not be loaded
#[arg(long, env, default_value_t = false, help_heading = "AWS Configuration")]
source_no_sign_request: bool,
```

- [ ] **Step 4: Add branch to `build_client_configs` source ladder**

Edit `src/config/args/mod.rs` at lines 876-894. Prepend one branch to the source credential ladder:

```rust
let source_credential = if self.source_no_sign_request {
    Some(S3Credentials::NoSignRequest)
} else if let Some(source_profile) = self.source_profile.clone() {
    Some(S3Credentials::Profile(source_profile))
} else if self.source_access_key.is_some() {
    self.source_access_key
        .clone()
        .map(|access_key| S3Credentials::Credentials {
            access_keys: AccessKeys {
                access_key,
                secret_access_key: self
                    .source_secret_access_key
                    .as_ref()
                    .unwrap()
                    .to_string(),
                session_token: self.source_session_token.clone(),
            },
        })
} else {
    Some(S3Credentials::FromEnvironment)
};
```

- [ ] **Step 5: Run the test — expect pass**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_produces_no_sign_request_credential
```

Expected: PASS.

- [ ] **Step 6: Verify fmt/clippy/tests**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

- [ ] **Step 7: Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests.rs
git commit -m "feat(cli): add --source-no-sign-request flag

Routes through to S3Credentials::NoSignRequest when set. No clap
conflicts or source-must-be-S3 guard yet — those land in follow-up
commits."
```

---

## Task 4: Add clap `conflicts_with_all` for the five incompatible flags

**Files:**
- Modify: `src/config/args/mod.rs` — the `#[arg]` attribute on `source_no_sign_request`.
- Test in: `src/config/args/tests.rs`

- [ ] **Step 1: Write five failing tests**

Append to `src/config/args/tests.rs`:

```rust
#[test]
fn source_no_sign_request_conflicts_with_source_profile() {
    let err = build_config_from_args(args_with_extra(
        "s3://b/k",
        "/tmp/out",
        &["--source-no-sign-request", "--source-profile", "myprofile"],
    ))
    .unwrap_err();
    assert!(
        err.contains("cannot be used with"),
        "unexpected error: {err}"
    );
}

#[test]
fn source_no_sign_request_conflicts_with_source_access_key() {
    let err = build_config_from_args(args_with_extra(
        "s3://b/k",
        "/tmp/out",
        &[
            "--source-no-sign-request",
            "--source-access-key",
            "AKIA...",
            "--source-secret-access-key",
            "secret",
        ],
    ))
    .unwrap_err();
    assert!(
        err.contains("cannot be used with"),
        "unexpected error: {err}"
    );
}

#[test]
fn source_no_sign_request_conflicts_with_source_secret_access_key() {
    // Bare --source-secret-access-key would be rejected by `requires`; pair
    // it with --source-access-key to exercise the --source-no-sign-request
    // conflict specifically.
    let err = build_config_from_args(args_with_extra(
        "s3://b/k",
        "/tmp/out",
        &[
            "--source-no-sign-request",
            "--source-access-key",
            "AKIA...",
            "--source-secret-access-key",
            "secret",
        ],
    ))
    .unwrap_err();
    assert!(
        err.contains("cannot be used with"),
        "unexpected error: {err}"
    );
}

#[test]
fn source_no_sign_request_conflicts_with_source_session_token() {
    let err = build_config_from_args(args_with_extra(
        "s3://b/k",
        "/tmp/out",
        &[
            "--source-no-sign-request",
            "--source-access-key",
            "AKIA...",
            "--source-secret-access-key",
            "secret",
            "--source-session-token",
            "token",
        ],
    ))
    .unwrap_err();
    assert!(
        err.contains("cannot be used with"),
        "unexpected error: {err}"
    );
}

#[test]
fn source_no_sign_request_conflicts_with_source_request_payer() {
    let err = build_config_from_args(args_with_extra(
        "s3://b/k",
        "/tmp/out",
        &["--source-no-sign-request", "--source-request-payer"],
    ))
    .unwrap_err();
    assert!(
        err.contains("cannot be used with"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 2: Run the tests — expect all five to fail**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_conflicts
```

Expected: all five tests fail (clap accepts the combinations; `build_config_from_args` returns Ok or a non-conflict error).

- [ ] **Step 3: Add `conflicts_with_all` to the flag**

Edit `src/config/args/mod.rs`, update the `#[arg(...)]` on `source_no_sign_request` added in Task 3:

```rust
/// Do not sign the request. If this argument is specified, credentials will not be loaded
#[arg(
    long,
    env,
    default_value_t = false,
    conflicts_with_all = [
        "source_profile",
        "source_access_key",
        "source_secret_access_key",
        "source_session_token",
        "source_request_payer",
    ],
    help_heading = "AWS Configuration"
)]
source_no_sign_request: bool,
```

- [ ] **Step 4: Run the tests — expect all five to pass**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_conflicts
```

Expected: PASS (×5).

- [ ] **Step 5: Verify fmt/clippy/tests**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

- [ ] **Step 6: Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests.rs
git commit -m "feat(cli): reject --source-no-sign-request with conflicting creds

Clap-level conflicts with --source-profile, the source access-key trio,
and --source-request-payer (the requester must be identifiable for
billing, which requires signed requests)."
```

---

## Task 5: Validate that source is S3 when `--source-no-sign-request` is set

**Files:**
- Modify: `src/config/args/mod.rs` — new error constant (near line 103), new check method (near line 798), wire into `validate_storage_config` (around line 585).
- Test in: `src/config/args/tests.rs`

- [ ] **Step 1: Write the failing test**

Append to `src/config/args/tests.rs`:

```rust
#[test]
fn source_no_sign_request_requires_s3_source() {
    // Local source + --source-no-sign-request is nonsensical; reject at
    // parse-time with a clear message (mirrors --source-endpoint-url).
    let err = build_config_from_args(args_with_extra(
        "/tmp/local-source",
        "s3://my-bucket/key",
        &["--source-no-sign-request"],
    ))
    .unwrap_err();
    assert!(
        err.contains("--source-no-sign-request, source must be s3://"),
        "unexpected error: {err}"
    );
}
```

- [ ] **Step 2: Run the test — expect fail**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_requires_s3_source
```

Expected: FAIL — no such error message.

- [ ] **Step 3: Add the error constant**

Edit `src/config/args/mod.rs`, append near the other `SOURCE_LOCAL_STORAGE_SPECIFIED_*` constants (around line 103):

```rust
const SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST: &str =
    "with --source-no-sign-request, source must be s3://\n";
```

- [ ] **Step 4: Add the check method**

Edit `src/config/args/mod.rs`, insert near the other `check_*_conflict` methods (e.g. right after `check_request_payer_conflict` around line 806):

```rust
fn check_source_no_sign_request_conflict(&self) -> Result<(), String> {
    if self.source_no_sign_request && !self.is_source_s3() {
        return Err(SOURCE_LOCAL_STORAGE_SPECIFIED_WITH_NO_SIGN_REQUEST.to_string());
    }
    Ok(())
}
```

- [ ] **Step 5: Wire into `validate_storage_config`**

Edit `src/config/args/mod.rs:564-590`. Add one line after `self.check_request_payer_conflict()?;` at line 585:

```rust
self.check_request_payer_conflict()?;
self.check_source_no_sign_request_conflict()?;
self.check_source_s3_key()?;
```

- [ ] **Step 6: Run the test — expect pass**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_requires_s3_source
```

Expected: PASS.

- [ ] **Step 7: Verify fmt/clippy/tests**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

- [ ] **Step 8: Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests.rs
git commit -m "feat(cli): reject --source-no-sign-request with non-S3 source

Parse-time guard mirroring --source-endpoint-url / --source-request-payer."
```

---

## Task 6: Verify env var support (`SOURCE_NO_SIGN_REQUEST`)

`#[arg(env)]` wires this automatically; the test is a regression guard.

**Files:**
- Test in: `src/config/args/tests.rs`

- [ ] **Step 1: Write the test**

Append to `src/config/args/tests.rs`:

```rust
#[test]
fn source_no_sign_request_env_var_enables_flag() {
    // Scope the env var via a guard so parallel tests don't see it. Tests in
    // this file run in a single binary; other tests do not read this var.
    struct EnvGuard(&'static str, Option<String>);
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            match &self.1 {
                Some(v) => unsafe { std::env::set_var(self.0, v) },
                None => unsafe { std::env::remove_var(self.0) },
            }
        }
    }
    let prev = std::env::var("SOURCE_NO_SIGN_REQUEST").ok();
    let _guard = EnvGuard("SOURCE_NO_SIGN_REQUEST", prev);
    unsafe { std::env::set_var("SOURCE_NO_SIGN_REQUEST", "true") };

    let config = build_config_from_args(args_with("s3://b/k", "/tmp/out")).unwrap();
    let source_credential = config.source_client_config.unwrap().credential;
    assert!(
        matches!(source_credential, S3Credentials::NoSignRequest),
        "expected NoSignRequest, got {source_credential:?}"
    );
}
```

- [ ] **Step 2: Run the test — expect pass immediately**

```
cargo test --lib config::args::tests::tests::source_no_sign_request_env_var_enables_flag
```

Expected: PASS (no implementation change — `#[arg(env)]` handles this).

- [ ] **Step 3: Verify fmt/clippy/tests**

```
cargo fmt
cargo clippy --all-features
cargo test --lib
```

- [ ] **Step 4: Commit**

```bash
git add src/config/args/tests.rs
git commit -m "test(cli): cover SOURCE_NO_SIGN_REQUEST env var

Regression guard for the #[arg(env)] attribute on
--source-no-sign-request."
```

---

## Task 7: Update README

**Files:**
- Modify: `README.md` lines 112-123 (AWS configuration table).

- [ ] **Step 1: Edit the table**

Edit `README.md`. In the AWS configuration table (line 112 onward), insert a row after the `--aws-shared-credentials-file` row:

```markdown
| `--aws-shared-credentials-file <FILE>`  | Alternate AWS credentials file. |
| `--source-no-sign-request`              | Do not sign the source request. Credentials will not be loaded. |
| `--source-profile <NAME>`               | Source AWS profile. |
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: document --source-no-sign-request"
```

---

## Task 8: E2E smoke test (user-run)

Per `CLAUDE.md`, Claude does not run e2e tests. This task **writes** the test but leaves execution to the user. The existing e2e `TestHelper` assumes authenticated access and provisions buckets, which is the opposite of what this test needs — it must work *without* credentials against an externally-owned public bucket. So this task creates a new, self-contained e2e file that drives the built binary via `std::process::Command`.

**Files:**
- Create: `tests/e2e_source_no_sign_request.rs` (new).

- [ ] **Step 1: Create the test file**

Create `tests/e2e_source_no_sign_request.rs` with this exact content:

```rust
//! E2E: `--source-no-sign-request` against a real AWS public bucket.
//!
//! Uses `s3://nyc-tlc/misc/taxi_zone_lookup.csv` (~12 KiB) from the AWS
//! Registry of Open Data. Requires network access; does NOT require AWS
//! credentials or config — that is the whole point of the test.
//!
//! If nyc-tlc is ever retired, replace with another Registry of Open Data
//! object and update the size lower bound below.
#![cfg(e2e_test)]

use std::process::{Command, Stdio};

#[test]
fn cp_from_public_bucket_without_credentials() {
    let bin = env!("CARGO_BIN_EXE_s3util");
    let tmp = tempfile::NamedTempFile::new().unwrap();

    let output = Command::new(bin)
        .args([
            "cp",
            "--source-no-sign-request",
            "--source-region",
            "us-east-1",
            "s3://nyc-tlc/misc/taxi_zone_lookup.csv",
            tmp.path().to_str().unwrap(),
        ])
        // Intentionally clear any AWS_* env vars that could interfere with
        // the "no credentials" guarantee the flag makes.
        .env_remove("AWS_ACCESS_KEY_ID")
        .env_remove("AWS_SECRET_ACCESS_KEY")
        .env_remove("AWS_SESSION_TOKEN")
        .env_remove("AWS_PROFILE")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "s3util cp failed.\nstatus: {:?}\nstderr:\n{stderr}",
        output.status.code(),
    );

    let len = std::fs::metadata(tmp.path()).unwrap().len();
    assert!(
        len > 1_000,
        "downloaded file suspiciously small: {len} bytes\nstderr:\n{stderr}",
    );
}
```

- [ ] **Step 2: Verify e2e code compiles (do NOT run)**

```
RUSTFLAGS="--cfg e2e_test" cargo check --tests
RUSTFLAGS="--cfg e2e_test" cargo clippy --tests --all-features
```

Expected: compile + lint clean. Do not run the test — user runs e2e.

- [ ] **Step 3: Commit**

```bash
git add tests/e2e_source_no_sign_request.rs
git commit -m "test(e2e): cover --source-no-sign-request against a public bucket

User-run. Targets the AWS Registry of Open Data nyc-tlc bucket,
scrubbing AWS_* env vars so the unsigned path is actually exercised.
Compile/lint verified via cargo check and cargo clippy under
--cfg e2e_test."
```

---

## Post-implementation verification

- [ ] `cargo fmt` clean
- [ ] `cargo clippy --all-features` clean
- [ ] `cargo test --lib` — all new tests pass
- [ ] `RUSTFLAGS="--cfg e2e_test" cargo check --tests` succeeds
- [ ] User runs the e2e test against a public bucket and confirms success

## Out of scope (explicitly)

- `--target-no-sign-request`. Not part of this plan.
- Any refactor of `S3Credentials` or the source-credential ladder beyond the new branch.
- Changes to `FromEnvironment` handling.
