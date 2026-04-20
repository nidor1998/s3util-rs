# `--source-no-sign-request` — design

Date: 2026-04-20
Scope: `s3util cp` (and any future subcommand that reuses `CpArgs`-style
credential wiring — none exist today).

## Summary

Add a `--source-no-sign-request` flag (mirroring `nidor1998/s3ls-rs`) that tells
s3util not to load any credentials for the source S3 client and to issue
anonymous (unsigned) requests. This enables copying from public S3 buckets
without needing `AWS_ACCESS_KEY_ID`, a profile, or any credential provider at
all.

Scope is **source only**. `--target-no-sign-request` is not included — unsigned
*writes* are rarely useful, and this matches s3ls-rs.

## CLI surface

New flag on `CpArgs` in `src/config/args/mod.rs`, grouped under the existing
**AWS Configuration** `help_heading`:

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

- `env` → reads `SOURCE_NO_SIGN_REQUEST`, matching every other flag in this
  file.
- clap-level `conflicts_with_all` rejects combinations that either supply
  credentials (profile / access-key trio) or require signed requests
  (`source_request_payer`, which bills the requester and therefore must be
  authenticated).
- Help text taken verbatim from the user's requested wording.

## Credential type

Add a new variant to `S3Credentials` in `src/types/mod.rs`:

```rust
pub enum S3Credentials {
    Credentials { access_keys: AccessKeys },
    Profile(String),
    FromEnvironment,
    NoSignRequest,   // new
}
```

The variant carries the "no sign" intent in the type system. No extra boolean
field is added to `ClientConfig`; the existing `credential: S3Credentials`
field is sufficient.

## Wiring — `build_client_configs`

In `src/config/args/mod.rs`, the existing source-credential ladder gets one
new branch at the top (highest precedence, since it conflicts with the others
at clap level but we still want a well-defined order):

```rust
let source_credential = if self.source_no_sign_request {
    Some(S3Credentials::NoSignRequest)
} else if let Some(source_profile) = self.source_profile.clone() {
    Some(S3Credentials::Profile(source_profile))
} else if self.source_access_key.is_some() {
    // ... existing Credentials arm ...
} else {
    Some(S3Credentials::FromEnvironment)
};
```

Target side is unchanged.

## Wiring — `client_builder.rs`

Two small changes in `src/storage/s3/client_builder.rs`.

### `load_config_credential`

Add a fourth arm:

```rust
crate::types::S3Credentials::NoSignRequest => {
    config_loader = config_loader.no_credentials();
}
```

`aws_config::ConfigLoader::no_credentials()` installs a `NoCredentialsCache`,
which causes the SDK to omit SigV4 signing and send anonymous requests. This
is the same mechanism AWS CLI uses for `--no-sign-request`.

### `build_region_provider`

`NoSignRequest` should behave like `FromEnvironment` for region resolution —
no profile-file lookup, just `--source-region` falling through to the default
region chain (env vars, IMDS, etc.):

```rust
let provider_region = if matches!(
    &self.credential,
    S3Credentials::FromEnvironment | S3Credentials::NoSignRequest,
) {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_default_provider()
} else {
    RegionProviderChain::first_try(self.region.clone().map(Region::new))
        .or_else(builder.build())
};
```

No other change in this file. Timeouts, endpoint URL, accelerate,
force-path-style, and stalled-stream protection are all credential-agnostic
and flow through unchanged.

## Validation

Added to `validate_storage_config` in `src/config/args/mod.rs`, mirroring the
existing `source_endpoint_url` guard around line 747:

```rust
if !self.is_source_s3() && self.source_no_sign_request {
    return Err("--source-no-sign-request requires an S3 source".to_string());
}
```

Runtime errors (e.g. 403 because the user pointed this flag at a private
bucket) are surfaced through the existing AWS SDK error path — no new
handling needed.

## Layers unaffected

- Storage layer (`src/storage/`): consumes a built `Client`, doesn't care
  how it was built.
- Transfer layer (`src/transfer/`): unchanged.
- Indicator, tracing, rate limiting: unchanged.
- Target-side wiring: unchanged.

## Testing

### Unit tests

`src/config/args/tests.rs`:

- `--source-no-sign-request` alone → produced `ClientConfig` has
  `credential == S3Credentials::NoSignRequest`.
- Each of the five conflicting flags paired with `--source-no-sign-request`
  (`--source-profile`, `--source-access-key`, `--source-secret-access-key`,
  `--source-session-token`, `--source-request-payer`) → clap parse error.
- `--source-no-sign-request` with a local source → `validate_storage_config`
  returns the expected error.
- `SOURCE_NO_SIGN_REQUEST=true` env var → parses identically to the CLI flag.

`src/storage/s3/client_builder.rs`:

- `create_client` with `credential: NoSignRequest` succeeds and returns a
  usable `Client`. Mirrors the existing `create_client_from_credentials`
  test; no network call.
- `build_region_provider` with `NoSignRequest` + explicit region returns that
  region.
- `build_region_provider` with `NoSignRequest` + no region falls back to the
  default region chain (assert on provider type rather than resolved region,
  since full resolution would need env/IMDS).

### E2E (user-run, gated on `cfg(e2e_test)`)

- One new test: `s3util cp s3://<public-bucket>/<key> /tmp/out --source-no-sign-request`
  against a known public bucket (Registry of Open Data candidate). Assert
  success and byte-exact download.
- Per `CLAUDE.md`, Claude does not run e2e tests; the user runs them.

## Non-goals

- `--target-no-sign-request`. Can be added later if a real use case emerges.
- Changing how `FromEnvironment` works.
- Any refactor of the source-credential ladder beyond adding the new branch.

## Files touched

- `src/config/args/mod.rs` — new arg, new branch in `build_client_configs`,
  new validation line.
- `src/config/args/tests.rs` — unit tests.
- `src/types/mod.rs` — new `S3Credentials::NoSignRequest` variant.
- `src/storage/s3/client_builder.rs` — new arm in `load_config_credential`,
  tweak to `build_region_provider`, unit tests.

Estimated diff: ~40 lines of production code plus ~80 lines of tests.
