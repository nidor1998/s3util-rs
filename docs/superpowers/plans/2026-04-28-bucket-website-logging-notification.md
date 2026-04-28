# Bucket-website / logging / notification — addendum plan

> **Pattern reference:** `docs/superpowers/plans/2026-04-28-bucket-config-subcommands.md` (the prior plan). Design and implementation guidelines are unchanged: same args/cli/api/json structure, same testing depth, same wiring, same CLAUDE.md constraints (no auto-commit, no E2E runs by Claude).

**Goal:** Add 7 more thin S3-API subcommands across 3 families, mirroring the existing pattern.

| Family | Commands added | Display order | Get-NotFound code |
|---|---|---|---|
| `bucket-website` | put / get / delete | 31 / 32 / 33 | `NoSuchWebsiteConfiguration` |
| `bucket-logging` | put / get | 34 / 35 | (none — empty body when no logging is configured) |
| `bucket-notification-configuration` | put / get | 36 / 37 | (none — empty body when no notifications are configured) |

`bucket-logging` and `bucket-notification-configuration` have **no delete subcommand**: AWS does not expose a `DeleteBucket{Logging,Notification}` API; you "remove" them by `put`ing an empty configuration. Match that — document it in the put runtime's doc-comment.

## Per-family deltas from the prior pattern

### bucket-website

- SDK input type: `WebsiteConfiguration`
- SDK methods: `put_bucket_website().website_configuration(cfg)`, `get_bucket_website()`, `delete_bucket_website()`
- Mirror struct `WebsiteConfigurationJson`:
  - `IndexDocument: Option<{ Suffix: String }>`
  - `ErrorDocument: Option<{ Key: String }>`
  - `RedirectAllRequestsTo: Option<{ HostName: String, Protocol: Option<String> }>`
  - `RoutingRules: Option<Vec<RoutingRuleJson>>`
- The mirror struct accepts both shapes (site config OR redirect-only) and lets S3 reject invalid combinations — same approach as lifecycle's `Filter` one-of.
- Output JSON top level (FLAT): `{"IndexDocument": …, "ErrorDocument": …, "RedirectAllRequestsTo": …, "RoutingRules": [...]}` with absent fields omitted (matching `aws s3api get-bucket-website --output json`).
- `GET_BUCKET_WEBSITE_NOT_FOUND_CODES = &["NoSuchWebsiteConfiguration"]`
- E2E sample fixture: minimal IndexDocument-only site config.

### bucket-logging

- SDK input type: `BucketLoggingStatus`
- SDK methods: `put_bucket_logging().bucket_logging_status(cfg)`, `get_bucket_logging()`
- Mirror struct `BucketLoggingStatusJson`:
  - `LoggingEnabled: Option<{ TargetBucket: String, TargetPrefix: String, TargetObjectKeyFormat: Option<...> }>`
- Empty `{}` JSON → builds a `BucketLoggingStatus` with no `LoggingEnabled` → disables logging on the bucket.
- Output JSON top level: `{"LoggingEnabled": {...}}` if configured, `{}` otherwise. Matches `aws s3api get-bucket-logging --output json`.
- No NotFound code constant (S3 returns success+empty when no logging).
- `get-bucket-logging` runtime: success-with-empty is OK (exit 0); only generic errors propagate as exit 1.
- Sample E2E fixture: small "log to a target bucket" configuration. **Note:** the target bucket must exist and have permissions for S3 to write logs — the E2E test will need to create both source and target buckets, set up the bucket policy on target, then test put/get. **Document this complexity in the test file.**

  Or, simpler: skip the "actually log to a target bucket" E2E (it requires an additional bucket + policy setup) and only test:
  - `put` with empty JSON (disables logging) succeeds, `get` returns `{}`
  - `put` on missing bucket exits 1
  - `get` on nonexistent bucket exits 4
  - The "with target bucket" round-trip can be a future test.

  Choose the simpler approach. The implementer should add a `// TODO(future)` note for the target-bucket round-trip.

### bucket-notification-configuration

- SDK input type: `NotificationConfiguration`
- SDK methods: `put_bucket_notification_configuration().notification_configuration(cfg)`, `get_bucket_notification_configuration()`
- Mirror struct `NotificationConfigurationJson`:
  - `TopicConfigurations: Option<Vec<TopicConfigurationJson>>`
  - `QueueConfigurations: Option<Vec<QueueConfigurationJson>>`
  - `LambdaFunctionConfigurations: Option<Vec<LambdaFunctionConfigurationJson>>`
  - `EventBridgeConfiguration: Option<EventBridgeConfigurationJson>` (a marker — empty struct)
- Each Topic/Queue/Lambda config has:
  - `Id: Option<String>`
  - `(TopicArn|QueueArn|LambdaFunctionArn): String`
  - `Events: Vec<String>` (e.g. `["s3:ObjectCreated:*"]`)
  - `Filter: Option<NotificationConfigurationFilterJson>` with `Key: { FilterRules: Vec<{ Name: String, Value: String }> }`
- Empty `{}` JSON disables all notifications.
- Output JSON: same top-level keys, present-when-set.
- No NotFound code constant.
- Sample E2E fixture: empty config (disables notifications) — round-trip is put empty + get empty. The "with real SNS/SQS/Lambda ARN" round-trip needs out-of-band setup (creating an SNS topic, granting S3 publish permission, etc.) — out of scope for this E2E; document as TODO.

## Wiring deltas

- `src/config/args/mod.rs`: add 7 `pub mod`, 7 `pub use`, 7 `Commands` variants (with display orders 31-37 in put-get-delete order), 7 `build_config_from_args` arms.
- `src/bin/s3util/cli/mod.rs`: 7 `pub mod` + 7 `pub use`.
- `src/bin/s3util/main.rs`: 7 dispatch arms.
- `src/bin/s3util/cli/ui_config.rs`: 7 arms in test-only `cp_args_from` match.
- `src/lib.rs`: no change (already exposes `pub mod input;`).

## Tests (per existing pattern)

- 7 process-level CLI test files (`tests/cli_*.rs`)
- 3 E2E test files (`tests/e2e_bucket_website.rs`, `tests/e2e_bucket_logging.rs`, `tests/e2e_bucket_notification_configuration.rs`)
- Args-struct unit tests inside each new args file
- Per-resource unit tests in `src/input/json.rs` and `src/output/json.rs`
- Pinned NOT_FOUND_CODES test for `GET_BUCKET_WEBSITE_NOT_FOUND_CODES` only

## README updates

Same shape as prior families:

- Add 7 rows to the command table (put → get → delete order):
  - `put-bucket-website` / `get-bucket-website` / `delete-bucket-website`
  - `put-bucket-logging` / `get-bucket-logging`
  - `put-bucket-notification-configuration` / `get-bucket-notification-configuration`
- Update exit-code-4 list to include `get-bucket-website` and `NoSuchWebsiteConfiguration`. (Logging/notification do NOT need to be added — they don't return NotFound; success+empty is the no-config state.)
- Update "Read-side NotFound" list with `get-bucket-website` (NOT logging/notification).
- Update "destructive thin wrappers" list with `delete-bucket-website` (NOT logging/notification — no delete commands).
- Update Scope paragraph: append "website, logging, notifications" to the bucket-management list.

## Verification gates (per CLAUDE.md)

- `cargo fmt --check`
- `cargo clippy --all-features -- -D warnings`
- `cargo test`
- `RUSTFLAGS="--cfg e2e_test" cargo check --tests`

## Per project memory (DO NOT change)

- Never auto-commit. The user does the commits.
- Never run E2E tests. They hit real AWS. Use `cargo check --tests` under the e2e cfg flag instead.
- E2E test profile is `s3util-e2e-test`.
