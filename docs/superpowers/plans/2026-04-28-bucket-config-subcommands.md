# Bucket-configuration Subcommand Expansion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **Per project memory: NEVER auto-commit.** Every commit step says "PAUSE — wait for the user to approve and run the commit." Surface the proposed `git add` set and commit message; do not run `git commit` yourself.
>
> **Per project CLAUDE.md and memory: NEVER run E2E tests.** They hit real AWS. The plan only ever asks you to compile-check them with `RUSTFLAGS="--cfg e2e_test" cargo check --tests`.

**Goal:** Add 12 thin S3-API subcommands to `s3util` covering `bucket-lifecycle-configuration`, `bucket-encryption`, `bucket-cors`, and `public-access-block` (each with `get` / `put` / `delete`), with unit and E2E tests of equivalent quantity and quality to the existing `bucket-policy` family.

**Architecture:** User-facing CLI mirrors `bucket-policy` byte-for-byte — positional `<TARGET> [<CONFIG_FILE|->]`. Internally, because the AWS SDK requires typed-struct inputs (not raw JSON bodies) for these resources, a new `src/input/json.rs` module holds `serde::Deserialize` mirror structs that parse AWS-CLI-shape JSON and convert to SDK types. Output JSON serialisers extend `src/output/json.rs`. Get commands route NotFound codes through the existing `HeadError` / `classify_not_found` infrastructure to exit code 4.

**Tech Stack:** Rust 1.91+, `aws-sdk-s3`, `clap`, `serde` / `serde_json`, `anyhow`, `tracing`, `tokio`. Existing test infrastructure: `tests/common/mod.rs::TestHelper`. Existing CI gates: `cargo fmt`, `cargo clippy --all-features`, `cargo test`, `RUSTFLAGS="--cfg e2e_test" cargo check --tests`.

**Spec:** `docs/superpowers/specs/2026-04-28-bucket-config-subcommands-design.md`

---

## File-structure overview

This plan creates 26 new source files, 16 new test files, and modifies 6 existing files.

### New (per family — substitute `<family>` and SDK names)

```
src/config/args/{get,put,delete}_<family>.rs           # 12 args structs (3 × 4 families)
src/bin/s3util/cli/{get,put,delete}_<family>.rs        # 12 CLI runtime entries
src/input/mod.rs                                       # 1 — `pub mod json;`
src/input/json.rs                                      # 1 — Deserialize mirror structs
tests/cli_{get,put,delete}_<family>.rs                 # 12 process-level CLI tests
tests/e2e_bucket_<family>.rs                           # 4 E2E test files (1 per family)
```

### Modified

```
src/config/args/mod.rs        # +12 pub mods, +12 pub uses, +12 Commands variants, +12 dispatch arms
src/bin/s3util/cli/mod.rs     # +12 pub mods, +12 pub uses
src/bin/s3util/main.rs        # +12 dispatch arms in main()
src/lib.rs                    # +1 line: `pub mod input;`
src/output/json.rs            # +4 *_to_json() functions + tests
src/storage/s3/api.rs         # +12 wrappers, +4 NOT_FOUND_CODES consts, +4 pinned tests
README.md                     # +12 rows in command table, +per-command docs
```

---

## Plan layout

The plan is organized **family-by-family**. Each family is self-contained and produces a working, testable, committable unit. The lifecycle family (Section B) is documented in full detail; subsequent families (C, D, E) reference Section B for boilerplate and supply only the family-specific deltas (SDK type names, NotFound codes, JSON shapes, file body fixtures).

- **Section A (Task 1):** Foundation — `src/input/{mod.rs,json.rs}` skeleton + `lib.rs` registration.
- **Section B (Tasks 2–9):** Lifecycle family.
- **Section C (Tasks 10–17):** Encryption family.
- **Section D (Tasks 18–25):** CORS family.
- **Section E (Tasks 26–33):** Public-access-block family.
- **Section F (Tasks 34–35):** README updates and final whole-repo verification.

Total: 35 tasks. Each task is bite-sized (single logical unit). Five natural commit points (after Sections A/B/C/D/E + final docs commit after F).

---

## Section A — Foundation

### Task 1: Create `src/input/json.rs` skeleton and register in `lib.rs`

**Why first:** every put-* command depends on this module. Creating the skeleton (with no resource-specific structs yet) lets later family tasks add their structs incrementally.

**Files:**
- Create: `src/input/mod.rs`
- Create: `src/input/json.rs`
- Modify: `src/lib.rs` (add `pub mod input;` next to existing `pub mod output;`)

- [ ] **Step 1: Create `src/input/mod.rs`**

```rust
//! User-supplied JSON input deserialisation for `put-*` subcommands.
//!
//! Mirrors `src/output/json.rs` in spirit: hand-written serde-derived
//! types matching the AWS-CLI input JSON shape (PascalCase) for each
//! resource that `put_bucket_*` accepts as a typed SDK struct rather
//! than a raw body.

pub mod json;
```

- [ ] **Step 2: Create `src/input/json.rs` with module-level docs only (no structs yet)**

```rust
//! AWS-CLI-shape JSON deserialisation for S3 SDK input types used by the
//! `put-*` subcommands.
//!
//! The `aws-sdk-s3` input types are smithy-generated and do not derive
//! `serde::Deserialize`, so this module defines mirror structs shaped
//! exactly like `aws s3api put-* --generate-cli-skeleton input` (PascalCase
//! fields). Each top-level mirror exposes `into_sdk(self) -> Result<…>`
//! that builds the SDK type and returns the SDK builder error verbatim
//! (so S3-side error messages match what the user would get from the AWS
//! CLI).
//!
//! Per-resource structs are added by their respective family tasks.

use anyhow::Result;
```

(The empty `use anyhow::Result;` import is fine — it'll be used by all family additions and avoids a churn diff later.)

- [ ] **Step 3: Add `pub mod input;` to `src/lib.rs`**

Find the existing line `pub mod output;` and add `pub mod input;` immediately above (alphabetical sibling ordering). The result should look like:

```rust
pub mod config;
pub mod input;
pub mod output;
pub mod storage;
pub mod transfer;
pub mod types;
```

- [ ] **Step 4: Verify it compiles**

Run: `cargo check`
Expected: builds clean.

- [ ] **Step 5: PAUSE — ask user to commit**

Show the user the diff and propose a commit message; **do not run `git commit` yourself**:

```
chore(input): scaffold src/input/json.rs module for AWS-CLI-shape PUT inputs
```

Files staged: `src/input/mod.rs` (new), `src/input/json.rs` (new), `src/lib.rs` (modified).

---

## Section B — Lifecycle family (full detail)

This section is the canonical example. Sections C/D/E follow this exact structure with type-name and JSON-shape substitutions.

### Task 2: Add `LifecycleConfigurationJson` mirror struct + tests

**Files:**
- Modify: `src/input/json.rs` (append struct definitions and tests)

- [ ] **Step 1: Append the mirror structs and conversion to `src/input/json.rs`** (above the test module — add the test module in step 2)

```rust
use aws_sdk_s3::types::{
    AbortIncompleteMultipartUpload, BucketLifecycleConfiguration, ExpirationStatus,
    LifecycleExpiration, LifecycleRule, LifecycleRuleAndOperator, LifecycleRuleFilter,
    NoncurrentVersionExpiration, NoncurrentVersionTransition, Tag as SdkTag, Transition,
    TransitionStorageClass,
};
use aws_smithy_types::DateTime;
use serde::Deserialize;

/// Mirror of `BucketLifecycleConfiguration` for the AWS-CLI input shape.
/// Top-level wrapper for `put-bucket-lifecycle-configuration` input JSON.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleConfigurationJson {
    pub Rules: Vec<LifecycleRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleJson {
    pub ID: Option<String>,
    /// `Enabled` or `Disabled`. SDK rejects other values at builder time.
    pub Status: String,
    /// Deprecated S3 field, kept for AWS-CLI shape parity.
    pub Prefix: Option<String>,
    pub Filter: Option<LifecycleRuleFilterJson>,
    pub Expiration: Option<LifecycleExpirationJson>,
    pub NoncurrentVersionExpiration: Option<NoncurrentVersionExpirationJson>,
    pub Transitions: Option<Vec<TransitionJson>>,
    pub NoncurrentVersionTransitions: Option<Vec<NoncurrentVersionTransitionJson>>,
    pub AbortIncompleteMultipartUpload: Option<AbortIncompleteMultipartUploadJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleFilterJson {
    pub Prefix: Option<String>,
    pub Tag: Option<TagJson>,
    pub And: Option<LifecycleRuleAndOperatorJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleRuleAndOperatorJson {
    pub Prefix: Option<String>,
    pub Tags: Option<Vec<TagJson>>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TagJson {
    pub Key: String,
    pub Value: String,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct LifecycleExpirationJson {
    /// RFC3339 timestamp.
    pub Date: Option<String>,
    pub Days: Option<i32>,
    pub ExpiredObjectDeleteMarker: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionExpirationJson {
    pub NoncurrentDays: Option<i32>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct TransitionJson {
    pub Date: Option<String>,
    pub Days: Option<i32>,
    pub StorageClass: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct NoncurrentVersionTransitionJson {
    pub NoncurrentDays: Option<i32>,
    pub StorageClass: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct AbortIncompleteMultipartUploadJson {
    pub DaysAfterInitiation: Option<i32>,
}

impl LifecycleConfigurationJson {
    /// Build the SDK `BucketLifecycleConfiguration`. Returns the SDK builder
    /// error verbatim so error messages match what the AWS CLI would emit.
    pub fn into_sdk(self) -> Result<BucketLifecycleConfiguration> {
        let rules: Result<Vec<LifecycleRule>> =
            self.Rules.into_iter().map(LifecycleRuleJson::into_sdk).collect();
        Ok(BucketLifecycleConfiguration::builder()
            .set_rules(Some(rules?))
            .build()?)
    }
}

impl LifecycleRuleJson {
    fn into_sdk(self) -> Result<LifecycleRule> {
        let mut b = LifecycleRule::builder()
            .status(self.Status.parse::<ExpirationStatus>().unwrap_or_else(ExpirationStatus::from));
        if let Some(id) = self.ID {
            b = b.id(id);
        }
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(f) = self.Filter {
            b = b.filter(f.into_sdk()?);
        }
        if let Some(e) = self.Expiration {
            b = b.expiration(e.into_sdk()?);
        }
        if let Some(n) = self.NoncurrentVersionExpiration {
            b = b.noncurrent_version_expiration(n.into_sdk());
        }
        if let Some(ts) = self.Transitions {
            for t in ts {
                b = b.transitions(t.into_sdk()?);
            }
        }
        if let Some(nts) = self.NoncurrentVersionTransitions {
            for n in nts {
                b = b.noncurrent_version_transitions(n.into_sdk());
            }
        }
        if let Some(a) = self.AbortIncompleteMultipartUpload {
            b = b.abort_incomplete_multipart_upload(a.into_sdk());
        }
        Ok(b.build()?)
    }
}

impl LifecycleRuleFilterJson {
    fn into_sdk(self) -> Result<LifecycleRuleFilter> {
        // S3 lifecycle Filter is a one-of (Prefix XOR Tag XOR And); when
        // multiple are supplied, the SDK builder accepts the last setter.
        // We honour AWS-CLI semantics by passing values as-is and letting
        // S3 reject bad combinations.
        let mut b = LifecycleRuleFilter::builder();
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(t) = self.Tag {
            b = b.tag(SdkTag::builder().key(t.Key).value(t.Value).build()?);
        }
        if let Some(and) = self.And {
            b = b.and(and.into_sdk()?);
        }
        Ok(b.build())
    }
}

impl LifecycleRuleAndOperatorJson {
    fn into_sdk(self) -> Result<LifecycleRuleAndOperator> {
        let mut b = LifecycleRuleAndOperator::builder();
        if let Some(p) = self.Prefix {
            b = b.prefix(p);
        }
        if let Some(tags) = self.Tags {
            for t in tags {
                b = b.tags(SdkTag::builder().key(t.Key).value(t.Value).build()?);
            }
        }
        Ok(b.build()?)
    }
}

impl LifecycleExpirationJson {
    fn into_sdk(self) -> Result<LifecycleExpiration> {
        let mut b = LifecycleExpiration::builder();
        if let Some(d) = self.Date {
            b = b.date(parse_rfc3339(&d)?);
        }
        if let Some(days) = self.Days {
            b = b.days(days);
        }
        if let Some(eodm) = self.ExpiredObjectDeleteMarker {
            b = b.expired_object_delete_marker(eodm);
        }
        Ok(b.build())
    }
}

impl NoncurrentVersionExpirationJson {
    fn into_sdk(self) -> NoncurrentVersionExpiration {
        let mut b = NoncurrentVersionExpiration::builder();
        if let Some(n) = self.NoncurrentDays {
            b = b.noncurrent_days(n);
        }
        b.build()
    }
}

impl TransitionJson {
    fn into_sdk(self) -> Result<Transition> {
        let mut b = Transition::builder();
        if let Some(d) = self.Date {
            b = b.date(parse_rfc3339(&d)?);
        }
        if let Some(days) = self.Days {
            b = b.days(days);
        }
        if let Some(sc) = self.StorageClass {
            b = b.storage_class(sc.parse::<TransitionStorageClass>().unwrap_or_else(TransitionStorageClass::from));
        }
        Ok(b.build())
    }
}

impl NoncurrentVersionTransitionJson {
    fn into_sdk(self) -> NoncurrentVersionTransition {
        let mut b = NoncurrentVersionTransition::builder();
        if let Some(n) = self.NoncurrentDays {
            b = b.noncurrent_days(n);
        }
        if let Some(sc) = self.StorageClass {
            b = b.storage_class(sc.parse::<TransitionStorageClass>().unwrap_or_else(TransitionStorageClass::from));
        }
        b.build()
    }
}

impl AbortIncompleteMultipartUploadJson {
    fn into_sdk(self) -> AbortIncompleteMultipartUpload {
        let mut b = AbortIncompleteMultipartUpload::builder();
        if let Some(d) = self.DaysAfterInitiation {
            b = b.days_after_initiation(d);
        }
        b.build()
    }
}

fn parse_rfc3339(s: &str) -> Result<DateTime> {
    DateTime::from_str(s, aws_smithy_types::date_time::Format::DateTime)
        .map_err(|e| anyhow::anyhow!("invalid RFC3339 timestamp {s:?}: {e}"))
}
```

- [ ] **Step 2: Append the test module to `src/input/json.rs`**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // ----- LifecycleConfigurationJson -----

    #[test]
    fn lifecycle_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "Rules": [
            {
              "ID": "ExpireOldLogs",
              "Status": "Enabled",
              "Filter": { "Prefix": "logs/" },
              "Expiration": { "Days": 365 }
            }
          ]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).expect("parses");
        assert_eq!(parsed.Rules.len(), 1);
        assert_eq!(parsed.Rules[0].ID.as_deref(), Some("ExpireOldLogs"));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_id_and_status() {
        let json = r#"{"Rules":[{"ID":"r1","Status":"Enabled","Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let rules = cfg.rules();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0].id(), Some("r1"));
        assert_eq!(rules[0].status(), &ExpirationStatus::Enabled);
    }

    #[test]
    fn lifecycle_into_sdk_preserves_filter_prefix() {
        let json = r#"{"Rules":[{"Status":"Enabled","Filter":{"Prefix":"logs/"},"Expiration":{"Days":1}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let f = cfg.rules()[0].filter().expect("filter");
        assert_eq!(f.prefix(), Some("logs/"));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_filter_and_with_tags() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Filter":{"And":{"Prefix":"x/","Tags":[{"Key":"a","Value":"1"}]}},
            "Expiration":{"Days":1}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let and = cfg.rules()[0].filter().unwrap().and().expect("and");
        assert_eq!(and.prefix(), Some("x/"));
        assert_eq!(and.tags().len(), 1);
        assert_eq!(and.tags()[0].key(), "a");
        assert_eq!(and.tags()[0].value(), "1");
    }

    #[test]
    fn lifecycle_into_sdk_preserves_transitions() {
        let json = r#"{
          "Rules":[{
            "Status":"Enabled",
            "Transitions":[{"Days":30,"StorageClass":"GLACIER"}],
            "Expiration":{"Days":365}
          }]
        }"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let t = &cfg.rules()[0].transitions()[0];
        assert_eq!(t.days(), Some(30));
        assert_eq!(t.storage_class(), Some(&TransitionStorageClass::Glacier));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_noncurrent_version_expiration() {
        let json = r#"{"Rules":[{"Status":"Enabled","NoncurrentVersionExpiration":{"NoncurrentDays":7}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let n = cfg.rules()[0]
            .noncurrent_version_expiration()
            .expect("noncurrent version expiration");
        assert_eq!(n.noncurrent_days(), Some(7));
    }

    #[test]
    fn lifecycle_into_sdk_preserves_abort_incomplete_multipart_upload() {
        let json = r#"{"Rules":[{"Status":"Enabled","AbortIncompleteMultipartUpload":{"DaysAfterInitiation":3}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let a = cfg.rules()[0]
            .abort_incomplete_multipart_upload()
            .expect("abort");
        assert_eq!(a.days_after_initiation(), Some(3));
    }

    #[test]
    fn lifecycle_invalid_json_errors() {
        let res: Result<LifecycleConfigurationJson, _> = serde_json::from_str("{not json");
        assert!(res.is_err());
    }

    #[test]
    fn lifecycle_missing_rules_errors() {
        let res: Result<LifecycleConfigurationJson, _> = serde_json::from_str("{}");
        assert!(res.is_err(), "missing required `Rules` must error");
    }

    #[test]
    fn lifecycle_invalid_date_errors_at_into_sdk() {
        let json = r#"{"Rules":[{"Status":"Enabled","Expiration":{"Date":"not-a-date"}}]}"#;
        let parsed: LifecycleConfigurationJson = serde_json::from_str(json).unwrap();
        let res = parsed.into_sdk();
        assert!(res.is_err(), "invalid date must error at into_sdk()");
    }
}
```

- [ ] **Step 3: Run the new tests to verify they pass**

Run: `cargo test --lib input::json::tests::lifecycle`

Expected: 10 tests passing.

If the tests fail because the SDK type signatures (e.g. `BucketLifecycleConfiguration::builder().build()`) differ from what's shown above, adjust the `.build()` / `.build()?` calls to match the SDK's `Result<T, BuildError>` shape — recent `aws-sdk-s3` versions vary. Run `cargo doc --open -p aws-sdk-s3` to check the local-version signatures if needed.

- [ ] **Step 4: Verify no clippy regressions**

Run: `cargo clippy --all-features --lib -- -D warnings 2>&1 | grep -i "input::json"`

Expected: no output (no warnings in `src/input/json.rs`).

Do **not** commit yet — defer to the family-end commit (Task 9).

---

### Task 3: Add `get_bucket_lifecycle_configuration_to_json` to `src/output/json.rs` + tests

**Files:**
- Modify: `src/output/json.rs` (append a new function and tests)

- [ ] **Step 1: Add the import to the existing `use` block at the top of `src/output/json.rs`**

```rust
use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
```

- [ ] **Step 2: Append the serialiser function before the existing `#[cfg(test)] mod tests {` block**

```rust
/// Serialise a `GetBucketLifecycleConfigurationOutput` to AWS CLI v2
/// `--output json` shape.
///
/// Top level: `{"Rules": [ … ]}` (always emits `Rules`, as `[]` if empty).
/// Each rule emits its present fields with PascalCase keys; absent fields
/// are omitted (never `null`).
pub fn get_bucket_lifecycle_configuration_to_json(
    out: &GetBucketLifecycleConfigurationOutput,
) -> Value {
    let mut map = Map::new();
    let rules: Vec<Value> = out.rules().iter().map(serialize_lifecycle_rule).collect();
    map.insert("Rules".to_string(), Value::Array(rules));
    Value::Object(map)
}

fn serialize_lifecycle_rule(r: &aws_sdk_s3::types::LifecycleRule) -> Value {
    let mut m = Map::new();
    if let Some(id) = r.id() {
        m.insert("ID".to_string(), Value::String(id.to_string()));
    }
    m.insert(
        "Status".to_string(),
        Value::String(r.status().as_str().to_string()),
    );
    if let Some(p) = r.prefix() {
        m.insert("Prefix".to_string(), Value::String(p.to_string()));
    }
    if let Some(f) = r.filter() {
        m.insert("Filter".to_string(), serialize_lifecycle_filter(f));
    }
    if let Some(e) = r.expiration() {
        m.insert("Expiration".to_string(), serialize_lifecycle_expiration(e));
    }
    if let Some(n) = r.noncurrent_version_expiration() {
        let mut nm = Map::new();
        if let Some(d) = n.noncurrent_days() {
            nm.insert(
                "NoncurrentDays".to_string(),
                Value::Number(serde_json::Number::from(d)),
            );
        }
        m.insert(
            "NoncurrentVersionExpiration".to_string(),
            Value::Object(nm),
        );
    }
    if !r.transitions().is_empty() {
        let arr: Vec<Value> = r
            .transitions()
            .iter()
            .map(serialize_transition)
            .collect();
        m.insert("Transitions".to_string(), Value::Array(arr));
    }
    if !r.noncurrent_version_transitions().is_empty() {
        let arr: Vec<Value> = r
            .noncurrent_version_transitions()
            .iter()
            .map(|n| {
                let mut nm = Map::new();
                if let Some(d) = n.noncurrent_days() {
                    nm.insert(
                        "NoncurrentDays".to_string(),
                        Value::Number(serde_json::Number::from(d)),
                    );
                }
                if let Some(sc) = n.storage_class() {
                    nm.insert(
                        "StorageClass".to_string(),
                        Value::String(sc.as_str().to_string()),
                    );
                }
                Value::Object(nm)
            })
            .collect();
        m.insert(
            "NoncurrentVersionTransitions".to_string(),
            Value::Array(arr),
        );
    }
    if let Some(a) = r.abort_incomplete_multipart_upload() {
        let mut am = Map::new();
        if let Some(d) = a.days_after_initiation() {
            am.insert(
                "DaysAfterInitiation".to_string(),
                Value::Number(serde_json::Number::from(d)),
            );
        }
        m.insert(
            "AbortIncompleteMultipartUpload".to_string(),
            Value::Object(am),
        );
    }
    Value::Object(m)
}

fn serialize_lifecycle_filter(f: &aws_sdk_s3::types::LifecycleRuleFilter) -> Value {
    let mut m = Map::new();
    if let Some(p) = f.prefix() {
        m.insert("Prefix".to_string(), Value::String(p.to_string()));
    }
    if let Some(t) = f.tag() {
        let mut tm = Map::new();
        tm.insert("Key".to_string(), Value::String(t.key().to_string()));
        tm.insert("Value".to_string(), Value::String(t.value().to_string()));
        m.insert("Tag".to_string(), Value::Object(tm));
    }
    if let Some(and) = f.and() {
        let mut am = Map::new();
        if let Some(p) = and.prefix() {
            am.insert("Prefix".to_string(), Value::String(p.to_string()));
        }
        if !and.tags().is_empty() {
            let arr: Vec<Value> = and
                .tags()
                .iter()
                .map(|t| {
                    let mut tm = Map::new();
                    tm.insert("Key".to_string(), Value::String(t.key().to_string()));
                    tm.insert("Value".to_string(), Value::String(t.value().to_string()));
                    Value::Object(tm)
                })
                .collect();
            am.insert("Tags".to_string(), Value::Array(arr));
        }
        m.insert("And".to_string(), Value::Object(am));
    }
    Value::Object(m)
}

fn serialize_lifecycle_expiration(e: &aws_sdk_s3::types::LifecycleExpiration) -> Value {
    let mut m = Map::new();
    if let Some(d) = e.date() {
        if let Ok(dt) = d.to_chrono_utc() {
            m.insert("Date".to_string(), Value::String(dt.to_rfc3339()));
        }
    }
    if let Some(d) = e.days() {
        m.insert("Days".to_string(), Value::Number(serde_json::Number::from(d)));
    }
    if let Some(eodm) = e.expired_object_delete_marker() {
        m.insert("ExpiredObjectDeleteMarker".to_string(), Value::Bool(eodm));
    }
    Value::Object(m)
}

fn serialize_transition(t: &aws_sdk_s3::types::Transition) -> Value {
    let mut m = Map::new();
    if let Some(d) = t.date() {
        if let Ok(dt) = d.to_chrono_utc() {
            m.insert("Date".to_string(), Value::String(dt.to_rfc3339()));
        }
    }
    if let Some(d) = t.days() {
        m.insert("Days".to_string(), Value::Number(serde_json::Number::from(d)));
    }
    if let Some(sc) = t.storage_class() {
        m.insert(
            "StorageClass".to_string(),
            Value::String(sc.as_str().to_string()),
        );
    }
    Value::Object(m)
}
```

- [ ] **Step 3: Append unit tests inside the existing `#[cfg(test)] mod tests` block** (add after the head_object tests)

```rust
    // ----- get_bucket_lifecycle_configuration_to_json -----

    #[test]
    fn get_bucket_lifecycle_configuration_empty_rules_yields_empty_array() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .set_rules(Some(vec![]))
            .build()
            .unwrap();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(json["Rules"], Value::Array(vec![]));
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_minimal_rule() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule};
        let rule = LifecycleRule::builder()
            .id("r1")
            .status(ExpirationStatus::Enabled)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build()
            .unwrap();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        let rules = json["Rules"].as_array().unwrap();
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["ID"], Value::String("r1".into()));
        assert_eq!(rules[0]["Status"], Value::String("Enabled".into()));
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_filter_prefix() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleRule, LifecycleRuleFilter};
        let filter = LifecycleRuleFilter::builder().prefix("logs/").build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .filter(filter)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build()
            .unwrap();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Filter"]["Prefix"],
            Value::String("logs/".into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_expiration_days() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{ExpirationStatus, LifecycleExpiration, LifecycleRule};
        let exp = LifecycleExpiration::builder().days(365).build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .expiration(exp)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build()
            .unwrap();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Expiration"]["Days"],
            Value::Number(365i32.into())
        );
    }

    #[test]
    fn get_bucket_lifecycle_configuration_with_transitions() {
        use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
        use aws_sdk_s3::types::{
            ExpirationStatus, LifecycleRule, Transition, TransitionStorageClass,
        };
        let t = Transition::builder()
            .days(30)
            .storage_class(TransitionStorageClass::Glacier)
            .build();
        let rule = LifecycleRule::builder()
            .status(ExpirationStatus::Enabled)
            .transitions(t)
            .build()
            .unwrap();
        let out = GetBucketLifecycleConfigurationOutput::builder()
            .rules(rule)
            .build()
            .unwrap();
        let json = get_bucket_lifecycle_configuration_to_json(&out);
        assert_eq!(
            json["Rules"][0]["Transitions"][0]["StorageClass"],
            Value::String("GLACIER".into())
        );
    }
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test --lib output::json::tests::get_bucket_lifecycle`

Expected: 5 tests passing.

If the SDK signature for `LifecycleRuleFilter::builder().build()` differs (returns `Result` vs. plain), adapt the `.build()` calls.

---

### Task 4: Add lifecycle API wrappers + pinned NOT_FOUND_CODES test

**Files:**
- Modify: `src/storage/s3/api.rs`

- [ ] **Step 1: Add SDK operation imports near the top of `src/storage/s3/api.rs`** (next to existing `delete_bucket_policy`/etc. imports)

```rust
use aws_sdk_s3::operation::delete_bucket_lifecycle::DeleteBucketLifecycleOutput;
use aws_sdk_s3::operation::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationOutput;
use aws_sdk_s3::operation::put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationOutput;
use aws_sdk_s3::types::BucketLifecycleConfiguration;
```

- [ ] **Step 2: Add the NOT_FOUND_CODES constant** (next to the other `GET_*_NOT_FOUND_CODES` consts)

```rust
/// S3 error codes that `get-bucket-lifecycle-configuration` treats as a
/// subresource NotFound. `NoSuchLifecycleConfiguration` covers the case
/// where the bucket exists but no lifecycle rules are configured.
const GET_BUCKET_LIFECYCLE_CONFIGURATION_NOT_FOUND_CODES: &[&str] =
    &["NoSuchLifecycleConfiguration"];
```

- [ ] **Step 3: Add the three wrapper functions** (next to `get_bucket_policy`/`put_bucket_policy`/`delete_bucket_policy`)

```rust
/// Issue `GetBucketLifecycleConfiguration` for `bucket`. Returns the SDK
/// response on success, `HeadError::BucketNotFound` when S3 returns
/// `NoSuchBucket`, `HeadError::NotFound` when S3 returns
/// `NoSuchLifecycleConfiguration` (the bucket exists but no lifecycle
/// rules are configured), and `HeadError::Other` for any other failure.
pub async fn get_bucket_lifecycle_configuration(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketLifecycleConfigurationOutput, HeadError> {
    client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            let code = e
                .as_service_error()
                .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
            match classify_not_found(code, GET_BUCKET_LIFECYCLE_CONFIGURATION_NOT_FOUND_CODES) {
                Some(he) => he,
                None => HeadError::Other(
                    anyhow::Error::new(e).context(format!(
                        "get-bucket-lifecycle-configuration on s3://{bucket}"
                    )),
                ),
            }
        })
}

/// Issue `PutBucketLifecycleConfiguration` for `bucket` with the given
/// configuration. Returns the SDK response on success.
pub async fn put_bucket_lifecycle_configuration(
    client: &Client,
    bucket: &str,
    cfg: BucketLifecycleConfiguration,
) -> Result<PutBucketLifecycleConfigurationOutput> {
    client
        .put_bucket_lifecycle_configuration()
        .bucket(bucket)
        .lifecycle_configuration(cfg)
        .send()
        .await
        .with_context(|| format!("put-bucket-lifecycle-configuration on s3://{bucket}"))
}

/// Issue `DeleteBucketLifecycle` for `bucket`. Returns the SDK response on
/// success.
///
/// Wrapped under the symmetric CLI name `delete-bucket-lifecycle-configuration`.
/// AWS CLI uses the asymmetric `delete-bucket-lifecycle`; we choose symmetry
/// for predictability with the `get-` / `put-` siblings.
pub async fn delete_bucket_lifecycle_configuration(
    client: &Client,
    bucket: &str,
) -> Result<DeleteBucketLifecycleOutput> {
    client
        .delete_bucket_lifecycle()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket-lifecycle-configuration on s3://{bucket}"))
}
```

- [ ] **Step 4: Add the pinned-codes test inside the existing `#[cfg(test)] mod tests`** (after the other `*_not_found_codes_pinned` tests)

```rust
    #[test]
    fn get_bucket_lifecycle_configuration_not_found_codes_pinned() {
        assert_eq!(
            GET_BUCKET_LIFECYCLE_CONFIGURATION_NOT_FOUND_CODES,
            &["NoSuchLifecycleConfiguration"]
        );
    }
```

- [ ] **Step 5: Verify tests pass**

Run: `cargo test --lib storage::s3::api::tests::get_bucket_lifecycle`

Expected: 1 test passing.

Run: `cargo check --all-features`

Expected: clean.

---

### Task 5: Add the three lifecycle args structs + their tests

**Files:**
- Create: `src/config/args/get_bucket_lifecycle_configuration.rs`
- Create: `src/config/args/put_bucket_lifecycle_configuration.rs`
- Create: `src/config/args/delete_bucket_lifecycle_configuration.rs`

For each, copy the analogous policy file verbatim and substitute the family name in the constants and command names (no logic differences).

- [ ] **Step 1: Create `src/config/args/get_bucket_lifecycle_configuration.rs`**

```rust
use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str =
    "get-bucket-lifecycle-configuration target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "get-bucket-lifecycle-configuration target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct GetBucketLifecycleConfigurationArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl GetBucketLifecycleConfigurationArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    pub fn bucket_name(&self) -> Result<String, String> {
        let raw = self
            .target
            .as_deref()
            .ok_or_else(|| TARGET_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if !prefix.is_empty() {
                    return Err(TARGET_HAS_KEY_OR_PREFIX.to_string());
                }
                Ok(bucket)
            }
            _ => Err(TARGET_NOT_S3.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(name = "test")]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestSub,
    }

    #[derive(clap::Subcommand, Debug)]
    enum TestSub {
        GetBucketLifecycleConfiguration(GetBucketLifecycleConfigurationArgs),
    }

    fn parse(args: &[&str]) -> GetBucketLifecycleConfigurationArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::GetBucketLifecycleConfiguration(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_bucket_only_path() {
        let a = parse(&["test", "get-bucket-lifecycle-configuration", "s3://my-bucket"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn accepts_bucket_with_trailing_slash() {
        let a = parse(&["test", "get-bucket-lifecycle-configuration", "s3://my-bucket/"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&["test", "get-bucket-lifecycle-configuration", "s3://my-bucket/key"]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "get-bucket-lifecycle-configuration",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
```

- [ ] **Step 2: Create `src/config/args/put_bucket_lifecycle_configuration.rs`**

```rust
use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str =
    "put-bucket-lifecycle-configuration target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "put-bucket-lifecycle-configuration target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct PutBucketLifecycleConfigurationArgs {
    #[arg(
        env = "TARGET",
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    /// Path to a file containing the lifecycle-configuration JSON (AWS-CLI
    /// shape, top-level `Rules` array), or `-` to read from stdin.
    #[arg(env = "LIFECYCLE_CONFIGURATION", required_unless_present = "auto_complete_shell")]
    pub lifecycle_configuration: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl PutBucketLifecycleConfigurationArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    pub fn bucket_name(&self) -> Result<String, String> {
        let raw = self
            .target
            .as_deref()
            .ok_or_else(|| TARGET_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if !prefix.is_empty() {
                    return Err(TARGET_HAS_KEY_OR_PREFIX.to_string());
                }
                Ok(bucket)
            }
            _ => Err(TARGET_NOT_S3.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(name = "test")]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestSub,
    }

    #[derive(clap::Subcommand, Debug)]
    enum TestSub {
        PutBucketLifecycleConfiguration(PutBucketLifecycleConfigurationArgs),
    }

    fn parse(args: &[&str]) -> PutBucketLifecycleConfigurationArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::PutBucketLifecycleConfiguration(a) = cli.cmd;
        a
    }

    fn try_parse(args: &[&str]) -> Result<PutBucketLifecycleConfigurationArgs, clap::Error> {
        let cli = TestCli::try_parse_from(args)?;
        let TestSub::PutBucketLifecycleConfiguration(a) = cli.cmd;
        Ok(a)
    }

    #[test]
    fn accepts_bucket_and_file_path() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket",
            "/tmp/lifecycle.json",
        ]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
        assert_eq!(a.lifecycle_configuration.as_deref(), Some("/tmp/lifecycle.json"));
    }

    #[test]
    fn accepts_bucket_and_stdin_dash() {
        let a = parse(&["test", "put-bucket-lifecycle-configuration", "s3://my-bucket", "-"]);
        assert_eq!(a.lifecycle_configuration.as_deref(), Some("-"));
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "s3://my-bucket/key",
            "/tmp/x.json",
        ]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn missing_config_positional_errors() {
        let res = try_parse(&["test", "put-bucket-lifecycle-configuration", "s3://my-bucket"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_both_positionals_errors() {
        let res = try_parse(&["test", "put-bucket-lifecycle-configuration"]);
        assert!(res.is_err());
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "put-bucket-lifecycle-configuration",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.lifecycle_configuration.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
```

- [ ] **Step 3: Create `src/config/args/delete_bucket_lifecycle_configuration.rs`**

(Same shape as the get-* args struct but with name substituted to `DeleteBucketLifecycleConfigurationArgs` and the constants beginning with `delete-bucket-lifecycle-configuration target must be …`. Tests are the same as `delete_bucket_policy.rs`'s tests, with the command renamed.)

```rust
use crate::config::args::common_client::CommonClientArgs;
use crate::config::args::value_parser::storage_path;
use crate::types::StoragePath;
use clap::Parser;

const TARGET_NOT_S3: &str =
    "delete-bucket-lifecycle-configuration target must be s3://<BUCKET>\n";
const TARGET_HAS_KEY_OR_PREFIX: &str =
    "delete-bucket-lifecycle-configuration target must be s3://<BUCKET> with no key or prefix\n";

#[derive(Parser, Clone, Debug)]
pub struct DeleteBucketLifecycleConfigurationArgs {
    #[arg(
        env,
        help = "s3://<BUCKET_NAME>",
        value_parser = storage_path::check_storage_path,
        required_unless_present = "auto_complete_shell"
    )]
    pub target: Option<String>,

    #[command(flatten)]
    pub common: CommonClientArgs,
}

impl DeleteBucketLifecycleConfigurationArgs {
    pub fn auto_complete_shell(&self) -> Option<clap_complete::shells::Shell> {
        self.common.auto_complete_shell
    }

    pub fn bucket_name(&self) -> Result<String, String> {
        let raw = self
            .target
            .as_deref()
            .ok_or_else(|| TARGET_NOT_S3.to_string())?;
        match storage_path::parse_storage_path(raw) {
            StoragePath::S3 { bucket, prefix } => {
                if !prefix.is_empty() {
                    return Err(TARGET_HAS_KEY_OR_PREFIX.to_string());
                }
                Ok(bucket)
            }
            _ => Err(TARGET_NOT_S3.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser, Debug)]
    #[command(name = "test")]
    struct TestCli {
        #[command(subcommand)]
        cmd: TestSub,
    }

    #[derive(clap::Subcommand, Debug)]
    enum TestSub {
        DeleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationArgs),
    }

    fn parse(args: &[&str]) -> DeleteBucketLifecycleConfigurationArgs {
        let cli = TestCli::try_parse_from(args).unwrap();
        let TestSub::DeleteBucketLifecycleConfiguration(a) = cli.cmd;
        a
    }

    #[test]
    fn accepts_bucket_only_path() {
        let a = parse(&["test", "delete-bucket-lifecycle-configuration", "s3://my-bucket"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn accepts_bucket_with_trailing_slash() {
        let a = parse(&["test", "delete-bucket-lifecycle-configuration", "s3://my-bucket/"]);
        assert_eq!(a.bucket_name().unwrap(), "my-bucket");
    }

    #[test]
    fn rejects_path_with_key() {
        let a = parse(&["test", "delete-bucket-lifecycle-configuration", "s3://my-bucket/key"]);
        assert!(a.bucket_name().is_err());
    }

    #[test]
    fn rejects_local_path() {
        let res = TestCli::try_parse_from([
            "test",
            "delete-bucket-lifecycle-configuration",
            "/tmp/foo",
        ]);
        if let Ok(cli) = res {
            let TestSub::DeleteBucketLifecycleConfiguration(a) = cli.cmd;
            assert!(a.bucket_name().is_err());
        }
    }

    #[test]
    fn missing_positional_with_auto_complete_shell_is_ok() {
        let a = parse(&[
            "test",
            "delete-bucket-lifecycle-configuration",
            "--auto-complete-shell",
            "bash",
        ]);
        assert!(a.target.is_none());
        assert!(a.auto_complete_shell().is_some());
    }
}
```

- [ ] **Step 4: Verify the new tests pass (just unit tests for the new args modules)**

Run: `cargo test --lib config::args::get_bucket_lifecycle_configuration config::args::put_bucket_lifecycle_configuration config::args::delete_bucket_lifecycle_configuration`

Expected: ~14 tests across 3 modules, all passing.

The 3 modules are not yet wired into `mod.rs` — they will compile because they reference only items already in scope (`CommonClientArgs`, `storage_path`, `StoragePath`). They're not yet **discoverable** by the binary; that comes in Task 7.

---

### Task 6: Add the three lifecycle CLI runtime modules

**Files:**
- Create: `src/bin/s3util/cli/get_bucket_lifecycle_configuration.rs`
- Create: `src/bin/s3util/cli/put_bucket_lifecycle_configuration.rs`
- Create: `src/bin/s3util/cli/delete_bucket_lifecycle_configuration.rs`

- [ ] **Step 1: Create `src/bin/s3util/cli/get_bucket_lifecycle_configuration.rs`**

```rust
use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationArgs;
use s3util_rs::output::json::get_bucket_lifecycle_configuration_to_json;
use s3util_rs::storage::s3::api::{self, HeadError};

use super::ExitStatus;

/// Runtime entry for `s3util get-bucket-lifecycle-configuration s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues
/// `GetBucketLifecycleConfiguration`, and prints the response as
/// AWS-CLI-shape pretty-printed JSON followed by a newline. Returns
/// `ExitStatus::NotFound` (exit code 4) when S3 reports `NoSuchBucket`
/// (logged as "bucket … not found") or `NoSuchLifecycleConfiguration`
/// (logged as "lifecycle configuration for … not found").
pub async fn run_get_bucket_lifecycle_configuration(
    args: GetBucketLifecycleConfigurationArgs,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    match api::get_bucket_lifecycle_configuration(&client, &bucket).await {
        Ok(out) => {
            let json = get_bucket_lifecycle_configuration_to_json(&out);
            let pretty = serde_json::to_string_pretty(&json)?;
            println!("{pretty}");
            Ok(ExitStatus::Success)
        }
        Err(HeadError::BucketNotFound) => {
            tracing::error!("bucket s3://{bucket} not found");
            Ok(ExitStatus::NotFound)
        }
        Err(HeadError::NotFound) => {
            tracing::error!("lifecycle configuration for s3://{bucket} not found");
            Ok(ExitStatus::NotFound)
        }
        Err(HeadError::Other(e)) => Err(e),
    }
}
```

- [ ] **Step 2: Create `src/bin/s3util/cli/put_bucket_lifecycle_configuration.rs`**

```rust
use anyhow::{Context, Result};
use tracing::info;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationArgs;
use s3util_rs::input::json::LifecycleConfigurationJson;
use s3util_rs::storage::s3::api;

/// Runtime entry for
/// `s3util put-bucket-lifecycle-configuration s3://<BUCKET> <CONFIG_FILE|->`.
///
/// Reads the configuration JSON from a file path or stdin (`-`), parses it
/// into a `LifecycleConfigurationJson` mirror struct (AWS-CLI input shape),
/// converts to the SDK type, and issues `PutBucketLifecycleConfiguration`.
/// Exits silently on success.
pub async fn run_put_bucket_lifecycle_configuration(
    args: PutBucketLifecycleConfigurationArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;

    let config_arg = args
        .lifecycle_configuration
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("lifecycle-configuration source required"))?;

    let body = if config_arg == "-" {
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut std::io::stdin(), &mut buf)?;
        buf
    } else {
        std::fs::read_to_string(config_arg)
            .with_context(|| format!("reading lifecycle configuration from {config_arg}"))?
    };

    let parsed: LifecycleConfigurationJson = serde_json::from_str(&body)
        .with_context(|| format!("parsing JSON from {config_arg}"))?;
    let cfg = parsed.into_sdk()?;

    let client = client_config.create_client().await;
    api::put_bucket_lifecycle_configuration(&client, &bucket, cfg).await?;
    info!(bucket = %bucket, "Bucket lifecycle configuration set.");
    Ok(())
}
```

- [ ] **Step 3: Create `src/bin/s3util/cli/delete_bucket_lifecycle_configuration.rs`**

```rust
use anyhow::Result;
use tracing::info;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::delete_bucket_lifecycle_configuration::DeleteBucketLifecycleConfigurationArgs;
use s3util_rs::storage::s3::api;

/// Runtime entry for `s3util delete-bucket-lifecycle-configuration s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `DeleteBucketLifecycle`
/// (the symmetric `delete-bucket-lifecycle-configuration` CLI name wraps
/// the SDK's asymmetric `DeleteBucketLifecycle` operation), and returns
/// silently on success.
pub async fn run_delete_bucket_lifecycle_configuration(
    args: DeleteBucketLifecycleConfigurationArgs,
    client_config: ClientConfig,
) -> Result<()> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    api::delete_bucket_lifecycle_configuration(&client, &bucket).await?;
    info!(bucket = %bucket, "Bucket lifecycle configuration deleted.");
    Ok(())
}
```

- [ ] **Step 4: Verify these compile (cargo will warn about unused modules until Task 7 wires them up — that's expected)**

Run: `cargo check`

Expected: clean (the new files are valid `mod` files but not yet `pub mod`-imported).

---

### Task 7: Wire lifecycle commands into args/mod.rs, cli/mod.rs, and main.rs

**Files:**
- Modify: `src/config/args/mod.rs`
- Modify: `src/bin/s3util/cli/mod.rs`
- Modify: `src/bin/s3util/main.rs`

- [ ] **Step 1: In `src/config/args/mod.rs`, add the three `pub mod` declarations** (alphabetical insertion)

After `pub mod delete_bucket_policy;`:
```rust
pub mod delete_bucket_lifecycle_configuration;
```
After `pub mod get_bucket_policy;`:
```rust
pub mod get_bucket_lifecycle_configuration;
```
After `pub mod put_bucket_policy;`:
```rust
pub mod put_bucket_lifecycle_configuration;
```

- [ ] **Step 2: Add the three `pub use` re-exports** (alphabetical, next to existing siblings)

```rust
pub use delete_bucket_lifecycle_configuration::DeleteBucketLifecycleConfigurationArgs;
pub use get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationArgs;
pub use put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationArgs;
```

- [ ] **Step 3: Add three `Commands` enum variants** (with `display_order` slotting in after the existing 17/18 versioning entries — existing pattern is put → get → delete, e.g. PutBucketPolicy=11, GetBucketPolicy=12, DeleteBucketPolicy=13)

```rust
    /// Delete the lifecycle configuration from an S3 bucket
    #[command(display_order = 21)]
    DeleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationArgs),
    /// Retrieve the lifecycle configuration of an S3 bucket and print it as JSON
    #[command(display_order = 20)]
    GetBucketLifecycleConfiguration(GetBucketLifecycleConfigurationArgs),
    /// Set the lifecycle configuration on an S3 bucket
    #[command(display_order = 19)]
    PutBucketLifecycleConfiguration(PutBucketLifecycleConfigurationArgs),
```

- [ ] **Step 4: Add three matching arms in `build_config_from_args`** (next to the policy `Err(...)` arms, returning the standard "dispatched in main.rs" error)

```rust
        Commands::DeleteBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
```

- [ ] **Step 5: In `src/bin/s3util/cli/mod.rs`, add the three `pub mod` and `pub use`** (alphabetical, mirroring step 1/2)

```rust
pub mod delete_bucket_lifecycle_configuration;
pub mod get_bucket_lifecycle_configuration;
pub mod put_bucket_lifecycle_configuration;
…
pub use delete_bucket_lifecycle_configuration::run_delete_bucket_lifecycle_configuration;
pub use get_bucket_lifecycle_configuration::run_get_bucket_lifecycle_configuration;
pub use put_bucket_lifecycle_configuration::run_put_bucket_lifecycle_configuration;
```

- [ ] **Step 6: In `src/bin/s3util/main.rs`, add three new dispatch arms**

Add these inside the existing `match cli_args.command { … }` block, identical in shape to the corresponding policy arms (with `?` capitalisation and exit-code wiring). Place them next to the policy arms.

Get arm:
```rust
        Commands::GetBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_lifecycle_configuration(args, client_config)
                .await
            {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
```

Put arm:
```rust
        Commands::PutBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_lifecycle_configuration(args, client_config)
                .await
            {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
```

Delete arm:
```rust
        Commands::DeleteBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_lifecycle_configuration(
                args,
                client_config,
            )
            .await
            {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
```

- [ ] **Step 7: Verify the binary compiles**

Run: `cargo build --bin s3util`

Expected: clean.

- [ ] **Step 8: Sanity-check CLI exposure**

Run: `cargo run --bin s3util -- --help`

Expected: the three new commands appear in the help output. Exit code 0.

Run: `cargo run --bin s3util -- get-bucket-lifecycle-configuration --help`

Expected: help text for the get command, including `AWS Configuration`, `Retry Options`, `Timeout Options` sections.

---

### Task 8: Add process-level CLI tests for the three lifecycle commands

**Files:**
- Create: `tests/cli_get_bucket_lifecycle_configuration.rs`
- Create: `tests/cli_put_bucket_lifecycle_configuration.rs`
- Create: `tests/cli_delete_bucket_lifecycle_configuration.rs`

These mirror `tests/cli_get_bucket_policy.rs`, `tests/cli_put_bucket_policy.rs`, and `tests/cli_delete_bucket_policy.rs` exactly, with the command name substituted.

- [ ] **Step 1: Create `tests/cli_get_bucket_lifecycle_configuration.rs`**

```rust
//! Process-level CLI tests for the `get-bucket-lifecycle-configuration` subcommand.
//! These run without AWS credentials or network access.

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

fn run(cmd: &mut Command) -> (bool, String, String, Option<i32>) {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (
        output.status.success(),
        stdout,
        stderr,
        output.status.code(),
    )
}

#[test]
fn help_succeeds_and_lists_option_groups() {
    let (ok, stdout, _stderr, _code) =
        run(s3util().args(["get-bucket-lifecycle-configuration", "--help"]));
    assert!(ok, "get-bucket-lifecycle-configuration --help must succeed");
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("get-bucket-lifecycle-configuration"));
    assert!(!ok);
    assert_eq!(
        code,
        Some(2),
        "clap missing-arg should exit 2; stderr: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"),
        "expected 'required' or 'usage' in stderr; got: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "get-bucket-lifecycle-configuration",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(ok, "auto-complete-shell must succeed without a target");
    assert!(stdout.contains("_s3util"));
}

#[test]
fn bucket_with_key_exits_1() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "get-bucket-lifecycle-configuration",
        "s3://example/key",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "bucket path with key should exit 1 (validation)"
    );
    assert!(
        !stderr.is_empty(),
        "should have an error message on stderr; got empty"
    );
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "get-bucket-lifecycle-configuration",
        "s3://example",
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key"),
        "expected clap error about missing secret key; got: {stderr}"
    );
}
```

- [ ] **Step 2: Create `tests/cli_put_bucket_lifecycle_configuration.rs`**

```rust
//! Process-level CLI tests for the `put-bucket-lifecycle-configuration` subcommand.
//! These run without AWS credentials or network access.

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

fn run(cmd: &mut Command) -> (bool, String, String, Option<i32>) {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (
        output.status.success(),
        stdout,
        stderr,
        output.status.code(),
    )
}

#[test]
fn help_shows_both_positionals() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "--help",
    ]));
    assert!(ok, "put-bucket-lifecycle-configuration --help must succeed");
    assert!(
        stdout.contains("TARGET") || stdout.contains("BUCKET"),
        "expected TARGET or BUCKET in help; got: {stdout}"
    );
    assert!(
        stdout.contains("LIFECYCLE_CONFIGURATION"),
        "expected LIFECYCLE_CONFIGURATION in help; got: {stdout}"
    );
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn missing_both_positionals_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().arg("put-bucket-lifecycle-configuration"));
    assert!(!ok);
    assert_eq!(code, Some(2), "stderr: {stderr}");
    assert!(
        stderr.to_lowercase().contains("required") || stderr.to_lowercase().contains("usage"),
        "expected 'required' or 'usage'; got: {stderr}"
    );
}

#[test]
fn missing_config_positional_exits_2() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "s3://example-bucket",
    ]));
    assert!(!ok);
    assert_eq!(code, Some(2), "stderr: {stderr}");
}

#[test]
fn nonexistent_config_file_exits_1() {
    let (ok, _stdout, stderr, code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "s3://example-bucket",
        "/nonexistent/path/lifecycle-xyz-does-not-exist.json",
    ]));
    assert!(!ok);
    assert_eq!(
        code,
        Some(1),
        "reading non-existent file must exit 1; got {code:?}; stderr: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("no such file")
            || stderr.to_lowercase().contains("not found")
            || stderr.to_lowercase().contains("os error"),
        "expected file-not-found error in stderr; got: {stderr}"
    );
}

#[test]
fn auto_complete_shell_short_circuits_without_positionals() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(ok);
    assert!(stdout.contains("_s3util"));
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "put-bucket-lifecycle-configuration",
        "s3://example",
        tmp.path().to_str().unwrap(),
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key"),
        "expected clap error about missing secret key; got: {stderr}"
    );
}
```

- [ ] **Step 3: Create `tests/cli_delete_bucket_lifecycle_configuration.rs`**

```rust
//! Process-level CLI tests for the `delete-bucket-lifecycle-configuration` subcommand.
//! These run without AWS credentials or network access.

use std::process::{Command, Stdio};

fn s3util() -> Command {
    Command::new(env!("CARGO_BIN_EXE_s3util"))
}

fn run(cmd: &mut Command) -> (bool, String, String, Option<i32>) {
    let output = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to spawn s3util binary");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();
    (
        output.status.success(),
        stdout,
        stderr,
        output.status.code(),
    )
}

#[test]
fn help_succeeds_and_lists_option_groups() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "delete-bucket-lifecycle-configuration",
        "--help",
    ]));
    assert!(ok);
    assert!(stdout.contains("AWS Configuration"));
    assert!(stdout.contains("Retry Options"));
    assert!(stdout.contains("Timeout Options"));
}

#[test]
fn missing_target_exits_non_zero() {
    let (ok, _stdout, stderr, code) =
        run(s3util().arg("delete-bucket-lifecycle-configuration"));
    assert!(!ok);
    assert_eq!(code, Some(2), "stderr: {stderr}");
}

#[test]
fn auto_complete_shell_short_circuits_without_target() {
    let (ok, stdout, _stderr, _code) = run(s3util().args([
        "delete-bucket-lifecycle-configuration",
        "--auto-complete-shell",
        "bash",
    ]));
    assert!(ok);
    assert!(stdout.contains("_s3util"));
}

#[test]
fn target_access_key_without_secret_exits_non_zero() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "delete-bucket-lifecycle-configuration",
        "s3://example",
        "--target-access-key",
        "AKIA",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("required")
            || stderr.to_lowercase().contains("--target-secret-access-key"),
        "expected clap error; got: {stderr}"
    );
}

#[test]
fn target_no_sign_request_conflicts_with_target_profile() {
    let (ok, _stdout, stderr, _code) = run(s3util().args([
        "delete-bucket-lifecycle-configuration",
        "s3://example",
        "--target-no-sign-request",
        "--target-profile",
        "default",
    ]));
    assert!(!ok);
    assert!(
        stderr.to_lowercase().contains("cannot be used")
            || stderr.to_lowercase().contains("conflict"),
        "expected clap conflict; got: {stderr}"
    );
}
```

- [ ] **Step 4: Run the new tests**

Run: `cargo test --test cli_get_bucket_lifecycle_configuration --test cli_put_bucket_lifecycle_configuration --test cli_delete_bucket_lifecycle_configuration`

Expected: ~16 tests passing across 3 files.

---

### Task 9: Add E2E tests for the lifecycle family + verify family compiles end-to-end + commit

**Files:**
- Create: `tests/e2e_bucket_lifecycle_configuration.rs`

- [ ] **Step 1: Create `tests/e2e_bucket_lifecycle_configuration.rs`**

This file mirrors `tests/e2e_bucket_policy.rs`. The fixture is a minimal but useful lifecycle configuration (expires `logs/` after 365 days), chosen to round-trip cleanly.

```rust
#![cfg(e2e_test)]

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::process::{Command, Stdio};

    fn run_s3util(args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn s3util")
    }

    fn run_s3util_with_stdin(args: &[&str], stdin_data: &[u8]) -> std::process::Output {
        use std::io::Write;
        let mut child = Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn s3util");
        if let Some(stdin) = child.stdin.take() {
            let mut stdin = stdin;
            stdin.write_all(stdin_data).ok();
        }
        child.wait_with_output().expect("wait s3util")
    }

    fn sample_lifecycle_json() -> &'static str {
        // A minimal but useful rule: expire objects under `logs/` after 365 days.
        // S3 accepts this regardless of bucket contents — it just sets the rule.
        r#"{
          "Rules": [
            {
              "ID": "ExpireOldLogs",
              "Status": "Enabled",
              "Filter": { "Prefix": "logs/" },
              "Expiration": { "Days": 365 }
            }
          ]
        }"#
    }

    #[tokio::test]
    async fn put_get_delete_get_round_trip_via_file() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let tmp_dir = TestHelper::create_temp_dir();
        let config_file = TestHelper::create_test_file(
            &tmp_dir,
            "lifecycle.json",
            sample_lifecycle_json().as_bytes(),
        );
        let config_file_str = config_file.to_str().unwrap();

        let put_out = run_s3util(&[
            "put-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            config_file_str,
        ]);
        assert!(
            put_out.status.success(),
            "put should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&put_out.stdout).trim(),
            "",
            "put must produce no stdout"
        );

        let get_out = run_s3util(&[
            "get-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let json: serde_json::Value =
            serde_json::from_slice(&get_out.stdout).expect("get stdout must be JSON");
        let rules = json["Rules"].as_array().expect("Rules array");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["Status"], "Enabled");

        let del_out = run_s3util(&[
            "delete-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(del_out.status.success());

        let get_after_del = run_s3util(&[
            "get-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(!get_after_del.status.success());
        assert_eq!(
            get_after_del.status.code(),
            Some(4),
            "get after delete must exit 4 (NoSuchLifecycleConfiguration)"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        std::fs::remove_dir_all(&tmp_dir).ok();
    }

    #[tokio::test]
    async fn put_via_stdin_and_get_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let put_out = run_s3util_with_stdin(
            &[
                "put-bucket-lifecycle-configuration",
                "--target-profile",
                "s3util-e2e-test",
                &bucket_arg,
                "-",
            ],
            sample_lifecycle_json().as_bytes(),
        );
        assert!(
            put_out.status.success(),
            "put via stdin should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        let get_out = run_s3util(&[
            "get-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(get_out.status.success());
        let json: serde_json::Value = serde_json::from_slice(&get_out.stdout).unwrap();
        assert!(json.get("Rules").is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn get_on_bucket_without_lifecycle_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "get-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(4),
            "get on bucket without lifecycle must exit 4 (NoSuchLifecycleConfiguration)"
        );
    }

    #[tokio::test]
    async fn put_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let tmp_dir = TestHelper::create_temp_dir();
        let config_file = TestHelper::create_test_file(
            &tmp_dir,
            "lifecycle.json",
            sample_lifecycle_json().as_bytes(),
        );

        let out = run_s3util(&[
            "put-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            config_file.to_str().unwrap(),
        ]);

        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(!out.status.success());
        assert_eq!(out.status.code(), Some(1));
    }

    #[tokio::test]
    async fn get_on_nonexistent_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let out = run_s3util(&[
            "get-bucket-lifecycle-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(!out.status.success());
        assert_eq!(out.status.code(), Some(4));
    }
}
```

- [ ] **Step 2: Verify the E2E file compiles** (per CLAUDE.md, do NOT run E2E tests)

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests`

Expected: clean compile.

- [ ] **Step 3: Run the full non-E2E test suite to make sure the family-wide changes haven't broken anything**

Run: `cargo test`

Expected: all tests pass; new tests appear in the output.

- [ ] **Step 4: Run cargo fmt and clippy**

Run: `cargo fmt --check`
Expected: no diff. (If diff, run `cargo fmt`.)

Run: `cargo clippy --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 5: PAUSE — ask user to commit lifecycle family**

Show user the diff and propose:

```
feat: add bucket-lifecycle-configuration subcommands (get/put/delete)

Adds get/put/delete-bucket-lifecycle-configuration subcommands wrapping
the SDK's GetBucketLifecycleConfiguration / PutBucketLifecycleConfiguration
/ DeleteBucketLifecycle operations (the symmetric CLI name for the last).
Mirror struct in src/input/json.rs handles AWS-CLI-shape JSON input.
Includes process-level CLI tests and E2E round-trip tests of the same
quantity and shape as the bucket-policy family.
```

Files staged: all the new lifecycle files plus modifications to `mod.rs` files, `main.rs`, `api.rs`, `input/json.rs`, `output/json.rs`.

---

## Section C — Encryption family

Same task structure as Section B. The deltas are listed below; otherwise follow Tasks 2–9 verbatim with the substitutions applied.

### Substitution table (encryption)

| Slot | Encryption value |
|---|---|
| Family slug (file/cmd name) | `bucket_encryption` / `bucket-encryption` |
| Args struct prefix | `BucketEncryption` (e.g. `GetBucketEncryptionArgs`) |
| Runtime fn names | `run_*_bucket_encryption` |
| Mirror struct top-level | `ServerSideEncryptionConfigurationJson` |
| Mirror inner | `ServerSideEncryptionRuleJson`, `ServerSideEncryptionByDefaultJson` |
| SDK input type | `ServerSideEncryptionConfiguration` |
| SDK builder method on the put op | `.server_side_encryption_configuration(cfg)` |
| API wrapper return for delete | `DeleteBucketEncryptionOutput` |
| API wrapper return for get | `GetBucketEncryptionOutput` |
| API wrapper return for put | `PutBucketEncryptionOutput` |
| `NOT_FOUND_CODES` const name | `GET_BUCKET_ENCRYPTION_NOT_FOUND_CODES` |
| `NOT_FOUND_CODES` value | `&["ServerSideEncryptionConfigurationNotFoundError"]` |
| Output JSON top-level wrapper key | `ServerSideEncryptionConfiguration` |
| Display order (put/get/delete) | put=22, get=23, delete=24 |
| `display_order` in `Commands` enum | as above |
| Args env var (put positional) | `SERVER_SIDE_ENCRYPTION_CONFIGURATION` |
| Args field name (put) | `server_side_encryption_configuration` |
| help string (put positional) | `LIFECYCLE_CONFIGURATION` → `SERVER_SIDE_ENCRYPTION_CONFIGURATION` |
| info!(…) message (put) | `Bucket encryption set.` |
| info!(…) message (delete) | `Bucket encryption deleted.` |
| Get NotFound log message | `encryption configuration for s3://… not found` |

### Task 10 — `ServerSideEncryptionConfigurationJson` mirror struct

(Equivalent to Task 2.) Append to `src/input/json.rs`:

```rust
use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};

/// Mirror of `ServerSideEncryptionConfiguration` for the AWS-CLI input shape.
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionConfigurationJson {
    pub Rules: Vec<ServerSideEncryptionRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ServerSideEncryptionRuleJson {
    pub ApplyServerSideEncryptionByDefault: Option<ApplyServerSideEncryptionByDefaultJson>,
    pub BucketKeyEnabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct ApplyServerSideEncryptionByDefaultJson {
    /// `AES256` or `aws:kms` or `aws:kms:dsse`.
    pub SSEAlgorithm: String,
    pub KMSMasterKeyID: Option<String>,
}

impl ServerSideEncryptionConfigurationJson {
    pub fn into_sdk(self) -> Result<ServerSideEncryptionConfiguration> {
        let rules: Result<Vec<ServerSideEncryptionRule>> = self
            .Rules
            .into_iter()
            .map(ServerSideEncryptionRuleJson::into_sdk)
            .collect();
        Ok(ServerSideEncryptionConfiguration::builder()
            .set_rules(Some(rules?))
            .build()?)
    }
}

impl ServerSideEncryptionRuleJson {
    fn into_sdk(self) -> Result<ServerSideEncryptionRule> {
        let mut b = ServerSideEncryptionRule::builder();
        if let Some(d) = self.ApplyServerSideEncryptionByDefault {
            let mut bb = ServerSideEncryptionByDefault::builder()
                .sse_algorithm(d.SSEAlgorithm.parse::<ServerSideEncryption>().unwrap_or_else(ServerSideEncryption::from));
            if let Some(k) = d.KMSMasterKeyID {
                bb = bb.kms_master_key_id(k);
            }
            b = b.apply_server_side_encryption_by_default(bb.build()?);
        }
        if let Some(bke) = self.BucketKeyEnabled {
            b = b.bucket_key_enabled(bke);
        }
        Ok(b.build())
    }
}
```

Tests (append below the lifecycle tests in `mod tests`):

```rust
    // ----- ServerSideEncryptionConfigurationJson -----

    #[test]
    fn encryption_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "Rules": [
            { "ApplyServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" } }
          ]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson =
            serde_json::from_str(json).unwrap();
        assert_eq!(parsed.Rules.len(), 1);
    }

    #[test]
    fn encryption_into_sdk_preserves_aes256() {
        let json = r#"{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}"#;
        let parsed: ServerSideEncryptionConfigurationJson =
            serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        assert_eq!(
            r.apply_server_side_encryption_by_default()
                .unwrap()
                .sse_algorithm(),
            &ServerSideEncryption::Aes256
        );
    }

    #[test]
    fn encryption_into_sdk_preserves_kms_with_key_id() {
        let json = r#"{
          "Rules":[{
            "ApplyServerSideEncryptionByDefault":{
              "SSEAlgorithm":"aws:kms",
              "KMSMasterKeyID":"arn:aws:kms:us-east-1:111111111111:key/abc"
            },
            "BucketKeyEnabled": true
          }]
        }"#;
        let parsed: ServerSideEncryptionConfigurationJson =
            serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.rules()[0];
        let d = r.apply_server_side_encryption_by_default().unwrap();
        assert_eq!(d.sse_algorithm(), &ServerSideEncryption::AwsKms);
        assert_eq!(
            d.kms_master_key_id(),
            Some("arn:aws:kms:us-east-1:111111111111:key/abc")
        );
        assert_eq!(r.bucket_key_enabled(), Some(true));
    }

    #[test]
    fn encryption_invalid_json_errors() {
        assert!(serde_json::from_str::<ServerSideEncryptionConfigurationJson>("{not json").is_err());
    }

    #[test]
    fn encryption_missing_rules_errors() {
        assert!(serde_json::from_str::<ServerSideEncryptionConfigurationJson>("{}").is_err());
    }
```

Then run: `cargo test --lib input::json::tests::encryption`

### Task 11 — `get_bucket_encryption_to_json` in output/json.rs

Add the import:
```rust
use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
```

Then the function:
```rust
/// Serialise a `GetBucketEncryptionOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"ServerSideEncryptionConfiguration": {"Rules": [ … ]}}`.
/// Mirrors `aws s3api get-bucket-encryption --output json`.
pub fn get_bucket_encryption_to_json(out: &GetBucketEncryptionOutput) -> Value {
    let mut top = Map::new();
    if let Some(cfg) = out.server_side_encryption_configuration() {
        let mut inner = Map::new();
        let rules: Vec<Value> = cfg
            .rules()
            .iter()
            .map(|r| {
                let mut rm = Map::new();
                if let Some(d) = r.apply_server_side_encryption_by_default() {
                    let mut dm = Map::new();
                    dm.insert(
                        "SSEAlgorithm".to_string(),
                        Value::String(d.sse_algorithm().as_str().to_string()),
                    );
                    if let Some(k) = d.kms_master_key_id() {
                        dm.insert(
                            "KMSMasterKeyID".to_string(),
                            Value::String(k.to_string()),
                        );
                    }
                    rm.insert(
                        "ApplyServerSideEncryptionByDefault".to_string(),
                        Value::Object(dm),
                    );
                }
                if let Some(b) = r.bucket_key_enabled() {
                    rm.insert("BucketKeyEnabled".to_string(), Value::Bool(b));
                }
                Value::Object(rm)
            })
            .collect();
        inner.insert("Rules".to_string(), Value::Array(rules));
        top.insert(
            "ServerSideEncryptionConfiguration".to_string(),
            Value::Object(inner),
        );
    }
    Value::Object(top)
}
```

Tests (append to existing `mod tests`):

```rust
    // ----- get_bucket_encryption_to_json -----

    #[test]
    fn get_bucket_encryption_empty_output_yields_empty_object() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        let out = GetBucketEncryptionOutput::builder().build();
        let json = get_bucket_encryption_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_bucket_encryption_with_aes256_rule() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            ServerSideEncryption, ServerSideEncryptionByDefault,
            ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::Aes256)
            .build()
            .unwrap();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let inner = &json["ServerSideEncryptionConfiguration"];
        assert_eq!(
            inner["Rules"][0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"],
            Value::String("AES256".into())
        );
    }

    #[test]
    fn get_bucket_encryption_with_kms_rule_includes_key_id_and_bucket_key() {
        use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
        use aws_sdk_s3::types::{
            ServerSideEncryption, ServerSideEncryptionByDefault,
            ServerSideEncryptionConfiguration, ServerSideEncryptionRule,
        };
        let d = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::AwsKms)
            .kms_master_key_id("arn:aws:kms:us-east-1:111111111111:key/abc")
            .build()
            .unwrap();
        let r = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(d)
            .bucket_key_enabled(true)
            .build();
        let cfg = ServerSideEncryptionConfiguration::builder()
            .rules(r)
            .build()
            .unwrap();
        let out = GetBucketEncryptionOutput::builder()
            .server_side_encryption_configuration(cfg)
            .build();
        let json = get_bucket_encryption_to_json(&out);
        let inner = &json["ServerSideEncryptionConfiguration"];
        assert_eq!(
            inner["Rules"][0]["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"],
            Value::String("arn:aws:kms:us-east-1:111111111111:key/abc".into())
        );
        assert_eq!(inner["Rules"][0]["BucketKeyEnabled"], Value::Bool(true));
    }
```

Run: `cargo test --lib output::json::tests::get_bucket_encryption`

### Task 12 — Encryption API wrappers + pinned NOT_FOUND_CODES test

Imports:
```rust
use aws_sdk_s3::operation::delete_bucket_encryption::DeleteBucketEncryptionOutput;
use aws_sdk_s3::operation::get_bucket_encryption::GetBucketEncryptionOutput;
use aws_sdk_s3::operation::put_bucket_encryption::PutBucketEncryptionOutput;
use aws_sdk_s3::types::ServerSideEncryptionConfiguration;
```

Constant:
```rust
const GET_BUCKET_ENCRYPTION_NOT_FOUND_CODES: &[&str] =
    &["ServerSideEncryptionConfigurationNotFoundError"];
```

Wrappers (mirror Task 4 with substitutions):
```rust
pub async fn get_bucket_encryption(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketEncryptionOutput, HeadError> {
    client
        .get_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .map_err(|e| {
            let code = e
                .as_service_error()
                .and_then(aws_smithy_types::error::metadata::ProvideErrorMetadata::code);
            match classify_not_found(code, GET_BUCKET_ENCRYPTION_NOT_FOUND_CODES) {
                Some(he) => he,
                None => HeadError::Other(
                    anyhow::Error::new(e)
                        .context(format!("get-bucket-encryption on s3://{bucket}")),
                ),
            }
        })
}

pub async fn put_bucket_encryption(
    client: &Client,
    bucket: &str,
    cfg: ServerSideEncryptionConfiguration,
) -> Result<PutBucketEncryptionOutput> {
    client
        .put_bucket_encryption()
        .bucket(bucket)
        .server_side_encryption_configuration(cfg)
        .send()
        .await
        .with_context(|| format!("put-bucket-encryption on s3://{bucket}"))
}

pub async fn delete_bucket_encryption(
    client: &Client,
    bucket: &str,
) -> Result<DeleteBucketEncryptionOutput> {
    client
        .delete_bucket_encryption()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket-encryption on s3://{bucket}"))
}
```

Pinned test:
```rust
    #[test]
    fn get_bucket_encryption_not_found_codes_pinned() {
        assert_eq!(
            GET_BUCKET_ENCRYPTION_NOT_FOUND_CODES,
            &["ServerSideEncryptionConfigurationNotFoundError"]
        );
    }
```

Run: `cargo test --lib storage::s3::api::tests::get_bucket_encryption` and `cargo check --all-features`.

### Task 13 — Encryption args structs (3 files)

Mirror Task 5 with name substitutions. Files:
- `src/config/args/get_bucket_encryption.rs`
- `src/config/args/put_bucket_encryption.rs` — second positional is `server_side_encryption_configuration` / env `SERVER_SIDE_ENCRYPTION_CONFIGURATION`
- `src/config/args/delete_bucket_encryption.rs`

(Tests follow the same shape as Task 5.)

### Task 14 — Encryption CLI runtime modules (3 files)

Mirror Task 6 with name substitutions. The put module reads body, parses `ServerSideEncryptionConfigurationJson`, calls `into_sdk()?`, dispatches `api::put_bucket_encryption`. The get module logs `"encryption configuration for s3://{bucket} not found"` for the `HeadError::NotFound` arm.

### Task 15 — Encryption wiring (args/mod.rs, cli/mod.rs, main.rs)

Mirror Task 7. Use `display_order` put=22 / get=23 / delete=24 (preserving the existing put-get-delete order). After the wiring, run `cargo build --bin s3util` and verify `--help` shows the new commands.

### Task 16 — Encryption process-level CLI tests (3 files)

Mirror Task 8 with name substitutions and `LIFECYCLE_CONFIGURATION` → `SERVER_SIDE_ENCRYPTION_CONFIGURATION` in the `help_shows_both_positionals` assertion.

### Task 17 — Encryption E2E tests + verify + commit

Create `tests/e2e_bucket_encryption.rs` mirroring Task 9. Sample fixture:

```rust
fn sample_encryption_json() -> &'static str {
    // AES256 default (no KMS key needed; works on any account/region).
    r#"{
      "Rules": [
        {
          "ApplyServerSideEncryptionByDefault": { "SSEAlgorithm": "AES256" }
        }
      ]
    }"#
}
```

Round-trip JSON-shape assertion uses `json["ServerSideEncryptionConfiguration"]["Rules"]`. The "no encryption set" case for the `get-after-delete-exits-4` and `get_on_bucket_without_encryption_exits_4` paths: note that AWS S3 has applied default encryption (AES256) on all buckets since 2023, so a bucket with no explicit configuration may still return a response. **If E2E reveals this behaviour**, adjust the test assertions to match observed reality (the user runs E2E and reports back). Document the live observation in the PR description.

`RUSTFLAGS="--cfg e2e_test" cargo check --tests` then `cargo test` then `cargo fmt --check` then `cargo clippy --all-features -- -D warnings`.

PAUSE — propose commit:
```
feat: add bucket-encryption subcommands (get/put/delete)
```

---

## Section D — CORS family

### Substitution table (cors)

| Slot | CORS value |
|---|---|
| Family slug | `bucket_cors` / `bucket-cors` |
| Args struct prefix | `BucketCors` |
| Runtime fn names | `run_*_bucket_cors` |
| Mirror struct top-level | `CorsConfigurationJson` |
| Mirror inner | `CorsRuleJson` |
| SDK input type | `CorsConfiguration` |
| SDK builder method on the put op | `.cors_configuration(cfg)` |
| `NOT_FOUND_CODES` const name | `GET_BUCKET_CORS_NOT_FOUND_CODES` |
| `NOT_FOUND_CODES` value | `&["NoSuchCORSConfiguration"]` |
| Output JSON top-level | flat `{"CORSRules": [ … ]}` (no wrapper) |
| Display order (put/get/delete) | put=25, get=26, delete=27 |
| Args env var (put) | `CORS_CONFIGURATION` |
| Args field name (put) | `cors_configuration` |
| info!(…) message (put) | `Bucket CORS configuration set.` |
| info!(…) message (delete) | `Bucket CORS configuration deleted.` |
| Get NotFound log message | `CORS configuration for s3://… not found` |

### Task 18 — `CorsConfigurationJson` mirror struct

Append to `src/input/json.rs`:

```rust
use aws_sdk_s3::types::{CorsConfiguration, CorsRule};

/// Mirror of `CorsConfiguration` for the AWS-CLI input shape.
/// Top-level field name `CORSRules` matches the AWS CLI exactly
/// (note the all-uppercase `CORS`).
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct CorsConfigurationJson {
    pub CORSRules: Vec<CorsRuleJson>,
}

#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct CorsRuleJson {
    pub ID: Option<String>,
    pub AllowedHeaders: Option<Vec<String>>,
    pub AllowedMethods: Vec<String>,
    pub AllowedOrigins: Vec<String>,
    pub ExposeHeaders: Option<Vec<String>>,
    pub MaxAgeSeconds: Option<i32>,
}

impl CorsConfigurationJson {
    pub fn into_sdk(self) -> Result<CorsConfiguration> {
        let rules: Result<Vec<CorsRule>> =
            self.CORSRules.into_iter().map(CorsRuleJson::into_sdk).collect();
        Ok(CorsConfiguration::builder()
            .set_cors_rules(Some(rules?))
            .build()?)
    }
}

impl CorsRuleJson {
    fn into_sdk(self) -> Result<CorsRule> {
        let mut b = CorsRule::builder()
            .set_allowed_methods(Some(self.AllowedMethods))
            .set_allowed_origins(Some(self.AllowedOrigins));
        if let Some(id) = self.ID {
            b = b.id(id);
        }
        if let Some(h) = self.AllowedHeaders {
            b = b.set_allowed_headers(Some(h));
        }
        if let Some(eh) = self.ExposeHeaders {
            b = b.set_expose_headers(Some(eh));
        }
        if let Some(m) = self.MaxAgeSeconds {
            b = b.max_age_seconds(m);
        }
        Ok(b.build()?)
    }
}
```

Tests (append to `mod tests`):

```rust
    // ----- CorsConfigurationJson -----

    #[test]
    fn cors_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "CORSRules": [
            {
              "AllowedMethods": ["GET", "HEAD"],
              "AllowedOrigins": ["*"],
              "AllowedHeaders": ["*"],
              "MaxAgeSeconds": 3000
            }
          ]
        }"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.CORSRules.len(), 1);
    }

    #[test]
    fn cors_into_sdk_preserves_methods_and_origins() {
        let json = r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["https://example.com"]}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.cors_rules()[0];
        assert_eq!(r.allowed_methods(), &["GET".to_string()]);
        assert_eq!(r.allowed_origins(), &["https://example.com".to_string()]);
    }

    #[test]
    fn cors_into_sdk_preserves_max_age_and_id() {
        let json = r#"{"CORSRules":[{"ID":"r1","AllowedMethods":["GET"],"AllowedOrigins":["*"],"MaxAgeSeconds":600}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        let r = &cfg.cors_rules()[0];
        assert_eq!(r.id(), Some("r1"));
        assert_eq!(r.max_age_seconds(), Some(600));
    }

    #[test]
    fn cors_into_sdk_preserves_expose_headers() {
        let json = r#"{"CORSRules":[{"AllowedMethods":["GET"],"AllowedOrigins":["*"],"ExposeHeaders":["x-amz-id-2"]}]}"#;
        let parsed: CorsConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(
            cfg.cors_rules()[0].expose_headers(),
            &["x-amz-id-2".to_string()]
        );
    }

    #[test]
    fn cors_invalid_json_errors() {
        assert!(serde_json::from_str::<CorsConfigurationJson>("{not json").is_err());
    }

    #[test]
    fn cors_missing_cors_rules_errors() {
        assert!(serde_json::from_str::<CorsConfigurationJson>("{}").is_err());
    }
```

Run: `cargo test --lib input::json::tests::cors`

### Task 19 — `get_bucket_cors_to_json`

Import:
```rust
use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
```

Function:
```rust
/// Serialise a `GetBucketCorsOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"CORSRules": [ … ]}`.
pub fn get_bucket_cors_to_json(out: &GetBucketCorsOutput) -> Value {
    let mut top = Map::new();
    let arr: Vec<Value> = out
        .cors_rules()
        .iter()
        .map(|r| {
            let mut m = Map::new();
            if let Some(id) = r.id() {
                m.insert("ID".to_string(), Value::String(id.to_string()));
            }
            if !r.allowed_headers().is_empty() {
                m.insert(
                    "AllowedHeaders".to_string(),
                    Value::Array(
                        r.allowed_headers()
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            m.insert(
                "AllowedMethods".to_string(),
                Value::Array(
                    r.allowed_methods()
                        .iter()
                        .map(|s| Value::String(s.clone()))
                        .collect(),
                ),
            );
            m.insert(
                "AllowedOrigins".to_string(),
                Value::Array(
                    r.allowed_origins()
                        .iter()
                        .map(|s| Value::String(s.clone()))
                        .collect(),
                ),
            );
            if !r.expose_headers().is_empty() {
                m.insert(
                    "ExposeHeaders".to_string(),
                    Value::Array(
                        r.expose_headers()
                            .iter()
                            .map(|s| Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            if let Some(m_age) = r.max_age_seconds() {
                m.insert(
                    "MaxAgeSeconds".to_string(),
                    Value::Number(serde_json::Number::from(m_age)),
                );
            }
            Value::Object(m)
        })
        .collect();
    top.insert("CORSRules".to_string(), Value::Array(arr));
    Value::Object(top)
}
```

Tests:
```rust
    // ----- get_bucket_cors_to_json -----

    #[test]
    fn get_bucket_cors_empty_yields_empty_array() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        let out = GetBucketCorsOutput::builder().set_cors_rules(Some(vec![])).build();
        let json = get_bucket_cors_to_json(&out);
        assert_eq!(json["CORSRules"], Value::Array(vec![]));
    }

    #[test]
    fn get_bucket_cors_with_typical_rule() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        use aws_sdk_s3::types::CorsRule;
        let r = CorsRule::builder()
            .id("r1")
            .allowed_methods("GET")
            .allowed_methods("HEAD")
            .allowed_origins("*")
            .max_age_seconds(3000)
            .build()
            .unwrap();
        let out = GetBucketCorsOutput::builder().cors_rules(r).build();
        let json = get_bucket_cors_to_json(&out);
        let r0 = &json["CORSRules"][0];
        assert_eq!(r0["ID"], Value::String("r1".into()));
        assert_eq!(
            r0["AllowedMethods"],
            Value::Array(vec!["GET".into(), "HEAD".into()])
        );
        assert_eq!(r0["AllowedOrigins"], Value::Array(vec!["*".into()]));
        assert_eq!(r0["MaxAgeSeconds"], Value::Number(3000i32.into()));
    }

    #[test]
    fn get_bucket_cors_with_allowed_and_expose_headers() {
        use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
        use aws_sdk_s3::types::CorsRule;
        let r = CorsRule::builder()
            .allowed_methods("GET")
            .allowed_origins("*")
            .allowed_headers("*")
            .expose_headers("x-amz-id-2")
            .build()
            .unwrap();
        let out = GetBucketCorsOutput::builder().cors_rules(r).build();
        let json = get_bucket_cors_to_json(&out);
        assert_eq!(
            json["CORSRules"][0]["AllowedHeaders"],
            Value::Array(vec!["*".into()])
        );
        assert_eq!(
            json["CORSRules"][0]["ExposeHeaders"],
            Value::Array(vec!["x-amz-id-2".into()])
        );
    }
```

### Task 20 — CORS API wrappers + pinned NOT_FOUND_CODES test

```rust
use aws_sdk_s3::operation::delete_bucket_cors::DeleteBucketCorsOutput;
use aws_sdk_s3::operation::get_bucket_cors::GetBucketCorsOutput;
use aws_sdk_s3::operation::put_bucket_cors::PutBucketCorsOutput;
use aws_sdk_s3::types::CorsConfiguration;

const GET_BUCKET_CORS_NOT_FOUND_CODES: &[&str] = &["NoSuchCORSConfiguration"];

pub async fn get_bucket_cors(
    client: &Client,
    bucket: &str,
) -> Result<GetBucketCorsOutput, HeadError> { /* mirror Task 4 */ }

pub async fn put_bucket_cors(
    client: &Client,
    bucket: &str,
    cfg: CorsConfiguration,
) -> Result<PutBucketCorsOutput> {
    client
        .put_bucket_cors()
        .bucket(bucket)
        .cors_configuration(cfg)
        .send()
        .await
        .with_context(|| format!("put-bucket-cors on s3://{bucket}"))
}

pub async fn delete_bucket_cors(
    client: &Client,
    bucket: &str,
) -> Result<DeleteBucketCorsOutput> {
    client
        .delete_bucket_cors()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-bucket-cors on s3://{bucket}"))
}
```

Pinned test:
```rust
    #[test]
    fn get_bucket_cors_not_found_codes_pinned() {
        assert_eq!(GET_BUCKET_CORS_NOT_FOUND_CODES, &["NoSuchCORSConfiguration"]);
    }
```

### Tasks 21–24 — Args, runtime, wiring, process-level tests

Same shape as Tasks 5–8 with the substitutions.

### Task 25 — CORS E2E tests + verify + commit

`tests/e2e_bucket_cors.rs` mirroring Task 9. Sample fixture:

```rust
fn sample_cors_json() -> &'static str {
    r#"{
      "CORSRules": [
        {
          "ID": "r1",
          "AllowedMethods": ["GET", "HEAD"],
          "AllowedOrigins": ["*"],
          "AllowedHeaders": ["*"],
          "MaxAgeSeconds": 3000
        }
      ]
    }"#
}
```

Verify, PAUSE, propose commit:
```
feat: add bucket-cors subcommands (get/put/delete)
```

---

## Section E — Public-access-block family

### Substitution table (public-access-block)

| Slot | PAB value |
|---|---|
| Family slug | `public_access_block` / `public-access-block` (note: no `bucket-` prefix; matches SDK op name) |
| Args struct prefix | `PublicAccessBlock` |
| Runtime fn names | `run_*_public_access_block` |
| Mirror struct | `PublicAccessBlockConfigurationJson` |
| SDK input type | `PublicAccessBlockConfiguration` |
| SDK builder method on the put op | `.public_access_block_configuration(cfg)` |
| `NOT_FOUND_CODES` const name | `GET_PUBLIC_ACCESS_BLOCK_NOT_FOUND_CODES` |
| `NOT_FOUND_CODES` value | `&["NoSuchPublicAccessBlockConfiguration"]` |
| Output JSON top-level wrapper key | `PublicAccessBlockConfiguration` |
| Display order (put/get/delete) | put=28, get=29, delete=30 |
| Args env var (put) | `PUBLIC_ACCESS_BLOCK_CONFIGURATION` |
| Args field name (put) | `public_access_block_configuration` |
| info!(…) message (put) | `Public access block set.` |
| info!(…) message (delete) | `Public access block deleted.` |
| Get NotFound log message | `public access block configuration for s3://… not found` |

### Task 26 — `PublicAccessBlockConfigurationJson` mirror struct

```rust
use aws_sdk_s3::types::PublicAccessBlockConfiguration;

/// Mirror of `PublicAccessBlockConfiguration` for the AWS-CLI input shape.
/// All four fields are optional in the input JSON; absent fields are
/// passed to the SDK as `Some(false)` (matching AWS CLI v2 behaviour).
#[derive(Debug, Clone, Deserialize)]
#[allow(non_snake_case)]
pub struct PublicAccessBlockConfigurationJson {
    pub BlockPublicAcls: Option<bool>,
    pub IgnorePublicAcls: Option<bool>,
    pub BlockPublicPolicy: Option<bool>,
    pub RestrictPublicBuckets: Option<bool>,
}

impl PublicAccessBlockConfigurationJson {
    pub fn into_sdk(self) -> Result<PublicAccessBlockConfiguration> {
        Ok(PublicAccessBlockConfiguration::builder()
            .block_public_acls(self.BlockPublicAcls.unwrap_or(false))
            .ignore_public_acls(self.IgnorePublicAcls.unwrap_or(false))
            .block_public_policy(self.BlockPublicPolicy.unwrap_or(false))
            .restrict_public_buckets(self.RestrictPublicBuckets.unwrap_or(false))
            .build())
    }
}
```

Tests:
```rust
    // ----- PublicAccessBlockConfigurationJson -----

    #[test]
    fn pab_parses_aws_cli_skeleton_shape() {
        let json = r#"{
          "BlockPublicAcls": true,
          "IgnorePublicAcls": true,
          "BlockPublicPolicy": true,
          "RestrictPublicBuckets": true
        }"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        assert_eq!(parsed.BlockPublicAcls, Some(true));
        assert_eq!(parsed.RestrictPublicBuckets, Some(true));
    }

    #[test]
    fn pab_into_sdk_all_true() {
        let json = r#"{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":true,"RestrictPublicBuckets":true}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(true));
        assert_eq!(cfg.ignore_public_acls(), Some(true));
        assert_eq!(cfg.block_public_policy(), Some(true));
        assert_eq!(cfg.restrict_public_buckets(), Some(true));
    }

    #[test]
    fn pab_into_sdk_absent_fields_default_to_false() {
        let json = r#"{}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(false));
        assert_eq!(cfg.ignore_public_acls(), Some(false));
        assert_eq!(cfg.block_public_policy(), Some(false));
        assert_eq!(cfg.restrict_public_buckets(), Some(false));
    }

    #[test]
    fn pab_into_sdk_partial_input() {
        let json = r#"{"BlockPublicAcls":true,"BlockPublicPolicy":true}"#;
        let parsed: PublicAccessBlockConfigurationJson = serde_json::from_str(json).unwrap();
        let cfg = parsed.into_sdk().unwrap();
        assert_eq!(cfg.block_public_acls(), Some(true));
        assert_eq!(cfg.ignore_public_acls(), Some(false));
        assert_eq!(cfg.block_public_policy(), Some(true));
        assert_eq!(cfg.restrict_public_buckets(), Some(false));
    }

    #[test]
    fn pab_invalid_json_errors() {
        assert!(serde_json::from_str::<PublicAccessBlockConfigurationJson>("{not json").is_err());
    }
```

### Task 27 — `get_public_access_block_to_json`

```rust
use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;

/// Serialise a `GetPublicAccessBlockOutput` to AWS CLI v2 `--output json` shape.
///
/// Top level: `{"PublicAccessBlockConfiguration": { … }}`.
pub fn get_public_access_block_to_json(out: &GetPublicAccessBlockOutput) -> Value {
    let mut top = Map::new();
    if let Some(c) = out.public_access_block_configuration() {
        let mut inner = Map::new();
        if let Some(v) = c.block_public_acls() {
            inner.insert("BlockPublicAcls".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.ignore_public_acls() {
            inner.insert("IgnorePublicAcls".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.block_public_policy() {
            inner.insert("BlockPublicPolicy".to_string(), Value::Bool(v));
        }
        if let Some(v) = c.restrict_public_buckets() {
            inner.insert("RestrictPublicBuckets".to_string(), Value::Bool(v));
        }
        top.insert(
            "PublicAccessBlockConfiguration".to_string(),
            Value::Object(inner),
        );
    }
    Value::Object(top)
}
```

Tests:
```rust
    // ----- get_public_access_block_to_json -----

    #[test]
    fn get_pab_empty_output_yields_empty_object() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        let out = GetPublicAccessBlockOutput::builder().build();
        let json = get_public_access_block_to_json(&out);
        assert_eq!(json, Value::Object(Map::new()));
    }

    #[test]
    fn get_pab_all_true() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        use aws_sdk_s3::types::PublicAccessBlockConfiguration;
        let cfg = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .ignore_public_acls(true)
            .block_public_policy(true)
            .restrict_public_buckets(true)
            .build();
        let out = GetPublicAccessBlockOutput::builder()
            .public_access_block_configuration(cfg)
            .build();
        let json = get_public_access_block_to_json(&out);
        let inner = &json["PublicAccessBlockConfiguration"];
        assert_eq!(inner["BlockPublicAcls"], Value::Bool(true));
        assert_eq!(inner["IgnorePublicAcls"], Value::Bool(true));
        assert_eq!(inner["BlockPublicPolicy"], Value::Bool(true));
        assert_eq!(inner["RestrictPublicBuckets"], Value::Bool(true));
    }

    #[test]
    fn get_pab_partial_fields() {
        use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
        use aws_sdk_s3::types::PublicAccessBlockConfiguration;
        let cfg = PublicAccessBlockConfiguration::builder()
            .block_public_acls(true)
            .build();
        let out = GetPublicAccessBlockOutput::builder()
            .public_access_block_configuration(cfg)
            .build();
        let json = get_public_access_block_to_json(&out);
        let inner = &json["PublicAccessBlockConfiguration"];
        assert_eq!(inner["BlockPublicAcls"], Value::Bool(true));
        assert!(inner.get("IgnorePublicAcls").is_none());
    }
```

### Task 28 — PAB API wrappers + pinned NOT_FOUND_CODES test

```rust
use aws_sdk_s3::operation::delete_public_access_block::DeletePublicAccessBlockOutput;
use aws_sdk_s3::operation::get_public_access_block::GetPublicAccessBlockOutput;
use aws_sdk_s3::operation::put_public_access_block::PutPublicAccessBlockOutput;
use aws_sdk_s3::types::PublicAccessBlockConfiguration;

const GET_PUBLIC_ACCESS_BLOCK_NOT_FOUND_CODES: &[&str] =
    &["NoSuchPublicAccessBlockConfiguration"];

pub async fn get_public_access_block(
    client: &Client,
    bucket: &str,
) -> Result<GetPublicAccessBlockOutput, HeadError> { /* mirror Task 4 */ }

pub async fn put_public_access_block(
    client: &Client,
    bucket: &str,
    cfg: PublicAccessBlockConfiguration,
) -> Result<PutPublicAccessBlockOutput> {
    client
        .put_public_access_block()
        .bucket(bucket)
        .public_access_block_configuration(cfg)
        .send()
        .await
        .with_context(|| format!("put-public-access-block on s3://{bucket}"))
}

pub async fn delete_public_access_block(
    client: &Client,
    bucket: &str,
) -> Result<DeletePublicAccessBlockOutput> {
    client
        .delete_public_access_block()
        .bucket(bucket)
        .send()
        .await
        .with_context(|| format!("delete-public-access-block on s3://{bucket}"))
}
```

Pinned test:
```rust
    #[test]
    fn get_public_access_block_not_found_codes_pinned() {
        assert_eq!(
            GET_PUBLIC_ACCESS_BLOCK_NOT_FOUND_CODES,
            &["NoSuchPublicAccessBlockConfiguration"]
        );
    }
```

### Tasks 29–32 — PAB args, runtime, wiring, process-level tests

Same shape as Tasks 5–8 with substitutions. **Note:** the CLI command names are `get-public-access-block` / `put-public-access-block` / `delete-public-access-block` (no `bucket-` prefix). The args struct constants:
```rust
const TARGET_NOT_S3: &str = "get-public-access-block target must be s3://<BUCKET>\n";
```
etc.

### Task 33 — PAB E2E tests + verify + commit

`tests/e2e_public_access_block.rs` mirroring Task 9. Sample fixture:

```rust
fn sample_pab_json() -> &'static str {
    r#"{
      "BlockPublicAcls": true,
      "IgnorePublicAcls": true,
      "BlockPublicPolicy": true,
      "RestrictPublicBuckets": true
    }"#
}
```

**Behaviour note for the "no PAB set" assertion:** AWS S3 has applied default block-public-access on all new buckets (since 2023). A bucket with no explicit PAB configuration may still return a populated response (`AllSettingsTrue`). The `get_on_bucket_without_pab_exits_4` test should assert exit 4 only if observation confirms `NoSuchPublicAccessBlockConfiguration` is still returned in the test region. If E2E reveals the auto-enabled behaviour, change that test to assert `success && all-true response` instead. Document the live observation in the PR.

PAUSE — propose commit:
```
feat: add public-access-block subcommands (get/put/delete)
```

---

## Section F — Documentation and final verification

### Task 34: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Find the "All command line options" command-name table (around line 554)**

The existing table has rows for the bucket-policy family (around lines 128–130). Add 12 new rows in the same shape, alphabetically interleaved or at the end of the bucket-management section:

(Use put → get → delete order to match the existing bucket-policy / bucket-tagging rows.)

```markdown
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
```

- [ ] **Step 2: Update the "exit code 4" line (around line 482)** to mention the new resource codes

Change:
```
| 4    | Not found — … `get-bucket-policy` / `get-bucket-tagging` / `get-bucket-versioning` when the addressed resource is missing (incl. NoSuchBucketPolicy / NoSuchTagSet) |
```

To:
```
| 4    | Not found — … `get-bucket-policy` / `get-bucket-tagging` / `get-bucket-versioning` / `get-bucket-lifecycle-configuration` / `get-bucket-encryption` / `get-bucket-cors` / `get-public-access-block` when the addressed resource is missing (incl. NoSuchBucketPolicy / NoSuchTagSet / NoSuchLifecycleConfiguration / ServerSideEncryptionConfigurationNotFoundError / NoSuchCORSConfiguration / NoSuchPublicAccessBlockConfiguration) |
```

- [ ] **Step 3: Update "Read-side NotFound is a distinct exit code" (around line 786)** to add the new commands

Change:
```
**Read-side NotFound is a distinct exit code.** `head-bucket`, `head-object`, `get-object-tagging`, `get-bucket-policy`, `get-bucket-tagging`, and `get-bucket-versioning` map S3's …
```

To:
```
**Read-side NotFound is a distinct exit code.** `head-bucket`, `head-object`, `get-object-tagging`, `get-bucket-policy`, `get-bucket-tagging`, `get-bucket-versioning`, `get-bucket-lifecycle-configuration`, `get-bucket-encryption`, `get-bucket-cors`, and `get-public-access-block` map S3's …
```

- [ ] **Step 4: Update the destructive-thin-wrappers paragraph (around line 790)** to add the new delete commands

Change `…rm, delete-bucket, delete-bucket-policy, delete-bucket-tagging, and delete-object-tagging…` to add the new delete commands at the end (after `delete-object-tagging`):
```
…delete-object-tagging, delete-bucket-lifecycle-configuration, delete-bucket-encryption, delete-bucket-cors, and delete-public-access-block…
```

- [ ] **Step 5: Update README Scope section (around line 84)** to broaden the bucket-management scope

Change "common bucket management (creation/deletion, tagging, versioning, policy)" to "common bucket management (creation/deletion, tagging, versioning, policy, lifecycle, encryption, CORS, public-access-block)".

- [ ] **Step 6: Run `cargo run --bin s3util -- --help` and copy the alphabetised command list back into any per-command help-output sample in the README that needs refreshing.**

If the README's "Subcommands" section duplicates the binary help (some s3util forks do, some don't), refresh that section. Otherwise skip.

### Task 35: Final whole-repo verification

- [ ] **Step 1: Format**

Run: `cargo fmt`
Expected: no diff (or apply the diff).

- [ ] **Step 2: Lint**

Run: `cargo clippy --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 3: Run all non-E2E tests**

Run: `cargo test`
Expected: all green. Look for the new test names in the output:
- `cli_get_bucket_lifecycle_configuration` (×N tests)
- `cli_put_bucket_lifecycle_configuration` (×N)
- `cli_delete_bucket_lifecycle_configuration` (×N)
- ... (12 cli_* test files)
- `input::json::tests::lifecycle_*`, `input::json::tests::encryption_*`, etc.
- `output::json::tests::get_bucket_lifecycle_*`, etc.
- `storage::s3::api::tests::get_bucket_lifecycle_configuration_not_found_codes_pinned`, etc.

- [ ] **Step 4: Compile the E2E suite (do NOT run)**

Run: `RUSTFLAGS="--cfg e2e_test" cargo check --tests`
Expected: clean.

- [ ] **Step 5: PAUSE — ask user to commit README and final verification work**

Show user the README diff and propose:
```
docs(readme): document bucket-lifecycle/encryption/cors/public-access-block subcommands
```

This keeps docs as a separate commit from each family's `feat:` commit, matching the project's commit hygiene (recent commits like `docs(readme): collapse compat/parity non-goals into a single bullet`).

- [ ] **Step 6: Tell the user that the implementation is complete and remind them to run the E2E suite manually**

Suggest:
```
RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_bucket_lifecycle_configuration -- --test-threads=1
RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_bucket_encryption -- --test-threads=1
RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_bucket_cors -- --test-threads=1
RUSTFLAGS="--cfg e2e_test" cargo test --test e2e_public_access_block -- --test-threads=1
```

Note: the existing E2E test profile is `s3util-e2e-test` (per memory). The user runs these — Claude does not.

---

## Self-review notes

**Spec coverage check:**
- 12 commands × 4 families: ✅ Tasks 2–9 (lifecycle), 10–17 (encryption), 18–25 (cors), 26–33 (PAB)
- `delete-bucket-lifecycle-configuration` uses symmetric naming wrapping `DeleteBucketLifecycle`: ✅ Task 4 step 3, Task 6 step 3
- `src/input/json.rs` mirror module: ✅ Task 1
- Output JSON serialisers match AWS CLI shape: ✅ Tasks 3, 11, 19, 27
- API wrappers + pinned NOT_FOUND_CODES tests: ✅ Tasks 4, 12, 20, 28
- Args structs with bucket validation tests: ✅ Tasks 5, 13, 21, 29
- CLI runtime modules: ✅ Tasks 6, 14, 22, 30
- Wiring (mod.rs / main.rs): ✅ Tasks 7, 15, 23, 31
- Process-level CLI tests: ✅ Tasks 8, 16, 24, 32
- E2E tests (compile-only): ✅ Tasks 9, 17, 25, 33
- README updates: ✅ Task 34
- Final fmt + clippy + test gate: ✅ Task 35

**Per memory enforcements:**
- "Never auto-commit": every commit step says "PAUSE — ask user to commit"
- "Never run E2E": every E2E task says `RUSTFLAGS="--cfg e2e_test" cargo check --tests` (no `cargo test`)
- "E2E profile is `s3util-e2e-test`": all E2E test code uses that profile name

**Type/name consistency check:**
- `LifecycleConfigurationJson::into_sdk() -> Result<BucketLifecycleConfiguration>` — referenced by `put_bucket_lifecycle_configuration.rs` runtime (Task 6 step 2) and used in tests (Task 2 step 2): ✅ consistent
- `get_bucket_lifecycle_configuration_to_json` defined in Task 3, imported in Task 6 step 1: ✅
- `api::put_bucket_lifecycle_configuration(client, bucket, cfg)` signature defined in Task 4, called from Task 6 step 2: ✅
- All four families' wiring uses the same per-family display_order block (19–21, 22–24, 25–27, 28–30): ✅

**Risk callouts the implementer should know about:**
- AWS S3 default encryption (auto-enabled since 2023): may invalidate the "get on unencrypted bucket → exit 4" assertion. Task 17 documents this and asks the user to adapt based on live observation.
- Public-access-block default-enable (since 2023): same for Task 33.
- SDK `.build()` vs `.build()?`: vary across `aws-sdk-s3` minor versions. Each task that uses `.build()` notes that adjustment may be needed and references `cargo doc -p aws-sdk-s3` for verification.
