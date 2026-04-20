# CLI Directory-Traversal Guard Rework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the user write to `../` targets (currently rejected as traversal) by replacing the combined-path traversal regex with targeted arg-time validation in `extract_keys`.

**Architecture:** `s3util cp` is single-object only (confirmed: no `list_objects_v2` in the codebase). `extract_keys` already calls `Path::file_name()` on the source key, which strips any leading or interior `..`. So the only remaining hazard is a source key whose final segment is `.` or `..` — catchable at arg parse. The combined-path regex in `src/storage/local/fs_util.rs` is removed; its only real-world effect was false-positives on user-chosen `..` targets.

**Tech Stack:** Rust 2024 edition, `anyhow`, `thiserror`, `tokio`, `cargo test`, `cargo clippy`, `cargo fmt`. No new dependencies.

**Related spec:** `docs/superpowers/specs/2026-04-20-cli-traversal-guard-rework-design.md` (commit `d724f21`).

---

## File Structure

Files touched by this plan:

- **Modify:** `src/bin/s3util/cli/mod.rs` — extend `extract_keys` with three rejection rules; add unit tests.
- **Modify:** `src/storage/local/mod.rs` — remove two `check_directory_traversal` call sites.
- **Modify:** `src/storage/local/fs_util.rs` — delete `check_directory_traversal` and its unit test. Keep the `use regex::Regex;` import (`remove_root_slash` still uses it).
- **Modify:** `src/types/error.rs` — delete `DirectoryTraversalError` variant; update `is_cancelled_error_test` to drop the now-impossible reference.
- **Modify:** `src/config/args/mod.rs` — remove the `..`-segment deferral branch in `check_target_local_directory_exists`.
- **Modify:** `src/config/args/tests.rs` — delete the now-obsolete `target_with_parent_dir_segment_skips_directory_check`.
- **Modify:** `tests/e2e_s3_to_local.rs` — rewrite `s3_to_local_directory_traversal_rejected` to match new behavior. E2E test — **user runs**, Claude compile-checks only.

---

## Task 1: Arg-time rejection on raw source + remove combined-path guard (atomic behavior change)

**Amended 2026-04-20 (v2):** Validation moves to `CpArgs` on raw input. The earlier draft put the checks in `extract_keys`, but `url::Url::parse` normalizes `s3://` paths (strips trailing `.` / leading `..`, collapses `/foo/..` to `/`) before `extract_keys` sees them — so the checks there never fire for standard URLs. See the spec Amendment for details.

This task ships the user-visible fix: after this change, `s3util cp s3://bucket/key ../` works. Arg-time rejection and combined-path-guard removal must land together — doing only one leaves a broken intermediate state.

**Files:**
- Modify: `src/config/args/mod.rs` — new `CpArgs::check_source_s3_key()` method; wire it into `validate_storage_config()`.
- Modify: `src/config/args/tests.rs` — new tests using `build_config_from_args` / `args_with`.
- Modify: `src/storage/local/mod.rs` — remove two 3-line `check_directory_traversal` call blocks (around lines 388-390 and 531-533).
- Revert (if present from earlier iteration): any changes in `src/bin/s3util/cli/mod.rs` that added `ends_with('/')` / segment checks inside `extract_keys`. `extract_keys` stays at its original shape.

---

- [ ] **Step 1.1: (If needed) Revert earlier `extract_keys` changes**

If the working tree has uncommitted additions in `src/bin/s3util/cli/mod.rs` from the previous iteration (new S3-arm branches and/or four `extract_keys_s3_source_*` tests), revert them so `extract_keys` matches its pre-task state. Use `git diff src/bin/s3util/cli/mod.rs` to see what to remove. The storage-layer changes in `src/storage/local/mod.rs` should be kept.

Expected post-revert diff scope: `src/bin/s3util/cli/mod.rs` has **no** changes; `src/storage/local/mod.rs` still has the two 3-line deletions from the prior iteration.

- [ ] **Step 1.2: Add failing unit tests in `src/config/args/tests.rs`**

Append to the existing `#[cfg(test)] mod tests { ... }` block (the same module that contains `args_with` and tests like `target_with_parent_dir_segment_skips_directory_check`). Use the existing `build_config_from_args` helper.

```rust
    #[test]
    fn source_s3_url_trailing_slash_rejected() {
        let result = build_config_from_args(args_with("s3://b/dir/", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("URL ending in '/'"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_trailing_dot_rejected() {
        let result = build_config_from_args(args_with("s3://b/foo/.", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_trailing_dotdot_rejected() {
        let result = build_config_from_args(args_with("s3://b/foo/..", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_bare_dot_rejected() {
        // `s3://b/.` ends in `/.`.
        let result = build_config_from_args(args_with("s3://b/.", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_bare_dotdot_rejected() {
        // `s3://b/..` ends in `/..`.
        let result = build_config_from_args(args_with("s3://b/..", "/tmp/dst"));
        let err = result.unwrap_err();
        assert!(
            err.contains("invalid final segment"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn source_s3_url_mid_path_dotdot_accepted() {
        // Mid-path `..` is allowed per the rule (only trailing segments
        // are rejected). The user's intent: the key resolves safely under
        // basename semantics in the downstream transfer code.
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://b/foo/../etc/passwd", &dst));
        assert!(result.is_ok(), "{:?}", result.err());
    }

    #[test]
    fn source_s3_url_filename_ending_in_dot_accepted() {
        // `foo.` is a legitimate filename ending in `.` (not a path
        // segment equal to `.`). The raw `ends_with("/.")` check does NOT
        // match this input — regression guard against over-rejection.
        let tmp = tempfile::tempdir().unwrap();
        let dst = tmp.path().to_string_lossy().to_string();
        let result = build_config_from_args(args_with("s3://b/foo.", &dst));
        assert!(result.is_ok(), "{:?}", result.err());
    }
```

- [ ] **Step 1.3: Run the new tests — expect them to FAIL**

```
cargo test --lib source_s3_url_
```

Expected: the five `*_rejected` tests FAIL (no check implemented yet). The two `*_accepted` tests should already PASS because nothing currently rejects them.

- [ ] **Step 1.4: Implement `check_source_s3_key` on `CpArgs`**

In `src/config/args/mod.rs`, add a new method inside the `impl CpArgs { ... }` block — near `check_target_local_directory_exists` (currently starts at line 807). Match the existing style (returns `Result<(), String>`, uses plain string errors):

```rust
    fn check_source_s3_key(&self) -> Result<(), String> {
        if !self.is_source_s3() {
            return Ok(());
        }
        // Validate the raw CLI input. `url::Url::parse` in the value-parser
        // layer normalizes `s3://` paths (strips trailing `.`, collapses
        // `/foo/..`), so the post-parsed prefix no longer carries the signal
        // we want to reject. Read directly from the raw argument instead.
        let raw = self.source_str();
        if raw.ends_with('/') {
            return Err(
                "source S3 URL ending in '/' is not supported: \
                 `s3util cp` copies a single object, not a prefix."
                    .to_string(),
            );
        }
        if raw.ends_with("/.") || raw.ends_with("/..") {
            return Err(format!(
                "source S3 key has an invalid final segment ('.' or '..'): {raw}"
            ));
        }
        Ok(())
    }
```

Then wire it into `validate_storage_config` (currently ends at line 588). Add the call just before `check_target_local_directory_exists()?;`:

```rust
        self.check_source_s3_key()?;
        self.check_target_local_directory_exists()?;
```

- [ ] **Step 1.5: Re-run the new tests — expect them to PASS**

```
cargo test --lib source_s3_url_
```

Expected: all seven tests PASS.

- [ ] **Step 1.6: Confirm the combined-path guard removals in `src/storage/local/mod.rs` are still in place**

From the prior iteration, two 3-line blocks should already be deleted:

At (originally) lines 388-390 in `put_object_single_part`:

```rust
        if fs_util::check_directory_traversal(key) {
            return Err(anyhow!(S3syncError::DirectoryTraversalError));
        }
```

At (originally) lines 531-533 in `put_object_multipart`: identical 3-line block.

If either is still present, remove it now. `check_directory_traversal` itself and `DirectoryTraversalError` stay put until Task 2.

- [ ] **Step 1.7: Run the full test suite**

```
cargo test
```

Expected: all tests PASS.

- [ ] **Step 1.8: Lint / format**

```
cargo fmt
cargo clippy --all-features
```

Expected: no diagnostics. `check_directory_traversal` and `DirectoryTraversalError` are `pub` so no dead-code warnings (they remain for Task 2 to delete).

- [ ] **Step 1.9: STOP — do NOT commit. Report back.**

Report with:
- Status (DONE / DONE_WITH_CONCERNS / BLOCKED / NEEDS_CONTEXT)
- Output of final `cargo test` + `cargo clippy`
- `git status` and `git diff --stat`
- Any concerns or observations

The controller surfaces the diff to the user for review before commit.

- [ ] **Step 1.10: (Controller, after user approval) Commit**

```bash
git add src/config/args/mod.rs src/config/args/tests.rs src/storage/local/mod.rs
git commit -m "$(cat <<'EOF'
fix(cli): allow user-chosen `..` targets; reject problematic source keys at arg parse

The combined-path `check_directory_traversal` regex in the local
storage layer rejected any `..` in the joined target+key path — which
includes user-chosen targets like `../`. Replace with a raw-input
arg-time check in CpArgs::check_source_s3_key for S3 source URLs:

- reject URLs ending in `/` (prefix/recursive copy is not a feature),
- reject keys whose final segment is `.` or `..`.

Validation reads the raw CLI argument because url::Url::parse
normalizes s3:// paths before StoragePath is built, erasing the
trailing-segment signal.

Fixes `s3util cp s3://bucket/key ../` false-positive rejection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 2: Delete now-unused traversal symbols

**Files:**
- Modify: `src/storage/local/fs_util.rs:10-13` (delete function), `:140-166` (delete its unit test).
- Modify: `src/types/error.rs:6-7` (delete variant), `:27-32` (update test).

---

- [ ] **Step 2.1: Delete `check_directory_traversal` in `src/storage/local/fs_util.rs`**

Delete lines 10-13 (the `pub fn check_directory_traversal(...)` function body). Leave `use regex::Regex;` at line 6 intact — `remove_root_slash` at line 97 still uses it.

- [ ] **Step 2.2: Delete the function's unit test in `src/storage/local/fs_util.rs`**

Delete the entire `#[test] fn check_directory_traversal_test()` function (approximately lines 140-166 — currently shows as the first test in the `mod tests { ... }` block).

- [ ] **Step 2.3: Delete `DirectoryTraversalError` variant in `src/types/error.rs`**

Change lines 4-12 from:

```rust
#[derive(Error, Debug, PartialEq)]
pub enum S3syncError {
    #[error("a object references a parent directory.")]
    DirectoryTraversalError,
    #[error("cancelled")]
    Cancelled,
    #[error("an error occurred while downloading an object")]
    DownloadForceRetryableError,
}
```

...to:

```rust
#[derive(Error, Debug, PartialEq)]
pub enum S3syncError {
    #[error("cancelled")]
    Cancelled,
    #[error("an error occurred while downloading an object")]
    DownloadForceRetryableError,
}
```

- [ ] **Step 2.4: Update `is_cancelled_error_test` in `src/types/error.rs`**

Change `is_cancelled_error_test` (currently lines 27-32) to reference the remaining non-`Cancelled` variant instead of `DirectoryTraversalError`:

```rust
    #[test]
    fn is_cancelled_error_test() {
        assert!(is_cancelled_error(&anyhow!(S3syncError::Cancelled)));
        assert!(!is_cancelled_error(&anyhow!(
            S3syncError::DownloadForceRetryableError
        )));
    }
```

- [ ] **Step 2.5: Verify `S3syncError::DirectoryTraversalError` and `check_directory_traversal` have no remaining references**

Use the repo's Grep tool (or `rg` from a shell):

```
rg -n 'DirectoryTraversalError|check_directory_traversal' src tests
```

Expected: 0 matches. The e2e test at `tests/e2e_s3_to_local.rs` does NOT reference the Rust type — it asserts against `stats.sync_error >= 1` — so nothing to update here; Task 4 handles the behavioral rewrite.

If any match appears, stop and address it before moving on.

- [ ] **Step 2.6: Run the full test suite + lint / format**

```bash
cargo fmt
cargo clippy --all-features
cargo test
```

Expected: no diagnostics, all tests PASS.

- [ ] **Step 2.7: Pause — ask user to review the diff**

Show `git diff`. Wait for approval before commit.

- [ ] **Step 2.8: Commit once approved**

```bash
git add src/storage/local/fs_util.rs src/types/error.rs
git commit -m "$(cat <<'EOF'
refactor: remove unused DirectoryTraversalError and check_directory_traversal

With arg-time validation replacing the combined-path regex guard
(prior commit), `check_directory_traversal` and its `DirectoryTraversalError`
variant have no remaining callers. Delete both along with their tests.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 3: Drop the `..`-segment deferral in target-directory validation

After Task 1, target paths with `..` are legitimate. The defer-branch in `check_target_local_directory_exists` — added in commit `f42d270` to avoid colliding with the now-deleted runtime guard — can be removed. Its existing on-disk check (`try_exists()`) correctly reports "does not exist" for synthetic traversal targets whose intermediate components are absent; that is now the right behavior.

**Files:**
- Modify: `src/config/args/mod.rs:814-825` (delete the deferral branch).
- Modify: `src/config/args/tests.rs:684-699` (delete the now-obsolete test).

---

- [ ] **Step 3.1: Remove the `..`-deferral branch in `src/config/args/mod.rs`**

Delete lines 814-825 — the entire `if target_path.components().any(...)` block and the comment above it:

```rust
        // Paths with `..` segments are handled by the runtime
        // directory-traversal guard (fs_util::check_directory_traversal).
        // Defer: intermediate components of synthetic traversal paths may
        // legitimately not exist on disk, so a try_exists() check would
        // produce a misleading "directory does not exist" error instead of
        // the correct traversal-rejection error.
        if target_path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
        {
            return Ok(());
        }

```

Nothing replaces it — the `ends_with_sep` / `is_dir` / `parent` logic that follows now applies uniformly, including for paths containing `..`.

- [ ] **Step 3.2: Delete the obsolete unit test in `src/config/args/tests.rs`**

Delete the entire `target_with_parent_dir_segment_skips_directory_check` test function (currently lines 684-699):

```rust
    #[test]
    fn target_with_parent_dir_segment_skips_directory_check() {
        // ...body...
    }
```

Rationale for deletion rather than rewrite: the test asserted "deferral works" — behavior we no longer want. A replacement would either assert "target with `..` and existing dir is accepted" (already covered transitively by the new `extract_keys_s3_source_leading_dotdot_basenames_safely` test, which uses a real tempdir target) or "synthetic `..` path with missing components is rejected with 'does not exist'" — a test of `try_exists()` behavior, not of our logic.

- [ ] **Step 3.3: Verify the user's reported command now passes arg validation**

Add one new positive test to `src/config/args/tests.rs` (in the same `#[cfg(test)] mod` block):

```rust
    #[test]
    fn target_parent_dir_slash_accepted_when_parent_exists() {
        // Regression guard: `s3util cp s3://bucket/key ../` must pass
        // arg validation. `..` (i.e. the parent of CWD) exists and is
        // a directory; the defer branch previously short-circuited this
        // — now the standard existence check handles it uniformly.
        let result = build_config_from_args(args_with("s3://my-bucket/key", "../"));
        assert!(result.is_ok(), "{:?}", result.err());
    }
```

- [ ] **Step 3.4: Run unit tests + lint**

```bash
cargo fmt
cargo clippy --all-features
cargo test
```

Expected: all PASS, no diagnostics.

- [ ] **Step 3.5: Pause — ask user to review the diff**

Show `git diff`. Wait for approval before commit.

- [ ] **Step 3.6: Commit once approved**

```bash
git add src/config/args/mod.rs src/config/args/tests.rs
git commit -m "$(cat <<'EOF'
refactor(cli): drop `..`-segment deferral in target-directory validation

Commit f42d270 added a defer-on-`..` branch so the runtime
directory-traversal guard could produce a clearer error than a
misleading "directory does not exist" from try_exists(). With the
runtime guard removed and user-chosen `..` targets now legitimate,
the standard existence check applies uniformly.

Add a positive test for `s3://bucket/key ../` — the user-reported case.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 4: Rewrite the e2e traversal test

The existing `s3_to_local_directory_traversal_rejected` asserts that a traversal-in-target path is rejected — exactly the behavior we just removed. Rewrite it to assert the inverse: the user's chosen traversal target is honored. Also add an e2e test for the new arg-time rejection of trailing-`..` source keys.

**Files:**
- Modify: `tests/e2e_s3_to_local.rs:985-1058` (rewrite the test and its doc comment).

**Important:** e2e tests run against real AWS and are **not executed by Claude** (per CLAUDE.md and user memory). Claude only verifies compilation via `RUSTFLAGS="--cfg e2e_test" cargo clippy --all-features --tests`. The user runs the actual e2e suite.

---

- [ ] **Step 4.1: Rewrite `s3_to_local_directory_traversal_rejected`**

Replace the test function and its doc comment (currently lines 985-1058) with two tests — one for the flipped positive-path case, one for the new arg-time rejection:

```rust
    /// Positive case: when the user explicitly chooses a target path
    /// containing `..`, `s3util cp` honors it. Basename semantics in
    /// `extract_keys` join the source's basename onto the target dir,
    /// so the file lands where the user asked for — including outside
    /// what used to be treated as a sandbox.
    ///
    /// This replaces the pre-2026-04-20 test that asserted traversal
    /// rejection. The combined-path regex guard was removed; user-chosen
    /// `..` is a legitimate filesystem concept, not an attack vector.
    #[tokio::test]
    async fn s3_to_local_user_chosen_parent_dir_target_accepted() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let probe_name = format!("parent_dir_probe_{}.dat", uuid::Uuid::new_v4());
        let test_content = b"user chose parent dir";
        helper
            .put_object(&bucket, &probe_name, test_content.to_vec())
            .await;

        // `nested_dir` exists; `..` walks back to its parent (local_dir's parent).
        let local_dir = TestHelper::create_temp_dir();
        let nested_dir = local_dir.join("nested");
        std::fs::create_dir_all(&nested_dir).unwrap();
        let target = nested_dir.join("..").to_string_lossy().to_string();

        let source = format!("s3://{}/{}", bucket, probe_name);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                target.as_str(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1, "stats = {stats:?}");
        assert_eq!(stats.sync_error, 0);

        // File lands in local_dir (parent of nested_dir), not inside nested_dir.
        let expected = local_dir.join(&probe_name);
        assert!(
            TestHelper::is_file_exist(expected.to_str().unwrap()),
            "expected file at {}",
            expected.display()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Arg-time guard: a source S3 key whose final segment is `..`
    /// is rejected before any network call. No bucket interaction
    /// required — the CLI fails during argument parsing.
    #[tokio::test]
    async fn s3_to_local_source_key_trailing_dotdot_rejected_at_arg_parse() {
        TestHelper::init_dummy_tracing_subscriber();

        let local_dir = TestHelper::create_temp_dir();

        let stats = TestHelper::new()
            .await
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "s3://any-bucket/foo/..",
                local_dir.to_str().unwrap(),
            ])
            .await;

        // Arg-time rejection surfaces as a non-zero sync_error and no transfer.
        assert!(stats.sync_error >= 1, "stats = {stats:?}");
        assert_eq!(stats.sync_complete, 0);

        let _ = std::fs::remove_dir_all(&local_dir);
    }
```

Note: verify how `cp_test_data` surfaces arg-parse failures in this harness. If arg-parse errors abort before `Stats` is populated, adjust the assertion (e.g., switch to whatever panic-or-error-capture mechanism the harness uses). Grep `cp_test_data` in `tests/common/` to confirm.

- [ ] **Step 4.2: Compile-check the e2e tests (Claude does NOT run them)**

```bash
RUSTFLAGS="--cfg e2e_test" cargo clippy --all-features --tests
```

Expected: clean — the new tests compile and lint.

- [ ] **Step 4.3: Lint / format default config**

```bash
cargo fmt
cargo clippy --all-features
cargo test
```

Expected: clean, all unit/integration tests PASS.

- [ ] **Step 4.4: Pause — ask user to review the diff AND to run the e2e suite locally**

Present:
- `git diff tests/e2e_s3_to_local.rs`
- A clear ask: "Please run `RUSTFLAGS='--cfg e2e_test' cargo test s3_to_local_user_chosen_parent_dir_target_accepted s3_to_local_source_key_trailing_dotdot_rejected_at_arg_parse` against AWS and confirm both pass. I won't run these."

Wait for user to confirm both tests pass before committing this task.

- [ ] **Step 4.5: Commit once user confirms e2e pass**

```bash
git add tests/e2e_s3_to_local.rs
git commit -m "$(cat <<'EOF'
test(e2e): update traversal tests for new arg-time guard

s3_to_local_directory_traversal_rejected asserted behavior that was
removed in the traversal-guard rework (combined-path regex). Replace
with two tests:

- s3_to_local_user_chosen_parent_dir_target_accepted: positive case
  proving user-chosen `..` targets are now honored.
- s3_to_local_source_key_trailing_dotdot_rejected_at_arg_parse: the
  new arg-time rejection for trailing-`..` source keys.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

## Task 5: Final end-to-end verification

Single-step checkpoint after all four behavior/test tasks land.

- [ ] **Step 5.1: Full verification pass**

```bash
cargo fmt -- --check
cargo clippy --all-features -- -D warnings
cargo test
RUSTFLAGS="--cfg e2e_test" cargo clippy --all-features --tests
```

Expected: all clean. (Fourth command is compile-only; e2e execution remains with the user.)

- [ ] **Step 5.2: Reproduce the user's original command against a real bucket (user-driven)**

Ask the user to re-run their original case against a bucket they control:

```
s3util cp s3://data.cpp17.org/hosts ../
```

Expected: success, no "references a parent directory" error, file written at `../hosts`.

- [ ] **Step 5.3: Done**

All behavior changes complete. Four commits landed; e2e test file updated and user-confirmed.

---

## Self-review summary

**Spec coverage:** Every spec section maps to a task — (1) arg-time validation → Task 1 Step 1.3; (2) remove combined-path guard → Task 1 Step 1.5; (3) delete unused symbols → Task 2; (4) drop `..`-deferral branch → Task 3; (5) test updates (unit) → Tasks 1 & 3, (e2e) → Task 4.

**Placeholder scan:** Clean — no "TBD"/"TODO" in implementation code. Step 2.5 uses a Grep verification rather than concrete code; that is the right shape (a check step, not a write step).

**Type consistency:** `extract_keys` signature unchanged. New test names are consistent between plan and commit body. `S3syncError::DownloadForceRetryableError` (used as the non-removed variant in Step 2.4) matches the enum definition at `src/types/error.rs:10-11`.
