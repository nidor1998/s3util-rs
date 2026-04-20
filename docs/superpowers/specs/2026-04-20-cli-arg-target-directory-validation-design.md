# Validate Target Local Directory at CLI-Arg Time

## Summary

Move the "target local directory must exist" check from transfer-time to
CLI-argument validation time, so that an invocation with a non-existent target
directory fails before any AWS client is built or any S3 connection is opened.

The runtime check already exists via `fs_util::require_parent_directory`
(called from `src/storage/local/mod.rs:393` and `:536`). This design adds an
equivalent check earlier, in `impl CpArgs::validate_storage_config()`
(`src/config/args/mod.rs`). Runtime behavior is unchanged; the new check is
purely additive.

## Motivation

Commit 776f6e0 renamed `create_*` fs helpers to `require_*`, locking in the
project's intent: **require directories to exist, do not create them**. That
verify-only behavior currently surfaces only at transfer-time. A user with a
typo'd target path pays the cost of AWS client setup, credential resolution,
and (for S3→local) at least one S3 call before the error surfaces.

Validating at arg-validation time makes the failure fast and cheap, with the
same error semantics as runtime.

## Scope

**In scope**
- For a local target, validate that the directory which would contain the
  written file exists.
- Skip the check for S3 and stdio targets.

**Out of scope**
- Source local path existence (read-time failure is sufficient).
- Write-permission probing (error-prone on NFS / ACL edge cases).
- Directory creation (contradicts the `create_* → require_*` rename).
- Changes to runtime behavior in `LocalStorage` / `fs_util`.

## Design

### New validation method

Add to `impl CpArgs` in `src/config/args/mod.rs`:

```rust
fn check_target_local_directory_exists(&self) -> Result<(), String>
```

Wire it into `validate_storage_config()` alongside the other `check_*`
methods.

### Algorithm

Parse the target with `storage_path::parse_storage_path(self.target_str())`.

- `StoragePath::S3 { .. }` or `StoragePath::Stdio` → `Ok(())` immediately.
- `StoragePath::Local(target_path)` → resolve the "effective directory" and
  check `try_exists()`.

Effective-directory resolution (mirrors `src/bin/s3util/cli/mod.rs:414` and
`fs_util::require_parent_directory`):

| Condition on `target_path`                                | Effective directory              |
|-----------------------------------------------------------|----------------------------------|
| Ends with `std::path::MAIN_SEPARATOR`                     | `target_path` with separator stripped |
| `target_path.is_dir()` is true                            | `target_path`                    |
| Otherwise (file-style)                                    | `target_path.parent()`           |
| `target_path.parent()` is `None` or empty                 | *skip — cwd, trivially exists*   |

Check `effective_dir.try_exists().unwrap_or(false)` — matching the runtime
idiom in `fs_util::require_parent_directory`. `Err(_)` (permission denied,
etc.) is treated as "does not exist" at this layer; the downstream transfer
code will produce a more specific error if that case actually arises at
write-time. If the result is `true` → `Ok(())`; if `false` → return the
error below.

### Error message

Module-level `&str` constant (matching existing convention in `args/mod.rs`):

```
target directory does not exist: '<path>'. Please create it before running this command.
```

`<path>` is the resolved effective directory rendered via `to_string_lossy()`
(matching `fs_util::require_parent_directory`), not the raw CLI string.
Wording and tone match the runtime error so users see an indistinguishable
message whether the check fires at arg-validation or transfer-time.

## Files touched

- `src/config/args/mod.rs`
  - Add error-message constant.
  - Add `check_target_local_directory_exists()` on `CpArgs`.
  - Add the call inside `validate_storage_config()`.
- `src/config/args/tests.rs`
  - Add the unit tests listed below.

No other files are modified.

## Testing

Unit tests in `src/config/args/tests.rs`. Use `tempfile::tempdir()` for
positive cases. Use an obviously-nonexistent path
(e.g. `/definitely/does/not/exist/abc123`) for negative cases.

| Test name (intent)                                  | Expectation           |
|-----------------------------------------------------|-----------------------|
| target in existing directory (file-style)           | passes                |
| target inside nonexistent directory (file-style)    | rejected, msg matches |
| target is existing directory with trailing sep      | passes                |
| target is nonexistent directory with trailing sep   | rejected              |
| target is existing directory (no trailing sep)      | passes                |
| target is relative filename (no parent)             | passes (cwd)          |
| target is `s3://bucket/key`                         | passes (skipped)      |
| target is `-` (stdio)                               | passes (skipped)      |

## Risk

- **Behavior change.** Invocations that relied on AWS-setup errors surfacing
  before the directory check will now fail earlier with a different message.
  This is a strict improvement — the new failure is a subset of outcomes that
  used to reach runtime with the same root cause.
- **Windows paths.** `MAIN_SEPARATOR` is `\\` on Windows; `std::path` handles
  both without OS-specific code.
- **Existing tests.** `both_local_rejected` and
  `check_at_least_one_s3_or_stdio_rejects_both_local_direct` use `/tmp/...`.
  `/tmp` exists on typical dev / CI environments, so they remain green.

## Order of build

1. Add error-message constant and `check_target_local_directory_exists()`.
2. Wire into `validate_storage_config()`.
3. Add unit tests.
4. `cargo fmt` + `cargo clippy --all-features` clean.
5. Manual smoke check: `s3util cp s3://bucket/key /nonexistent/out.bin`
   surfaces the new error without any AWS call.
