# CLI Directory-Traversal Guard Rework

**Date:** 2026-04-20

## Problem

`s3util cp s3://data.cpp17.org/hosts ../` fails with:

```
ERROR copy failed. error="failed to write to target file: ../hosts: a object references a parent directory."
```

The user has explicitly chosen `../` as the target directory. Rejecting it forces object-storage semantics onto a path the user typed themselves.

### Root cause

`src/storage/local/fs_util.rs:10` defines:

```rust
pub fn check_directory_traversal(key: &str) -> bool {
    let re = Regex::new(r"\.\.[/\\]").unwrap();
    re.is_match(key)
}
```

This runs at the storage layer (`src/storage/local/mod.rs:388` and `:531`) against the **combined** target-path-plus-basename string that `extract_keys` hands down. It cannot distinguish between a `..` the user typed for the target and a `..` that came from an S3 key. Any `..` in the final path fails.

### What actually protects us today

`src/bin/s3util/cli/mod.rs:389-392` already basenames the source S3 key via `Path::file_name()`:

```rust
let source_basename = std::path::Path::new(&source_key)
    .file_name()
    .map(|f| f.to_string_lossy().to_string())
    .unwrap_or(source_key.clone());
```

`cp` is single-object only — there is no `list_objects_v2` or recursive walk in the codebase (grep confirmed). The only S3 key in play is the one on the CLI, and `file_name()` strips any leading or interior `..` from it before it is joined to the target. So `s3://bucket/../../etc/passwd ./dst/` resolves to `./dst/passwd`, not outside `./dst/`.

The only residual key-side hazard is a source key whose **final** segment is `.` or `..` (e.g., `s3://bucket/foo/..`). In that case `file_name()` returns `None` and line 392 falls back to the entire source key as the "basename" — which is broken regardless of the traversal regex.

## Goal

Replace the combined-path regex with a targeted arg-time check, so:

- `s3util cp s3://bucket/key ../` — **works**, writes `../key`.
- `s3util cp s3://bucket/foo/.. ./dst/` — **rejected at arg parse**, "source object name is invalid".
- `s3util cp s3://bucket/prefix/ ./dst/` — **rejected at arg parse**, "source URL ending in `/` is not supported (prefix/recursive copy is not a feature of `s3util cp`)".
- `s3util cp s3://bucket/../etc/passwd ./dst/` — **works, safe**, writes `./dst/passwd` (basename semantics strip the `..`).

## Non-goals

- **Recursive / prefix copy support.** This spec does not add it. It only surfaces a clearer error when the user's URL implies they expected it.
- **Local-source trailing-`/` semantics.** `s3util cp /some/dir/ s3://bucket/` is out of scope. The user's framing was S3-URL-specific, and local filesystem behavior (dir vs. file) already fails naturally downstream.
- **Post-join normalization / canonicalization of the write path.** Basename semantics in `extract_keys` are already sufficient for single-object cp.

## Design

### 1. Arg-time validation in `extract_keys`

In `src/bin/s3util/cli/mod.rs`, extend the source-side match for `StoragePath::S3`:

```rust
StoragePath::S3 { prefix, .. } => {
    if prefix.is_empty() {
        return Err(anyhow!("source S3 key is required (e.g. s3://bucket/key)"));
    }
    if prefix.ends_with('/') {
        return Err(anyhow!(
            "source S3 URL ending in '/' is not supported: \
             `s3util cp` copies a single object, not a prefix"
        ));
    }
    // Reject keys whose final path segment is `.` or `..`.
    // These would produce a `file_name()` of `None` and fall through to
    // the whole key as the "basename" — nonsensical as a filename.
    let last_segment = prefix.rsplit('/').next().unwrap_or("");
    if last_segment == "." || last_segment == ".." {
        return Err(anyhow!(
            "source S3 key has an invalid final segment ('.' or '..'): {prefix}"
        ));
    }
    prefix.clone()
}
```

Rationale for hand-rolled segment split instead of `Path::file_name()`:

- S3 keys are forward-slash-separated regardless of host OS.
- `Path::file_name()` on Windows would also accept `\` as a separator, which is wrong for S3.
- `rsplit('/').next()` gives the last segment even when it is `.` / `..`; `file_name()` returns `None` in that case, which is what we are *testing for*, but the explicit string form is clearer at the callsite.

### 2. Remove the combined-path traversal guard

`src/storage/local/mod.rs`:

- Delete lines `388-390` (the `check_directory_traversal` call in `put_object_single_part`).
- Delete lines `531-533` (the matching call in `put_object_multipart`).

### 3. Delete now-unused symbols

Grep confirms `check_directory_traversal` and `S3syncError::DirectoryTraversalError` are only referenced at those two call sites plus their own tests. Remove:

- `pub fn check_directory_traversal` (`src/storage/local/fs_util.rs:10-13`)
- Its unit test `check_directory_traversal_test` (`src/storage/local/fs_util.rs:140-166`)
- `S3syncError::DirectoryTraversalError` variant (`src/types/error.rs:6-7`)
- Its reference in `is_cancelled_error_test` (`src/types/error.rs:29-31`) — replace with a non-removed variant or drop the negative assertion.

Keep `use regex::Regex;` in `fs_util.rs` — `normalize_key` at line 97 still uses it. The `regex` crate is also used in `src/config/args/value_parser/{tagging,storage_path,metadata}.rs` (grep-confirmed); `Cargo.toml` is untouched.

### 4. Drop the defer-on-`..` branch in arg validation

`src/config/args/mod.rs:811-818` added a special-case for target paths containing `..` that deferred to the now-deleted runtime guard. With the new design, target paths with `..` are legitimate — `try_exists()` on `../` is a valid existence check and succeeds. The `..`-in-path branch can be removed; the directory-existence check runs uniformly for all target paths that reach the check.

One subtlety: for synthetic traversal paths where intermediate components don't exist (e.g., `./nonexistent/../../../etc`), `try_exists()` on the full path returns `Ok(false)` because `nonexistent` is absent. That now produces a "target directory does not exist" error instead of the previous traversal error. That is the correct message — the path literally doesn't resolve to an existing directory. The test `target_with_parent_dir_segment_skips_directory_check` (`src/config/args/tests.rs:685`) becomes obsolete and should be removed or rewritten to assert the "does not exist" error for such paths.

### 5. Test updates

Add to `src/bin/s3util/cli/mod.rs` (`#[cfg(test)]` block):

- `extract_keys_s3_source_ending_in_slash_errors` — `s3://b/dir/` → error mentions "prefix/recursive copy is not a feature".
- `extract_keys_s3_source_ending_in_dot_errors` — `s3://b/foo/.` → error mentions "invalid final segment".
- `extract_keys_s3_source_ending_in_dotdot_errors` — `s3://b/foo/..` → error mentions "invalid final segment".
- `extract_keys_s3_source_with_leading_dotdot_basenames_safely` — `s3://b/../etc/passwd` with target `./dst/` → target_key is `./dst/passwd` (regression guard for the leading-`..` case we chose not to reject).

Remove / rewrite as noted:

- `src/storage/local/fs_util.rs::check_directory_traversal_test` — deleted with the function.
- `src/config/args/tests.rs::target_with_parent_dir_segment_skips_directory_check` — remove or rewrite against "does not exist" error.
- Any e2e test that depends on `DirectoryTraversalError` (the commit message for `f42d270` mentions `s3_to_local_directory_traversal_rejected`). That test must be rewritten to assert the new arg-time error for an S3 source with trailing `..`, not the removed storage-layer error. E2E tests are run by the user — the plan needs to flag this.

## Verification

Per `CLAUDE.md`:

- `cargo fmt`
- `cargo clippy --all-features` — clean.
- `RUSTFLAGS="--cfg e2e_test" cargo clippy --all-features --tests` — clean (compile check only; e2e tests are not run by Claude).
- `cargo test` — all unit / integration tests pass.
- **E2E suite re-run is required** after the rewrite of `s3_to_local_directory_traversal_rejected`. The user runs that.
- **Review before commit**, per project convention.

## File impact

- `src/bin/s3util/cli/mod.rs` — extended `extract_keys`, new unit tests.
- `src/storage/local/mod.rs` — remove two 3-line guards.
- `src/storage/local/fs_util.rs` — delete `check_directory_traversal` + test.
- `src/types/error.rs` — remove `DirectoryTraversalError` variant + test reference.
- `src/config/args/mod.rs` — drop the `..`-deferral branch.
- `src/config/args/tests.rs` — remove or rewrite `target_with_parent_dir_segment_skips_directory_check`.
- `tests/` (e2e) — rewrite `s3_to_local_directory_traversal_rejected` to assert the new arg-time error.
- `Cargo.toml` — **unchanged**; `regex` is used in several other modules.

## Out of scope (explicit)

- Recursive / prefix S3 copy.
- Local-source recursive semantics.
- Path canonicalization of the final write path.
- Handling of S3 keys containing literal `\` (S3 allows them; they pass through basename and join like any other char).
