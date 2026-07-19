//! E2E tests for `s3util rename`.
//!
//! These tests hit real AWS and are reserved for the user to run. The
//! executor only compile-checks them (under `RUSTFLAGS="--cfg e2e_test"`)
//! to keep rename's end-to-end behavior covered without spending money or
//! touching production buckets in CI.
//!
//! Pattern: invoke the compiled `s3util` binary directly via
//! `env!("CARGO_BIN_EXE_s3util")` (matching `tests/e2e_mv.rs`).
//! Process-level invocation is the only way to assert rename's exit-code
//! mapping together with its atomic move side effect, since `cli::run_rename`
//! lives inside the binary and is not exposed by the library.
//!
//! Coverage:
//! - Basic rename: source key moves to destination, source no longer exists.
//! - Content preservation: renamed object's bytes match the original.
//! - Source-not-found: exit 1, no crash.
//! - Dry run: no actual rename, exit 0.
//! - Conditional `--source-if-match`: matching ETag succeeds, wrong ETag fails.
//! - Conditional `--source-if-none-match <ETAG>`: when the source ETag matches
//!   the provided value the condition is false → 412; when it differs the
//!   condition is true → succeeds.
//! - Conditional `--target-if-none-match <ETAG>`: when the destination ETag
//!   matches the provided value the condition is false → 412; when it differs
//!   the condition is true → succeeds.
//! - Conditional `--target-if-match`: matching destination ETag succeeds;
//!   wrong ETag fails.
//! - Special characters in key: spaces, slashes, Unicode percent-encoded correctly.
//! - CLI validation: non-Express-One-Zone bucket name → exit 2.
//! - CLI validation: source and target in different buckets → exit 2.

#![cfg(e2e_test)]
#[cfg(test)]
mod common;

pub const EXPRESS_ONE_ZONE_BUCKET_SUFFIX: &str = "--apne1-az4--x-s3";

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::process::{Command, Output, Stdio};
    use uuid::Uuid;

    /// Process exit codes (src/bin/s3util/cli/mod.rs):
    ///   EXIT_CODE_SUCCESS = 0
    ///   EXIT_CODE_ERROR   = 1
    ///   EXIT_CODE_NOT_FOUND = 2 (same as clap arg errors in some contexts, but here it's
    ///                             the HeadError::NotFound / BucketNotFound path)
    ///
    /// Clap argument/validation errors exit 2.
    const EXIT_CODE_SUCCESS: i32 = 0;
    const EXIT_CODE_ERROR: i32 = 1;
    const EXIT_CODE_CLAP_ARG_ERROR: i32 = 2;

    /// Run the compiled `s3util` binary. Stdout/stderr are captured for
    /// embedding in assertion messages.
    fn run_s3util(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap()
    }

    /// Create a fresh Express One Zone directory bucket, put a small object
    /// into it with the SDK, and return `(bucket_name, etag)`.
    async fn setup_bucket_with_object(
        helper: &TestHelper,
        key: &str,
        body: &[u8],
    ) -> (String, String) {
        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;
        helper.put_object(&bucket, key, body.to_vec()).await;
        let head = helper.head_object(&bucket, key, None).await;
        let etag = head.e_tag().unwrap().to_string();
        (bucket, etag)
    }

    // ---------------------------------------------------------------
    // Basic rename (3 tests)
    // ---------------------------------------------------------------

    /// Happy path: rename an object within the same Express One Zone bucket.
    /// After rename: destination exists, source is gone.
    #[tokio::test]
    async fn rename_basic_source_gone_destination_present() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "src/basic.txt";
        let dst_key = "dst/basic.txt";
        let body = b"rename basic test body";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&["rename", "--source-profile", "s3util-e2e-test", &src, &dst]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            !helper.is_object_exist(&bucket, src_key, None).await,
            "source must be gone after rename"
        );
        assert!(
            helper.is_object_exist(&bucket, dst_key, None).await,
            "destination must exist after rename"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// Rename preserves object content: bytes at destination match original.
    #[tokio::test]
    async fn rename_preserves_content() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "preserve_src.txt";
        let dst_key = "preserve_dst.txt";
        let body = b"content to be preserved across rename";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&["rename", "--source-profile", "s3util-e2e-test", &src, &dst]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        let dst_bytes = helper.get_object_bytes(&bucket, dst_key, None).await;
        assert_eq!(
            dst_bytes, body,
            "renamed object content must match original"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// Rename when the source key does not exist must exit with the not-found
    /// exit code without panicking.
    #[tokio::test]
    async fn rename_missing_source_exits_not_found() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let src = format!("s3://{}/nonexistent.txt", bucket);
        let dst = format!("s3://{}/dest.txt", bucket);

        let output = run_s3util(&["rename", "--source-profile", "s3util-e2e-test", &src, &dst]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_ERROR),
            "rename of nonexistent source must exit 1; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Dry run (1 test)
    // ---------------------------------------------------------------

    /// `--dry-run` must exit 0 and leave both source present and destination
    /// absent (no actual rename performed).
    #[tokio::test]
    async fn rename_dry_run_no_side_effects() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "dryrun_src.txt";
        let dst_key = "dryrun_dst.txt";
        let body = b"dry run body";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--dry-run",
            &src,
            &dst,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename --dry-run must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket, src_key, None).await,
            "source must still exist after --dry-run"
        );
        assert!(
            !helper.is_object_exist(&bucket, dst_key, None).await,
            "destination must not exist after --dry-run"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Conditional checks — source-if-match (2 tests)
    // ---------------------------------------------------------------

    /// `--source-if-match <correct ETag>` must succeed and rename the object.
    #[tokio::test]
    async fn rename_source_if_match_correct_etag_succeeds() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "sim_src.txt";
        let dst_key = "sim_dst.txt";
        let body = b"source-if-match test";
        let (bucket, etag) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--source-if-match",
            &etag,
            &src,
            &dst,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with correct --source-if-match must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!helper.is_object_exist(&bucket, src_key, None).await);
        assert!(helper.is_object_exist(&bucket, dst_key, None).await);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// `--source-if-match <wrong ETag>` must fail (412 precondition) and
    /// leave the source object intact.
    #[tokio::test]
    async fn rename_source_if_match_wrong_etag_fails() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "sim_fail_src.txt";
        let dst_key = "sim_fail_dst.txt";
        let body = b"source-if-match fail test";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--source-if-match",
            "\"aaaabbbbccccdddd0000111122223333\"",
            &src,
            &dst,
        ]);

        assert_ne!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with wrong --source-if-match must not succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket, src_key, None).await,
            "source must remain after precondition failure"
        );
        assert!(
            !helper.is_object_exist(&bucket, dst_key, None).await,
            "destination must not be created after precondition failure"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Conditional checks — source-if-none-match (2 tests)
    // ---------------------------------------------------------------

    /// `--source-if-none-match <ETAG>` when the provided ETag matches the
    /// source object's actual ETag: condition "source ETag ≠ provided" is
    /// false → 412 precondition failure. Source must remain intact.
    #[tokio::test]
    async fn rename_source_if_none_match_matching_etag_fails() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "sinm_fail_src.txt";
        let dst_key = "sinm_fail_dst.txt";
        let body = b"source-if-none-match matching etag test";
        let (bucket, etag) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--source-if-none-match",
            &etag,
            &src,
            &dst,
        ]);

        assert_ne!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with --source-if-none-match matching the actual ETag must not succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket, src_key, None).await,
            "source must remain after precondition failure"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// `--source-if-none-match <ETAG>` when the provided ETag does not match
    /// the source object's actual ETag: condition "source ETag ≠ provided" is
    /// true → rename succeeds.
    #[tokio::test]
    async fn rename_source_if_none_match_non_matching_etag_succeeds() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "sinm_ok_src.txt";
        let dst_key = "sinm_ok_dst.txt";
        let body = b"source-if-none-match non-matching etag test";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--source-if-none-match",
            "\"aaaabbbbccccdddd0000111122223333\"",
            &src,
            &dst,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with --source-if-none-match not matching the actual ETag must succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!helper.is_object_exist(&bucket, src_key, None).await);
        assert!(helper.is_object_exist(&bucket, dst_key, None).await);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Conditional checks — target-if-none-match (2 tests)
    // ---------------------------------------------------------------

    /// `--target-if-none-match <ETAG>` when the provided ETag matches the
    /// destination object's actual ETag: condition "destination ETag ≠
    /// provided" is false → 412 precondition failure. Source and destination
    /// must remain unchanged.
    #[tokio::test]
    async fn rename_target_if_none_match_matching_etag_fails() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "tinm_fail_src.txt";
        let dst_key = "tinm_fail_dst.txt";
        let src_body = b"source body";
        let dst_body = b"pre-existing destination body";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, src_body).await;
        helper.put_object(&bucket, dst_key, dst_body.to_vec()).await;
        let dst_head = helper.head_object(&bucket, dst_key, None).await;
        let dst_etag = dst_head.e_tag().unwrap().to_string();

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--target-if-none-match",
            &dst_etag,
            &src,
            &dst,
        ]);

        assert_ne!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with --target-if-none-match matching destination ETag must not succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket, src_key, None).await,
            "source must remain"
        );
        let dst_bytes = helper.get_object_bytes(&bucket, dst_key, None).await;
        assert_eq!(dst_bytes, dst_body, "destination content must be unchanged");

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// `--target-if-none-match <ETAG>` when the provided ETag does not match
    /// the destination object's actual ETag: condition "destination ETag ≠
    /// provided" is true → rename succeeds and destination is overwritten.
    #[tokio::test]
    async fn rename_target_if_none_match_non_matching_etag_succeeds() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "tinm_ok_src.txt";
        let dst_key = "tinm_ok_dst.txt";
        let src_body = b"source body for non-matching target";
        let dst_body = b"pre-existing destination body";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, src_body).await;
        helper.put_object(&bucket, dst_key, dst_body.to_vec()).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--target-if-none-match",
            "\"aaaabbbbccccdddd0000111122223333\"",
            &src,
            &dst,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with --target-if-none-match not matching destination ETag must succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!helper.is_object_exist(&bucket, src_key, None).await);
        assert!(helper.is_object_exist(&bucket, dst_key, None).await);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Conditional checks — target-if-match (2 tests)
    // ---------------------------------------------------------------

    /// `--target-if-match <correct ETag>` with a pre-existing destination
    /// whose ETag matches must overwrite the destination and succeed.
    #[tokio::test]
    async fn rename_target_if_match_correct_etag_succeeds() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "tim_src.txt";
        let dst_key = "tim_dst.txt";
        let src_body = b"source for target-if-match";
        let dst_body = b"existing destination";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, src_body).await;
        helper.put_object(&bucket, dst_key, dst_body.to_vec()).await;
        let dst_head = helper.head_object(&bucket, dst_key, None).await;
        let dst_etag = dst_head.e_tag().unwrap().to_string();

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--target-if-match",
            &dst_etag,
            &src,
            &dst,
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with correct --target-if-match must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!helper.is_object_exist(&bucket, src_key, None).await);
        let new_dst_bytes = helper.get_object_bytes(&bucket, dst_key, None).await;
        assert_eq!(
            new_dst_bytes, src_body,
            "destination must now contain source content"
        );

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// `--target-if-match <wrong ETag>` must fail (412) and leave both
    /// source and destination unchanged.
    #[tokio::test]
    async fn rename_target_if_match_wrong_etag_fails() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let src_key = "tim_fail_src.txt";
        let dst_key = "tim_fail_dst.txt";
        let src_body = b"source for target-if-match failure";
        let dst_body = b"existing destination unchanged";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, src_body).await;
        helper.put_object(&bucket, dst_key, dst_body.to_vec()).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "--target-if-match",
            "\"aaaabbbbccccdddd0000111122223333\"",
            &src,
            &dst,
        ]);

        assert_ne!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with wrong --target-if-match must not succeed; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(
            helper.is_object_exist(&bucket, src_key, None).await,
            "source must remain"
        );
        let dst_bytes = helper.get_object_bytes(&bucket, dst_key, None).await;
        assert_eq!(dst_bytes, dst_body, "destination content must be unchanged");

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Special characters in key (1 test)
    // ---------------------------------------------------------------

    /// Rename with spaces, slashes, and Unicode characters in the key name.
    /// The CLI must percent-encode them correctly when building the
    /// `rename_source` header.
    #[tokio::test]
    async fn rename_special_characters_in_key() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        // Key with a space, a subdirectory component, and non-ASCII characters.
        let src_key = "dir/file with spaces & unicode \u{00e9}.txt";
        let dst_key = "dir/renamed \u{00e9} file.txt";
        let body = b"special chars rename body";
        let (bucket, _) = setup_bucket_with_object(&helper, src_key, body).await;

        let src = format!("s3://{}/{}", bucket, src_key);
        let dst = format!("s3://{}/{}", bucket, dst_key);

        let output = run_s3util(&["rename", "--source-profile", "s3util-e2e-test", &src, &dst]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "rename with special-character keys must exit 0; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        assert!(!helper.is_object_exist(&bucket, src_key, None).await);
        assert!(helper.is_object_exist(&bucket, dst_key, None).await);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // CLI validation — no AWS contact (2 tests)
    // ---------------------------------------------------------------

    /// Non-Express-One-Zone bucket name (does not end with `--<az>--x-s3`)
    /// must be rejected by `validate()` before any AWS call, exiting 2.
    #[tokio::test]
    async fn rename_non_express_onezone_bucket_exits_2() {
        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "s3://my-regular-bucket/source.txt",
            "s3://my-regular-bucket/destination.txt",
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_CLAP_ARG_ERROR),
            "rename to non-Express-One-Zone bucket must exit 2; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// Source and destination in different buckets must be rejected by
    /// `validate()` before any AWS call, exiting 2.
    #[tokio::test]
    async fn rename_different_buckets_exits_2() {
        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            "s3://bucket-a--apne1-az4--x-s3/source.txt",
            "s3://bucket-b--apne1-az4--x-s3/destination.txt",
        ]);

        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_CLAP_ARG_ERROR),
            "rename across different buckets must exit 2; stdout={}, stderr={}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    /// rename against a directory bucket that does not exist must exit 1
    /// (consistent with the documented exit-code policy: unexpected
    /// operational failure, not the NotFound query exit 4).
    ///
    /// On current AWS the failure arrives as a dispatch-wrapped NoSuchBucket
    /// from the S3 Express CreateSession step (classified HeadError::Other),
    /// so the CLI's dedicated BucketNotFound arm is defensive; the assertion
    /// accepts either message shape and pins only the exit-1 policy.
    #[tokio::test]
    async fn rename_missing_bucket_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        // Never created; shape passes the client-side directory-bucket check.
        let bucket = format!(
            "s3util-e2e-{}--{}--x-s3",
            uuid::Uuid::new_v4().simple(),
            EXPRESS_ONE_ZONE_AZ
        );
        let source_arg = format!("s3://{bucket}/src-key");
        let target_arg = format!("s3://{bucket}/dst-key");
        let output = run_s3util(&[
            "rename",
            "--source-profile",
            "s3util-e2e-test",
            &source_arg,
            &target_arg,
        ]);

        assert_eq!(
            output.status.code(),
            Some(1),
            "rename on a missing directory bucket must exit 1; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("not found") || stderr.contains("NoSuchBucket"),
            "expected a bucket-related error message; stderr: {stderr}"
        );
    }
}
