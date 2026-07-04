//! E2E tests for cp/mv `--enable-sync-object-annotations` and
//! `--disable-check-annotation-etag` (ported from s3sync PR #243's
//! tests/s3_to_s3.rs annotation suite).
//!
//! These tests hit real AWS and are reserved for the user to run. The
//! executor only compile-checks them (under `RUSTFLAGS="--cfg e2e_test"`).
//!
//! Mapping notes vs s3sync (see the design spec for the full table):
//! - `..._max_keys` is dropped: s3util has no `--max-keys` flag.
//! - `..._with_prefix` becomes the renamed-key test (cp is single-object).
//! - sync-latest / report / lua tests are dropped (features not ported).
//! - Dry-run and mv tests drive the compiled binary because the cp_test_data
//!   helper bypasses the CLI's dry-run short-circuit.

#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::process::{Command, Output, Stdio};

    const EXIT_CODE_SUCCESS: i32 = 0;

    /// 9 MiB: crosses the 8 MiB default multipart threshold.
    const MULTIPART_SIZE: usize = 9 * 1024 * 1024;

    fn run_s3util(args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .unwrap()
    }

    async fn cp_with_annotations(source: &str, target: &str, extra: &[&str]) -> StatsCount {
        let helper = TestHelper::new().await;
        let mut args = vec![
            "s3util",
            "cp",
            "--source-profile",
            "s3util-e2e-test",
            "--target-profile",
            "s3util-e2e-test",
            "--enable-sync-object-annotations",
        ];
        args.extend_from_slice(extra);
        args.push(source);
        args.push(target);
        helper.cp_test_data(args).await
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_without_prefix.
    /// Two objects (one cp invocation each), three annotations, values
    /// verified on the target.
    #[tokio::test]
    async fn cp_sync_annotations_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object(&bucket1, "test_object2", b"test_object_2_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name2",
                "test_annotation_value2",
                None,
            )
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object2",
                None,
                "test_annotation_name3",
                "test_annotation_value3",
                None,
            )
            .await;

        let source1 = format!("s3://{}/test_object", bucket1);
        let target1 = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source1, &target1, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let source2 = format!("s3://{}/test_object2", bucket1);
        let target2 = format!("s3://{}/test_object2", bucket2);
        let stats = cp_with_annotations(&source2, &target2, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );
        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name2")
                .await,
            "test_annotation_value2"
        );
        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object2", None, "test_annotation_name3")
                .await,
            "test_annotation_value3"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_with_prefix (adapted).
    /// cp renames the key; annotations must follow the target key.
    #[tokio::test]
    async fn cp_sync_annotations_renamed_key() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "prefix/test_object",
                b"test_object_content".to_vec(),
            )
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "prefix/test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/prefix/test_object", bucket1);
        let target = format!("s3://{}/prefix2/renamed_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(
                    &bucket2,
                    "prefix2/renamed_object",
                    None,
                    "test_annotation_name1"
                )
                .await,
            "test_annotation_value1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_dry_run. Binary-driven:
    /// dry-run must not copy the object nor any annotation.
    #[tokio::test]
    async fn cp_sync_annotations_dry_run() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3util-e2e-test",
            "--target-profile",
            "s3util-e2e-test",
            "--enable-sync-object-annotations",
            "--dry-run",
            &source,
            &target,
        ]);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        assert!(!helper.is_object_exist(&bucket2, "test_object", None).await);
        assert!(
            !helper
                .is_object_annotation_exist(&bucket2, "test_object", None, "test_annotation_name1")
                .await
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_dry_run_server_side_copy.
    #[tokio::test]
    async fn cp_sync_annotations_dry_run_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let output = run_s3util(&[
            "cp",
            "--source-profile",
            "s3util-e2e-test",
            "--target-profile",
            "s3util-e2e-test",
            "--enable-sync-object-annotations",
            "--server-side-copy",
            "--dry-run",
            &source,
            &target,
        ]);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        assert!(!helper.is_object_exist(&bucket2, "test_object", None).await);
        assert!(
            !helper
                .is_object_annotation_exist(&bucket2, "test_object", None, "test_annotation_name1")
                .await
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_with_kms. SSE-KMS source
    /// objects: object ETag verify is skipped (non-MD5 ETag) and the
    /// annotation post-put ETag verification takes its skip path.
    #[tokio::test]
    async fn cp_sync_annotations_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object_kms(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_with_sha256. The SHA256
    /// additional checksum on the annotation must survive the copy.
    #[tokio::test]
    async fn cp_sync_annotations_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                TEST_ANNOTATION_NAME,
                TEST_ANNOTATION_VALUE,
                Some(TEST_ANNOTATION_VALUE_SHA2.to_string()),
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, TEST_ANNOTATION_NAME)
                .await,
            TEST_ANNOTATION_VALUE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_server_side_copy.
    /// Single-part CopyObject carries annotations inside S3 (manual sync is
    /// skipped) — annotations must still be present on the target.
    #[tokio::test]
    async fn cp_sync_annotations_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &["--server-side-copy"]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_multipart_upload.
    #[tokio::test]
    async fn cp_sync_annotations_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "large_file", vec![0_u8; MULTIPART_SIZE])
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "large_file",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/large_file", bucket1);
        let target = format!("s3://{}/large_file", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "large_file", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent: s3_to_s3_sync_annotation_multipart_upload_server_side_copy.
    /// UploadPartCopy does not carry annotations — manual sync must fill in.
    #[tokio::test]
    async fn cp_sync_annotations_multipart_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "large_file", vec![0_u8; MULTIPART_SIZE])
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "large_file",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/large_file", bucket1);
        let target = format!("s3://{}/large_file", bucket2);
        let stats = cp_with_annotations(&source, &target, &["--server-side-copy"]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "large_file", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3sync equivalent:
    /// s3_to_s3_sync_annotation_multipart_upload_server_side_copy_no_annotation.
    /// Control: WITHOUT the flag, a multipart server-side copy must NOT
    /// bring annotations along.
    #[tokio::test]
    async fn cp_sync_annotations_multipart_server_side_copy_without_flag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "large_file", vec![0_u8; MULTIPART_SIZE])
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "large_file",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/large_file", bucket1);
        let target = format!("s3://{}/large_file", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            !helper
                .is_object_annotation_exist(&bucket2, "large_file", None, "test_annotation_name1")
                .await
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Adapted from s3sync's s3_to_s3_versioning annotation tests:
    /// `--source-version-id` must pin which version's annotations are copied.
    #[tokio::test]
    async fn cp_sync_annotations_source_version_id() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let v1 = helper
            .put_object_with_version_id(&bucket1, "test_object", b"version1".to_vec())
            .await
            .expect("versioned bucket must return a version id");
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                Some(v1.clone()),
                "test_annotation_name1",
                "value_for_v1",
                None,
            )
            .await;

        let _v2 = helper
            .put_object_with_version_id(&bucket1, "test_object", b"version2".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "value_for_v2",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &["--source-version-id", &v1]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "value_for_v1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Adapted from s3sync's
    /// s3_to_s3_sync_annotation_with_sync_latest_disable_etag_verification
    /// (its sync-latest half is not ported): re-copy with
    /// --disable-check-annotation-etag after modifying the source annotation;
    /// the target must end up matching the source.
    #[tokio::test]
    async fn cp_sync_annotations_disable_check_annotation_etag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_error, 0);

        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1_modified",
                None,
            )
            .await;

        let stats =
            cp_with_annotations(&source, &target, &["--disable-check-annotation-etag"]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1_modified"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3util-specific: cp onto a target that already exists (second copy)
    /// after the source annotation set changed. Regardless of whether S3
    /// preserves annotations across overwrites, the final target annotations
    /// must equal the current source set.
    #[tokio::test]
    async fn cp_sync_annotations_overwrite_existing_target() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name2",
                "test_annotation_value2",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_error, 0);

        // Change the source annotation set: delete name2, add name_added.
        helper
            .delete_object_annotation(&bucket1, "test_object", None, "test_annotation_name2")
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name_added",
                "test_annotation_value_added",
                None,
            )
            .await;

        let stats = cp_with_annotations(&source, &target, &[]).await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );
        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name_added")
                .await,
            "test_annotation_value_added"
        );
        assert!(
            !helper
                .is_object_annotation_exist(&bucket2, "test_object", None, "test_annotation_name2")
                .await
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// s3util-specific: mv must copy annotations AND delete the source.
    #[tokio::test]
    async fn mv_sync_annotations() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "test_object", b"test_object_content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name1",
                "test_annotation_value1",
                None,
            )
            .await;
        helper
            .put_object_annotation(
                &bucket1,
                "test_object",
                None,
                "test_annotation_name2",
                "test_annotation_value2",
                None,
            )
            .await;

        let source = format!("s3://{}/test_object", bucket1);
        let target = format!("s3://{}/test_object", bucket2);
        let output = run_s3util(&[
            "mv",
            "--source-profile",
            "s3util-e2e-test",
            "--target-profile",
            "s3util-e2e-test",
            "--enable-sync-object-annotations",
            &source,
            &target,
        ]);
        assert_eq!(
            output.status.code(),
            Some(EXIT_CODE_SUCCESS),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name1")
                .await,
            "test_annotation_value1"
        );
        assert_eq!(
            helper
                .get_object_annotation(&bucket2, "test_object", None, "test_annotation_name2")
                .await,
            "test_annotation_value2"
        );
        assert!(!helper.is_object_exist(&bucket1, "test_object", None).await);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }
}
