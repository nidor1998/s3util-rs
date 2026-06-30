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

    /// Happy path: create an object, put an annotation, delete it, assert exit 0
    /// and empty stdout, then confirm the annotation is gone by running
    /// get-object-annotation for the same name and asserting exit 4.
    ///
    /// Note: after deletion the get command returns exit 4 because the annotation
    /// is absent and S3 classifies that as NoSuchAnnotation (mapped by
    /// GET_OBJECT_ANNOTATION_NOT_FOUND_CODES → HeadError::NotFound → exit 4).
    #[tokio::test]
    async fn delete_object_annotation_happy_path_exits_0_and_annotation_is_gone() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "del-annotation-target.txt";
        helper
            .put_object(&bucket, key, b"object body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "payload.txt", b"annotation to delete");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Seed the annotation so we have something to delete.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "del-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation must succeed before delete test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Delete the annotation.
        let out = run_s3util(&[
            "delete-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "del-note",
            &object_arg,
        ]);

        assert!(
            out.status.success(),
            "delete-object-annotation should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));
        // Stdout must be empty on success (the runner emits nothing to stdout).
        assert!(
            out.stdout.is_empty(),
            "stdout must be empty on success; got: {}",
            String::from_utf8_lossy(&out.stdout)
        );

        // Confirm the annotation is gone: get-object-annotation must exit 4.
        let get_out_file = tmp_dir.join("after-delete.bin");
        let get_out_path = get_out_file.to_str().unwrap();
        let get_out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "del-note",
            &object_arg,
            get_out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !get_out.status.success(),
            "get-object-annotation on deleted annotation must not succeed"
        );
        assert_eq!(
            get_out.status.code(),
            Some(4),
            "deleted annotation must cause get to exit 4 (NoSuchAnnotation); stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
    }

    /// Missing object: delete-object-annotation on a key that does not exist
    /// must exit 4 (S3 returns NoSuchKey, mapped by
    /// DELETE_OBJECT_ANNOTATION_NOT_FOUND_CODES → HeadError::NotFound → exit 4).
    #[tokio::test]
    async fn delete_object_annotation_missing_key_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "delete-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "note",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "delete-object-annotation on missing key must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing key must exit 4 (NoSuchKey); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Missing bucket: delete-object-annotation on a nonexistent bucket must
    /// exit 4 (S3 returns NoSuchBucket → HeadError::BucketNotFound → exit 4).
    #[tokio::test]
    async fn delete_object_annotation_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());

        let object_arg = format!("s3://{nonexistent}/some-key");
        let out = run_s3util(&[
            "delete-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "note",
            &object_arg,
        ]);

        assert!(
            !out.status.success(),
            "delete-object-annotation on missing bucket must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// --dry-run: exits 0 and the annotation is still present afterward
    /// (a follow-up get-object-annotation must exit 0).
    ///
    /// The dry-run path in the runner returns ExitStatus::Success immediately
    /// after emitting an info log, making no S3 call at all, so the annotation
    /// is untouched and can still be retrieved.
    #[tokio::test]
    async fn delete_object_annotation_dry_run_exits_0_and_annotation_survives() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "dry-run-del-target.txt";
        helper
            .put_object(&bucket, key, b"dry-run body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "dry-run-payload.txt", b"dry-run annotation");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Seed the annotation.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "dry-run-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation must succeed before dry-run delete test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Dry-run delete — must exit 0 without sending a delete request.
        let out = run_s3util(&[
            "delete-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "dry-run-note",
            "--dry-run",
            &object_arg,
        ]);

        assert!(
            out.status.success(),
            "delete-object-annotation --dry-run should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        // Annotation must still exist: get-object-annotation must exit 0.
        let get_out_file = tmp_dir.join("dry-run-get.bin");
        let get_out_path = get_out_file.to_str().unwrap();
        let get_out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "dry-run-note",
            &object_arg,
            get_out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            get_out.status.success(),
            "get-object-annotation after dry-run delete must exit 0 (annotation survives); stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        assert_eq!(get_out.status.code(), Some(0));
    }
}
