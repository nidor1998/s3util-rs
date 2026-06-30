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

    /// Seed an object with one annotation via put-object-annotation, then run
    /// list-object-annotations; assert exit 0, `AnnotationCount` >= 1, and the
    /// `Annotations` array contains an entry whose `AnnotationName` matches the
    /// one that was put.
    #[tokio::test]
    async fn list_object_annotations_returns_seeded_annotation_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "list-annotation-target.txt";
        helper
            .put_object(&bucket, key, b"object body for list test".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "list-payload.txt", b"list annotation payload");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Seed an annotation so that list-object-annotations can return it.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "list-test-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation must succeed before list test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Now list the annotations.
        let out = run_s3util(&[
            "list-object-annotations",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "list-object-annotations should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        // stdout must be valid JSON.
        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");

        // AnnotationCount must be >= 1.
        let count = json["AnnotationCount"]
            .as_i64()
            .expect("AnnotationCount must be a number");
        assert!(count >= 1, "AnnotationCount must be >= 1; got: {json}");

        // Annotations array must contain an entry with the seeded AnnotationName.
        let annotations = json["Annotations"]
            .as_array()
            .expect("Annotations must be an array");
        let found = annotations
            .iter()
            .any(|a| a.get("AnnotationName").and_then(|v| v.as_str()) == Some("list-test-note"));
        assert!(
            found,
            "Annotations must contain AnnotationName=list-test-note; got: {json}"
        );
    }

    /// Seed two annotations with distinct prefixes (`alpha-` and `beta-`), then
    /// list-object-annotations with `--annotation-prefix alpha-`; assert exit 0
    /// and that only the `alpha-` annotation appears in the result.
    #[tokio::test]
    async fn list_object_annotations_prefix_filters_results_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "list-prefix-target.txt";
        helper
            .put_object(&bucket, key, b"prefix filter test body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let object_arg = format!("s3://{bucket}/{key}");

        // Seed annotation with prefix `alpha-`.
        let alpha_payload =
            TestHelper::create_test_file(&tmp_dir, "alpha-payload.txt", b"alpha payload");
        let put_alpha = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "alpha-note",
            "--annotation-payload",
            alpha_payload.to_str().unwrap(),
            &object_arg,
        ]);
        assert!(
            put_alpha.status.success(),
            "put-object-annotation (alpha-note) must succeed; stderr: {}",
            String::from_utf8_lossy(&put_alpha.stderr)
        );

        // Seed annotation with prefix `beta-`.
        let beta_payload =
            TestHelper::create_test_file(&tmp_dir, "beta-payload.txt", b"beta payload");
        let put_beta = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "beta-note",
            "--annotation-payload",
            beta_payload.to_str().unwrap(),
            &object_arg,
        ]);
        assert!(
            put_beta.status.success(),
            "put-object-annotation (beta-note) must succeed; stderr: {}",
            String::from_utf8_lossy(&put_beta.stderr)
        );

        // List with --annotation-prefix alpha- — only alpha-note should appear.
        let out = run_s3util(&[
            "list-object-annotations",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-prefix",
            "alpha-",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "list-object-annotations --annotation-prefix should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");

        let annotations = json["Annotations"]
            .as_array()
            .expect("Annotations must be an array");

        // alpha-note must be present.
        let has_alpha = annotations
            .iter()
            .any(|a| a.get("AnnotationName").and_then(|v| v.as_str()) == Some("alpha-note"));
        assert!(
            has_alpha,
            "Annotations must contain alpha-note when prefix=alpha-; got: {json}"
        );

        // beta-note must NOT be present.
        let has_beta = annotations
            .iter()
            .any(|a| a.get("AnnotationName").and_then(|v| v.as_str()) == Some("beta-note"));
        assert!(
            !has_beta,
            "Annotations must NOT contain beta-note when prefix=alpha-; got: {json}"
        );
    }

    /// Not-found: list-object-annotations on a key that does not exist must exit
    /// 4 (S3 returns NotFound / NoSuchKey).
    #[tokio::test]
    async fn list_object_annotations_missing_key_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "list-object-annotations",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "list-object-annotations on missing key must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing key must exit 4 (NoSuchKey); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// `--target-version-id`: annotate a specific (older) version, then list that
    /// version's annotations and confirm the entry appears. The current version
    /// carries no annotation, so finding the entry proves version targeting.
    #[tokio::test]
    async fn list_object_annotations_with_target_version_id_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-list-target.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"version one body".to_vec())
            .await;
        // Newer version so v1 is non-current.
        helper
            .put_object_with_version(&bucket, key, b"version two body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "v1-list-payload.txt", b"v1 list annotation");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Seed an annotation on v1 specifically.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "v1-list-note",
            "--annotation-payload",
            payload_path,
            "--target-version-id",
            &v1,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation on v1 must succeed before list test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // List v1's annotations by version ID.
        let out = run_s3util(&[
            "list-object-annotations",
            "--target-profile",
            "s3util-e2e-test",
            "--target-version-id",
            &v1,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "list-object-annotations --target-version-id should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        let annotations = json["Annotations"]
            .as_array()
            .expect("Annotations must be an array");
        let found = annotations
            .iter()
            .any(|a| a.get("AnnotationName").and_then(|v| v.as_str()) == Some("v1-list-note"));
        assert!(
            found,
            "Annotations for v1 must contain v1-list-note; got: {json}"
        );
    }

    /// Not-found: list-object-annotations on a bucket that does not exist must
    /// exit 4 (S3 returns NoSuchBucket → `HeadError::BucketNotFound`).
    #[tokio::test]
    async fn list_object_annotations_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());

        let object_arg = format!("s3://{nonexistent}/some-key");
        let out = run_s3util(&[
            "list-object-annotations",
            "--target-profile",
            "s3util-e2e-test",
            &object_arg,
        ]);

        assert!(
            !out.status.success(),
            "list-object-annotations on missing bucket must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
