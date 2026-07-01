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

    /// Round-trip to a file: create an object, put-object-annotation a known
    /// payload, then get-object-annotation to a temp file; assert the file bytes
    /// equal the original payload and the printed JSON contains `ContentLength`,
    /// `ETag`, and `ChecksumCRC64NVME`.
    #[tokio::test]
    async fn get_object_annotation_roundtrip_to_file_exits_0_and_returns_json() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "annotation-get-target.txt";
        helper
            .put_object(&bucket, key, b"object body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload = b"hello get annotation";
        let payload_file = TestHelper::create_test_file(&tmp_dir, "payload.txt", payload);
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // First put the annotation so that get can retrieve it.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "get-test-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation must succeed before get test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Now get the annotation to a file.
        let out_file = tmp_dir.join("got-annotation.bin");
        let out_path = out_file.to_str().unwrap();
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "get-test-note",
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "get-object-annotation should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        // The output file must contain exactly the original payload bytes.
        let got_bytes = std::fs::read(out_path).expect("output file must exist");
        assert_eq!(
            got_bytes.as_slice(),
            payload,
            "retrieved annotation payload must equal the put payload"
        );

        // stdout must be valid JSON containing ContentLength, ETag, ChecksumCRC64NVME.
        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert!(
            json.get("ContentLength").is_some(),
            "JSON must contain ContentLength; got: {json}"
        );
        assert!(
            json.get("ETag").is_some(),
            "JSON must contain ETag; got: {json}"
        );
        assert!(
            json.get("ChecksumCRC64NVME").is_some(),
            "JSON must contain ChecksumCRC64NVME; got: {json}"
        );
        assert!(
            !json["ChecksumCRC64NVME"].as_str().unwrap_or("").is_empty(),
            "ChecksumCRC64NVME must not be empty; got: {json}"
        );
    }

    /// Round-trip to stdout: put-object-annotation a known payload, then
    /// get-object-annotation with `-` as outfile; assert stdout bytes equal the
    /// payload bytes and that no JSON object is printed to stdout.
    #[tokio::test]
    async fn get_object_annotation_roundtrip_to_stdout_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "annotation-get-stdout-target.txt";
        helper
            .put_object(&bucket, key, b"stdout test body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload = b"payload for stdout get";
        let payload_file = TestHelper::create_test_file(&tmp_dir, "stdout-payload.txt", payload);
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Put the annotation first.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "stdout-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation must succeed before get test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Get the annotation to stdout (`-`).
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "stdout-note",
            &object_arg,
            "-",
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "get-object-annotation to stdout should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        // stdout must be exactly the raw payload bytes (no JSON wrapping).
        assert_eq!(
            out.stdout.as_slice(),
            payload,
            "stdout content must equal the put payload bytes"
        );
        // stdout must NOT be a JSON object (raw bytes only).
        assert!(
            serde_json::from_slice::<serde_json::Value>(&out.stdout).is_err(),
            "stdout must be raw bytes, not JSON"
        );
    }

    /// Not-found: get-object-annotation on a key that does not exist must exit 4
    /// (S3 returns NotFound / NoSuchKey).
    #[tokio::test]
    async fn get_object_annotation_missing_key_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        let out_file = tmp_dir.join("nf-out.bin");
        let out_path = out_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "nf-note",
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "get-object-annotation on missing key must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing key must exit 4 (NoSuchKey); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// `--target-version-id` round-trip: on a versioned bucket, annotate a
    /// specific (older) version and read that annotation back by the same
    /// version ID. Exit 0 plus the correct payload proves version targeting
    /// worked — the current version carries no annotation, so a non-targeted get
    /// would have exited 4.
    #[tokio::test]
    async fn get_object_annotation_with_target_version_id_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-get-target.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"version one body".to_vec())
            .await;
        // Newer version so v1 is non-current.
        helper
            .put_object_with_version(&bucket, key, b"version two body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload = b"annotation for v1 get";
        let payload_file = TestHelper::create_test_file(&tmp_dir, "v1-get-payload.txt", payload);
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Annotate v1 specifically.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "v1-get-note",
            "--annotation-payload",
            payload_path,
            "--target-version-id",
            &v1,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation on v1 must succeed before get test; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Get v1's annotation back by version ID.
        let out_file = tmp_dir.join("v1-got.bin");
        let out_path = out_file.to_str().unwrap();
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "v1-get-note",
            "--target-version-id",
            &v1,
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "get-object-annotation --target-version-id should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let got_bytes = std::fs::read(out_path).expect("output file must exist");
        assert_eq!(
            got_bytes.as_slice(),
            payload,
            "retrieved v1 annotation payload must equal the put payload"
        );
    }

    /// Not-found: get-object-annotation on a bucket that does not exist must exit
    /// 4 (S3 returns NoSuchBucket → `HeadError::BucketNotFound`).
    #[tokio::test]
    async fn get_object_annotation_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());

        let tmp_dir = TestHelper::create_temp_dir();
        let out_file = tmp_dir.join("nb-out.bin");
        let out_path = out_file.to_str().unwrap();

        let object_arg = format!("s3://{nonexistent}/some-key");
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "nb-note",
            &object_arg,
            out_path,
        ]);

        assert!(
            !out.status.success(),
            "get-object-annotation on missing bucket must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Object-version not-found: the object exists, but we request a *real,
    /// well-formed* version ID that belongs to a different object, so it is
    /// absent for this key. S3 returns NoSuchVersion (mapped to
    /// HeadError::NotFound → exit 4). Using a real version ID avoids the
    /// 400 InvalidArgument that a fabricated ID could trigger.
    #[tokio::test]
    async fn get_object_annotation_version_id_from_other_object_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "version-nf-get-target.txt";
        helper
            .put_object_with_version(&bucket, key, b"current body".to_vec())
            .await;
        let other_version = helper
            .put_object_with_version(&bucket, "other-get-object.txt", b"other body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let out_file = tmp_dir.join("nv-out.bin");
        let out_path = out_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "nv-note",
            "--target-version-id",
            &other_version,
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "get-object-annotation with a nonexistent version must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "nonexistent version must exit 4 (NoSuchVersion); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Object-version *truly* not-found. Unlike
    /// `get_object_annotation_version_id_from_other_object_exits_4` — which borrows a
    /// still-existing version ID from a *different* object — this test deletes a
    /// real version of the *same* key, so the requested version ID no longer
    /// refers to any existing version. The ID is guaranteed well-formed (S3
    /// generated it), so S3 returns NoSuchVersion (→ HeadError::NotFound → exit
    /// 4) rather than the 400 InvalidArgument a fabricated ID could trigger. The
    /// annotation name is one that *does* exist on the current version, so the
    /// exit 4 can only come from the missing version, not a missing annotation.
    #[tokio::test]
    async fn get_object_annotation_deleted_version_id_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "deleted-version-get-target.txt";
        // v1 is deleted below; v2 stays current so the *key* still exists
        // (isolating NoSuchVersion from NoSuchKey).
        let v1 = helper
            .put_object_with_version(&bucket, key, b"body v1".to_vec())
            .await;
        helper
            .put_object_with_version(&bucket, key, b"body v2".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload = b"annotation on the current version";
        let payload_file = TestHelper::create_test_file(&tmp_dir, "live-payload.txt", payload);
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");

        // Annotate the current version so the annotation name genuinely exists;
        // the get below can then only fail on the missing version.
        let put_out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "live-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-annotation on the current version must succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Permanently delete v1: its (well-formed) version ID now refers to no
        // existing version of this key.
        helper.delete_object(&bucket, key, Some(v1.clone())).await;

        let out_file = tmp_dir.join("deleted-version-out.bin");
        let out_path = out_file.to_str().unwrap();
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "live-note",
            "--target-version-id",
            &v1,
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "get-object-annotation on a deleted (truly nonexistent) version must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "deleted version must exit 4 (NoSuchVersion); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
