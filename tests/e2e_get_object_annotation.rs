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
    /// 4 (S3 returns NoSuchBucket → `ObjectAnnotationError::BucketNotFound`).
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
    /// ObjectAnnotationError::NotFound → exit 4). Using a real version ID avoids the
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
    /// generated it), so S3 returns NoSuchVersion (→ ObjectAnnotationError::NotFound → exit
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

    /// The outfile may be a bare filename: it must be resolved against the
    /// working directory (the temp-file-then-rename dance needs a usable
    /// parent even when the path has no directory component).
    ///
    /// Note: the "integrity could not be verified" warning branch is not
    /// reachable through real AWS — PutObjectAnnotation exposes no SSE
    /// parameters and annotation objects do not inherit the bucket's default
    /// KMS encryption, so a checksum-less annotation always carries a plain
    /// MD5 ETag and verifies.
    #[tokio::test]
    async fn get_object_annotation_to_bare_filename_outfile_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(&bucket, "test_object", b"content".to_vec())
            .await;
        helper
            .put_object_annotation(
                &bucket,
                "test_object",
                None,
                TEST_ANNOTATION_NAME,
                TEST_ANNOTATION_VALUE,
                None,
            )
            .await;

        let work_dir = TestHelper::create_temp_dir();
        let object_arg = format!("s3://{bucket}/test_object");
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args([
                "get-object-annotation",
                "--target-profile",
                "s3util-e2e-test",
                "--annotation-name",
                TEST_ANNOTATION_NAME,
                &object_arg,
                "annotation_out.json",
            ])
            .current_dir(&work_dir)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn s3util");

        let payload = std::fs::read_to_string(work_dir.join("annotation_out.json")).ok();
        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&work_dir);

        assert_eq!(
            output.status.code(),
            Some(0),
            "bare-filename outfile must work; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(
            payload.as_deref(),
            Some(TEST_ANNOTATION_VALUE),
            "the payload must be written to the bare-filename outfile"
        );
    }

    /// An annotation that carries an `algorithm` additional checksum (SHA512,
    /// MD5, XXHASH128, XXHASH3 or XXHASH64) must be verified and written, both
    /// to a file and to stdout. Before s3util could calculate these
    /// algorithms, such an annotation was an integrity error (exit 1).
    ///
    /// The annotation is put through the SDK with an explicit checksum,
    /// because `put-object-annotation` always sends CRC64NVME.
    ///
    /// Exit 0 alone does not prove the checksum was checked: annotations are
    /// AES256-encrypted, so the ETag (MD5) check always passes too. Because
    /// the command exits 1 if a returned checksum is unsupported or does not
    /// match, the test also asserts that S3 returned this checksum, that it
    /// was the only one, and that it is not COMPOSITE (a composite checksum is
    /// skipped).
    async fn get_object_annotation_with_checksum(algorithm: &str) {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "annotation-checksum-target.txt";
        helper
            .put_object(&bucket, key, b"object body".to_vec())
            .await;

        let annotation_name = "checksum-note";
        let payload = format!("annotation payload with a {algorithm} checksum");
        helper
            .put_object_annotation_with_checksum(
                &bucket,
                key,
                None,
                annotation_name,
                &payload,
                algorithm,
            )
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let out_file = tmp_dir.join("got-annotation.bin");
        let out_path = out_file.to_str().unwrap();
        let object_arg = format!("s3://{bucket}/{key}");

        let file_out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            annotation_name,
            &object_arg,
            out_path,
        ]);
        let stdout_out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            annotation_name,
            &object_arg,
            "-",
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;
        let written = std::fs::read(out_path).ok();
        let _ = std::fs::remove_dir_all(&tmp_dir);

        // File output.
        assert_eq!(
            file_out.status.code(),
            Some(0),
            "get-object-annotation with a {algorithm} checksum must exit 0; stderr: {}",
            String::from_utf8_lossy(&file_out.stderr)
        );
        assert_eq!(
            written.as_deref(),
            Some(payload.as_bytes()),
            "the outfile must contain the annotation payload"
        );

        let json: serde_json::Value =
            serde_json::from_slice(&file_out.stdout).expect("stdout must be valid JSON");
        let checksum_key = format!("Checksum{algorithm}");
        assert_eq!(
            json[checksum_key.as_str()].as_str(),
            Some(TestHelper::compute_additional_checksum(algorithm, payload.as_bytes()).as_str()),
            "JSON must contain the {algorithm} checksum of the payload; got: {json}"
        );
        // get-object-annotation verifies only the first checksum it finds (the
        // CRC family first), so this checksum is the verified one only when it
        // is the sole checksum S3 returned.
        let other_checksums: Vec<&String> = json
            .as_object()
            .expect("JSON must be an object")
            .keys()
            .filter(|k| k.starts_with("Checksum") && *k != "ChecksumType" && **k != checksum_key)
            .collect();
        assert!(
            other_checksums.is_empty(),
            "{checksum_key} must be the only checksum; got: {json}"
        );
        assert_ne!(
            json.get("ChecksumType").and_then(|v| v.as_str()),
            Some("COMPOSITE"),
            "a COMPOSITE checksum is not verified; got: {json}"
        );

        // Stdout output: verified against the in-transit payload before it is
        // written.
        assert_eq!(
            stdout_out.status.code(),
            Some(0),
            "get-object-annotation to stdout with a {algorithm} checksum must exit 0; stderr: {}",
            String::from_utf8_lossy(&stdout_out.stderr)
        );
        assert_eq!(
            stdout_out.stdout.as_slice(),
            payload.as_bytes(),
            "stdout must be the annotation payload"
        );
    }

    #[tokio::test]
    async fn get_object_annotation_with_checksum_sha512_exits_0() {
        get_object_annotation_with_checksum("SHA512").await;
    }

    #[tokio::test]
    async fn get_object_annotation_with_checksum_md5_exits_0() {
        get_object_annotation_with_checksum("MD5").await;
    }

    #[tokio::test]
    async fn get_object_annotation_with_checksum_xxhash128_exits_0() {
        get_object_annotation_with_checksum("XXHASH128").await;
    }

    #[tokio::test]
    async fn get_object_annotation_with_checksum_xxhash3_exits_0() {
        get_object_annotation_with_checksum("XXHASH3").await;
    }

    #[tokio::test]
    async fn get_object_annotation_with_checksum_xxhash64_exits_0() {
        get_object_annotation_with_checksum("XXHASH64").await;
    }

    /// `--target-version-id` with an XXHASH3 annotation checksum. v1
    /// (non-current) and v2 (current) each carry an annotation of the same
    /// name with an XXHASH3 checksum of a different payload, so the command
    /// must fetch, verify and write v1's annotation: reading v2's writes v2's
    /// payload and reports v2's checksum.
    ///
    /// As in `get_object_annotation_with_checksum`, the checksum must be the
    /// only one S3 returned and not COMPOSITE, so exit 0 proves it was
    /// verified.
    #[tokio::test]
    async fn get_object_annotation_with_target_version_id_and_checksum_xxhash3_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-annotation-checksum.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"version one body".to_vec())
            .await;
        let v2 = helper
            .put_object_with_version(&bucket, key, b"version two body".to_vec())
            .await;

        let annotation_name = "versioned-checksum-note";
        let v1_payload = "v1 annotation payload with an XXHASH3 checksum";
        let v2_payload = "v2 annotation payload, also with an XXHASH3 checksum";
        helper
            .put_object_annotation_with_checksum(
                &bucket,
                key,
                Some(v1.clone()),
                annotation_name,
                v1_payload,
                "XXHASH3",
            )
            .await;
        helper
            .put_object_annotation_with_checksum(
                &bucket,
                key,
                Some(v2),
                annotation_name,
                v2_payload,
                "XXHASH3",
            )
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let out_file = tmp_dir.join("v1-checksum-got.bin");
        let out_path = out_file.to_str().unwrap();
        let object_arg = format!("s3://{bucket}/{key}");

        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            annotation_name,
            "--target-version-id",
            &v1,
            &object_arg,
            out_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;
        let written = std::fs::read(out_path).ok();
        let _ = std::fs::remove_dir_all(&tmp_dir);

        assert_eq!(
            out.status.code(),
            Some(0),
            "get-object-annotation --target-version-id with an XXHASH3 checksum must exit 0; \
             stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            written.as_deref(),
            Some(v1_payload.as_bytes()),
            "the outfile must contain v1's annotation payload"
        );

        let json: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("stdout must be valid JSON");
        assert_eq!(
            json["ChecksumXXHASH3"].as_str(),
            Some(
                TestHelper::compute_additional_checksum("XXHASH3", v1_payload.as_bytes()).as_str()
            ),
            "JSON must contain the XXHASH3 checksum of v1's payload; got: {json}"
        );
        let other_checksums: Vec<&String> = json
            .as_object()
            .expect("JSON must be an object")
            .keys()
            .filter(|k| {
                k.starts_with("Checksum") && *k != "ChecksumType" && *k != "ChecksumXXHASH3"
            })
            .collect();
        assert!(
            other_checksums.is_empty(),
            "ChecksumXXHASH3 must be the only checksum; got: {json}"
        );
        assert_ne!(
            json.get("ChecksumType").and_then(|v| v.as_str()),
            Some("COMPOSITE"),
            "a COMPOSITE checksum is not verified; got: {json}"
        );
    }
}
