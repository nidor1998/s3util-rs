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

    /// Roundtrip: create an object, run put-object-annotation with a small
    /// payload file, assert exit 0, and confirm the JSON response contains
    /// both `AnnotationName` and `ChecksumCRC64NVME`.
    ///
    /// This also exercises the CRC64NVME verification success path: when the
    /// command exits 0 the local CRC has been matched against the S3 response.
    #[tokio::test]
    async fn put_object_annotation_roundtrip_exits_0_and_returns_json() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "annotation-target.txt";
        helper
            .put_object(&bucket, key, b"object body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "payload.txt", b"hello annotation");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "test-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "put-object-annotation should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert_eq!(
            json.get("AnnotationName").and_then(|v| v.as_str()),
            Some("test-note"),
            "JSON must contain AnnotationName=test-note; got: {json}"
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

    /// stdin (`-`) payload path: pipe the annotation payload via stdin and
    /// assert exit 0 and well-formed JSON response.
    #[tokio::test]
    async fn put_object_annotation_stdin_payload_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "annotation-stdin-target.txt";
        helper
            .put_object(&bucket, key, b"stdin test body".to_vec())
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util_with_stdin(
            &[
                "put-object-annotation",
                "--target-profile",
                "s3util-e2e-test",
                "--annotation-name",
                "stdin-note",
                "--annotation-payload",
                "-",
                &object_arg,
            ],
            b"payload from stdin",
        );

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "put-object-annotation with stdin payload should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert_eq!(
            json.get("AnnotationName").and_then(|v| v.as_str()),
            Some("stdin-note"),
            "JSON must contain AnnotationName=stdin-note; got: {json}"
        );
        assert!(
            json.get("ChecksumCRC64NVME").is_some(),
            "JSON must contain ChecksumCRC64NVME; got: {json}"
        );
    }

    /// `--target-version-id` against a versioned bucket: enable versioning,
    /// put two versions of the same key, then annotate the first version by
    /// its version ID and assert exit 0.
    #[tokio::test]
    async fn put_object_annotation_with_target_version_id_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-annotation-target.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"version one body".to_vec())
            .await;
        // Put a second version so there is a non-current version to address.
        helper
            .put_object_with_version(&bucket, key, b"version two body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "v1-payload.txt", b"annotation for v1");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "v1-note",
            "--annotation-payload",
            payload_path,
            "--target-version-id",
            &v1,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "put-object-annotation --target-version-id should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));

        let stdout = String::from_utf8_lossy(&out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert_eq!(
            json.get("AnnotationName").and_then(|v| v.as_str()),
            Some("v1-note"),
            "JSON must contain AnnotationName=v1-note; got: {json}"
        );
    }

    /// `--target-request-payer` against a normal bucket: verifies the CLI
    /// accepts the flag and exits 0. On a non-requester-pays bucket the
    /// x-amz-request-payer header is accepted (or ignored) by S3 server-side.
    #[tokio::test]
    async fn put_object_annotation_with_target_request_payer_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "rp-annotation-target.txt";
        helper
            .put_object(&bucket, key, b"rp test body".to_vec())
            .await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "rp-payload.txt", b"request payer payload");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "rp-note",
            "--annotation-payload",
            payload_path,
            "--target-request-payer",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "put-object-annotation --target-request-payer should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));
    }

    /// Size boundary (minimum): a 1-byte payload succeeds (exit 0).
    #[tokio::test]
    async fn put_object_annotation_one_byte_payload_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "one-byte-target.txt";
        helper.put_object(&bucket, key, b"body".to_vec()).await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file = TestHelper::create_test_file(&tmp_dir, "one-byte.bin", b"x");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "one-byte-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "put-object-annotation with 1-byte payload should exit 0; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(out.status.code(), Some(0));
    }

    /// Size boundary (maximum exceeded): a payload larger than 1 MiB must fail
    /// locally with exit 1 before making any network call.
    ///
    /// No bucket or object is created — the validation must short-circuit
    /// before the client is even constructed.
    #[tokio::test]
    async fn put_object_annotation_oversized_payload_exits_1_without_network() {
        // 1 MiB + 1 byte exceeds the local validation limit.
        const OVERSIZED: usize = 1024 * 1024 + 1;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_sized_file(&tmp_dir, "oversized-payload.bin", OVERSIZED);
        let payload_path = payload_file.to_str().unwrap();

        // Use a clearly non-existent bucket name — if the network is reached
        // the test would fail with exit 4 (not 1), which would also catch a
        // regression where validation is skipped.
        let object_arg = "s3://s3util-e2e-nonexistent-bucket-for-size-check/key";

        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "oversized-note",
            "--annotation-payload",
            payload_path,
            object_arg,
        ]);

        assert!(!out.status.success(), "oversized payload must not succeed");
        assert_eq!(
            out.status.code(),
            Some(1),
            "oversized payload must exit 1 (local validation); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Not-found: put-object-annotation on a key that does not exist must
    /// exit 4 (S3 returns NotFound / NoSuchKey).
    #[tokio::test]
    async fn put_object_annotation_missing_key_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "nf-payload.txt", b"not found payload");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "nf-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "put-object-annotation on missing key must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing key must exit 4 (NoSuchKey); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// Not-found: put-object-annotation on a bucket that does not exist must
    /// exit 4 (S3 returns NoSuchBucket).
    #[tokio::test]
    async fn put_object_annotation_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());

        let tmp_dir = TestHelper::create_temp_dir();
        let payload_file =
            TestHelper::create_test_file(&tmp_dir, "nb-payload.txt", b"no bucket payload");
        let payload_path = payload_file.to_str().unwrap();

        let object_arg = format!("s3://{nonexistent}/some-key");
        let out = run_s3util(&[
            "put-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "nb-note",
            "--annotation-payload",
            payload_path,
            &object_arg,
        ]);

        assert!(
            !out.status.success(),
            "put-object-annotation on missing bucket must not succeed"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket); stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
