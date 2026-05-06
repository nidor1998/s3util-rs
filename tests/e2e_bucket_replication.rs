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

    /// Build a minimal-but-valid replication-configuration JSON. S3 requires:
    /// - Versioning enabled on both source and destination buckets
    /// - An IAM role ARN that S3 can assume (a dummy ARN is sufficient for
    ///   put — S3 only validates the role at rule application time)
    /// - At least one rule with Status, Destination, and (when filter is
    ///   present) DeleteMarkerReplication
    ///
    /// To keep tests self-contained without IAM setup, we let put-bucket-replication
    /// fail server-side and instead exercise the request-shape via dry-run for
    /// the happy path; round-trip tests rely on fully provisioning IAM, which
    /// is out of scope for these tests.
    fn sample_replication(dest_bucket: &str) -> String {
        format!(
            r#"{{
                "Role": "arn:aws:iam::000000000000:role/s3-replication-test",
                "Rules": [
                    {{
                        "ID": "rule-1",
                        "Priority": 1,
                        "Filter": {{}},
                        "Status": "Enabled",
                        "DeleteMarkerReplication": {{ "Status": "Disabled" }},
                        "Destination": {{ "Bucket": "arn:aws:s3:::{dest_bucket}" }}
                    }}
                ]
            }}"#
        )
    }

    /// get-bucket-replication on a bucket without replication should exit 4
    /// (ReplicationConfigurationNotFoundError → NotFound).
    #[tokio::test]
    async fn get_replication_on_bucket_without_replication_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "get-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "get-bucket-replication on bucket without replication should fail"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "must exit 4 (ReplicationConfigurationNotFoundError)"
        );
    }

    /// get-bucket-replication on a non-existent bucket should exit 4 (NoSuchBucket).
    #[tokio::test]
    async fn get_replication_on_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "get-bucket-replication on missing bucket should fail"
        );
        assert_eq!(
            output.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket)"
        );
    }

    /// delete-bucket-replication on a bucket without replication is idempotent
    /// in S3 — succeeds silently. This confirms our wrapper exits 0.
    #[tokio::test]
    async fn delete_replication_on_bucket_without_replication_succeeds() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "delete-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            out.status.success(),
            "delete-bucket-replication on bucket without replication should succeed; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// delete-bucket-replication on a non-existent bucket should fail with exit 1.
    #[tokio::test]
    async fn delete_replication_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "delete-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "delete-bucket-replication on missing bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }

    /// put-bucket-replication on a non-existent bucket should fail with exit 1.
    /// The body is sent and S3 rejects with NoSuchBucket.
    #[tokio::test]
    async fn put_replication_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let dest = format!("s3util-nonexistent-dest-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let tmp_dir = TestHelper::create_temp_dir();
        let cfg_file = TestHelper::create_test_file(
            &tmp_dir,
            "replication.json",
            sample_replication(&dest).as_bytes(),
        );

        let out = run_s3util(&[
            "put-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            cfg_file.to_str().unwrap(),
        ]);

        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(
            !out.status.success(),
            "put-bucket-replication on missing bucket should fail"
        );
        assert_eq!(out.status.code(), Some(1));
    }

    /// put-bucket-replication with malformed JSON via file exits 1 at parse stage.
    #[tokio::test]
    async fn malformed_json_via_file_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let tmp_dir = TestHelper::create_temp_dir();
        let cfg_file =
            TestHelper::create_test_file(&tmp_dir, "replication.json", b"not valid json {");

        let out = run_s3util(&[
            "put-bucket-replication",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            cfg_file.to_str().unwrap(),
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;
        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(
            !out.status.success(),
            "put-bucket-replication with invalid JSON must fail"
        );
        assert_eq!(out.status.code(), Some(1));
    }

    /// put-bucket-replication with malformed JSON via stdin (`-`) exits 1.
    #[tokio::test]
    async fn malformed_json_via_stdin_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let out = run_s3util_with_stdin(
            &[
                "put-bucket-replication",
                "--target-profile",
                "s3util-e2e-test",
                &bucket_arg,
                "-",
            ],
            b"not valid json {",
        );

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "put-bucket-replication via stdin with invalid JSON must fail"
        );
        assert_eq!(out.status.code(), Some(1));
    }
}
