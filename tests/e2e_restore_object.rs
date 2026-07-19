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

    /// restore-object on a Standard-storage object should fail with
    /// `InvalidObjectState` (object is not in an archive tier) — exit 1.
    #[tokio::test]
    async fn restore_standard_class_object_exits_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "test-object.txt";
        helper.put_object(&bucket, key, b"hello".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "restore-object",
            "--target-profile",
            "s3util-e2e-test",
            "--days",
            "1",
            "--tier",
            "Standard",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "restore-object on Standard-class object should fail (InvalidObjectState)"
        );
        assert_eq!(out.status.code(), Some(1));
    }

    /// restore-object on a non-existent object should exit 4
    /// (S3 returns NoSuchKey → NotFound).
    #[tokio::test]
    async fn restore_missing_object_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let out = run_s3util(&[
            "restore-object",
            "--target-profile",
            "s3util-e2e-test",
            "--days",
            "1",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing object must exit 4 (NoSuchKey)"
        );
    }

    /// restore-object on a non-existent bucket should exit 4 (NoSuchBucket).
    #[tokio::test]
    async fn restore_on_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let object_arg = format!("s3://{nonexistent}/key");
        let out = run_s3util(&[
            "restore-object",
            "--target-profile",
            "s3util-e2e-test",
            "--days",
            "1",
            &object_arg,
        ]);

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket)"
        );
    }

    /// restore-object accepts all three tiers (Standard, Bulk, Expedited) at
    /// the parse stage. Server-side rejection (InvalidObjectState on Standard
    /// storage) is the same regardless of tier — but we want to confirm none
    /// of the tier values trigger a clap parse error.
    #[tokio::test]
    async fn restore_accepts_each_tier_value() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "test-object.txt";
        helper.put_object(&bucket, key, b"hello".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");

        for tier in ["Standard", "Bulk", "Expedited"] {
            let out = run_s3util(&[
                "restore-object",
                "--target-profile",
                "s3util-e2e-test",
                "--days",
                "1",
                "--tier",
                tier,
                &object_arg,
            ]);
            // Exit 1 is expected (InvalidObjectState) — we just need to
            // confirm the tier value parses (i.e. exit code is not 2).
            assert_ne!(
                out.status.code(),
                Some(2),
                "tier {tier} should not trigger clap parse error; stderr: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Initiating a restore of a GLACIER-class object succeeds immediately
    /// (S3 answers 202 and the retrieval happens asynchronously) — the
    /// success path must log "Restore initiated." and exit 0.
    #[tokio::test]
    async fn restore_glacier_object_initiates_and_exits_0() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .client
            .put_object()
            .bucket(&bucket)
            .key("glacier-object")
            .body(aws_sdk_s3::primitives::ByteStream::from(
                b"cold data".to_vec(),
            ))
            .storage_class(aws_sdk_s3::types::StorageClass::Glacier)
            .send()
            .await
            .unwrap();

        let object_arg = format!("s3://{bucket}/glacier-object");
        let output = run_s3util(&[
            "restore-object",
            "--target-profile",
            "s3util-e2e-test",
            "--days",
            "1",
            "--tier",
            "Bulk",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert_eq!(
            output.status.code(),
            Some(0),
            "restore-object on a GLACIER object must exit 0; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
