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

    /// create-bucket + delete-bucket round-trip without tagging.
    #[tokio::test]
    async fn create_and_delete_bucket_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = TestHelper::generate_bucket_name();
        let bucket_arg = format!("s3://{bucket}");

        // Create the bucket
        let create_output = run_s3util(&[
            "create-bucket",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        assert!(
            create_output.status.success(),
            "create-bucket should succeed; stderr: {}",
            String::from_utf8_lossy(&create_output.stderr)
        );

        // Confirm bucket exists via head-bucket
        let helper = TestHelper::new().await;
        assert!(
            helper.is_bucket_exist(&bucket).await,
            "bucket should exist after create-bucket"
        );

        // Delete the bucket
        let delete_output = run_s3util(&[
            "delete-bucket",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        assert!(
            delete_output.status.success(),
            "delete-bucket should succeed; stderr: {}",
            String::from_utf8_lossy(&delete_output.stderr)
        );

        // Confirm bucket no longer exists. S3 has eventual consistency for
        // bucket deletion: HeadBucket may still succeed briefly after
        // DeleteBucket returns 200. Poll for up to 30s (15 attempts × 2s)
        // for the deletion to propagate.
        let mut gone = false;
        for _ in 0..15 {
            if !helper.is_bucket_exist(&bucket).await {
                gone = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        assert!(
            gone,
            "bucket should not exist after delete-bucket (waited 30s for S3 eventual consistency)"
        );
    }

    /// create-bucket with --tagging applies tags on success.
    #[tokio::test]
    async fn create_bucket_with_tagging_applies_tags() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = TestHelper::generate_bucket_name();
        let bucket_arg = format!("s3://{bucket}");

        let output = run_s3util(&[
            "create-bucket",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-region",
            REGION,
            "--tagging",
            "env=test&team=sre",
            &bucket_arg,
        ]);

        let helper = TestHelper::new().await;

        if !output.status.success() {
            let _ = helper.delete_bucket_with_cascade(&bucket).await;
            panic!(
                "create-bucket with tagging should succeed; stderr: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }

        // Verify exit code is 0 (not 3/Warning)
        assert_eq!(output.status.code(), Some(0));

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// delete-bucket on a non-empty bucket exits non-zero (BucketNotEmpty).
    #[tokio::test]
    async fn delete_bucket_non_empty_exits_non_zero() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.put_empty_object(&bucket, "sentinel").await;

        let bucket_arg = format!("s3://{bucket}");
        let output = run_s3util(&[
            "delete-bucket",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !output.status.success(),
            "delete-bucket on non-empty bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }

    /// delete-bucket on a non-existent bucket exits non-zero.
    #[tokio::test]
    async fn delete_bucket_missing_bucket_exits_non_zero() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "delete-bucket",
            "--target-profile",
            "s3sync-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "delete-bucket on missing bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }
}
