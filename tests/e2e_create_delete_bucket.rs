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
            "s3util-e2e-test",
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
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        assert!(
            delete_output.status.success(),
            "delete-bucket should succeed; stderr: {}",
            String::from_utf8_lossy(&delete_output.stderr)
        );

        // Confirm bucket no longer exists. S3 has eventual consistency on
        // bucket deletion: HeadBucket may briefly return 301 PermanentRedirect
        // or 403 (the bucket name stays reserved) before settling on 404.
        // Use s3util's own head-bucket exit code as the check — it treats
        // *any* HeadBucket error as "not accessible" (exit 1), which is
        // what we actually care about. Poll for up to 60s (30 × 2s).
        let mut gone = false;
        for _ in 0..30 {
            let head_out = run_s3util(&[
                "head-bucket",
                "--target-profile",
                "s3util-e2e-test",
                "--target-region",
                REGION,
                &bucket_arg,
            ]);
            if !head_out.status.success() {
                gone = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        assert!(
            gone,
            "bucket should not exist after delete-bucket (waited 60s for S3 eventual consistency)"
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
            "s3util-e2e-test",
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

        // Verify tags were actually applied by reading them back.
        let get_out = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        let get_ok = get_out.status.success();
        let get_stderr = String::from_utf8_lossy(&get_out.stderr).to_string();
        let get_stdout = String::from_utf8_lossy(&get_out.stdout).to_string();

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            get_ok,
            "get-bucket-tagging after create should succeed; stderr: {get_stderr}"
        );
        let json: serde_json::Value =
            serde_json::from_str(&get_stdout).expect("get-bucket-tagging stdout must be JSON");
        let tag_set = json["TagSet"].as_array().expect("TagSet must be an array");
        assert_eq!(
            tag_set.len(),
            2,
            "expected 2 tags applied by create-bucket --tagging; got: {get_stdout}"
        );
        let pairs: std::collections::HashMap<String, String> = tag_set
            .iter()
            .map(|t| {
                (
                    t["Key"].as_str().unwrap_or("").to_string(),
                    t["Value"].as_str().unwrap_or("").to_string(),
                )
            })
            .collect();
        assert_eq!(pairs.get("env").map(String::as_str), Some("test"));
        assert_eq!(pairs.get("team").map(String::as_str), Some("sre"));
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
            "s3util-e2e-test",
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

    /// create-bucket + delete-bucket round-trip for a Single-AZ directory
    /// bucket (S3 Express One Zone). Exercises the directory-bucket branch
    /// of `parse_directory_bucket_zone` (1-hyphen zone id → AvailabilityZone
    /// + SingleAvailabilityZone) end-to-end through the s3util CLI.
    #[tokio::test]
    async fn create_and_delete_directory_bucket_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        // Directory bucket names follow `<base>--<zone-id>--x-s3` and have
        // a 63-char total cap; the `s3e2e-<uuid>` base keeps us well under it.
        let bucket = format!(
            "s3e2e-{}--{}--x-s3",
            uuid::Uuid::new_v4(),
            EXPRESS_ONE_ZONE_AZ
        );
        let bucket_arg = format!("s3://{bucket}");

        let create_output = run_s3util(&[
            "create-bucket",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);

        let helper = TestHelper::new().await;

        if !create_output.status.success() {
            helper.delete_directory_bucket_with_cascade(&bucket).await;
            panic!(
                "create-bucket on directory bucket should succeed; stderr: {}",
                String::from_utf8_lossy(&create_output.stderr)
            );
        }
        assert!(
            helper.is_bucket_exist(&bucket).await,
            "directory bucket should exist after create-bucket"
        );

        let delete_output = run_s3util(&[
            "delete-bucket",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        if !delete_output.status.success() {
            helper.delete_directory_bucket_with_cascade(&bucket).await;
            panic!(
                "delete-bucket on directory bucket should succeed; stderr: {}",
                String::from_utf8_lossy(&delete_output.stderr)
            );
        }

        // Same eventual-consistency dance as the general-purpose round-trip.
        let mut gone = false;
        for _ in 0..30 {
            let head_out = run_s3util(&[
                "head-bucket",
                "--target-profile",
                "s3util-e2e-test",
                "--target-region",
                REGION,
                &bucket_arg,
            ]);
            if !head_out.status.success() {
                gone = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
        assert!(
            gone,
            "directory bucket should not exist after delete-bucket (waited 60s)"
        );
    }

    /// `s3util create-bucket` on a directory-bucket name lands a bucket
    /// whose AZ location attributes match the zone id encoded in the name.
    /// Verifies via SDK head-bucket so the assertion bypasses our own
    /// JSON-shaping layer (we want to confirm S3 actually accepted the
    /// CreateBucketConfiguration we sent).
    #[tokio::test]
    async fn create_directory_bucket_sets_az_location_attributes() {
        use aws_sdk_s3::types::LocationType;

        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!(
            "s3e2e-{}--{}--x-s3",
            uuid::Uuid::new_v4(),
            EXPRESS_ONE_ZONE_AZ
        );
        let bucket_arg = format!("s3://{bucket}");

        let create_output = run_s3util(&[
            "create-bucket",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);

        let helper = TestHelper::new().await;

        if !create_output.status.success() {
            helper.delete_directory_bucket_with_cascade(&bucket).await;
            panic!(
                "create-bucket on directory bucket should succeed; stderr: {}",
                String::from_utf8_lossy(&create_output.stderr)
            );
        }

        let head = helper.head_bucket(&bucket).await;
        let location_type = head.bucket_location_type().cloned();
        let location_name = head.bucket_location_name().map(|s| s.to_string());

        helper.delete_directory_bucket_with_cascade(&bucket).await;

        assert_eq!(
            location_type,
            Some(LocationType::AvailabilityZone),
            "directory bucket created via s3util CLI must report \
             BucketLocationType=AvailabilityZone"
        );
        assert_eq!(
            location_name.as_deref(),
            Some(EXPRESS_ONE_ZONE_AZ),
            "BucketLocationName must echo back the zone id encoded in the bucket name"
        );
    }

    /// delete-bucket on a non-existent bucket exits non-zero.
    #[tokio::test]
    async fn delete_bucket_missing_bucket_exits_non_zero() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "delete-bucket",
            "--target-profile",
            "s3util-e2e-test",
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

    // -----------------------------------------------------------------
    // create-bucket --if-not-exists
    // -----------------------------------------------------------------

    #[tokio::test]
    async fn create_bucket_if_not_exists_skips_when_bucket_exists() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let output = run_s3util(&[
            "create-bucket",
            "--if-not-exists",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // Bucket must still be there (and accessible) after the no-op create.
        let still_exists = helper.is_bucket_exist(&bucket).await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "create-bucket --if-not-exists must exit 0 when bucket exists; stderr: {stderr}"
        );
        assert!(
            still_exists,
            "create-bucket --if-not-exists must NOT delete or recreate the bucket"
        );
        // The "Bucket exists; skipping create." line is logged at info!
        // level. Default verbosity for create-bucket is Warn, so the line is
        // filtered out — silent-on-skip is the deliberate design choice. Use
        // `-v` if you want to see it. Asserting only on observable side
        // effects (exit 0 + bucket still present) keeps the test honest.
    }

    #[tokio::test]
    async fn create_bucket_if_not_exists_proceeds_when_bucket_missing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let bucket_arg = format!("s3://{bucket}");

        // No pre-existence sanity check here: HeadBucket on a fresh,
        // never-created name primes AWS's negative-cache for a few seconds,
        // which then makes the post-create HeadBucket transiently report
        // 404 even though CreateBucket succeeded. The bucket name is
        // freshly UUID-generated, so a real pre-existence collision is
        // impossible.

        let output = run_s3util(&[
            "create-bucket",
            "--if-not-exists",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exists_after = helper.is_bucket_exist(&bucket).await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "create-bucket --if-not-exists must exit 0 on success; stderr: {stderr}"
        );
        assert!(
            exists_after,
            "create-bucket --if-not-exists must create the bucket when missing"
        );
    }

    #[tokio::test]
    async fn create_bucket_if_not_exists_with_tagging_skips_when_exists() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        // Create the bucket WITHOUT tags so we can detect a buggy retroactive
        // PutBucketTagging via the post-run get-bucket-tagging response.
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let output = run_s3util(&[
            "create-bucket",
            "--if-not-exists",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            "--tagging",
            "team=sre",
            &bucket_arg,
        ]);
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();

        // Issue GetBucketTagging via the SDK directly. The early-return
        // after the existence check means the tagging branch must NOT have
        // run, so AWS should respond with NoSuchTagSet (the bucket exists
        // but has no tags configured).
        let tag_result = helper
            .client
            .get_bucket_tagging()
            .bucket(&bucket)
            .send()
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "create-bucket --if-not-exists must exit 0 when bucket exists; stderr: {stderr}"
        );

        match tag_result {
            Ok(out) => panic!(
                "create-bucket --if-not-exists must NOT have applied tagging when the \
                 bucket already exists; got TagSet: {:?}",
                out.tag_set()
            ),
            Err(e) => {
                use aws_smithy_types::error::metadata::ProvideErrorMetadata;
                let code = e
                    .as_service_error()
                    .and_then(ProvideErrorMetadata::code)
                    .unwrap_or("")
                    .to_string();
                assert_eq!(
                    code, "NoSuchTagSet",
                    "expected NoSuchTagSet (no tags applied); got error code: {code:?}; \
                     full error: {e:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn create_bucket_if_not_exists_with_dry_run_does_not_create() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        let bucket_arg = format!("s3://{bucket}");

        // Sanity: bucket must not pre-exist.
        assert!(!helper.is_bucket_exist(&bucket).await);

        let output = run_s3util(&[
            "create-bucket",
            "--dry-run",
            "--if-not-exists",
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &bucket_arg,
        ]);
        let stderr = String::from_utf8_lossy(&output.stderr).to_string();
        let exists_after = helper.is_bucket_exist(&bucket).await;

        // Defensive: in case the dry-run did create it (which would be a bug).
        if exists_after {
            helper.delete_bucket_with_cascade(&bucket).await;
        }

        assert!(
            output.status.success(),
            "create-bucket --dry-run --if-not-exists must exit 0; stderr: {stderr}"
        );
        assert!(
            stderr.contains("[dry-run]"),
            "stderr must contain [dry-run] prefix; got: {stderr}"
        );
        assert!(
            !exists_after,
            "create-bucket --dry-run --if-not-exists must NOT create the bucket"
        );
    }
}
