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

    /// Bucket-policy status on a bucket with a non-public Deny policy attached
    /// must report IsPublic=false. Mirrors the e2e_bucket_policy.rs sample,
    /// which is intentionally non-public so AWS Block Public Access doesn't
    /// reject it.
    fn sample_policy(bucket: &str) -> String {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"DenyInsecureTransport","Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::{bucket}","arn:aws:s3:::{bucket}/*"],"Condition":{{"Bool":{{"aws:SecureTransport":"false"}}}}}}]}}"#
        )
    }

    /// Round-trip: put policy → get-bucket-policy-status → expect IsPublic=false.
    #[tokio::test]
    async fn get_policy_status_after_put_returns_is_public_false() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let policy_json = sample_policy(&bucket);
        let tmp_dir = TestHelper::create_temp_dir();
        let policy_file =
            TestHelper::create_test_file(&tmp_dir, "policy.json", policy_json.as_bytes());

        // Put policy
        let put_out = run_s3util(&[
            "put-bucket-policy",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            policy_file.to_str().unwrap(),
        ]);
        assert!(
            put_out.status.success(),
            "put-bucket-policy should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // Get policy status
        let get_out = run_s3util(&[
            "get-bucket-policy-status",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;
        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(
            get_out.status.success(),
            "get-bucket-policy-status should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let json: serde_json::Value = serde_json::from_slice(&get_out.stdout)
            .expect("get-bucket-policy-status stdout must be JSON");
        assert_eq!(
            json.get("PolicyStatus")
                .and_then(|p| p.get("IsPublic"))
                .and_then(|v| v.as_bool()),
            Some(false),
            "expected IsPublic=false; got: {json}"
        );
    }

    /// get-bucket-policy-status on a bucket with no policy attached should
    /// exit 4 (NoSuchBucketPolicy → NotFound).
    #[tokio::test]
    async fn get_policy_status_on_bucket_without_policy_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "get-bucket-policy-status",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(4),
            "must exit 4 (NoSuchBucketPolicy)"
        );
    }

    /// get-bucket-policy-status on a non-existent bucket should exit 4.
    #[tokio::test]
    async fn get_policy_status_on_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-policy-status",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        assert!(!output.status.success());
        assert_eq!(
            output.status.code(),
            Some(4),
            "missing bucket must exit 4 (NoSuchBucket)"
        );
    }
}
