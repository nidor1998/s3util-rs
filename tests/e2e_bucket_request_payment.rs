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

    /// Round-trip: get default → put Requester → get → put BucketOwner → get.
    #[tokio::test]
    async fn request_payment_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // 1. Get default — should be BucketOwner
        let get_default = run_s3util(&[
            "get-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_default.status.success(),
            "get-bucket-request-payment should succeed; stderr: {}",
            String::from_utf8_lossy(&get_default.stderr)
        );
        let json_default: serde_json::Value = serde_json::from_slice(&get_default.stdout)
            .expect("get-bucket-request-payment stdout must be JSON");
        assert_eq!(
            json_default.get("Payer").and_then(|v| v.as_str()),
            Some("BucketOwner"),
            "expected default Payer=BucketOwner; got: {json_default}"
        );

        // 2. Put Requester
        let put_requester = run_s3util(&[
            "put-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--requester",
        ]);
        assert!(
            put_requester.status.success(),
            "put --requester should succeed; stderr: {}",
            String::from_utf8_lossy(&put_requester.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&put_requester.stdout).trim(),
            "",
            "put-bucket-request-payment must produce no stdout"
        );

        // 3. Get — expect Requester
        let get_req = run_s3util(&[
            "get-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(get_req.status.success());
        let json_req: serde_json::Value = serde_json::from_slice(&get_req.stdout).unwrap();
        assert_eq!(
            json_req.get("Payer").and_then(|v| v.as_str()),
            Some("Requester")
        );

        // 4. Put BucketOwner (back to default)
        let put_owner = run_s3util(&[
            "put-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--bucket-owner",
        ]);
        assert!(put_owner.status.success());

        // 5. Get — expect BucketOwner
        let get_owner = run_s3util(&[
            "get-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(get_owner.status.success());
        let json_owner: serde_json::Value = serde_json::from_slice(&get_owner.stdout).unwrap();
        assert_eq!(
            json_owner.get("Payer").and_then(|v| v.as_str()),
            Some("BucketOwner")
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// put-bucket-request-payment on a non-existent bucket should fail with exit 1.
    #[tokio::test]
    async fn put_request_payment_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "put-bucket-request-payment",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--requester",
        ]);

        assert!(!output.status.success());
        assert_eq!(output.status.code(), Some(1));
    }

    /// get-bucket-request-payment on a non-existent bucket should exit 4 (NoSuchBucket).
    #[tokio::test]
    async fn get_request_payment_on_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-request-payment",
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
