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

    /// Round-trip: put Enabled → get → put Suspended → get.
    /// Bucket name must not contain dots (S3 Transfer Acceleration restriction).
    #[tokio::test]
    async fn accelerate_enable_get_suspend_get_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // 1. Enable accelerate
        let put_enabled = run_s3util(&[
            "put-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--enabled",
        ]);
        assert!(
            put_enabled.status.success(),
            "put-bucket-accelerate --enabled should succeed; stderr: {}",
            String::from_utf8_lossy(&put_enabled.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&put_enabled.stdout).trim(),
            "",
            "put-bucket-accelerate-configuration must produce no stdout"
        );

        // 2. Get accelerate — expect Status=Enabled
        let get_enabled = run_s3util(&[
            "get-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_enabled.status.success(),
            "get-bucket-accelerate should succeed; stderr: {}",
            String::from_utf8_lossy(&get_enabled.stderr)
        );
        let json_enabled: serde_json::Value = serde_json::from_slice(&get_enabled.stdout)
            .expect("get-bucket-accelerate stdout must be JSON");
        assert_eq!(
            json_enabled.get("Status").and_then(|v| v.as_str()),
            Some("Enabled"),
            "expected Status=Enabled; got: {json_enabled}"
        );

        // 3. Suspend
        let put_suspended = run_s3util(&[
            "put-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--suspended",
        ]);
        assert!(
            put_suspended.status.success(),
            "put-bucket-accelerate --suspended should succeed; stderr: {}",
            String::from_utf8_lossy(&put_suspended.stderr)
        );

        // 4. Get — expect Status=Suspended
        let get_suspended = run_s3util(&[
            "get-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(get_suspended.status.success());
        let json_suspended: serde_json::Value = serde_json::from_slice(&get_suspended.stdout)
            .expect("get-bucket-accelerate stdout must be JSON");
        assert_eq!(
            json_suspended.get("Status").and_then(|v| v.as_str()),
            Some("Suspended"),
            "expected Status=Suspended; got: {json_suspended}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// On a freshly-created bucket that has never had Transfer Acceleration
    /// configured, get-bucket-accelerate-configuration should emit nothing on
    /// stdout, matching `aws s3api get-bucket-accelerate-configuration --output json`.
    #[tokio::test]
    async fn get_accelerate_on_unconfigured_bucket_yields_no_output() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let output = run_s3util(&[
            "get-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "get-bucket-accelerate on unconfigured bucket should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            output.stdout.is_empty(),
            "expected empty stdout for unconfigured bucket; got: {:?}",
            String::from_utf8_lossy(&output.stdout)
        );
    }

    /// put-bucket-accelerate on a non-existent bucket should fail with exit 1.
    #[tokio::test]
    async fn put_accelerate_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "put-bucket-accelerate-configuration",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            "--enabled",
        ]);

        assert!(!output.status.success());
        assert_eq!(output.status.code(), Some(1));
    }

    /// get-bucket-accelerate on a non-existent bucket should exit 4 (NoSuchBucket).
    #[tokio::test]
    async fn get_accelerate_on_missing_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-accelerate-configuration",
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
