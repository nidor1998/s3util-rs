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
    #[tokio::test]
    async fn versioning_enable_get_suspend_get_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // 1. Enable versioning
        let put_enabled = run_s3util(&[
            "put-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
            "--enabled",
        ]);
        assert!(
            put_enabled.status.success(),
            "put-bucket-versioning --enabled should succeed; stderr: {}",
            String::from_utf8_lossy(&put_enabled.stderr)
        );
        // put-bucket-versioning is silent on success
        assert_eq!(
            String::from_utf8_lossy(&put_enabled.stdout).trim(),
            "",
            "put-bucket-versioning must produce no stdout output"
        );

        // 2. Get versioning — expect Status=Enabled
        let get_enabled = run_s3util(&[
            "get-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_enabled.status.success(),
            "get-bucket-versioning should succeed; stderr: {}",
            String::from_utf8_lossy(&get_enabled.stderr)
        );
        let json_enabled: serde_json::Value = serde_json::from_slice(&get_enabled.stdout)
            .expect("get-bucket-versioning stdout must be JSON");
        assert_eq!(
            json_enabled.get("Status").and_then(|v| v.as_str()),
            Some("Enabled"),
            "expected Status=Enabled; got: {json_enabled}"
        );

        // 3. Suspend versioning
        let put_suspended = run_s3util(&[
            "put-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
            "--suspended",
        ]);
        assert!(
            put_suspended.status.success(),
            "put-bucket-versioning --suspended should succeed; stderr: {}",
            String::from_utf8_lossy(&put_suspended.stderr)
        );

        // 4. Get versioning — expect Status=Suspended
        let get_suspended = run_s3util(&[
            "get-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_suspended.status.success(),
            "get-bucket-versioning should succeed; stderr: {}",
            String::from_utf8_lossy(&get_suspended.stderr)
        );
        let json_suspended: serde_json::Value = serde_json::from_slice(&get_suspended.stdout)
            .expect("get-bucket-versioning stdout must be JSON");
        assert_eq!(
            json_suspended.get("Status").and_then(|v| v.as_str()),
            Some("Suspended"),
            "expected Status=Suspended; got: {json_suspended}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// On a freshly-created bucket that has never had versioning configured,
    /// get-bucket-versioning should emit `{}` (no Status element).
    #[tokio::test]
    async fn get_versioning_on_unconfigured_bucket_yields_empty_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let output = run_s3util(&[
            "get-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "get-bucket-versioning on unconfigured bucket should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let json: serde_json::Value =
            serde_json::from_slice(&output.stdout).expect("stdout must be valid JSON");
        assert!(
            json.as_object().map(|m| m.is_empty()).unwrap_or(false),
            "expected {{}} for unconfigured bucket; got: {json}"
        );
    }

    /// put-bucket-versioning on a non-existent bucket should fail with exit code 1.
    #[tokio::test]
    async fn put_versioning_on_missing_bucket_exits_non_zero() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "put-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
            "--enabled",
        ]);

        assert!(
            !output.status.success(),
            "put-bucket-versioning on missing bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }

    /// get-bucket-versioning on a non-existent bucket should fail with exit code 1.
    #[tokio::test]
    async fn get_versioning_on_missing_bucket_exits_non_zero() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-versioning",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "get-bucket-versioning on missing bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }
}
