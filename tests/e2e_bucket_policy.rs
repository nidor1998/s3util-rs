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

    fn sample_policy(bucket: &str) -> String {
        // Use a Deny statement so the policy is not classified as "public"
        // by AWS Block Public Access (BlockPublicPolicy is on by default since
        // 2023, which would reject Allow + "*" Principal). This is the standard
        // "require TLS" pattern — a real-world useful policy that round-trips
        // through put/get/delete cleanly.
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Sid":"DenyInsecureTransport","Effect":"Deny","Principal":"*","Action":"s3:*","Resource":["arn:aws:s3:::{bucket}","arn:aws:s3:::{bucket}/*"],"Condition":{{"Bool":{{"aws:SecureTransport":"false"}}}}}}]}}"#
        )
    }

    /// Round-trip: put (file) → get → delete → get (error).
    #[tokio::test]
    async fn put_get_delete_get_round_trip_via_file() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let policy_json = sample_policy(&bucket);

        // Write policy to a temp file
        let tmp_dir = TestHelper::create_temp_dir();
        let policy_file =
            TestHelper::create_test_file(&tmp_dir, "policy.json", policy_json.as_bytes());
        let policy_file_str = policy_file.to_str().unwrap();

        // 1. put-bucket-policy from file
        let put_out = run_s3util(&[
            "put-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
            policy_file_str,
        ]);
        assert!(
            put_out.status.success(),
            "put-bucket-policy should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&put_out.stdout).trim(),
            "",
            "put-bucket-policy must produce no stdout"
        );

        // 2. get-bucket-policy — should return JSON with Policy field
        let get_out = run_s3util(&[
            "get-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get-bucket-policy should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let json: serde_json::Value =
            serde_json::from_slice(&get_out.stdout).expect("get-bucket-policy stdout must be JSON");
        assert!(
            json.get("Policy").is_some(),
            "expected Policy key in response; got: {json}"
        );

        // 3. delete-bucket-policy
        let del_out = run_s3util(&[
            "delete-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            del_out.status.success(),
            "delete-bucket-policy should succeed; stderr: {}",
            String::from_utf8_lossy(&del_out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&del_out.stdout).trim(),
            "",
            "delete-bucket-policy must produce no stdout"
        );

        // 4. get-bucket-policy after delete — expect NotFound (NoSuchBucketPolicy → exit 4)
        let get_after_del = run_s3util(&[
            "get-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            !get_after_del.status.success(),
            "get-bucket-policy after delete should fail"
        );
        assert_eq!(
            get_after_del.status.code(),
            Some(4),
            "get-bucket-policy after delete must exit 4 (NoSuchBucketPolicy)"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        std::fs::remove_dir_all(&tmp_dir).ok();
    }

    /// Round-trip: put (stdin) → get — verifies stdin path.
    #[tokio::test]
    async fn put_via_stdin_and_get_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let policy_json = sample_policy(&bucket);

        // put-bucket-policy reading from stdin
        let put_out = run_s3util_with_stdin(
            &[
                "put-bucket-policy",
                "--target-profile",
                "s3sync-e2e-test",
                &bucket_arg,
                "-",
            ],
            policy_json.as_bytes(),
        );
        assert!(
            put_out.status.success(),
            "put-bucket-policy via stdin should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // get-bucket-policy — verify Policy key present
        let get_out = run_s3util(&[
            "get-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get-bucket-policy should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let json: serde_json::Value =
            serde_json::from_slice(&get_out.stdout).expect("stdout must be JSON");
        assert!(
            json.get("Policy").is_some(),
            "expected Policy key; got: {json}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// get-bucket-policy on a bucket with no policy attached should exit 4
    /// (NoSuchBucketPolicy is treated as NotFound).
    #[tokio::test]
    async fn get_policy_on_bucket_without_policy_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "get-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            !out.status.success(),
            "get-bucket-policy on bucket without policy should fail"
        );
        assert_eq!(
            out.status.code(),
            Some(4),
            "get-bucket-policy on bucket without policy must exit 4 (NoSuchBucketPolicy)"
        );
    }

    /// put-bucket-policy on a non-existent bucket should fail with exit code 1.
    #[tokio::test]
    async fn put_policy_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let tmp_dir = TestHelper::create_temp_dir();
        let policy_file = TestHelper::create_test_file(
            &tmp_dir,
            "policy.json",
            br#"{"Version":"2012-10-17","Statement":[]}"#,
        );

        let out = run_s3util(&[
            "put-bucket-policy",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
            policy_file.to_str().unwrap(),
        ]);

        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(
            !out.status.success(),
            "put-bucket-policy on missing bucket should fail"
        );
        assert_eq!(out.status.code(), Some(1));
    }
}
