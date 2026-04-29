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

    fn sample_cors_json() -> &'static str {
        r#"{
          "CORSRules": [
            {
              "ID": "r1",
              "AllowedMethods": ["GET", "HEAD"],
              "AllowedOrigins": ["*"],
              "AllowedHeaders": ["*"],
              "MaxAgeSeconds": 3000
            }
          ]
        }"#
    }

    #[tokio::test]
    async fn put_get_delete_get_round_trip_via_file() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let tmp_dir = TestHelper::create_temp_dir();
        let config_file =
            TestHelper::create_test_file(&tmp_dir, "cors.json", sample_cors_json().as_bytes());
        let config_file_str = config_file.to_str().unwrap();

        let put_out = run_s3util(&[
            "put-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            config_file_str,
        ]);
        assert!(
            put_out.status.success(),
            "put should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&put_out.stdout).trim(),
            "",
            "put must produce no stdout"
        );

        let get_out = run_s3util(&[
            "get-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let json: serde_json::Value =
            serde_json::from_slice(&get_out.stdout).expect("get stdout must be JSON");
        let rules = json["CORSRules"].as_array().expect("CORSRules array");
        assert_eq!(rules.len(), 1);
        assert_eq!(rules[0]["ID"], "r1");
        assert_eq!(rules[0]["MaxAgeSeconds"], 3000);

        let del_out = run_s3util(&[
            "delete-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(del_out.status.success());

        let get_after_del = run_s3util(&[
            "get-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(!get_after_del.status.success());
        assert_eq!(
            get_after_del.status.code(),
            Some(4),
            "get after delete must exit 4 (NoSuchCORSConfiguration)"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        std::fs::remove_dir_all(&tmp_dir).ok();
    }

    #[tokio::test]
    async fn put_via_stdin_and_get_round_trip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        let put_out = run_s3util_with_stdin(
            &[
                "put-bucket-cors",
                "--target-profile",
                "s3util-e2e-test",
                &bucket_arg,
                "-",
            ],
            sample_cors_json().as_bytes(),
        );
        assert!(
            put_out.status.success(),
            "put via stdin should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        let get_out = run_s3util(&[
            "get-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(get_out.status.success());
        let json: serde_json::Value = serde_json::from_slice(&get_out.stdout).unwrap();
        assert!(json.get("CORSRules").is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn get_on_bucket_without_cors_exits_4() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");
        let out = run_s3util(&[
            "get-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(4),
            "get on bucket without CORS must exit 4 (NoSuchCORSConfiguration)"
        );
    }

    #[tokio::test]
    async fn put_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let tmp_dir = TestHelper::create_temp_dir();
        let config_file =
            TestHelper::create_test_file(&tmp_dir, "cors.json", sample_cors_json().as_bytes());

        let out = run_s3util(&[
            "put-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            config_file.to_str().unwrap(),
        ]);

        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(!out.status.success());
        assert_eq!(out.status.code(), Some(1));
    }

    #[tokio::test]
    async fn delete_on_missing_bucket_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let out = run_s3util(&[
            "delete-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        assert!(!out.status.success());
        assert_eq!(
            out.status.code(),
            Some(1),
            "delete on missing bucket must exit 1 (delete uses Result<()>, not HeadError, so NoSuchBucket → exit 1)"
        );
    }

    #[tokio::test]
    async fn get_on_nonexistent_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let out = run_s3util(&[
            "get-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(!out.status.success());
        assert_eq!(out.status.code(), Some(4));
    }

    /// Invalid JSON via file is rejected client-side by `serde_json::from_str`
    /// before any AWS call, so no real bucket is required.
    #[tokio::test]
    async fn malformed_json_via_file_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let tmp_dir = TestHelper::create_temp_dir();
        let config_file = TestHelper::create_test_file(&tmp_dir, "cors.json", b"not valid json {");

        let out = run_s3util(&[
            "put-bucket-cors",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
            config_file.to_str().unwrap(),
        ]);

        std::fs::remove_dir_all(&tmp_dir).ok();

        assert!(
            !out.status.success(),
            "put-bucket-cors with invalid JSON file must fail"
        );
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.to_lowercase().contains("parsing json from"),
            "expected 'parsing JSON from' in stderr; got: {stderr}"
        );
    }

    /// Invalid JSON via stdin is also rejected client-side; the file-path
    /// substring in the error context becomes `-`.
    #[tokio::test]
    async fn malformed_json_via_stdin_exits_1() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");

        let out = run_s3util_with_stdin(
            &[
                "put-bucket-cors",
                "--target-profile",
                "s3util-e2e-test",
                &bucket_arg,
                "-",
            ],
            b"not valid json {",
        );

        assert!(
            !out.status.success(),
            "put-bucket-cors with invalid JSON via stdin must fail"
        );
        assert_eq!(out.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.to_lowercase().contains("parsing json from"),
            "expected 'parsing JSON from' in stderr; got: {stderr}"
        );
    }
}
