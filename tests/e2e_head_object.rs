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

    #[tokio::test]
    async fn head_object_emits_json_with_etag_and_content_length() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "test-object.txt";
        let body = b"hello world".to_vec();
        helper.put_object(&bucket, key, body).await;

        let object_arg = format!("s3://{bucket}/{key}");
        let output = run_s3util(&[
            "head-object",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "head-object should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert!(
            json.get("ETag").is_some(),
            "expected ETag in head-object response: {stdout}"
        );
        assert!(
            json.get("ContentLength").is_some(),
            "expected ContentLength in head-object response: {stdout}"
        );
    }

    #[tokio::test]
    async fn head_object_on_missing_key_exits_4() {
        let bucket = "s3util-e2e-nonexistent-bucket";
        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let output = run_s3util(&[
            "head-object",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        assert!(
            !output.status.success(),
            "head-object on missing key should fail"
        );
        assert_eq!(
            output.status.code(),
            Some(4),
            "head-object on missing key/bucket must exit 4 (NotFound)"
        );
    }
}
