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

    /// `head-object --source-version-id` must target the specified version
    /// rather than the current one. Verified by stamping bodies of
    /// distinguishable lengths and asserting both `VersionId` and
    /// `ContentLength` in the JSON output match the targeted version.
    #[tokio::test]
    async fn head_object_with_source_version_id_returns_targeted_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-object.txt";
        // 2-byte body for v1, 11-byte body for v2 — distinguishable via ContentLength.
        let v1 = helper
            .put_object_with_version(&bucket, key, b"v1".to_vec())
            .await;
        let _v2 = helper
            .put_object_with_version(&bucket, key, b"version-two".to_vec())
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let output = run_s3util(&[
            "head-object",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v1,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "head-object with --source-version-id should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert_eq!(
            json["VersionId"].as_str(),
            Some(v1.as_str()),
            "head-object must echo back the targeted VersionId; got: {stdout}"
        );
        assert_eq!(
            json["ContentLength"].as_i64(),
            Some(2),
            "head-object must report v1's body length, not the current version's; got: {stdout}"
        );
    }

    #[tokio::test]
    async fn head_object_on_missing_key_exits_4() {
        let bucket = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
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
