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
    async fn rm_existing_object_exits_0_and_object_is_gone() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "test-rm-object.txt";
        let body = b"hello world".to_vec();
        helper.put_object(&bucket, key, body).await;

        assert!(
            helper.is_object_exist(&bucket, key, None).await,
            "object should exist before rm"
        );

        let object_arg = format!("s3://{bucket}/{key}");
        let output = run_s3util(&["rm", "--target-profile", "s3util-e2e-test", &object_arg]);

        let still_exists = helper.is_object_exist(&bucket, key, None).await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "rm should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.code(), Some(0));
        // Silent on success — stdout must be empty
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.is_empty(), "rm stdout must be silent; got: {stdout}");
        assert!(!still_exists, "object must not exist after rm exits 0");
    }

    #[tokio::test]
    async fn rm_with_source_version_id_deletes_specific_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "test-rm-versioned.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"version1".to_vec())
            .await;
        let v2 = helper
            .put_object_with_version(&bucket, key, b"version2".to_vec())
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let output = run_s3util(&[
            "rm",
            "--target-profile",
            "s3util-e2e-test",
            "--source-version-id",
            &v1,
            &object_arg,
        ]);

        // Verify version-targeting before tearing the bucket down: only the
        // specified version should be gone, the other must still resolve.
        let v1_exists = helper.is_object_exist(&bucket, key, Some(v1.clone())).await;
        let v2_exists = helper.is_object_exist(&bucket, key, Some(v2.clone())).await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "rm --source-version-id should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.code(), Some(0));
        assert!(!v1_exists, "v1 must be gone after rm --source-version-id");
        assert!(
            v2_exists,
            "v2 must still exist; rm --source-version-id v1 must not touch v2"
        );
    }
}
