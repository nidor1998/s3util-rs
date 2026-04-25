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

    // ------------------------------------------------------------------
    // get-object-tagging
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn get_object_tagging_emits_json_with_tag_set() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "test-object.txt";
        helper
            .put_object_with_tagging(&bucket, key, b"hello".to_vec(), "env=prod&team=backend")
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let output = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "get-object-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let stdout = String::from_utf8_lossy(&output.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        assert!(
            json.get("TagSet").is_some(),
            "expected TagSet in response: {stdout}"
        );
        let tag_set = json["TagSet"].as_array().unwrap();
        assert_eq!(tag_set.len(), 2, "expected 2 tags; got: {stdout}");
    }

    #[tokio::test]
    async fn get_object_tagging_on_missing_key_exits_4() {
        let bucket = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let output = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        assert!(
            !output.status.success(),
            "get-object-tagging on missing key should fail"
        );
        assert_eq!(
            output.status.code(),
            Some(4),
            "get-object-tagging on missing key/bucket must exit 4 (NotFound)"
        );
    }

    // ------------------------------------------------------------------
    // put-object-tagging + delete-object-tagging round-trip
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn put_then_get_then_delete_then_get_empty() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "tagged-object.bin";
        helper.put_object(&bucket, key, b"data".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");

        // put-object-tagging
        let put_out = run_s3util(&[
            "put-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--tagging",
            "env=staging&owner=alice",
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // get-object-tagging — verify tags were applied
        let get_out = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get-object-tagging should succeed"
        );
        let stdout = String::from_utf8_lossy(&get_out.stdout);
        let json: serde_json::Value = serde_json::from_str(&stdout).unwrap();
        assert_eq!(
            json["TagSet"].as_array().unwrap().len(),
            2,
            "expected 2 tags after put; got: {stdout}"
        );

        // delete-object-tagging
        let del_out = run_s3util(&[
            "delete-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);
        assert!(
            del_out.status.success(),
            "delete-object-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&del_out.stderr)
        );

        // get-object-tagging — TagSet should now be empty
        let get_after = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);
        assert!(
            get_after.status.success(),
            "get-object-tagging after delete should succeed"
        );
        let stdout2 = String::from_utf8_lossy(&get_after.stdout);
        let json2: serde_json::Value = serde_json::from_str(&stdout2).unwrap();
        assert_eq!(
            json2["TagSet"].as_array().unwrap().len(),
            0,
            "expected empty TagSet after delete; got: {stdout2}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn delete_object_tagging_on_missing_key_exits_non_zero() {
        let bucket = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let object_arg = format!("s3://{bucket}/nonexistent-key");
        let output = run_s3util(&[
            "delete-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        assert!(
            !output.status.success(),
            "delete-object-tagging on missing key should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }
}
