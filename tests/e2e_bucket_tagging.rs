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
    // put-bucket-tagging + get-bucket-tagging + delete-bucket-tagging round-trip
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn put_then_get_then_delete_then_get_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // put-bucket-tagging
        let put_out = run_s3util(&[
            "put-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--tagging",
            "env=staging&owner=alice",
            &bucket_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-bucket-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // get-bucket-tagging — verify tags were applied
        let get_out = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            get_out.status.success(),
            "get-bucket-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let stdout = String::from_utf8_lossy(&get_out.stdout);
        let json: serde_json::Value = serde_json::from_str(&stdout).expect("stdout must be JSON");
        let tag_set = json["TagSet"].as_array().unwrap();
        assert_eq!(tag_set.len(), 2, "expected 2 tags after put; got: {stdout}");

        // delete-bucket-tagging
        let del_out = run_s3util(&[
            "delete-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            del_out.status.success(),
            "delete-bucket-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&del_out.stderr)
        );

        // get-bucket-tagging after delete — S3 returns NoSuchTagSet → exit 1
        let get_after = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            !get_after.status.success(),
            "get-bucket-tagging after delete should fail with NoSuchTagSet"
        );
        assert_eq!(get_after.status.code(), Some(1));

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn get_bucket_tagging_on_nonexistent_bucket_exits_1() {
        let bucket_arg = "s3://s3util-e2e-nonexistent-bucket-xyz";
        let output = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "get-bucket-tagging on nonexistent bucket should fail"
        );
        assert_eq!(output.status.code(), Some(1));
    }

    #[tokio::test]
    async fn get_bucket_tagging_emits_json_with_tag_set() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // apply tags first
        let put_out = run_s3util(&[
            "put-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--tagging",
            "project=myapp",
            &bucket_arg,
        ]);
        assert!(put_out.status.success());

        let output = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            output.status.success(),
            "get-bucket-tagging should succeed; stderr: {}",
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
        assert_eq!(tag_set.len(), 1, "expected 1 tag; got: {stdout}");
    }
}
