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
            "s3util-e2e-test",
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
            "s3util-e2e-test",
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
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            del_out.status.success(),
            "delete-bucket-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&del_out.stderr)
        );

        // get-bucket-tagging after delete — S3 returns NoSuchTagSet → exit 4 (NotFound)
        let get_after = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);
        assert!(
            !get_after.status.success(),
            "get-bucket-tagging after delete should fail with NoSuchTagSet"
        );
        assert_eq!(
            get_after.status.code(),
            Some(4),
            "get-bucket-tagging after delete must exit 4 (NoSuchTagSet)"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// `--tagging` is URL-query-encoded, so non-ASCII keys/values must be
    /// percent-encoded by the caller. `parse_tagging_to_tags` decodes them
    /// before handing raw strings to the SDK (which serializes to XML for
    /// `PutBucketTagging`). This test pins the round-trip end-to-end:
    /// percent-encoded UTF-8 in → decoded UTF-8 stored → decoded UTF-8 out.
    ///
    /// Tag string encodes キー=バリュー (Japanese, 3 bytes per character in
    /// UTF-8) — the same fixture s3sync's tagging value-parser tests use.
    #[tokio::test]
    async fn put_then_get_bucket_tagging_with_utf8_3byte_codes_round_trips() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let bucket_arg = format!("s3://{bucket}");

        // %E3%82%AD%E3%83%BC = キー, %E3%83%90%E3%83%AA%E3%83%A5%E3%83%BC = バリュー
        let put_out = run_s3util(&[
            "put-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
            "--tagging",
            "%E3%82%AD%E3%83%BC=%E3%83%90%E3%83%AA%E3%83%A5%E3%83%BC",
            &bucket_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-bucket-tagging with UTF-8 should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        let get_out = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            get_out.status.success(),
            "get-bucket-tagging should succeed; stderr: {}",
            String::from_utf8_lossy(&get_out.stderr)
        );
        let stdout = String::from_utf8_lossy(&get_out.stdout);
        let json: serde_json::Value =
            serde_json::from_str(&stdout).expect("stdout must be valid JSON");
        let tag_set = json["TagSet"].as_array().expect("TagSet must be array");
        assert_eq!(tag_set.len(), 1, "expected 1 tag; got: {stdout}");
        // Stored values must be decoded UTF-8, never percent-encoded.
        assert_eq!(
            tag_set[0]["Key"].as_str(),
            Some("キー"),
            "tag Key must round-trip as decoded UTF-8; got: {stdout}"
        );
        assert_eq!(
            tag_set[0]["Value"].as_str(),
            Some("バリュー"),
            "tag Value must round-trip as decoded UTF-8; got: {stdout}"
        );
    }

    #[tokio::test]
    async fn get_bucket_tagging_on_nonexistent_bucket_exits_4() {
        let nonexistent = format!("s3util-nonexistent-{}", uuid::Uuid::new_v4());
        let bucket_arg = format!("s3://{nonexistent}");
        let output = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
            &bucket_arg,
        ]);

        assert!(
            !output.status.success(),
            "get-bucket-tagging on nonexistent bucket should fail"
        );
        assert_eq!(
            output.status.code(),
            Some(4),
            "get-bucket-tagging on nonexistent bucket must exit 4 (NoSuchBucket)"
        );
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
            "s3util-e2e-test",
            "--tagging",
            "project=myapp",
            &bucket_arg,
        ]);
        assert!(put_out.status.success());

        let output = run_s3util(&[
            "get-bucket-tagging",
            "--target-profile",
            "s3util-e2e-test",
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
