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

    // ------------------------------------------------------------------
    // --source-version-id targets the correct version
    // ------------------------------------------------------------------

    /// Put two versions, seed each with a distinguishing tag via the SDK,
    /// then verify `get-object-tagging --source-version-id` returns the
    /// targeted version's tags (and echoes back the targeted VersionId).
    #[tokio::test]
    async fn get_object_tagging_with_source_version_id_returns_targeted_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "versioned-tagged.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"v1".to_vec())
            .await;
        let v2 = helper
            .put_object_with_version(&bucket, key, b"v2".to_vec())
            .await;

        helper
            .put_object_tagging(&bucket, key, Some(v1.clone()), &[("version", "one")])
            .await;
        helper
            .put_object_tagging(&bucket, key, Some(v2.clone()), &[("version", "two")])
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let v1_out = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v1,
            &object_arg,
        ]);
        let v2_out = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v2,
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            v1_out.status.success(),
            "get-object-tagging v1 must succeed"
        );
        assert!(
            v2_out.status.success(),
            "get-object-tagging v2 must succeed"
        );

        let v1_json: serde_json::Value =
            serde_json::from_str(&String::from_utf8_lossy(&v1_out.stdout)).unwrap();
        let v2_json: serde_json::Value =
            serde_json::from_str(&String::from_utf8_lossy(&v2_out.stdout)).unwrap();

        assert_eq!(v1_json["VersionId"].as_str(), Some(v1.as_str()));
        let v1_tags = v1_json["TagSet"].as_array().unwrap();
        assert_eq!(v1_tags.len(), 1);
        assert_eq!(v1_tags[0]["Key"].as_str(), Some("version"));
        assert_eq!(v1_tags[0]["Value"].as_str(), Some("one"));

        assert_eq!(v2_json["VersionId"].as_str(), Some(v2.as_str()));
        let v2_tags = v2_json["TagSet"].as_array().unwrap();
        assert_eq!(v2_tags.len(), 1);
        assert_eq!(v2_tags[0]["Key"].as_str(), Some("version"));
        assert_eq!(v2_tags[0]["Value"].as_str(), Some("two"));
    }

    /// `put-object-tagging --source-version-id` must apply tags only to
    /// the targeted version; the other version must remain untagged.
    #[tokio::test]
    async fn put_object_tagging_with_source_version_id_targets_correct_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "put-tag-versioned.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"v1".to_vec())
            .await;
        let v2 = helper
            .put_object_with_version(&bucket, key, b"v2".to_vec())
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let put_out = run_s3util(&[
            "put-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v1,
            "--tagging",
            "applied=v1",
            &object_arg,
        ]);

        // Capture per-version tags via SDK before bucket teardown.
        let v1_tags = helper
            .get_object_tagging(&bucket, key, Some(v1.clone()))
            .await;
        let v2_tags = helper
            .get_object_tagging(&bucket, key, Some(v2.clone()))
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            put_out.status.success(),
            "put-object-tagging --source-version-id should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        // v1 must carry the new tag.
        assert_eq!(v1_tags.tag_set().len(), 1);
        assert_eq!(v1_tags.tag_set()[0].key(), "applied");
        assert_eq!(v1_tags.tag_set()[0].value(), "v1");

        // v2 must be unaffected.
        assert_eq!(
            v2_tags.tag_set().len(),
            0,
            "v2 must not be tagged when put-object-tagging targets v1"
        );
    }

    /// `delete-object-tagging --source-version-id` must clear tags only
    /// on the targeted version; the other version's tags must survive.
    #[tokio::test]
    async fn delete_object_tagging_with_source_version_id_targets_correct_version() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "delete-tag-versioned.txt";
        let v1 = helper
            .put_object_with_version(&bucket, key, b"v1".to_vec())
            .await;
        let v2 = helper
            .put_object_with_version(&bucket, key, b"v2".to_vec())
            .await;

        helper
            .put_object_tagging(&bucket, key, Some(v1.clone()), &[("a", "1")])
            .await;
        helper
            .put_object_tagging(&bucket, key, Some(v2.clone()), &[("b", "2")])
            .await;

        let object_arg = format!("s3://{bucket}/{key}");
        let del_out = run_s3util(&[
            "delete-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--source-version-id",
            &v1,
            &object_arg,
        ]);

        let v1_tags = helper
            .get_object_tagging(&bucket, key, Some(v1.clone()))
            .await;
        let v2_tags = helper
            .get_object_tagging(&bucket, key, Some(v2.clone()))
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            del_out.status.success(),
            "delete-object-tagging --source-version-id should succeed; stderr: {}",
            String::from_utf8_lossy(&del_out.stderr)
        );

        // v1 tags must be cleared.
        assert_eq!(
            v1_tags.tag_set().len(),
            0,
            "v1 tags must be cleared by delete-object-tagging --source-version-id"
        );

        // v2 tags must be intact.
        assert_eq!(v2_tags.tag_set().len(), 1);
        assert_eq!(v2_tags.tag_set()[0].key(), "b");
        assert_eq!(v2_tags.tag_set()[0].value(), "2");
    }

    // ------------------------------------------------------------------
    // UTF-8 3-byte (CJK) tag key/value round-trip
    // ------------------------------------------------------------------

    /// `--tagging` is URL-query-encoded, so non-ASCII keys/values must be
    /// percent-encoded by the caller. `parse_tagging_to_tags` decodes them
    /// before handing raw strings to the SDK (which serializes to XML for
    /// `PutObjectTagging`). This test pins the round-trip end-to-end:
    /// percent-encoded UTF-8 in → decoded UTF-8 stored → decoded UTF-8 out.
    ///
    /// Tag string encodes キー=バリュー (Japanese, 3 bytes per character in
    /// UTF-8) — the same fixture s3sync's tagging value-parser tests use.
    #[tokio::test]
    async fn put_then_get_object_tagging_with_utf8_3byte_codes_round_trips() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let key = "utf8-tagged-object.bin";
        helper.put_object(&bucket, key, b"data".to_vec()).await;

        let object_arg = format!("s3://{bucket}/{key}");

        // %E3%82%AD%E3%83%BC = キー, %E3%83%90%E3%83%AA%E3%83%A5%E3%83%BC = バリュー
        let put_out = run_s3util(&[
            "put-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            "--tagging",
            "%E3%82%AD%E3%83%BC=%E3%83%90%E3%83%AA%E3%83%A5%E3%83%BC",
            &object_arg,
        ]);
        assert!(
            put_out.status.success(),
            "put-object-tagging with UTF-8 should succeed; stderr: {}",
            String::from_utf8_lossy(&put_out.stderr)
        );

        let get_out = run_s3util(&[
            "get-object-tagging",
            "--target-profile",
            "s3sync-e2e-test",
            &object_arg,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert!(
            get_out.status.success(),
            "get-object-tagging should succeed; stderr: {}",
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
