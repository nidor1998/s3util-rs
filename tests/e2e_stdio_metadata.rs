#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Upload from stdin with the full set of system and user metadata, then
    /// verify every field via `verify_test_object_metadata`.
    #[tokio::test]
    async fn stdin_to_s3_full_metadata_and_tags() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/full_meta.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--content-type",
                    TEST_CONTENT_TYPE,
                    "--cache-control",
                    TEST_CACHE_CONTROL,
                    "--content-disposition",
                    TEST_CONTENT_DISPOSITION,
                    "--content-encoding",
                    TEST_CONTENT_ENCODING,
                    "--content-language",
                    TEST_CONTENT_LANGUAGE,
                    "--expires",
                    TEST_EXPIRES,
                    "--website-redirect",
                    TEST_WEBSITE_REDIRECT,
                    "--metadata",
                    TEST_METADATA_STRING,
                    "--tagging",
                    TEST_TAGGING,
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        assert!(
            helper
                .verify_test_object_metadata(&bucket, "full_meta.dat", None)
                .await
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// The same full metadata set, but with enough data to cross
    /// `--multipart-threshold` so the upload takes the STREAMING path
    /// (`upload_stream` → CreateMultipartUpload) instead of the buffered
    /// single-part path.
    ///
    /// `stdin_to_s3_full_metadata_and_tags` above sends 1 KiB and therefore only
    /// ever exercised the buffered path, which is why the streaming path could
    /// silently drop cache-control, content-disposition, content-encoding,
    /// content-language, expires and website-redirect: the same command
    /// produced a differently-tagged object purely because the input was
    /// larger. Both paths must yield identical metadata.
    #[tokio::test]
    async fn stdin_to_s3_full_metadata_and_tags_streaming_path() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // 6 MiB with a 5 MiB threshold ⇒ the probe fills and the remainder is
        // streamed, so this is a genuine multipart upload from stdin.
        let stdin_bytes = TestHelper::generate_random_bytes(6 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/full_meta_streaming.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
                    "--content-type",
                    TEST_CONTENT_TYPE,
                    "--cache-control",
                    TEST_CACHE_CONTROL,
                    "--content-disposition",
                    TEST_CONTENT_DISPOSITION,
                    "--content-encoding",
                    TEST_CONTENT_ENCODING,
                    "--content-language",
                    TEST_CONTENT_LANGUAGE,
                    "--expires",
                    TEST_EXPIRES,
                    "--website-redirect",
                    TEST_WEBSITE_REDIRECT,
                    "--metadata",
                    TEST_METADATA_STRING,
                    "--tagging",
                    TEST_TAGGING,
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        let head = helper
            .head_object(&bucket, "full_meta_streaming.dat", None)
            .await;
        assert!(
            head.e_tag().unwrap().contains('-'),
            "fixture must have taken the multipart streaming path, got ETag: {}",
            head.e_tag().unwrap()
        );

        assert!(
            helper
                .verify_test_object_metadata(&bucket, "full_meta_streaming.dat", None)
                .await
        );

        // `verify_test_object_metadata` does not cover website-redirect, so
        // assert it here — it is one of the headers the streaming path dropped.
        assert_eq!(
            head.website_redirect_location(),
            Some(TEST_WEBSITE_REDIRECT),
            "--website-redirect must survive the streaming path"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// `--put-last-modified-metadata` on the streaming path. The buffered path
    /// records the upload time via its synthetic GetObjectOutput; the streaming
    /// path used to record nothing at all, so the user-metadata key was simply
    /// missing above `--multipart-threshold`.
    #[tokio::test]
    async fn stdin_to_s3_put_last_modified_metadata_streaming_path() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(6 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/last_modified_streaming.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
                    "--put-last-modified-metadata",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket, "last_modified_streaming.dat", None)
            .await;
        assert!(
            head.e_tag().unwrap().contains('-'),
            "fixture must have taken the multipart streaming path"
        );
        let metadata = head.metadata().expect("user metadata must be present");
        assert!(
            metadata.contains_key("s3sync_origin_last_modified"),
            "--put-last-modified-metadata must record the key on the streaming \
             path too; got: {metadata:?}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// `--if-none-match` must protect an existing key on the buffered
    /// (sub-threshold) stdin path. It was hard-coded to `None`, so the upload
    /// silently overwrote the object.
    #[tokio::test]
    async fn stdin_to_s3_if_none_match_rejects_existing_key_buffered_path() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let original = b"original contents".to_vec();
        helper
            .put_object(&bucket, "guarded.dat", original.clone())
            .await;

        let target = format!("s3://{}/guarded.dat", bucket);
        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--if-none-match",
                    "-",
                    &target,
                ],
                b"replacement contents".to_vec(),
            )
            .await;

        assert_eq!(
            stats.sync_error, 1,
            "--if-none-match must refuse to overwrite an existing key (S3 returns 412)"
        );
        assert_eq!(stats.sync_complete, 0);
        assert_eq!(
            helper.get_object_bytes(&bucket, "guarded.dat", None).await,
            original,
            "the existing object must be left untouched"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// The same protection on the streaming (at-or-above-threshold) path.
    #[tokio::test]
    async fn stdin_to_s3_if_none_match_rejects_existing_key_streaming_path() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let original = b"original contents".to_vec();
        helper
            .put_object(&bucket, "guarded_stream.dat", original.clone())
            .await;

        let target = format!("s3://{}/guarded_stream.dat", bucket);
        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
                    "--if-none-match",
                    "-",
                    &target,
                ],
                TestHelper::generate_random_bytes(6 * 1024 * 1024).unwrap(),
            )
            .await;

        assert_eq!(
            stats.sync_error, 1,
            "--if-none-match must refuse to overwrite on the streaming path too"
        );
        assert_eq!(stats.sync_complete, 0);
        assert_eq!(
            helper
                .get_object_bytes(&bucket, "guarded_stream.dat", None)
                .await,
            original,
            "the existing object must be left untouched"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Upload from stdin with a custom content-type and verify it round-trips.
    #[tokio::test]
    async fn stdin_to_s3_content_type_custom() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/ctype.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--content-type",
                    "application/json",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "ctype.dat", None).await;
        assert_eq!(head.content_type().unwrap(), "application/json");

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// `--disable-tagging` should result in no tags on the uploaded object.
    /// (Note: `--tagging` and `--disable-tagging` are mutually exclusive at
    /// the CLI level, so we only pass `--disable-tagging` here — matching the
    /// pattern already used in `e2e_local_to_s3.rs:1958-1966`.)
    #[tokio::test]
    async fn stdin_to_s3_disable_tagging() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/notag.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--disable-tagging",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let tagging = helper.get_object_tagging(&bucket, "notag.dat", None).await;
        assert!(
            tagging.tag_set().is_empty(),
            "disable-tagging should strip tags, got: {:?}",
            tagging.tag_set()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
