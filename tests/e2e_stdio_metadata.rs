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
                    "s3sync-e2e-test",
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

        assert!(
            helper
                .verify_test_object_metadata(&bucket, "full_meta.dat", None)
                .await
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
                    "s3sync-e2e-test",
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
                    "s3sync-e2e-test",
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

        let tagging = helper.get_object_tagging(&bucket, "notag.dat", None).await;
        assert!(
            tagging.tag_set().is_empty(),
            "disable-tagging should strip tags, got: {:?}",
            tagging.tag_set()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
