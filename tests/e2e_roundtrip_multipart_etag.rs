#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    // Expected ETag values for deterministic data from create_random_data_file.
    // These are derived by uploading once and recording the ETag.
    // create_random_data_file generates: (0..1024).map(|i| (i % 256) as u8) repeated.
    //
    // IMPORTANT: If create_random_data_file changes, these constants must be updated.
    // Run the discover_etag_constants test to get the current values.

    // 8 MiB file (8 * 1024 * 1024 bytes), default 8 MiB threshold = single multipart part
    // Format: "\"<md5>-<part_count>\""
    const ETAG_8M: &str = "DISCOVER_BY_RUNNING_TEST";
    // 8 MiB + 1 byte = 2 parts
    const ETAG_8M_PLUS_1: &str = "DISCOVER_BY_RUNNING_TEST";
    // 8 MiB - 1 byte = single put (below threshold)
    const ETAG_8M_MINUS_1: &str = "DISCOVER_BY_RUNNING_TEST";

    /// Helper: upload a deterministic file and return its ETag.
    /// Run this test with --nocapture to see the ETag values, then
    /// update the constants above.
    #[tokio::test]
    async fn discover_etag_constants() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();

        // 8 MiB exact
        let file_8m = TestHelper::create_random_data_file(&local_dir, "8m.bin", 8, 0).unwrap();
        let target_8m = format!("s3://{}/8m.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                file_8m.to_str().unwrap(),
                &target_8m,
            ])
            .await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head_8m = helper.head_object(&bucket, "8m.bin", None).await;
        eprintln!("ETAG_8M = {:?}", head_8m.e_tag().unwrap());

        // 8 MiB + 1
        let file_8m_plus =
            TestHelper::create_random_data_file(&local_dir, "8m_plus1.bin", 8, 1).unwrap();
        let target_8m_plus = format!("s3://{}/8m_plus1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                file_8m_plus.to_str().unwrap(),
                &target_8m_plus,
            ])
            .await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head_8m_plus = helper.head_object(&bucket, "8m_plus1.bin", None).await;
        eprintln!("ETAG_8M_PLUS_1 = {:?}", head_8m_plus.e_tag().unwrap());

        // 8 MiB - 1
        let file_8m_minus =
            TestHelper::create_random_data_file(&local_dir, "8m_minus1.bin", 8, -1).unwrap();
        let target_8m_minus = format!("s3://{}/8m_minus1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                file_8m_minus.to_str().unwrap(),
                &target_8m_minus,
            ])
            .await;
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head_8m_minus = helper.head_object(&bucket, "8m_minus1.bin", None).await;
        eprintln!("ETAG_8M_MINUS_1 = {:?}", head_8m_minus.e_tag().unwrap());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file: upload → verify exact ETag → S3→S3 → verify same ETag → download → SHA256 match.
    #[tokio::test]
    #[ignore = "Run discover_etag_constants first and update ETAG_8M constant"]
    async fn roundtrip_8mb_exact_etag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "8m.bin", 8, 0).unwrap();

        // Local to S3
        let target = format!("s3://{}/8m.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        let head = helper.head_object(&bucket1, "8m.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M);

        // S3 to S3
        let source = format!("s3://{}/8m.bin", bucket1);
        let target2 = format!("s3://{}/8m.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.sync_warning, 0);

        let head2 = helper.head_object(&bucket2, "8m.bin", None).await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M);

        // Download and verify SHA256
        let downloaded = helper.get_object_bytes(&bucket2, "8m.bin", None).await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 multipart parts).
    #[tokio::test]
    #[ignore = "Run discover_etag_constants first and update ETAG_8M_PLUS_1 constant"]
    async fn roundtrip_8mb_plus_1_exact_etag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8m_plus1.bin", 8, 1).unwrap();

        // Local to S3
        let target = format!("s3://{}/8m_plus1.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        let head = helper.head_object(&bucket1, "8m_plus1.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_PLUS_1);

        let head_len = head.content_length().unwrap();
        assert_eq!(head_len, (8 * 1024 * 1024 + 1) as i64);

        // S3 to S3
        let source = format!("s3://{}/8m_plus1.bin", bucket1);
        let target2 = format!("s3://{}/8m_plus1.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.sync_warning, 0);

        let head2 = helper.head_object(&bucket2, "8m_plus1.bin", None).await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M_PLUS_1);

        // Download and verify SHA256
        let downloaded = helper
            .get_object_bytes(&bucket2, "8m_plus1.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single PutObject, below multipart threshold).
    #[tokio::test]
    #[ignore = "Run discover_etag_constants first and update ETAG_8M_MINUS_1 constant"]
    async fn roundtrip_8mb_minus_1_exact_etag() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8m_minus1.bin", 8, -1).unwrap();

        let target = format!("s3://{}/8m_minus1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        let head = helper.head_object(&bucket, "8m_minus1.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_MINUS_1);
        assert_eq!(head.content_length().unwrap(), (8 * 1024 * 1024 - 1) as i64);

        // Download and verify content
        let downloaded = helper
            .get_object_bytes(&bucket, "8m_minus1.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
