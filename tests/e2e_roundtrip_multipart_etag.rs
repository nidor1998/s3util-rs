#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    /// 8 MiB file: upload → verify exact ETag → S3→S3 → verify same ETag → download → SHA256 match.
    #[tokio::test]
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
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket1, "8m.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_FILE_8M_CHUNK);

        // S3 to S3
        let source = format!("s3://{}/8m.bin", bucket1);
        let target2 = format!("s3://{}/8m.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.checksum_verified, 0);

        let head2 = helper.head_object(&bucket2, "8m.bin", None).await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M_FILE_8M_CHUNK);

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
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket1, "8m_plus1.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.checksum_verified, 0);

        let head2 = helper.head_object(&bucket2, "8m_plus1.bin", None).await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);

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
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "8m_minus1.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_MINUS_1_FILE_8M_CHUNK);
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
