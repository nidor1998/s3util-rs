#![cfg(e2e_test)]
#[cfg(test)]
mod common;

pub const EXPRESS_ONE_ZONE_BUCKET_SUFFIX: &str = "--apne1-az4--x-s3";

#[cfg(test)]
mod tests {
    use common::*;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn local_to_express_one_zone_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "express.txt", b"express one zone test");

        let target = format!("s3://{}/express.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_express_one_zone_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "express_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/express_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_to_local() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload first
        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload.txt", b"express download test");
        let s3_path = format!("s3://{}/download_test.txt", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Download
        let download_file = local_dir.join("downloaded.txt");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"express download test");

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_s3_to_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
            .await;
        helper
            .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload to bucket1
        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "s3copy.txt", b"express s3-to-s3 test");
        let source_path = format!("s3://{}/s3copy.txt", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                upload_file.to_str().unwrap(),
                &source_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);

        // Copy from bucket1 to bucket2
        let target_path = format!("s3://{}/s3copy.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &source_path,
                &target_path,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket2, "s3copy.txt", None).await);

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_express_one_zone_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc64.txt", b"express crc64nvme test");

        let target = format!("s3://{}/crc64.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "crc64.txt", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_express_one_zone_multipart_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_crc64.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_crc64.bin", None).await;
        assert!(head.checksum_crc64_nvme().is_some());
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_roundtrip_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload with CRC64NVME
        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "roundtrip.txt",
            b"express roundtrip crc64nvme test",
        );
        let s3_path = format!("s3://{}/roundtrip.txt", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Download
        let download_file = local_dir.join("downloaded.txt");
        let download_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(download_stats.sync_complete, 1);
        assert_eq!(download_stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"express roundtrip crc64nvme test");

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_s3_to_s3_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket1 = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let bucket2 = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket1, EXPRESS_ONE_ZONE_AZ)
            .await;
        helper
            .create_directory_bucket(&bucket2, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload with CRC64NVME to bucket1
        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "s3copy_crc.txt",
            b"express s3-to-s3 crc64nvme test",
        );
        let source_path = format!("s3://{}/s3copy_crc.txt", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                upload_file.to_str().unwrap(),
                &source_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Copy from bucket1 to bucket2
        let target_path = format!("s3://{}/s3copy_crc.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &source_path,
                &target_path,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            helper
                .is_object_exist(&bucket2, "s3copy_crc.txt", None)
                .await
        );

        helper.delete_directory_bucket_with_cascade(&bucket1).await;
        helper.delete_directory_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_multipart_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload large file
        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_roundtrip.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_roundtrip.bin", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_roundtrip.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        // Download
        let download_file = local_dir.join("downloaded.bin");
        let download_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(download_stats.sync_complete, 1);
        assert_eq!(download_stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);

        // Verify content matches by SHA256
        let upload_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let download_sha256 = TestHelper::get_sha256_from_file(download_file.to_str().unwrap());
        assert_eq!(upload_sha256, download_sha256);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload to an Express One Zone bucket with --disable-express-one-zone-additional-checksum.
    /// The default CRC64NVME additional checksum is disabled, so `checksum_verified` should be 0.
    #[tokio::test]
    async fn local_to_s3_with_disable_express_one_zone_additional_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "express_disable_crc.txt",
            b"disable express additional checksum",
        );

        let target = format!("s3://{}/express_disable_crc.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                "--disable-express-one-zone-additional-checksum",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.checksum_verified, 0);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
