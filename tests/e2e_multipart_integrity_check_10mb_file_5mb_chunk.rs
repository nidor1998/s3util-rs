#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    // Pre-computed values from s3sync (same seed file, same names)
    #[allow(dead_code)]
    const SHA256_10M_FILE_WHOLE: &str =
        "d5fc3f080e832d82161f9461291f87989b81a9e6281c33589d9563adefb46055";
    const SHA256_10M_PLUS_1_FILE_WHOLE: &str =
        "cbb719063c17ba48ec3925cc4ba8267addd8515b4f69e689da1dfc3a6683191a";

    /// Upload a 10 MiB file with 5 MiB chunk (2 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_10mb_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_5c.bin", 10, 0).unwrap();

        let target = format!("s3://{}/10mb_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "10mb_5c.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (10 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB file with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_5mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_5c_sha.bin", 10, 0).unwrap();

        let target = format!("s3://{}/10mb_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c_sha.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB file with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_5mb_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_5c_crc64.bin", 10, 0).unwrap();

        let target = format!("s3://{}/10mb_5c_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c_crc64.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with ETag only, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_5c.bin", 10, 1).unwrap();

        let target = format!("s3://{}/10mb_plus1_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_plus1_5c.bin",
                ETAG_10M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let head = helper.head_object(&bucket, "10mb_plus1_5c.bin", None).await;
        assert_eq!(
            head.content_length().unwrap(),
            (10 * 1024 * 1024 + 1) as i64
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB-1 file with ETag only, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_minus1_5c.bin", 10, -1).unwrap();

        let target = format!("s3://{}/10mb_minus1_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_minus1_5c.bin",
                ETAG_10M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_5mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_5c_sha.bin", 10, 1)
                .unwrap();

        let target = format!("s3://{}/10mb_plus1_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_plus1_5c_sha.bin",
                ETAG_10M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_5mb_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_5c_crc64.bin", 10, 1)
                .unwrap();

        let target = format!("s3://{}/10mb_plus1_5c_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_plus1_5c_crc64.bin",
                ETAG_10M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with KMS SSE and SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_kms_sha.bin", 10, 1)
                .unwrap();

        let target = format!("s3://{}/10mb_plus1_kms_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --auto-chunksize (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac.bin", 10, 1).unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);

        let target_s3 = format!("s3://{}/10mb_plus1_ac.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --auto-chunksize + SHA256.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac_sha.bin", 10, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac_sha.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/10mb_plus1_ac_sha.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac_sha.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --auto-chunksize + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_auto_chunksize_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_ac_crc64.bin", 10, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/10mb_plus1_ac_crc64.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/10mb_plus1_ac_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "10mb_plus1_ac_crc64.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_10M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --sse aws:kms (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_kms.bin", 10, 1).unwrap();

        let target = format!("s3://{}/10mb_plus1_kms.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --sse aws:kms + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_kms_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_plus1_kms_crc64.bin", 10, 1)
                .unwrap();

        let target = format!("s3://{}/10mb_plus1_kms_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB-1 with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_5mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_minus1_5c_sha.bin", 10, -1)
                .unwrap();

        let target = format!("s3://{}/10mb_minus1_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_minus1_5c_sha.bin",
                ETAG_10M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB roundtrip with server-side copy in between.
    #[tokio::test]
    async fn test_multipart_10mb_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_ssc.bin", 10, 0).unwrap();

        // Upload
        let s3_path1 = format!("s3://{}/10mb_ssc.bin", bucket1);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &s3_path1,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "10mb_ssc.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        // Server-side copy
        let s3_path2 = format!("s3://{}/10mb_ssc.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &s3_path1,
                &s3_path2,
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
