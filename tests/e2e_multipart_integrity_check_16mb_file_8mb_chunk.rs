#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    // Pre-computed values from s3sync (same seed file, same names)
    const SHA256_16M_PLUS_1_FILE_WHOLE: &str =
        "0fbb2466d100013b3716965c89ac0c1375bba2c8f126e63ee6bc5ffff68ef33b";

    /// Upload a 16 MiB file with 8 MiB chunk (2 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_16mb_8mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "16mb.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "16mb.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (16 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with SHA256 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_8mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_sha256.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_sha256.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with CRC64NVME checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_8mb_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_crc64.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_crc64.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Roundtrip: local->S3->S3->local, 16 MiB, verify content matches.
    #[tokio::test]
    async fn test_multipart_16mb_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_rt.bin", 16, 0).unwrap();

        // Local to S3
        let s3_path1 = format!("s3://{}/16mb_rt.bin", bucket1);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &s3_path1,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        assert_eq!(stats1.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "16mb_rt.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        // S3 to S3
        let s3_path2 = format!("s3://{}/16mb_rt.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &s3_path1,
                &s3_path2,
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "16mb_rt.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        // S3 to local
        let download_file = local_dir.join("16mb_dl.bin");
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path2,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.e_tag_verified, 1);

        let orig_sha = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let dl_sha = TestHelper::get_sha256_from_file(download_file.to_str().unwrap());
        assert_eq!(orig_sha, dl_sha);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 file, ETag only, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_8mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1.bin", 16, 1).unwrap();

        let target = format!("s3://{}/16mb_plus1.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_plus1.bin",
                ETAG_16M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper.head_object(&bucket, "16mb_plus1.bin", None).await;
        assert_eq!(
            head.content_length().unwrap(),
            (16 * 1024 * 1024 + 1) as i64
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 file, ETag only, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_8mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1.bin", 16, -1).unwrap();

        let target = format!("s3://{}/16mb_minus1.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.sync_warning, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_minus1.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and SHA256, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_kms_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_sha256.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
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

    /// 16 MiB+1 with --auto-chunksize (ETag only), default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_8c.bin", 16, 1).unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);

        let target_s3 = format!("s3://{}/16mb_plus1_ac_8c.bin", bucket2);
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
            .get_object_bytes(&bucket2, "16mb_plus1_ac_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + SHA256, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_auto_chunksize_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_sha_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_sha_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_sha_8c.bin", bucket2);
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
            .get_object_bytes(&bucket2, "16mb_plus1_ac_sha_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + CRC64NVME, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_auto_chunksize_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_crc64_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_crc64_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_crc64_8c.bin", bucket2);
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
            .get_object_bytes(&bucket2, "16mb_plus1_ac_crc64_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms (ETag only), default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_8c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_8c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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

    /// 16 MiB+1 with --sse aws:kms + CRC64NVME, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_kms_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_crc64_8c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_crc64_8c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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

    /// 16 MiB-1 with SHA256 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_8mb_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_sha256.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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
                "16mb_minus1_sha256.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with CRC64NVME checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_8mb_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_crc64.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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
                "16mb_minus1_crc64.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
