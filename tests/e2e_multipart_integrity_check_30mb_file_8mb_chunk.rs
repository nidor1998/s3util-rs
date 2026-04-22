#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_30M_FILE_8M_CHUNK: &str = "5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4";
    const CRC64NVME_30M_FILE_8M_CHUNK: &str = "rrk4q4lsMS4=";
    const ETAG_30M_FILE_8M_CHUNK: &str = "\"a81230a7666d413e511f9c2c2523947a-4\"";
    const SHA256_30M_PLUS_1_FILE_8M_CHUNK: &str = "jg2kwGbujn7CfNB2V2nywLmvYZ9j7LDbJHU5DLJZhLQ=-4";
    const CRC64NVME_30M_PLUS_1_FILE_8M_CHUNK: &str = "Qm3X/T+IwHo=";
    const ETAG_30M_PLUS_1_FILE_8M_CHUNK: &str = "\"e10f60edd59877a2d1cd80b837460b80-4\"";
    const SHA256_30M_MINUS_1_FILE_8M_CHUNK: &str = "jl7+N03GWlKugXq3+knSZbuisRI2aOHyfL4qF8OlmO0=-4";
    const CRC64NVME_30M_MINUS_1_FILE_8M_CHUNK: &str = "3vy9G9dhMlY=";
    const ETAG_30M_MINUS_1_FILE_8M_CHUNK: &str = "\"4f36b633babe3a74e08884d6056ab6df-4\"";
    const SHA256_30M_FILE_WHOLE_HEX: &str =
        "05c1c771d4886e4cefdf0a4c0b907913fe2f829dd767418c94ea278b0b8bc3f9";
    const SHA256_30M_PLUS_1_FILE_WHOLE: &str =
        "4be88d40a77bbb954cad4715fca1f28a5fd7261bc34f9d9d7f4c6f5ea0dfb095";
    const SHA256_30M_MINUS_1_FILE_WHOLE: &str =
        "15ec020d762780610650cc065415691069c35ca2a400b7801f615114edc0737f";

    /// Upload a 30 MiB file with 8 MiB chunk (4 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_30mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "30mb.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "30mb.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (30 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB file with SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_30mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_sha256.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb_sha256.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "30mb_sha256.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_30M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB file with CRC64NVME checksum.
    #[tokio::test]
    async fn test_multipart_upload_30mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_crc64.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb_crc64.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb_crc64.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "30mb_crc64.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB roundtrip local->S3->local with content verification.
    #[tokio::test]
    async fn test_multipart_30mb_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_rt.bin", 30, 0).unwrap();

        // Upload
        let s3_path = format!("s3://{}/30mb_rt.bin", bucket);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        assert_eq!(stats1.sync_error, 0);
        assert_eq!(stats1.sync_warning, 0);
        assert_eq!(stats1.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "30mb_rt.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        // Download
        let download_file = local_dir.join("30mb_dl.bin");
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);

        let orig = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let dl = TestHelper::get_sha256_from_file(download_file.to_str().unwrap());
        assert_eq!(orig, dl);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_30M_FILE_WHOLE_HEX,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 file, ETag only, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1.bin", 30, 1).unwrap();

        let target = format!("s3://{}/30mb_plus1.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_plus1.bin",
                ETAG_30M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper.head_object(&bucket, "30mb_plus1.bin", None).await;
        assert_eq!(
            head.content_length().unwrap(),
            (30 * 1024 * 1024 + 1) as i64
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB-1 file, ETag only, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_minus1.bin", 30, -1).unwrap();

        let target = format!("s3://{}/30mb_minus1.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_minus1.bin",
                ETAG_30M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let download_file = local_dir.join("30mb_minus1_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(dl_stats.sync_complete, 1);
        assert_eq!(dl_stats.sync_error, 0);
        assert_eq!(dl_stats.sync_warning, 0);
        assert_eq!(dl_stats.e_tag_verified, 1);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_30M_MINUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB with --sse aws:kms and SHA256, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_kms_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_kms_sha256.bin", 30, 0).unwrap();

        let target = format!("s3://{}/30mb_kms_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --auto-chunksize (ETag only), default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_ac.bin", 30, 1).unwrap();

        let source_s3 = format!("s3://{}/30mb_plus1_ac.bin", bucket1);
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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);

        let target_s3 = format!("s3://{}/30mb_plus1_ac.bin", bucket2);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "30mb_plus1_ac.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --auto-chunksize + SHA256, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_sha256_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_ac_sha.bin", 30, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/30mb_plus1_ac_sha.bin", bucket1);
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

        let target_s3 = format!("s3://{}/30mb_plus1_ac_sha.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "30mb_plus1_ac_sha.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --auto-chunksize + CRC64NVME, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_crc64nvme_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_ac_crc64.bin", 30, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/30mb_plus1_ac_crc64.bin", bucket1);
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

        let target_s3 = format!("s3://{}/30mb_plus1_ac_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "30mb_plus1_ac_crc64.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with SHA256 checksum, 8 MiB threshold and chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_sha256.bin", 30, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/30mb_plus1_sha256.bin", bucket1);
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

        let target_s3 = format!("s3://{}/30mb_plus1_sha256.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "30mb_plus1_sha256.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_30M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_30M_PLUS_1_FILE_8M_CHUNK
        );

        let bytes = helper
            .get_object_bytes(&bucket2, "30mb_plus1_sha256.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy (bucket1 → bucket2)
        helper.delete_all_objects(&bucket2).await;
        let ssc_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(ssc_stats.sync_complete, 1);
        assert_eq!(ssc_stats.sync_error, 0);
        assert_eq!(ssc_stats.sync_warning, 0);
        assert_eq!(ssc_stats.e_tag_verified, 1);
        assert_eq!(ssc_stats.checksum_verified, 1);
        let head3 = helper
            .head_object(&bucket2, "30mb_plus1_sha256.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_30M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_sha256().unwrap(),
            SHA256_30M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local (bucket2 → download file), verify SHA256
        let download_file = local_dir.join("30mb_plus1_sha256_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(dl_stats.sync_complete, 1);
        assert_eq!(dl_stats.sync_error, 0);
        assert_eq!(dl_stats.sync_warning, 0);
        assert_eq!(dl_stats.e_tag_verified, 1);
        assert_eq!(dl_stats.checksum_verified, 1);
        assert_eq!(
            TestHelper::get_sha256_from_file(download_file.to_str().unwrap()),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with CRC64NVME checksum, 8 MiB threshold and chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_crc64nvme.bin", 30, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/30mb_plus1_crc64nvme.bin", bucket1);
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

        let target_s3 = format!("s3://{}/30mb_plus1_crc64nvme.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "30mb_plus1_crc64nvme.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_30M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_PLUS_1_FILE_8M_CHUNK
        );

        let bytes = helper
            .get_object_bytes(&bucket2, "30mb_plus1_crc64nvme.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy (bucket1 → bucket2)
        helper.delete_all_objects(&bucket2).await;
        let ssc_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(ssc_stats.sync_complete, 1);
        assert_eq!(ssc_stats.sync_error, 0);
        assert_eq!(ssc_stats.sync_warning, 0);
        assert_eq!(ssc_stats.e_tag_verified, 1);
        assert_eq!(ssc_stats.checksum_verified, 1);
        let head3 = helper
            .head_object(&bucket2, "30mb_plus1_crc64nvme.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_30M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local (bucket2 → download file), verify SHA256
        let download_file = local_dir.join("30mb_plus1_crc64nvme_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(dl_stats.sync_complete, 1);
        assert_eq!(dl_stats.sync_error, 0);
        assert_eq!(dl_stats.sync_warning, 0);
        assert_eq!(dl_stats.e_tag_verified, 1);
        assert_eq!(dl_stats.checksum_verified, 1);
        assert_eq!(
            TestHelper::get_sha256_from_file(download_file.to_str().unwrap()),
            SHA256_30M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --sse aws:kms (ETag only), default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_kms.bin", 30, 1).unwrap();

        let target = format!("s3://{}/30mb_plus1_kms.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --sse aws:kms + SHA256, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_kms_sha256.bin", 30, 1)
                .unwrap();

        let target = format!("s3://{}/30mb_plus1_kms_sha256.bin", bucket);
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
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)
        let head = helper
            .head_object(&bucket, "30mb_plus1_kms_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_30M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB+1 with --sse aws:kms + CRC64NVME, default threshold.
    #[tokio::test]
    async fn test_multipart_upload_30mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_plus1_kms_crc64.bin", 30, 1)
                .unwrap();

        let target = format!("s3://{}/30mb_plus1_kms_crc64.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)
        let head = helper
            .head_object(&bucket, "30mb_plus1_kms_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB-1 with SHA256 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_minus1_sha256.bin", 30, -1)
                .unwrap();

        let target = format!("s3://{}/30mb_minus1_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_minus1_sha256.bin",
                ETAG_30M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "30mb_minus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_30M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB-1 with CRC64NVME checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_30mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_minus1_crc64.bin", 30, -1)
                .unwrap();

        let target = format!("s3://{}/30mb_minus1_crc64.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_minus1_crc64.bin",
                ETAG_30M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "30mb_minus1_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
