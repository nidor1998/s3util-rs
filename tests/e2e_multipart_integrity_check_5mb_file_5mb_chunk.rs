#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_5M_FILE_5M_CHUNK: &str = "MYqGkAXXRLk+5ZHLgr20ovud2DTYgzUpOygDmGBbh9k=-1";
    const CRC64NVME_5M_FILE_5M_CHUNK: &str = "L0elBfNQAM0=";
    const ETAG_5M_FILE_5M_CHUNK: &str = "\"41c54a21b664d10684a24bb15b86b81b-1\"";
    const SHA256_5M_PLUS_1_FILE_5M_CHUNK: &str = "YzoBvMkdS8iDkS8i/cbrtBN+hTpKC1ibfBjZ2MicyrI=-2";
    const CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK: &str = "f6iP2wb1XBg=";
    const ETAG_5M_PLUS_1_FILE_5M_CHUNK: &str = "\"c9f6c942564f9ebead5cb09e63b70dd7-2\"";
    const SHA256_5M_MINUS_1_FILE_5M_CHUNK: &str = "X7YKLr+rI8buCGZV5DmFaNsA5px6uk4kclHxIocUfHU=";
    const CRC64NVME_5M_MINUS_1_FILE_5M_CHUNK: &str = "k3BLMbXLWlU=";
    const ETAG_5M_MINUS_1_FILE_5M_CHUNK: &str = "\"74222dcf8ba716d84efe0dc716360087\"";
    const SHA256_5M_FILE_WHOLE: &str =
        "27d49a61d9a504bf66761f4d3143702d97876ddf5864d4ba22467cd04cdc67f0";
    const SHA256_5M_PLUS_1_FILE_WHOLE: &str =
        "e3bfafb553570dea7233d3a6d6d5d3ac3cff422cd1b5b5f7af767f0778511ef9";
    const SHA256_5M_MINUS_1_FILE_WHOLE: &str =
        "5fb60a2ebfab23c6ee086655e4398568db00e69c7aba4e247251f12287147c75";

    /// Upload a 5 MiB file with 5 MiB chunk (boundary case) and verify.
    #[tokio::test]
    async fn test_multipart_upload_5mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "5mb.bin", 5, 0).unwrap();

        let target = format!("s3://{}/5mb.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "5mb.bin", ETAG_5M_FILE_5M_CHUNK)
            .await;
        let bytes = helper.get_object_bytes(&bucket, "5mb.bin", None).await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_5M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB + 1 file with 5 MiB chunk (2 parts).
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1.bin", 5, 1).unwrap();

        let target = format!("s3://{}/5mb_plus1.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "5mb_plus1.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (5 * 1024 * 1024 + 1) as i64);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_plus1.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB - 1 file with 5 MiB threshold (single put, below threshold).
    #[tokio::test]
    async fn test_multipart_upload_5mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_minus1.bin", 5, -1).unwrap();

        let target = format!("s3://{}/5mb_minus1.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_minus1.bin",
                ETAG_5M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;
        let bytes = helper
            .get_object_bytes(&bucket, "5mb_minus1.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_5M_MINUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB with SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_5mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_sha256.bin", 5, 0).unwrap();

        let target = format!("s3://{}/5mb_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper.head_object(&bucket, "5mb_sha256.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_5M_FILE_5M_CHUNK,);
        helper
            .verify_uploaded_object_etag_value(&bucket, "5mb_sha256.bin", ETAG_5M_FILE_5M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB file with CRC64NVME checksum, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_crc64nvme.bin", 5, 0).unwrap();

        let target = format!("s3://{}/5mb_crc64nvme.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper.head_object(&bucket, "5mb_crc64nvme.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_5M_FILE_5M_CHUNK,
        );
        helper
            .verify_uploaded_object_etag_value(&bucket, "5mb_crc64nvme.bin", ETAG_5M_FILE_5M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB + 1 file with SHA256 checksum, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_sha256.bin", 5, 1).unwrap();

        let target = format!("s3://{}/5mb_plus1_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper
            .head_object(&bucket, "5mb_plus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_5M_PLUS_1_FILE_5M_CHUNK,
        );
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_plus1_sha256.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB + 1 file with CRC64NVME checksum, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_crc64nvme.bin", 5, 1)
                .unwrap();

        let target = format!("s3://{}/5mb_plus1_crc64nvme.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper
            .head_object(&bucket, "5mb_plus1_crc64nvme.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK,
        );
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_plus1_crc64nvme.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB - 1 file with SHA256 checksum, threshold=5MiB, chunksize=5MiB (single put).
    #[tokio::test]
    async fn test_multipart_upload_5mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_minus1_sha256.bin", 5, -1)
                .unwrap();

        let target = format!("s3://{}/5mb_minus1_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper
            .head_object(&bucket, "5mb_minus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_5M_MINUS_1_FILE_5M_CHUNK,
        );
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_minus1_sha256.bin",
                ETAG_5M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB - 1 file with CRC64NVME checksum, threshold=5MiB, chunksize=5MiB (single put).
    #[tokio::test]
    async fn test_multipart_upload_5mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_minus1_crc64nvme.bin", 5, -1)
                .unwrap();

        let target = format!("s3://{}/5mb_minus1_crc64nvme.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head = helper
            .head_object(&bucket, "5mb_minus1_crc64nvme.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_5M_MINUS_1_FILE_5M_CHUNK,
        );
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "5mb_minus1_crc64nvme.bin",
                ETAG_5M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload 5 MiB+1 with SHA256, then S3-to-S3 copy with --auto-chunksize + SHA256.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_sha256_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_autochunk_sha256.bin", 5, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/5mb_plus1_autochunk_sha256.bin", bucket1);
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
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "5mb_plus1_autochunk_sha256.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let target_s3 = format!("s3://{}/5mb_plus1_autochunk_sha256.bin", bucket2);
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
        assert_eq!(stats.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload 5 MiB+1 with CRC64NVME, then S3-to-S3 copy with --auto-chunksize + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_crc64nvme_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(
            &local_dir,
            "5mb_plus1_autochunk_crc64nvme.bin",
            5,
            1,
        )
        .unwrap();

        let source_s3 = format!("s3://{}/5mb_plus1_autochunk_crc64nvme.bin", bucket1);
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
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "5mb_plus1_autochunk_crc64nvme.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let target_s3 = format!("s3://{}/5mb_plus1_autochunk_crc64nvme.bin", bucket2);
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
        assert_eq!(stats.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB+1 with --sse aws:kms and SHA256, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_kms_sha256.bin", 5, 1)
                .unwrap();

        let target = format!("s3://{}/5mb_plus1_kms_sha256.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)
        let head = helper
            .head_object(&bucket, "5mb_plus1_kms_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_5M_PLUS_1_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB+1 with --sse aws:kms and CRC64NVME, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_kms_crc64nvme.bin", 5, 1)
                .unwrap();

        let target = format!("s3://{}/5mb_plus1_kms_crc64nvme.bin", bucket);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)
        let head = helper
            .head_object(&bucket, "5mb_plus1_kms_crc64nvme.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_5M_PLUS_1_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 5 MiB+1 with --sse aws:kms (ETag only), threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_kms.bin", 5, 1).unwrap();

        let target = format!("s3://{}/5mb_plus1_kms.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload 5 MiB+1 with 5MiB chunks, then S3-to-S3 copy with --auto-chunksize.
    #[tokio::test]
    async fn test_multipart_upload_5mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "5mb_plus1_autochunk.bin", 5, 1)
                .unwrap();

        // Upload with 5MiB chunks
        let source_s3 = format!("s3://{}/5mb_plus1_autochunk.bin", bucket1);
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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "5mb_plus1_autochunk.bin",
                ETAG_5M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        // S3-to-S3 with auto-chunksize
        let target_s3 = format!("s3://{}/5mb_plus1_autochunk.bin", bucket2);
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
        assert_eq!(stats.checksum_verified, 0);
        let bytes = helper
            .get_object_bytes(&bucket2, "5mb_plus1_autochunk.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_5M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
