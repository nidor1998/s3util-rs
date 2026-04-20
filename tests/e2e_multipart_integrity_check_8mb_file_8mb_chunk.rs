#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_8M_FILE_8M_CHUNK: &str = "U+ZIEj2OXjCTTOp7PlJy43aKT7mL2X5NzgfjpYzeozw=-1";
    const CRC64NVME_8M_FILE_8M_CHUNK: &str = "io2hnVvxKgU=";
    const CRC32_8M_FILE_8M_CHUNK: &str = "ghmN/g==-1";
    const CRC32C_8M_FILE_8M_CHUNK: &str = "m+F8yQ==-1";
    const SHA1_8M_FILE_8M_CHUNK: &str = "6HmzyyliR8bIKVsBsFcL3Ocs/oQ=-1";
    const ETAG_8M_FILE_8M_CHUNK: &str = "\"13698b45ee34dbf0611fe527f76abfc7-1\"";
    const SHA256_8M_PLUS_1_FILE_8M_CHUNK: &str = "fMRKvd1OLwQTon7VE4yRXsWmVDtF8uLVJT9aKIM2SC8=-2";
    const SHA1_8M_PLUS_1_FILE_8M_CHUNK: &str = "o71VQaKqumjAbkSbJyNYKS4RXwI=-2";
    const CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK: &str = "lCfg/yBs550=";
    const CRC32_8M_PLUS_1_FILE_8M_CHUNK: &str = "assFew==-2";
    const CRC32C_8M_PLUS_1_FILE_8M_CHUNK: &str = "oJX2nw==-2";
    const ETAG_8M_PLUS_1_FILE_8M_CHUNK: &str = "\"61b0524a157f9391c45c09ae2b48dde4-2\"";
    const SHA256_8M_MINUS_1_FILE_8M_CHUNK: &str = "LH/6UUEm4g2eT855uX/3OcshOFJADcHasHpSnaTsPkQ=";
    const SHA1_8M_MINUS_1_FILE_8M_CHUNK: &str = "2TX5C09wq0eial8QWuHx97tciVg=";
    const CRC64NVME_8M_MINUS_1_FILE_8M_CHUNK: &str = "GPMNBOmnUuA=";
    const CRC32_8M_MINUS_1_FILE_8M_CHUNK: &str = "p0SZTQ==";
    const CRC32C_8M_MINUS_1_FILE_8M_CHUNK: &str = "ClXOXA==";
    const ETAG_8M_MINUS_1_FILE_8M_CHUNK: &str = "\"c9c7b65a175f43ff8147d8027403e177\"";
    const SHA256_8M_FILE_WHOLE: &str =
        "cd5f57c6ffe3f685104aba6ec7268baab8790603034bdec830228b572d84c5a4";
    const SHA256_8M_PLUS_1_FILE_WHOLE: &str =
        "e0a269be5fbff701eba9a07f82027f5a1e22bebc8df2f2027840a02184b84b3c";
    const SHA256_8M_MINUS_1_FILE_WHOLE: &str =
        "2c7ffa514126e20d9e4fce79b97ff739cb213852400dc1dab07a529da4ec3e44";

    /// Upload an 8 MiB file with 8 MiB chunk (single-part multipart) and verify ETag.
    #[tokio::test]
    async fn test_multipart_upload_8mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "8mb.bin", 8, 0).unwrap();

        // Local to S3
        let target = format!("s3://{}/8mb.bin", bucket1);
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
            .verify_uploaded_object_etag_value(&bucket1, "8mb.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        // S3 to S3
        let source = format!("s3://{}/8mb.bin", bucket1);
        let target2 = format!("s3://{}/8mb.bin", bucket2);
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
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "8mb.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        // S3 to local
        let download_file = local_dir.join("downloaded_8mb.bin");
        let source2 = format!("s3://{}/8mb.bin", bucket2);
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);

        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_file(download_file.to_str().unwrap());
        assert_eq!(original_sha256, downloaded_sha256);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file with 8 MiB chunk (2 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1.bin", bucket);
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

        let head = helper.head_object(&bucket, "8mb_plus1.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (8 * 1024 * 1024 + 1) as i64);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "8mb_plus1.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (below multipart threshold, single put).
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1.bin", 8, -1).unwrap();

        let target = format!("s3://{}/8mb_minus1.bin", bucket);
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
                "8mb_minus1.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        // S3 to local
        let download_file = local_dir.join("downloaded_8mb_minus1.bin");
        let source2 = format!("s3://{}/8mb_minus1.bin", bucket);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_MINUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file with SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_sha256.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_sha256.bin", bucket);
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_sha256.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "8mb_sha256.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_8M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file with CRC64NVME checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_crc64.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_crc64.bin", bucket);
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_crc64.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "8mb_crc64.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file with CRC32 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_crc32.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_crc32.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_crc32.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "8mb_crc32.bin", None).await;
        assert_eq!(head.checksum_crc32().unwrap(), CRC32_8M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file with SHA1 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_sha1.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_sha1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_sha1.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "8mb_sha1.bin", None).await;
        assert_eq!(head.checksum_sha1().unwrap(), SHA1_8M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB file with CRC32C checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_crc32_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_crc32_c.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_crc32_c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_crc32_c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "8mb_crc32_c.bin", None).await;
        assert_eq!(head.checksum_crc32_c().unwrap(), CRC32C_8M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_sha256.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1_sha256.bin", bucket);
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
            .verify_uploaded_object_etag_value(
                &bucket,
                "8mb_plus1_sha256.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_plus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_8M_PLUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with CRC64NVME checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_crc64.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1_crc64.bin", bucket);
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
            .verify_uploaded_object_etag_value(
                &bucket,
                "8mb_plus1_crc64.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_plus1_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single put) with SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1_sha256.bin", 8, -1)
                .unwrap();

        let target = format!("s3://{}/8mb_minus1_sha256.bin", bucket);
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
            .verify_uploaded_object_etag_value(
                &bucket,
                "8mb_minus1_sha256.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_minus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_8M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single put) with CRC64NVME checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1_crc64.bin", 8, -1).unwrap();

        let target = format!("s3://{}/8mb_minus1_crc64.bin", bucket);
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
            .verify_uploaded_object_etag_value(
                &bucket,
                "8mb_minus1_crc64.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_minus1_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS and SHA256 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_sha256_kms.bin", 8, 1)
                .unwrap();

        let target = format!("s3://{}/8mb_plus1_sha256_kms.bin", bucket);
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

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS and CRC64NVME checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_crc64nvme_kms.bin", 8, 1)
                .unwrap();

        let target = format!("s3://{}/8mb_plus1_crc64nvme_kms.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB+1 with 8 MiB chunks, S3-to-S3 with --auto-chunksize, verify ETag + SHA256.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac.bin", 8, 1).unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac.bin", bucket1);
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

        let target_s3 = format!("s3://{}/8mb_plus1_ac.bin", bucket2);
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
            .get_object_bytes(&bucket2, "8mb_plus1_ac.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_8M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SHA1 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_sha1.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1_sha1.bin", bucket);
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
                "SHA1",
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
                "8mb_plus1_sha1.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_plus1_sha1.bin", None)
            .await;
        assert_eq!(head.checksum_sha1().unwrap(), SHA1_8M_PLUS_1_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single put) with SHA1 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1_sha1.bin", 8, -1).unwrap();

        let target = format!("s3://{}/8mb_minus1_sha1.bin", bucket);
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
                "SHA1",
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
                "8mb_minus1_sha1.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_minus1_sha1.bin", None)
            .await;
        assert_eq!(head.checksum_sha1().unwrap(), SHA1_8M_MINUS_1_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB+1 with --auto-chunksize + SHA1.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac_sha1.bin", 8, 1).unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac_sha1.bin", bucket1);
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
                "SHA1",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/8mb_plus1_ac_sha1.bin", bucket2);
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
                "SHA1",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "8mb_plus1_ac_sha1.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_8M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS and SHA1 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_kms_sha1.bin", 8, 1)
                .unwrap();

        let target = format!("s3://{}/8mb_plus1_kms_sha1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA1",
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

    /// 8 MiB + 1 byte file (2 parts) with CRC32 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_crc32.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1_crc32.bin", bucket);
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
                "CRC32",
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
                "8mb_plus1_crc32.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_plus1_crc32.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc32().unwrap(),
            CRC32_8M_PLUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single put) with CRC32 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1_crc32.bin", 8, -1).unwrap();

        let target = format!("s3://{}/8mb_minus1_crc32.bin", bucket);
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
                "CRC32",
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
                "8mb_minus1_crc32.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_minus1_crc32.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc32().unwrap(),
            CRC32_8M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB+1 with --auto-chunksize + CRC32.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac_crc32.bin", 8, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac_crc32.bin", bucket1);
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
                "CRC32",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/8mb_plus1_ac_crc32.bin", bucket2);
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
                "CRC32",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "8mb_plus1_ac_crc32.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_8M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS and CRC32 checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_kms_crc32.bin", 8, 1)
                .unwrap();

        let target = format!("s3://{}/8mb_plus1_kms_crc32.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "CRC32",
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

    /// 8 MiB + 1 byte file (2 parts) with CRC32C checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_crc32_c.bin", 8, 1).unwrap();

        let target = format!("s3://{}/8mb_plus1_crc32_c.bin", bucket);
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
                "CRC32C",
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
                "8mb_plus1_crc32_c.bin",
                ETAG_8M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_plus1_crc32_c.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc32_c().unwrap(),
            CRC32C_8M_PLUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB - 1 byte file (single put) with CRC32C checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_minus_1_crc32_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_minus1_crc32_c.bin", 8, -1)
                .unwrap();

        let target = format!("s3://{}/8mb_minus1_crc32_c.bin", bucket);
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
                "CRC32C",
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
                "8mb_minus1_crc32_c.bin",
                ETAG_8M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_minus1_crc32_c.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc32_c().unwrap(),
            CRC32C_8M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB+1 with --auto-chunksize + CRC32C.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32_c_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac_crc32_c.bin", 8, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac_crc32_c.bin", bucket1);
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
                "CRC32C",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/8mb_plus1_ac_crc32_c.bin", bucket2);
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
                "CRC32C",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "8mb_plus1_ac_crc32_c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_8M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS and CRC32C checksum.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc32_c_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_crc32_c_kms.bin", 8, 1)
                .unwrap();

        let target = format!("s3://{}/8mb_plus1_crc32_c_kms.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "CRC32C",
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

    /// 8 MiB + 1 byte file (2 parts) with SSE KMS (no additional checksum).
    /// Covers: local→S3, S3→S3 (client-side), S3→S3 (server-side), S3→local.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_kms.bin", 8, 1).unwrap();

        // Scope 1: Local → S3 (bucket1) with SSE-KMS.
        let source_s3 = format!("s3://{}/8mb_plus1_kms.bin", bucket1);
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
                &source_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        use aws_sdk_s3::types::ServerSideEncryption;
        let head = helper
            .head_object(&bucket1, "8mb_plus1_kms.bin", None)
            .await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        // Scope 2: S3 (bucket1) → S3 (bucket2) client-side copy with SSE-KMS.
        let target_s3 = format!("s3://{}/8mb_plus1_kms.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 0);
        assert_eq!(stats2.checksum_verified, 0);

        // Scope 3: S3 (bucket1) → S3 (bucket2) server-side copy with SSE-KMS.
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--sse",
                "aws:kms",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 0);
        assert_eq!(stats3.checksum_verified, 0);

        // Scope 4: S3 (bucket2) → local, verify file contents.
        let download_file = local_dir.join("downloaded_8mb_plus1_kms.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 0);
        assert_eq!(stats4.checksum_verified, 0);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with SHA256 checksum and auto-chunksize S3-to-S3 copy.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_sha256_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac_sha256.bin", 8, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac_sha256.bin", bucket1);
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

        let head = helper
            .head_object(&bucket1, "8mb_plus1_ac_sha256.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_8M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/8mb_plus1_ac_sha256.bin", bucket2);
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head2 = helper
            .head_object(&bucket2, "8mb_plus1_ac_sha256.bin", None)
            .await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head2.checksum_sha256().unwrap(),
            SHA256_8M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 3: S3 (bucket1) → S3 (bucket2) server-side copy with SHA256 checksum.
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
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
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
            .head_object(&bucket2, "8mb_plus1_ac_sha256.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_sha256().unwrap(),
            SHA256_8M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 (bucket2) → local, verify file contents.
        let download_file = local_dir.join("downloaded_8mb_plus1_ac_sha256.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB + 1 byte file (2 parts) with CRC64NVME checksum and auto-chunksize S3-to-S3 copy.
    #[tokio::test]
    async fn test_multipart_upload_8mb_plus_1_crc64nvme_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_plus1_ac_crc64nvme.bin", 8, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/8mb_plus1_ac_crc64nvme.bin", bucket1);
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

        let head = helper
            .head_object(&bucket1, "8mb_plus1_ac_crc64nvme.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/8mb_plus1_ac_crc64nvme.bin", bucket2);
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head2 = helper
            .head_object(&bucket2, "8mb_plus1_ac_crc64nvme.bin", None)
            .await;
        assert_eq!(head2.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head2.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 3: S3 (bucket1) → S3 (bucket2) server-side copy with CRC64NVME checksum.
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
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
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
            .head_object(&bucket2, "8mb_plus1_ac_crc64nvme.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_8M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 (bucket2) → local, verify file contents.
        let download_file = local_dir.join("downloaded_8mb_plus1_ac_crc64nvme.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
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

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
