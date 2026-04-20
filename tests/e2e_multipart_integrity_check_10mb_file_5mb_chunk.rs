#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_10M_FILE_5M_CHUNK: &str = "uLuYA9R1hbgmjJlZcP8jzEDygVtq4mopxM//prh1GNk=-2";
    const CRC64NVME_10M_FILE_5M_CHUNK: &str = "cHKhFzFyIPM=";
    const ETAG_10M_FILE_5M_CHUNK: &str = "\"fd863860e4b73868097377d43bd65a58-2\"";
    const SHA256_10M_PLUS_1_FILE_5M_CHUNK: &str = "kkwf92kzoqv1UUa5LcRJdQ5wwTE1KsJHmUGQM3EURu8=-3";
    const CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK: &str = "Z3VTxB3LlLM=";
    const ETAG_10M_PLUS_1_FILE_5M_CHUNK: &str = "\"527cc728f7dd89f1ef3256e2ff5c808c-3\"";
    const SHA256_10M_MINUS_1_FILE_5M_CHUNK: &str = "WC7KMpjlemoxe3Y8n/jdKlUTDZLGonZ26ktyBfuEBMo=-2";
    const CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK: &str = "6XaOQ2PhG04=";
    const ETAG_10M_MINUS_1_FILE_5M_CHUNK: &str = "\"6e11660e4457458f925f9a92227be331-2\"";
    const SHA256_10M_FILE_WHOLE: &str =
        "d5fc3f080e832d82161f9461291f87989b81a9e6281c33589d9563adefb46055";
    const SHA256_10M_PLUS_1_FILE_WHOLE: &str =
        "cbb719063c17ba48ec3925cc4ba8267addd8515b4f69e689da1dfc3a6683191a";
    const SHA256_10M_MINUS_1_FILE_WHOLE: &str =
        "15b3422113fb29d70652d87367f92c23520963d13b71ef8efc73e158850bbadf";

    /// Upload a 10 MiB file with 5 MiB chunk (2 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_10mb() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "10mb_5c.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (10 * 1024 * 1024) as i64);

        let download_file = local_dir.join("10mb_5c_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(dl_stats.sync_complete, 1);
        assert_eq!(dl_stats.sync_error, 0);
        assert_eq!(dl_stats.sync_warning, 0);
        assert_eq!(dl_stats.e_tag_verified, 1);
        assert_eq!(dl_stats.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_10M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB file with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_sha256() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c_sha.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "10mb_5c_sha.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_10M_FILE_5M_CHUNK,);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB file with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_crc64nvme() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "10mb_5c_crc64.bin", ETAG_10M_FILE_5M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "10mb_5c_crc64.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_10M_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with ETag only, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
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
    async fn test_multipart_upload_10mb_minus_1() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
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
    async fn test_multipart_upload_10mb_plus_1_sha256() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_plus1_5c_sha.bin",
                ETAG_10M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "10mb_plus1_5c_sha.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_10M_PLUS_1_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_crc64nvme() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_plus1_5c_crc64.bin",
                ETAG_10M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "10mb_plus1_5c_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_10M_PLUS_1_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 file with KMS SSE and SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_sha256_kms() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
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
    async fn test_multipart_upload_10mb_plus_1_sha256_auto_chunksize() {
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
    async fn test_multipart_upload_10mb_plus_1_crc64nvme_auto_chunksize() {
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
        assert_eq!(stats.e_tag_verified, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB+1 with --sse aws:kms + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_10mb_plus_1_crc64nvme_kms() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB-1 with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_sha256() {
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
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "10mb_minus1_5c_sha.bin",
                ETAG_10M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "10mb_minus1_5c_sha.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_10M_MINUS_1_FILE_5M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 10 MiB-1 with CRC64NVME checksum, 5 MiB chunk — full 4-scope roundtrip.
    #[tokio::test]
    async fn test_multipart_upload_10mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "10mb_minus1_5c_crc64.bin", 10, -1)
                .unwrap();

        // Scope 1: Local → S3 (bucket1)
        let source_s3 = format!("s3://{}/10mb_minus1_5c_crc64.bin", bucket1);
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
        let head1 = helper
            .head_object(&bucket1, "10mb_minus1_5c_crc64.bin", None)
            .await;
        assert_eq!(head1.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head1.checksum_crc64_nvme.unwrap(),
            CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
        );

        // Scope 2: S3 → S3 client-side copy (bucket1 → bucket2)
        let target_s3 = format!("s3://{}/10mb_minus1_5c_crc64.bin", bucket2);
        let s2s_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(s2s_stats.sync_complete, 1);
        assert_eq!(s2s_stats.sync_error, 0);
        assert_eq!(s2s_stats.sync_warning, 0);
        assert_eq!(s2s_stats.e_tag_verified, 1);
        assert_eq!(s2s_stats.checksum_verified, 1);
        let head2 = helper
            .head_object(&bucket2, "10mb_minus1_5c_crc64.bin", None)
            .await;
        assert_eq!(head2.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head2.checksum_crc64_nvme.unwrap(),
            CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
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
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            .head_object(&bucket2, "10mb_minus1_5c_crc64.bin", None)
            .await;
        assert_eq!(head3.e_tag.unwrap(), ETAG_10M_MINUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head3.checksum_crc64_nvme.unwrap(),
            CRC64NVME_10M_MINUS_1_FILE_5M_CHUNK
        );

        // Scope 4: S3 → Local (bucket2 → download file), verify SHA256
        let download_file = local_dir.join("10mb_minus1_5c_crc64_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
            SHA256_10M_MINUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
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
        assert_eq!(stats1.sync_error, 0);
        assert_eq!(stats1.sync_warning, 0);
        assert_eq!(stats1.e_tag_verified, 1);
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
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
