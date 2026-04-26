#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_30M_FILE_8M_CHUNK: &str = "5NrHBc0Z1wNCbADRDy8mJaIvc53oxncCrw/Fa48VhxY=-4";
    const SHA256_30M_FILE_NO_CHUNK: &str = "BcHHcdSIbkzv3wpMC5B5E/4vgp3XZ0GMlOoniwuLw/k=";
    const CRC64NVME_30M_FILE_8M_CHUNK: &str = "rrk4q4lsMS4=";
    const CRC64NVME_30M_FILE_NO_CHUNK: &str = "rrk4q4lsMS4=";
    const ETAG_30M_FILE_8M_CHUNK: &str = "\"a81230a7666d413e511f9c2c2523947a-4\"";
    const ETAG_30M_FILE_NO_CHUNK: &str = "\"94189ebb786dbc25aaf22d3d96e88aeb\"";
    const SHA256_1M_FILE: &str = "nHjjstwawGidD0Sn7WX1sNMcsjAoK7gP8bUyhDOlnRU=";
    const CRC64NVME_1M_FILE: &str = "IWamHyhf59M=";
    const ETAG_1M_FILE: &str = "\"1ebdcfc23acf32f84f462e721e9db32c\"";
    const SHA256_8M_FILE_8M_CHUNK: &str = "U+ZIEj2OXjCTTOp7PlJy43aKT7mL2X5NzgfjpYzeozw=-1";
    const CRC64NVME_8M_FILE_8M_CHUNK: &str = "io2hnVvxKgU=";
    const ETAG_8M_FILE_8M_CHUNK: &str = "\"13698b45ee34dbf0611fe527f76abfc7-1\"";
    const ETAG_8M_FILE_5M_CHUNK: &str = "\"ebff86fc334a63cefaad7a0b621a0109-2\"";
    const SHA256_8M_FILE_NO_CHUNK: &str = "zV9Xxv/j9oUQSrpuxyaLqrh5BgMDS97IMCKLVy2ExaQ=";
    const CRC64NVME_8M_FILE_NO_CHUNK: &str = "io2hnVvxKgU=";
    const ETAG_8M_FILE_NO_CHUNK: &str = "\"e9d3e2caa0ac28fd50b183dac706ee29\"";
    const SHA256_8M_FILE_5M_CHUNK: &str = "EZAvWUpvGrpch+0S5qFJhcwxd6bw9HtocRRVc/FAwQA=-2";
    const SHA256_30M_FILE_WHOLE_HEX: &str =
        "05c1c771d4886e4cefdf0a4c0b907913fe2f829dd767418c94ea278b0b8bc3f9";
    const SHA256_1M_WHOLE: &str =
        "9c78e3b2dc1ac0689d0f44a7ed65f5b0d31cb230282bb80ff1b5328433a59d15";
    const SHA256_8M_FILE_WHOLE: &str =
        "cd5f57c6ffe3f685104aba6ec7268baab8790603034bdec830228b572d84c5a4";

    /// Upload a 1 byte file (edge case: minimal file).
    #[tokio::test]
    async fn test_upload_1_byte() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "1byte.bin", &[42]);

        let target = format!("s3://{}/1byte.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "1byte.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 1);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload an empty file (0 bytes).
    #[tokio::test]
    async fn test_upload_empty_file() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "empty.bin", &[]);

        let target = format!("s3://{}/empty.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "empty.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload exactly at multipart threshold boundary (8 MiB).
    #[tokio::test]
    async fn test_upload_exact_threshold_boundary() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "exact_8mb.bin", 8, 0).unwrap();

        let target = format!("s3://{}/exact_8mb.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "exact_8mb.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Roundtrip for an empty file.
    #[tokio::test]
    async fn test_empty_file_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "empty_rt.bin", &[]);

        // Upload
        let s3_path = format!("s3://{}/empty_rt.bin", bucket);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        assert_eq!(stats1.sync_error, 0);
        assert_eq!(stats1.sync_warning, 0);
        assert_eq!(stats1.e_tag_verified, 1);
        assert_eq!(stats1.checksum_verified, 0);

        // Download
        let download_file = local_dir.join("empty_dl.bin");
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload a 1-byte file with SHA256 additional checksum algorithm.
    #[tokio::test]
    async fn test_upload_1_byte_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "1byte_sha256.bin", &[42]);

        let target = format!("s3://{}/1byte_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload a 1-byte file with CRC64NVME additional checksum algorithm.
    #[tokio::test]
    async fn test_upload_1_byte_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "1byte_crc64nvme.bin", &[42]);

        let target = format!("s3://{}/1byte_crc64nvme.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload an 8MiB file with 7MiB multipart threshold and 5MiB chunk size (forces multipart).
    #[tokio::test]
    async fn test_multipart_upload_8mb_7mb_threshold_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_multipart.bin", 8, 0).unwrap();

        // Scope 1: 8 MiB multipart (7 MiB threshold, 5 MiB chunk), no additional checksum.
        let target = format!("s3://{}/8mb_multipart.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "7MiB",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_multipart.bin", ETAG_8M_FILE_5M_CHUNK)
            .await;

        // Scope 2: 8 MiB multipart (7 MiB threshold, 5 MiB chunk) + SHA256 additional checksum.
        // Mirrors s3sync's first scope in test_multipart_upload_8mb_7mb_threshold_5mb_chunk.
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "7MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "8mb_multipart.bin", ETAG_8M_FILE_5M_CHUNK)
            .await;

        let head2 = helper.head_object(&bucket, "8mb_multipart.bin", None).await;
        assert_eq!(head2.checksum_sha256().unwrap(), SHA256_8M_FILE_5M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload an 8MiB file with 9MiB multipart threshold (forces single put).
    #[tokio::test]
    async fn test_upload_8mb_9mb_threshold() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_single_put.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_single_put.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "9MiB",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_single_put.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload an 8MiB file with 7MiB multipart threshold and 9MiB chunk (single chunk multipart).
    #[tokio::test]
    async fn test_multipart_upload_8mb_7mb_threshold_9mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_7t_9c.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_7t_9c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "7MiB",
                "--multipart-chunksize",
                "9MiB",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_7t_9c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload an 8MiB file with 9MiB multipart threshold and 5MiB chunk (single put).
    #[tokio::test]
    async fn test_multipart_upload_8mb_9mb_threshold_5mb_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_9t_5c.bin", 8, 0).unwrap();

        let target = format!("s3://{}/8mb_9t_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "9MiB",
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
            .verify_uploaded_object_etag_value(&bucket, "8mb_9t_5c.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB with SHA256, threshold=8MiB, chunksize=8MiB.
    #[tokio::test]
    async fn test_multipart_upload_30mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_edge_sha256.bin", 30, 0).unwrap();

        // Scope 1: 30 MiB multipart (8 MiB threshold, 8 MiB chunk) + SHA256
        let target = format!("s3://{}/30mb_edge_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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
                "30mb_edge_sha256.bin",
                ETAG_30M_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "30mb_edge_sha256.bin", None)
            .await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_30M_FILE_8M_CHUNK);

        // Scope 2: 30 MiB single-put (50 MiB threshold, no multipart) + SHA256.
        // Mirrors s3sync's first scope in test_multipart_upload_30mb_sha256.
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_edge_sha256.bin",
                ETAG_30M_FILE_NO_CHUNK,
            )
            .await;

        let head2 = helper
            .head_object(&bucket, "30mb_edge_sha256.bin", None)
            .await;
        assert_eq!(head2.checksum_sha256().unwrap(), SHA256_30M_FILE_NO_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB with CRC64NVME, threshold=8MiB, chunksize=8MiB.
    #[tokio::test]
    async fn test_multipart_upload_30mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_edge_crc64.bin", 30, 0).unwrap();

        // Scope 1: 30 MiB multipart (8 MiB threshold, 8 MiB chunk) + CRC64NVME
        let target = format!("s3://{}/30mb_edge_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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
                "30mb_edge_crc64.bin",
                ETAG_30M_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "30mb_edge_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_FILE_8M_CHUNK
        );

        // Scope 2: 30 MiB single-put (50 MiB threshold, no multipart) + CRC64NVME.
        // Mirrors s3sync's first scope in test_multipart_upload_30mb_crc64nvme.
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "30mb_edge_crc64.bin",
                ETAG_30M_FILE_NO_CHUNK,
            )
            .await;

        let head2 = helper
            .head_object(&bucket, "30mb_edge_crc64.bin", None)
            .await;
        assert_eq!(
            head2.checksum_crc64_nvme().unwrap(),
            CRC64NVME_30M_FILE_NO_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8MiB with SHA256, threshold=9MiB (single put).
    #[tokio::test]
    async fn test_multipart_upload_8mb_no_chunk_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_no_chunk_sha256.bin", 8, 0)
                .unwrap();

        let target = format!("s3://{}/8mb_no_chunk_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "9MiB",
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
                "8mb_no_chunk_sha256.bin",
                ETAG_8M_FILE_NO_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_no_chunk_sha256.bin", None)
            .await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_8M_FILE_NO_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8MiB with CRC64NVME, threshold=9MiB (single put).
    #[tokio::test]
    async fn test_multipart_upload_8mb_no_chunk_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_no_chunk_crc64.bin", 8, 0)
                .unwrap();

        let target = format!("s3://{}/8mb_no_chunk_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "9MiB",
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
                "8mb_no_chunk_crc64.bin",
                ETAG_8M_FILE_NO_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "8mb_no_chunk_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_FILE_NO_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload a 1 MiB random file (single put). Mirrors s3sync's `test_upload_1m`.
    #[tokio::test]
    async fn test_upload_1m() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "1m.bin", 1, 0).unwrap();

        // Scope 1: Local → S3
        let target = format!("s3://{}/1m.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "1m.bin", None).await;
        assert_eq!(head.e_tag().unwrap(), ETAG_1M_FILE);

        // Scope 2: S3 → Local with --auto-chunksize, verify SHA256.
        // Mirrors s3sync's download-verify scope in test_upload_1m.
        let download_file = local_dir.join("1m_dl.bin");
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_1M_WHOLE);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload a 1 MiB random file with SHA256 additional checksum.
    /// Mirrors s3sync's `test_upload_1m_sha256`.
    #[tokio::test]
    async fn test_upload_1m_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "1m_sha256.bin", 1, 0).unwrap();

        let target = format!("s3://{}/1m_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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

        let head = helper.head_object(&bucket, "1m_sha256.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_1M_FILE);
        assert_eq!(head.e_tag().unwrap(), ETAG_1M_FILE);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload a 1 MiB random file with CRC64NVME additional checksum.
    /// Mirrors s3sync's `test_upload_1m_crc64nvme`.
    #[tokio::test]
    async fn test_upload_1m_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "1m_crc64nvme.bin", 1, 0).unwrap();

        let target = format!("s3://{}/1m_crc64nvme.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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

        let head = helper.head_object(&bucket, "1m_crc64nvme.bin", None).await;
        assert_eq!(head.checksum_crc64_nvme().unwrap(), CRC64NVME_1M_FILE);
        assert_eq!(head.e_tag().unwrap(), ETAG_1M_FILE);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload with auto-chunksize flag.
    #[tokio::test]
    async fn test_multipart_upload_with_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "auto_chunk.bin", 9, 0).unwrap();

        // Upload
        let s3_path = format!("s3://{}/auto_chunk.bin", bucket1);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        assert_eq!(stats1.sync_error, 0);
        assert_eq!(stats1.sync_warning, 0);
        assert_eq!(stats1.e_tag_verified, 1);
        assert_eq!(stats1.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "auto_chunk.bin", ETAG_9M_FILE_8M_CHUNK)
            .await;

        // S3 to S3 with auto-chunksize
        let target_s3 = format!("s3://{}/auto_chunk.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &s3_path,
                &target_s3,
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);

        let head = helper.head_object(&bucket2, "auto_chunk.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (9 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 30 MiB with no additional checksum, threshold=8MiB, chunksize=8MiB.
    /// Mirrors s3sync's `test_multipart_upload_30mb` (7 scopes).
    #[tokio::test]
    async fn test_multipart_upload_30mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "30mb_edge.bin", 30, 0).unwrap();

        // Scope 1: Local → S3 (high threshold → single put)
        let target = format!("s3://{}/30mb_edge.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
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
            .verify_uploaded_object_etag_value(&bucket1, "30mb_edge.bin", ETAG_30M_FILE_NO_CHUNK)
            .await;

        // Scope 2: S3 → S3 client-side (no auto-chunksize; default chunk → multipart ETag, warning)
        let source2 = format!("s3://{}/30mb_edge.bin", bucket1);
        let target2 = format!("s3://{}/30mb_edge.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source2,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 1);
        assert_eq!(stats2.e_tag_verified, 0);
        assert_eq!(stats2.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "30mb_edge.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        // Scope 3: S3 → S3 server-side copy (no auto-chunksize; multipart ETag, warning)
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source2,
                &target2,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 1);
        assert_eq!(stats3.e_tag_verified, 0);
        assert_eq!(stats3.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "30mb_edge.bin", ETAG_30M_FILE_8M_CHUNK)
            .await;

        // Scope 4: S3 → S3 client-side with --auto-chunksize (matches source ETag, no warning)
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &source2,
                &target2,
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "30mb_edge.bin", ETAG_30M_FILE_NO_CHUNK)
            .await;

        // Scope 5: S3 → S3 server-side with --auto-chunksize (matches source ETag, no warning)
        helper.delete_all_objects(&bucket2).await;
        let stats5 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &source2,
                &target2,
            ])
            .await;

        assert_eq!(stats5.sync_complete, 1);
        assert_eq!(stats5.sync_error, 0);
        assert_eq!(stats5.sync_warning, 0);
        assert_eq!(stats5.e_tag_verified, 1);
        assert_eq!(stats5.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "30mb_edge.bin", ETAG_30M_FILE_NO_CHUNK)
            .await;

        // Scope 6: S3 → Local (no auto-chunksize; ETag mismatch → warning, SHA256 verify)
        let download_file = local_dir.join("30mb_dl_no_auto.bin");
        let stats6 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &target2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats6.sync_complete, 1);
        assert_eq!(stats6.sync_error, 0);
        assert_eq!(stats6.sync_warning, 1);
        assert_eq!(stats6.e_tag_verified, 0);
        assert_eq!(stats6.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_30M_FILE_WHOLE_HEX,
        );

        // Scope 7: S3 → Local with --auto-chunksize (ETag matches → no warning, SHA256 verify)
        let download_file2 = local_dir.join("30mb_dl_auto.bin");
        let stats7 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &target2,
                download_file2.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats7.sync_complete, 1);
        assert_eq!(stats7.sync_error, 0);
        assert_eq!(stats7.sync_warning, 0);
        assert_eq!(stats7.e_tag_verified, 1);
        assert_eq!(stats7.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file2.to_str().unwrap(),
            SHA256_30M_FILE_WHOLE_HEX,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB with no additional checksum, threshold=8MiB, chunksize=8MiB (single-part multipart).
    /// Mirrors s3sync's `test_multipart_upload_8mb` (4 scopes).
    #[tokio::test]
    async fn test_multipart_upload_8mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_8t_8c.bin", 8, 0).unwrap();

        // Scope 1: Local → S3
        let source1 = format!("s3://{}/8mb_8t_8c.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &source1,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "8mb_8t_8c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        // Scope 2: S3 → S3 client-side with --auto-chunksize
        let target2 = format!("s3://{}/8mb_8t_8c.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "8mb_8t_8c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        // Scope 3: S3 → S3 server-side copy with --auto-chunksize
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "8mb_8t_8c.bin", ETAG_8M_FILE_8M_CHUNK)
            .await;

        // Scope 4: S3 → Local with --auto-chunksize, verify SHA256
        let download_file = local_dir.join("8mb_8t_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &target2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB with SHA256 additional checksum, threshold=8MiB, chunksize=8MiB (single-part multipart).
    /// Mirrors s3sync's `test_multipart_upload_8mb_sha256` (4 scopes).
    #[tokio::test]
    async fn test_multipart_upload_8mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_sha256_8t_8c.bin", 8, 0).unwrap();

        // Scope 1: Local → S3 with SHA256 checksum
        let source1 = format!("s3://{}/8mb_sha256_8t_8c.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source1,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "8mb_sha256_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket1, "8mb_sha256_8t_8c.bin", None)
            .await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_8M_FILE_8M_CHUNK);

        // Scope 2: S3 → S3 client-side with SHA256 checksum + --auto-chunksize
        let target2 = format!("s3://{}/8mb_sha256_8t_8c.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket2,
                "8mb_sha256_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head2 = helper
            .head_object(&bucket2, "8mb_sha256_8t_8c.bin", None)
            .await;
        assert_eq!(head2.checksum_sha256().unwrap(), SHA256_8M_FILE_8M_CHUNK);

        // Scope 3: S3 → S3 server-side copy with SHA256 + --auto-chunksize
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket2,
                "8mb_sha256_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head3 = helper
            .head_object(&bucket2, "8mb_sha256_8t_8c.bin", None)
            .await;
        assert_eq!(head3.checksum_sha256().unwrap(), SHA256_8M_FILE_8M_CHUNK);

        // Scope 4: S3 → Local with --auto-chunksize + --enable-additional-checksum, verify SHA256
        let download_file = local_dir.join("8mb_sha256_8t_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &target2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 1);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB with CRC64NVME additional checksum, threshold=8MiB, chunksize=8MiB (single-part multipart).
    /// Mirrors s3sync's `test_multipart_upload_8mb_crc64nvme` (4 scopes).
    #[tokio::test]
    async fn test_multipart_upload_8mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_crc64_8t_8c.bin", 8, 0).unwrap();

        // Scope 1: Local → S3 with CRC64NVME checksum
        let source1 = format!("s3://{}/8mb_crc64_8t_8c.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source1,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "8mb_crc64_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket1, "8mb_crc64_8t_8c.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_FILE_8M_CHUNK
        );

        // Scope 2: S3 → S3 client-side with CRC64NVME checksum + --auto-chunksize
        let target2 = format!("s3://{}/8mb_crc64_8t_8c.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket2,
                "8mb_crc64_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head2 = helper
            .head_object(&bucket2, "8mb_crc64_8t_8c.bin", None)
            .await;
        assert_eq!(
            head2.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_FILE_8M_CHUNK
        );

        // Scope 3: S3 → S3 server-side copy with CRC64NVME + --auto-chunksize
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket2,
                "8mb_crc64_8t_8c.bin",
                ETAG_8M_FILE_8M_CHUNK,
            )
            .await;

        let head3 = helper
            .head_object(&bucket2, "8mb_crc64_8t_8c.bin", None)
            .await;
        assert_eq!(
            head3.checksum_crc64_nvme().unwrap(),
            CRC64NVME_8M_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local with --auto-chunksize + --enable-additional-checksum, verify SHA256
        let download_file = local_dir.join("8mb_crc64_8t_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &target2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 1);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 8 MiB uploaded as single put (no multipart chunk) via high threshold=50MiB.
    /// Mirrors s3sync's `test_multipart_upload_8mb_no_chunk` (4 scopes).
    #[tokio::test]
    async fn test_multipart_upload_8mb_no_chunk() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "8mb_no_chunk.bin", 8, 0).unwrap();

        // Scope 1: Local → S3 — high threshold forces single put
        let source1 = format!("s3://{}/8mb_no_chunk.bin", bucket1);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "50MiB",
                "--multipart-chunksize",
                "50MiB",
                test_file.to_str().unwrap(),
                &source1,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "8mb_no_chunk.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        // Scope 2: S3 → S3 client-side with --auto-chunksize
        let target2 = format!("s3://{}/8mb_no_chunk.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "8mb_no_chunk.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        // Scope 3: S3 → S3 server-side copy with --auto-chunksize
        helper.delete_all_objects(&bucket2).await;
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &source1,
                &target2,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "8mb_no_chunk.bin", ETAG_8M_FILE_NO_CHUNK)
            .await;

        // Scope 4: S3 → Local with --auto-chunksize, verify SHA256
        let download_file = local_dir.join("8mb_no_chunk_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &target2,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_8M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
