#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    // ---------------------------------------------------------------
    // stdin → S3 — additional-checksum tests (5 algorithms × single/multipart)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_checksum_sha256_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA256",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_sha256().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_sha256_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA256",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_sha256().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc32_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC32",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc32().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc32_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC32",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc32().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc32c_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC32C",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc32_c().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc32c_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC32C",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc32_c().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc64nvme_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC64NVME",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_crc64nvme_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC64NVME",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_sha1_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA1",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_sha1().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_checksum_sha1_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/checksum.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA1",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum.dat", None).await;
        assert!(head.checksum_sha1().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // stdin → S3 — ETag verification (automatic, no SSE, no checksum)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_e_tag_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/etag.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "etag.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            !etag.contains('-'),
            "single-part ETag should not contain '-N' suffix, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_e_tag_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/etag.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "etag.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "multipart ETag should contain '-2' for 2 parts, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // stdin → S3 — chunk-size / multipart-threshold behavior
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_threshold_below_default() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(7 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/size.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let head = helper.head_object(&bucket, "size.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            !etag.contains('-'),
            "7 MiB < 8 MiB threshold → single-part ETag, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_threshold_above_default() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/size.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let head = helper.head_object(&bucket, "size.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "9 MiB > 8 MiB threshold → 2-part multipart, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_chunk_size_5mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/size.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let head = helper.head_object(&bucket, "size.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-4"),
            "16 MiB / 5 MiB → 4 parts expected, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_chunk_size_8mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/size.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let head = helper.head_object(&bucket, "size.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "16 MiB / 8 MiB → 2 parts expected, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // S3 → stdout — additional-checksum tests (5 algorithms × single/multipart source)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_stdout_checksum_sha256_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload source object with the checksum algorithm so it has a stored checksum.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_sha256_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc32_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload source object with the checksum algorithm so it has a stored checksum.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc32_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc32c_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload source object with the checksum algorithm so it has a stored checksum.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc32c_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc64nvme_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload source object with the checksum algorithm so it has a stored checksum.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_crc64nvme_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_sha1_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload source object with the checksum algorithm so it has a stored checksum.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_checksum_sha1_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/download.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // S3 → stdout — ETag verification (automatic)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_stdout_e_tag_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        helper
            .put_object(&bucket, "etag.dat", src_bytes.clone())
            .await;
        let s3_path = format!("s3://{}/etag.dat", bucket);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn s3_to_stdout_e_tag_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload a multipart object via local→S3 so the source has an -N ETag.
        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/etag.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // S3 → stdout — source multipart layout variations
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_stdout_source_multipart_5mb_parts() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
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
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Download must use matching chunksize — s3_to_stdio doesn't support
        // --auto-chunksize. Without matching, ETag computed locally won't match
        // the source's 4-part composite ETag.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        let head = helper.head_object(&bucket, "source.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-4"),
            "16 MiB / 5 MiB → 4 parts expected, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_source_multipart_8mb_parts() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        let head = helper.head_object(&bucket, "source.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "16 MiB / 8 MiB → 2 parts expected, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_source_threshold_boundary() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();

        // 7 MiB — below default 8 MiB threshold → single-part source
        let small_bytes = TestHelper::generate_random_bytes(7 * 1024 * 1024).unwrap();
        let small_file = local_dir.join("small.dat");
        std::fs::write(&small_file, &small_bytes).unwrap();
        let small_path = format!("s3://{}/small.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                small_file.to_str().unwrap(),
                &small_path,
            ])
            .await;

        let (stats_small, out_small) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &small_path,
                "-",
            ])
            .await;
        assert_eq!(stats_small.sync_complete, 1);
        assert_eq!(stats_small.e_tag_verified, 1);
        assert_eq!(out_small, small_bytes);
        let head_small = helper.head_object(&bucket, "small.dat", None).await;
        assert!(
            !head_small.e_tag().unwrap().contains('-'),
            "7 MiB source → single-part ETag"
        );

        // 9 MiB — above default 8 MiB threshold → 2-part multipart source
        let big_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let big_file = local_dir.join("big.dat");
        std::fs::write(&big_file, &big_bytes).unwrap();
        let big_path = format!("s3://{}/big.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                big_file.to_str().unwrap(),
                &big_path,
            ])
            .await;

        let (stats_big, out_big) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &big_path,
                "-",
            ])
            .await;
        assert_eq!(stats_big.sync_complete, 1);
        assert_eq!(stats_big.e_tag_verified, 1);
        assert_eq!(out_big, big_bytes);
        let head_big = helper.head_object(&bucket, "big.dat", None).await;
        assert!(
            head_big.e_tag().unwrap().contains("-2"),
            "9 MiB source → 2-part multipart ETag"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // stdin → S3 — --full-object-checksum with CRC multipart
    //
    // Exercises the CRC full-object variant of `compute_source_checksum`
    // where `finalize_all()` returns the rolling full-object CRC rather
    // than the composite multipart format.
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_full_object_checksum_crc32_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/foc32_mp.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3sync-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC32",
                    "--full-object-checksum",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "foc32_mp.dat", None).await;
        assert!(head.checksum_crc32().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
    }
}
