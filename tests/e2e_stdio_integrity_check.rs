#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::ChecksumType;
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
                    "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);

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
                    "s3util-e2e-test",
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

        let head = helper.head_object(&bucket, "size.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "16 MiB / 8 MiB → 2 parts expected, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // stdin → S3 — streaming-MPU path (post-threshold) coverage
    //
    // These pin the streaming dispatch added in commits 6376443/e9272c1/17360f2/22af3ae:
    // probe_up_to reads up to multipart_threshold; if the reader hits EOF first the
    // buffered path is used, otherwise the chained reader is streamed via
    // put_object_stream → upload_parts_stream. Existing tests above mostly verify the
    // S3 side ETag/checksum exists; these additionally verify byte-level content
    // equivalence and exercise the boundary/edge cases of the dispatch logic.
    // ---------------------------------------------------------------

    /// Streaming MPU path roundtrip: pipe known random bytes (>threshold), then
    /// download via SDK and compare SHA256 of the raw bytes. Pins the actual
    /// content equivalence end-to-end, not just "checksum field is set".
    #[tokio::test]
    async fn stdin_to_s3_streaming_roundtrip_sha256_verifies_content() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // 12 MiB > default 8 MiB threshold → exercises transfer_streaming.
        let stdin_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let expected_sha256 = TestHelper::get_sha256_from_bytes(&stdin_bytes);
        let target = format!("s3://{}/streamed.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper.get_object_bytes(&bucket, "streamed.dat", None).await;
        assert_eq!(downloaded.len(), 12 * 1024 * 1024);
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&downloaded),
            expected_sha256,
            "streaming MPU roundtrip must reproduce source bytes exactly"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Streaming MPU with > 2 parts and roundtrip verification. Catches part-ordering
    /// or buffer-flush bugs in upload_parts_stream that 2-part tests would miss.
    #[tokio::test]
    async fn stdin_to_s3_streaming_roundtrip_four_parts_verifies_content() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // 32 MiB / 8 MiB chunks = 4 parts (default threshold/chunksize).
        let stdin_bytes = TestHelper::generate_random_bytes(32 * 1024 * 1024).unwrap();
        let expected_sha256 = TestHelper::get_sha256_from_bytes(&stdin_bytes);
        let target = format!("s3://{}/streamed.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
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

        let head = helper.head_object(&bucket, "streamed.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-4"),
            "32 MiB / 8 MiB → 4 parts expected, got: {etag}"
        );

        let downloaded = helper.get_object_bytes(&bucket, "streamed.dat", None).await;
        assert_eq!(downloaded.len(), 32 * 1024 * 1024);
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&downloaded),
            expected_sha256,
            "4-part streaming roundtrip must reproduce source bytes exactly"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Boundary: stdin length == multipart_threshold. The dispatch uses a strict `<`
    /// check, so length-equal-threshold takes the streaming path. Pin this so a future
    /// switch to `<=` wouldn't go undetected.
    #[tokio::test]
    async fn stdin_to_s3_threshold_exact_takes_streaming_path() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Exactly 5 MiB with --multipart-threshold=5MiB → streaming path expected.
        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024).unwrap();
        let expected_sha256 = TestHelper::get_sha256_from_bytes(&stdin_bytes);
        let target = format!("s3://{}/threshold_exact.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);

        let head = helper
            .head_object(&bucket, "threshold_exact.dat", None)
            .await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-1"),
            "exactly-at-threshold should take streaming/multipart path with 1 part, got: {etag}"
        );

        let downloaded = helper
            .get_object_bytes(&bucket, "threshold_exact.dat", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&downloaded),
            expected_sha256
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Byte-exact boundary: stdin length == multipart_threshold - 1 byte → single-part.
    #[tokio::test]
    async fn stdin_to_s3_threshold_minus_one_is_single_part() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024 - 1).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            !etag.contains('-'),
            "5 MiB - 1 byte < 5 MiB threshold → single-part ETag, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Byte-exact boundary: stdin length == multipart_threshold → multipart
    /// (matches local_to_s3 behavior; dispatch uses strict `<`).
    #[tokio::test]
    async fn stdin_to_s3_threshold_equal_is_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-1"),
            "5 MiB == 5 MiB threshold → multipart with 1 part, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Byte-exact boundary: stdin length == multipart_threshold + 1 byte → multipart (2 parts).
    #[tokio::test]
    async fn stdin_to_s3_threshold_plus_one_is_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024 + 1).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        let etag = head.e_tag().unwrap();
        assert!(
            etag.contains("-2"),
            "5 MiB + 1 byte > 5 MiB threshold → multipart with 2 parts, got: {etag}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Empty stdin: 0 bytes piped in. Falls through the buffered path
    /// (probe_up_to returns empty < threshold) and must produce a zero-byte object.
    #[tokio::test]
    async fn stdin_to_s3_empty_input_creates_zero_byte_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let target = format!("s3://{}/empty.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "-",
                    &target,
                ],
                Vec::new(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "empty.dat", None).await;
        assert_eq!(head.content_length().unwrap(), 0);
        let etag = head.e_tag().unwrap();
        assert!(
            !etag.contains('-'),
            "0-byte stdin should produce single-part ETag, got: {etag}"
        );

        let downloaded = helper.get_object_bytes(&bucket, "empty.dat", None).await;
        assert!(downloaded.is_empty());

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Streaming MPU + --full-object-checksum: exercises the full-object CRC64NVME
    /// path through upload_parts_stream and verifies content equivalence. CRC64NVME
    /// is always full-object regardless of the flag, but combining it with streaming
    /// + content verification covers the streaming checksum-finalize path.
    #[tokio::test]
    async fn stdin_to_s3_streaming_full_object_crc64nvme_verifies_content() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // 16 MiB at default 8 MiB threshold/chunksize → 2-part streaming MPU.
        let stdin_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let expected_sha256 = TestHelper::get_sha256_from_bytes(&stdin_bytes);
        let target = format!("s3://{}/streamed_crc64.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--additional-checksum-algorithm",
                    "CRC64NVME",
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

        let head = helper
            .head_object(&bucket, "streamed_crc64.dat", None)
            .await;
        assert!(head.checksum_crc64_nvme().is_some());
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let downloaded = helper
            .get_object_bytes(&bucket, "streamed_crc64.dat", None)
            .await;
        assert_eq!(downloaded.len(), 16 * 1024 * 1024);
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&downloaded),
            expected_sha256,
            "streaming MPU + full-object CRC64NVME must reproduce source bytes"
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Now download via stdout with --enable-additional-checksum.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 0);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 0);

        // Download must use matching chunksize — s3_to_stdio doesn't support
        // --auto-chunksize. Without matching, ETag computed locally won't match
        // the source's 4-part composite ETag.
        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.checksum_verified, 0);
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
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 0);

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                small_file.to_str().unwrap(),
                &small_path,
            ])
            .await;

        let (stats_small, out_small) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &small_path,
                "-",
            ])
            .await;
        assert_eq!(stats_small.sync_complete, 1);
        assert_eq!(stats_small.e_tag_verified, 1);
        assert_eq!(stats_small.checksum_verified, 0);
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
                "s3util-e2e-test",
                big_file.to_str().unwrap(),
                &big_path,
            ])
            .await;

        let (stats_big, out_big) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &big_path,
                "-",
            ])
            .await;
        assert_eq!(stats_big.sync_complete, 1);
        assert_eq!(stats_big.e_tag_verified, 1);
        assert_eq!(stats_big.checksum_verified, 0);
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
                    "s3util-e2e-test",
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
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Upload with 5MiB chunks + SHA256; pipe to stdout at default chunksize.
    /// The source stores a composite (`-N`-suffixed) SHA256 computed over 5MiB
    /// parts, but the local recompute uses default 8MiB parts — the composite
    /// values disagree. Composite mismatches must warn, not error. This pins
    /// the else-branch of the full_object_checksum check in s3_to_stdio.rs.
    #[tokio::test]
    async fn s3_to_stdout_composite_sha256_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("mm.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/mm.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--disable-etag-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        // A sync_warning stat is not enough — the production binary reads the
        // has_warning atomic to pick ExitStatus::Warning (exit 3). Without this
        // assertion, a transfer path that emits SyncWarning but forgets to flip
        // the atomic would exit 0 and the test wouldn't notice.
        assert!(stats.has_warning_flag);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Tests below pin the parallel s3→stdout download path's behavior
    // around the dispatcher gate. The dispatcher in s3_to_stdio.rs
    // forwards to transfer_serial when:
    //   - max_parallel_uploads <= 1, OR
    //   - source_size < multipart_threshold AND auto_chunksize is unset
    // Otherwise it forwards to transfer_parallel, which runs ranged
    // GETs concurrently and computes per-part hashes that match the
    // source's part layout. auto_chunksize MUST take the parallel
    // path regardless of size — only that path calls
    // head_object_first_part to align chunk boundaries with the
    // source's actual part sizes. transfer_serial uses
    // multipart_chunksize directly for hash chunking, which produces
    // a wrong ETag/checksum for sources with non-uniform parts.
    // ---------------------------------------------------------------

    /// Generate `size` bytes of dynamic pseudo-random data using
    /// SplitMix64. Unlike `TestHelper::generate_random_bytes` (which
    /// repeats a 1 KiB seed), every byte here comes from a fresh PRNG
    /// step — no observable repeating period — so multipart hashes
    /// computed over different chunk boundaries actually differ. This
    /// matters for the 200 MiB stress test where a body with a 1 KiB
    /// period would let chunk-boundary regressions hide behind
    /// accidentally-equal sub-block hashes. Deterministic given `seed`
    /// so any locally-computed expected value stays stable across runs.
    fn dynamic_random_bytes(size: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(size);
        let mut state: u64 = seed;
        while out.len() < size {
            state = state.wrapping_add(0x9E3779B97F4A7C15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
            z ^= z >> 31;
            let bytes = z.to_le_bytes();
            let take = bytes.len().min(size - out.len());
            out.extend_from_slice(&bytes[..take]);
        }
        out
    }

    /// auto_chunksize takes the parallel path and aligns chunking with
    /// the source's actual 5 MiB parts; the per-part MD5s match the
    /// source's 4-part composite ETag. Without auto_chunksize the
    /// download would compute MD5s over default 8 MiB chunks and
    /// trigger an ETag mismatch warning — covered by
    /// `s3_to_stdout_composite_etag_mismatch_warns` above.
    #[tokio::test]
    async fn s3_to_stdout_auto_chunksize_verifies_etag_for_5mib_parts_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-4"),
            "16 MiB / 5 MiB → 4 parts expected, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(
            stats.sync_warning, 0,
            "auto-chunksize must verify cleanly against the 4-part composite ETag"
        );
        assert_eq!(stats.e_tag_verified, 1);
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Same as the previous test but for an additional checksum.
    /// auto_chunksize must align chunks so the per-part SHA256 finalize
    /// pattern reconstructs the source's composite (`-N`-suffixed)
    /// SHA256 exactly.
    #[tokio::test]
    async fn s3_to_stdout_auto_chunksize_verifies_sha256_for_5mib_parts_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(head.checksum_sha256().is_some());
        assert!(
            head.checksum_sha256().unwrap().contains("-4"),
            "16 MiB / 5 MiB → 4-part composite SHA256 expected, got: {}",
            head.checksum_sha256().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(
            stats.sync_warning, 0,
            "auto-chunksize must verify the 4-part composite SHA256 cleanly"
        );
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// REGRESSION: the dispatcher fix for auto_chunksize.
    ///
    /// Pre-fix, the dispatcher's gate `source_size < multipart_threshold`
    /// silently sent any below-default-threshold object to
    /// transfer_serial — even with --auto-chunksize. transfer_serial
    /// chunks hashes by multipart_chunksize and would compute a single
    /// MD5 over the whole 7 MiB body, mismatching the source's 2-part
    /// composite ETag. Post-fix, auto_chunksize ALWAYS takes the
    /// parallel path; head_object_first_part returns the source's 5 MiB
    /// first part, the parallel scaffold slices into 5 MiB + 2 MiB
    /// chunks, and the recomputed ETag matches.
    #[tokio::test]
    async fn s3_to_stdout_auto_chunksize_verifies_etag_when_source_below_default_threshold() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // 7 MiB body — strictly less than the default 8 MiB threshold,
        // so the pre-fix dispatcher would have routed to transfer_serial.
        let src_bytes = TestHelper::generate_random_bytes(7 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "source must be 2-part multipart (5 MiB + 2 MiB), got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(
            stats.sync_warning, 0,
            "post-fix: auto-chunksize takes the parallel path even below default threshold"
        );
        assert_eq!(stats.e_tag_verified, 1);
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Boundary: source_size = multipart_threshold - 1. Dispatcher's
    /// gate is `source_size >= multipart_threshold` (and matches
    /// `is_multipart_upload_required`'s `>=` boundary), so this lands
    /// strictly on the serial side: source uploaded as singlepart
    /// (plain MD5 ETag), download via transfer_serial (parts_count
    /// treated as 0 for the singlepart computation). Verifies cleanly.
    #[tokio::test]
    async fn s3_to_stdout_threshold_boundary_minus_one_byte_serial_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let size = 8 * 1024 * 1024 - 1;
        let src_bytes = TestHelper::generate_random_bytes(size).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            !head.e_tag().unwrap().contains('-'),
            "size = threshold - 1 < threshold ⇒ singlepart upload, got ETag: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Boundary: source_size = multipart_threshold exactly. Dispatcher
    /// takes the parallel path (`>=` boundary is inclusive), and the
    /// upload likewise uses multipart (`is_multipart_upload_required`
    /// agrees on `>=`). Single-chunk parallel: 1 worker, 1 ranged GET.
    /// Source's 1-part composite ETag (`-1` suffix) matches the
    /// recomputed `parts_count == 1` ETag. This pins that the
    /// inclusive-`>=` gates on both sides remain in sync.
    #[tokio::test]
    async fn s3_to_stdout_threshold_boundary_exact_parallel_single_chunk_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let size = 8 * 1024 * 1024;
        let src_bytes = TestHelper::generate_random_bytes(size).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-1"),
            "size == threshold ⇒ multipart upload with 1 part (-1 suffix), got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Boundary: source_size = multipart_threshold + 1. Source uploaded
    /// as 2-part multipart (8 MiB chunk + 1 byte chunk). Download takes
    /// the parallel path with 2 chunks. Computed 2-part ETag matches
    /// source's. Verifies the smallest-tail-chunk case end-to-end.
    #[tokio::test]
    async fn s3_to_stdout_threshold_boundary_plus_one_byte_parallel_two_chunks_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let size = 8 * 1024 * 1024 + 1;
        let src_bytes = TestHelper::generate_random_bytes(size).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "size == threshold + 1 ⇒ 2-part multipart, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 200 MiB stress with dynamic random data (SplitMix64, no
    /// observable repeating period). Default chunksize gives 25 parts;
    /// default --max-parallel-uploads (16) means up to 16 concurrent
    /// ranged GETs. Verifies that:
    ///   1. All 25 chunks reach stdout in source order, byte-for-byte
    ///      (the `assert_eq!` on the full 200 MiB Vec).
    ///   2. The locally-recomputed 25-part composite ETag matches the
    ///      source's (e_tag_verified == 1, sync_warning == 0).
    ///   3. The source's full SHA256 also matches.
    /// We additionally verify the SHA256 explicitly via head_object so
    /// a regression that produced *visually correct* bytes but a
    /// silently corrupted single chunk would still fail here.
    #[tokio::test]
    async fn s3_to_stdout_200mib_dynamic_random_data_full_verification() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let size = 200 * 1024 * 1024;
        let src_bytes = dynamic_random_bytes(size, /*seed*/ 0xC0FFEE_DEADBEEFu64);

        // Pre-compute the expected SHA256 over the body for a final
        // independent integrity check (separate from the streaming
        // composite SHA256 that S3 stores).
        let expected_full_sha256 = TestHelper::get_sha256_from_bytes(&src_bytes);

        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        // 200 MiB / default 8 MiB chunksize = 25 parts.
        assert!(
            head.e_tag().unwrap().contains("-25"),
            "expected 25-part composite ETag, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
        assert!(!stats.has_warning_flag);
        assert_eq!(
            stdout_bytes.len(),
            src_bytes.len(),
            "size mismatch: stdout {} bytes, source {} bytes",
            stdout_bytes.len(),
            src_bytes.len()
        );
        assert_eq!(
            stdout_bytes, src_bytes,
            "byte content mismatch in 200 MiB stream"
        );

        // Independent integrity check: SHA256 of bytes that hit stdout
        // must match the SHA256 of bytes we uploaded. A single
        // corrupted chunk reordered from the parallel scaffold would
        // pass the per-chunk MD5 (because the chunk itself is intact)
        // and could in pathological cases pass the composite ETag, but
        // would change the full-body SHA256.
        let actual_full_sha256 = TestHelper::get_sha256_from_bytes(&stdout_bytes);
        assert_eq!(actual_full_sha256, expected_full_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Sanity: with --max-parallel-uploads 1 the dispatcher must keep
    /// the existing pre-parallel behavior (no HEAD, single non-ranged
    /// GET via transfer_serial), even on a multipart source. Pin this
    /// so a future refactor doesn't accidentally route N=1 through
    /// the parallel scaffold.
    #[tokio::test]
    async fn s3_to_stdout_max_parallel_uploads_one_uses_serial_path() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--max-parallel-uploads",
                "1",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload with 5MiB chunks, then pipe to stdout at default chunksize
    /// without --enable-additional-checksum. The source stores a composite
    /// (`-N`-suffixed) ETag computed over 5MiB parts; the local recompute
    /// uses default 8MiB parts — the ETags disagree. A composite ETag
    /// mismatch from a remote source must warn (not error), and the warning
    /// must surface in the has_warning atomic so the binary exits 3.
    #[tokio::test]
    async fn s3_to_stdout_composite_etag_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("etag_mm.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/etag_mm.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        assert!(stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Verification-disable flags for s3→stdout. Three flags:
    //   --disable-etag-verify
    //       Skip the ETag verification block entirely. The dispatcher
    //       still runs the parallel scaffold; per-chunk MD5 work is
    //       skipped (the `if !config.disable_etag_verify` guard inside
    //       the drain loop). No ETagVerified/ETagMismatch events.
    //
    //   --disable-additional-checksum-verify
    //       Skip the additional-checksum verification block. With
    //       --enable-additional-checksum still set, S3 returns the
    //       checksum on HEAD/GET but the local recompute + compare is
    //       suppressed. No ChecksumVerified/ChecksumMismatch events.
    //       Doesn't affect ETag verification.
    //
    //   --disable-multipart-verify
    //       In `verify_e_tag`, sets `verify_multipart_upload = false`,
    //       which makes the function return None whenever EITHER side's
    //       ETag carries a `-N` suffix. So multipart sources skip ETag
    //       verification (None → no event); singlepart sources fall
    //       through to a normal MD5 compare (verified or mismatch).
    // ---------------------------------------------------------------

    /// `--disable-etag-verify` against a multipart source: the parallel
    /// scaffold still runs (download succeeds, bytes match), but no
    /// ETag computation or verification fires — even though the source
    /// is genuine and the ETags would have matched.
    #[tokio::test]
    async fn s3_to_stdout_disable_etag_verify_skips_etag_events_on_multipart_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "16 MiB / default 8 MiB chunksize → 2-part multipart ETag, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-etag-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "--disable-etag-verify must suppress the ETagVerified event"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// `--disable-etag-verify` against a source whose ETag would have
    /// MISMATCHED. Without the flag this would produce an ETagMismatch
    /// warning (see `s3_to_stdout_composite_etag_mismatch_warns`); with
    /// the flag, verification is short-circuited and no warning fires.
    /// Pins that the disable flag truly skips computation, not just the
    /// final compare.
    #[tokio::test]
    async fn s3_to_stdout_disable_etag_verify_silences_what_would_have_been_a_mismatch() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // Same setup as `s3_to_stdout_composite_etag_mismatch_warns`:
        // upload with 5 MiB parts, download at default 8 MiB → ETags
        // would disagree without the disable flag.
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-etag-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(
            stats.sync_warning, 0,
            "--disable-etag-verify must turn the would-be mismatch into silence"
        );
        assert_eq!(stats.e_tag_verified, 0);
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // `--disable-additional-checksum-verify` for s3 → stdout cannot
    // be exercised end-to-end through the CLI: the flag is gated on
    // `requires = "additional_checksum_algorithm"`
    // (src/config/args/common.rs:244), and
    // `--additional-checksum-algorithm` itself is rejected when the
    // target isn't S3 (common.rs:714-715). For the download
    // direction, the way to "skip additional-checksum verification"
    // is to simply not pass `--enable-additional-checksum`. The
    // underlying production gate (`!config.disable_additional_checksum_verify`
    // inside `transfer_parallel`) is exercised by the unit test
    // `parallel_disable_additional_checksum_verify_skips_compute_and_verify`
    // in src/transfer/s3_to_stdio.rs's `mod tests`, which constructs
    // a Config directly and bypasses CLI parsing.

    /// `--disable-multipart-verify` against a MULTIPART source:
    /// `verify_e_tag` returns None whenever either side's ETag has a
    /// `-N` suffix when this flag is set. So no ETagVerified/Mismatch
    /// event fires. The download succeeds; verification is silently
    /// skipped (debug log only).
    #[tokio::test]
    async fn s3_to_stdout_disable_multipart_verify_skips_etag_for_multipart_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "source must be 2-part multipart, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-multipart-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "--disable-multipart-verify must skip multipart-source ETag verification"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// `--disable-multipart-verify` against a SINGLEPART source: the
    /// flag only suppresses multipart-vs-multipart compares; singlepart
    /// ETags still fall through `verify_e_tag` to a normal MD5 compare
    /// and verify successfully. Pins that the flag's scope is narrow
    /// (multipart only), not "disable all ETag work".
    #[tokio::test]
    async fn s3_to_stdout_disable_multipart_verify_still_verifies_singlepart_source() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // 4 MiB body — below the default 8 MiB threshold → singlepart
        // upload, plain (no `-N` suffix) MD5 ETag.
        let src_bytes = TestHelper::generate_random_bytes(4 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "source.dat", None).await;
        assert!(
            !head.e_tag().unwrap().contains('-'),
            "source must be singlepart (no `-N` suffix), got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-multipart-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 1,
            "singlepart ETag verification is unaffected by --disable-multipart-verify"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Both CLI-reachable disable flags for s3 → stdout set on a
    /// multipart source. Bytes round-trip unchanged; no
    /// ETag-verification events of any kind fire. Pins that the
    /// disable flags compose without surprises.
    /// (`--disable-additional-checksum-verify` is omitted — see the
    /// comment block above for why it's not CLI-reachable for s3 → stdout.)
    #[tokio::test]
    async fn s3_to_stdout_disable_etag_and_multipart_verify_compose_silently() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/source.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-etag-verify",
                "--disable-multipart-verify",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Enhanced stdin → S3 boundary + stress tests.
    //
    // Existing `stdin_to_s3_threshold_minus_one_is_single_part`,
    // `..._equal_is_multipart`, `..._plus_one_is_multipart` cover the
    // size dispatch (which path runs for which size) but only assert
    // ETag verification (no per-part additional checksum). The tests
    // below add explicit `--additional-checksum-algorithm` coverage at
    // those same three boundary points, so the SHA256 (composite) and
    // CRC (full-object/composite) paths are exercised end-to-end at
    // the dispatch boundary — places where off-by-one in the chunking
    // or hashing pipeline would silently break verification.
    //
    // Plus a 200 MiB stress upload with dynamic random data (no
    // observable repeating period — see `dynamic_random_bytes` for
    // why patterned data would let chunk-boundary regressions hide).
    // ---------------------------------------------------------------

    /// Boundary -1: size = threshold - 1. Below threshold ⇒ singlepart
    /// upload ⇒ plain MD5 ETag, single-shot SHA256 (no `-N`).
    /// `e_tag_verified` and `checksum_verified` both fire.
    #[tokio::test]
    async fn stdin_to_s3_threshold_minus_one_with_sha256_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024 - 1).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
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

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        assert!(
            !head.e_tag().unwrap().contains('-'),
            "size = threshold - 1 ⇒ singlepart ETag, got: {}",
            head.e_tag().unwrap()
        );
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            !sha256.contains('-'),
            "size = threshold - 1 ⇒ singlepart SHA256 (no `-N`), got: {sha256}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Boundary ==: size = threshold exactly. Multipart with 1 part
    /// ⇒ ETag has `-1` suffix, SHA256 has `-1` composite suffix.
    /// Both verify.
    #[tokio::test]
    async fn stdin_to_s3_threshold_equal_with_sha256_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
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

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-1"),
            "size == threshold ⇒ multipart ETag with `-1` suffix, got: {}",
            head.e_tag().unwrap()
        );
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            sha256.contains("-1"),
            "size == threshold ⇒ composite SHA256 with `-1` suffix, got: {sha256}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Boundary +1: size = threshold + 1 byte. Multipart with 2 parts
    /// (chunksize MiB + 1 byte) ⇒ ETag `-2`, SHA256 composite `-2`.
    /// Smallest possible tail-chunk case for the hashing pipeline.
    #[tokio::test]
    async fn stdin_to_s3_threshold_plus_one_with_sha256_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024 + 1).unwrap();
        let target = format!("s3://{}/boundary.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
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

        let head = helper.head_object(&bucket, "boundary.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "size == threshold + 1 ⇒ multipart ETag with `-2` suffix, got: {}",
            head.e_tag().unwrap()
        );
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            sha256.contains("-2"),
            "size == threshold + 1 ⇒ composite SHA256 with `-2` suffix, got: {sha256}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// 200 MiB stdin → S3 stress with dynamic random data (SplitMix64
    /// — no observable repeating period). Default chunksize gives 25
    /// parts. Verifies that:
    ///   1. The streaming MPU pipeline assembles all 25 parts.
    ///   2. ETag verification fires on the upload side.
    ///   3. Composite SHA256 verification fires on the upload side.
    ///   4. The downloaded bytes match the source byte-for-byte
    ///      (independent SHA256 over the GET response).
    /// A regression that, e.g., dropped or reordered a chunk in the
    /// streaming pipeline could pass per-part hash checks (each chunk
    /// is intact) and even a server-side composite (S3 recomputes from
    /// what we sent) but would fail the independent SHA256 here.
    #[tokio::test]
    async fn stdin_to_s3_200mib_dynamic_random_data_full_verification() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let size = 200 * 1024 * 1024;
        let stdin_bytes = dynamic_random_bytes(size, /*seed*/ 0xBADC0FFEE0DDF00Du64);
        let expected_full_sha256 = TestHelper::get_sha256_from_bytes(&stdin_bytes);

        let target = format!("s3://{}/200mib.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA256",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert!(!stats.has_warning_flag);

        let head = helper.head_object(&bucket, "200mib.dat", None).await;
        // 200 MiB / default 8 MiB chunksize = 25 parts.
        assert!(
            head.e_tag().unwrap().contains("-25"),
            "expected 25-part composite ETag, got: {}",
            head.e_tag().unwrap()
        );
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            sha256.contains("-25"),
            "expected 25-part composite SHA256, got: {sha256}"
        );

        // Independent integrity check: GET the object back and verify
        // the SHA256 over the response body matches the SHA256 over
        // the bytes we uploaded. Catches any silent reordering or
        // chunk loss in the streaming MPU pipeline that wouldn't be
        // visible from S3's own composite hash (which is computed
        // from what we sent, regardless of source order).
        let downloaded = helper.get_object_bytes(&bucket, "200mib.dat", None).await;
        assert_eq!(
            downloaded.len(),
            stdin_bytes.len(),
            "size mismatch: uploaded {} bytes, downloaded {} bytes",
            stdin_bytes.len(),
            downloaded.len()
        );
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&downloaded),
            expected_full_sha256,
            "200 MiB streaming MPU roundtrip must reproduce source bytes exactly"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Boundary +1 with `--full-object-checksum CRC32`. The full-object
    /// regime stores a single rolling CRC32 over the whole body (no
    /// `-N` suffix) instead of a composite — exercises a different
    /// finalize path in the upload pipeline at the multipart boundary.
    #[tokio::test]
    async fn stdin_to_s3_threshold_plus_one_with_full_object_crc32_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(5 * 1024 * 1024 + 1).unwrap();
        let target = format!("s3://{}/boundary_foc.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
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

        let head = helper.head_object(&bucket, "boundary_foc.dat", None).await;
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
        let crc = head.checksum_crc32().unwrap();
        assert!(
            !crc.contains('-'),
            "full-object CRC32 has no `-N` suffix even on multipart, got: {crc}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    /// Tiny stdin (1 byte) with --additional-checksum-algorithm. Pins
    /// the smallest-input edge case for the singlepart-checksum path
    /// — a degenerate input that exercises the same code as larger
    /// singleparts but with a 1-byte read window.
    #[tokio::test]
    async fn stdin_to_s3_one_byte_with_sha256_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let target = format!("s3://{}/tiny.dat", bucket);
        let stdin_bytes = vec![0x42u8];

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--additional-checksum-algorithm",
                    "SHA256",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "tiny.dat", None).await;
        assert_eq!(head.content_length(), Some(1));
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            !sha256.contains('-'),
            "1-byte input ⇒ singlepart upload ⇒ no `-N` suffix on SHA256, got: {sha256}"
        );

        let downloaded = helper.get_object_bytes(&bucket, "tiny.dat", None).await;
        assert_eq!(downloaded, stdin_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // Directory-bucket (S3 Express One Zone) coverage for the stdio
    // paths.
    //
    // Express One Zone semantics that matter here:
    //   * ETag verification is SKIPPED (`is_express_onezone_storage()`
    //     returns true → both `transfer_serial` and `finalize_parallel`
    //     short-circuit the ETag block). All Express tests assert
    //     `e_tag_verified == 0` and no ETag-mismatch warning.
    //   * S3 Express auto-stores a CRC64NVME checksum on every upload
    //     (singlepart and per-part on multipart) without the user
    //     passing `--additional-checksum-algorithm`. So:
    //       - For uploads, `checksum_verified == 1` even with no
    //         checksum flag; the upload manager picks up the
    //         server-computed CRC64NVME and compares.
    //       - For downloads, the source has a stored CRC64NVME; with
    //         `--enable-additional-checksum` set, the parallel
    //         scaffold's `detect_additional_checksum_with_head_object`
    //         picks it up and verifies on read.
    //
    // Bucket name format is `s3e2e-{uuid}--apne1-az4--x-s3`
    // (the `--{az}--x-s3` suffix is required by S3 Express's bucket
    // naming convention).
    // ---------------------------------------------------------------

    const EXPRESS_BUCKET_SUFFIX: &str = "--apne1-az4--x-s3";

    fn directory_bucket_name() -> String {
        format!("s3e2e-{}{}", uuid::Uuid::new_v4(), EXPRESS_BUCKET_SUFFIX)
    }

    /// stdin → directory bucket, singlepart (1 KiB body, well below
    /// the default 8 MiB threshold). Express auto-adds a CRC64NVME
    /// even with no `--additional-checksum-algorithm`, so the upload
    /// manager verifies it post-upload. ETag is unconditionally
    /// skipped on Express.
    #[tokio::test]
    async fn stdin_to_directory_bucket_singlepart_auto_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/single.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "Express One Zone unconditionally skips ETag verification"
        );
        assert_eq!(
            stats.checksum_verified, 1,
            "Express auto-adds CRC64NVME on upload; verification fires on the upload side"
        );
        assert!(!stats.has_warning_flag);

        // Round-trip the bytes back via GetObject for an independent
        // integrity check.
        let downloaded = helper.get_object_bytes(&bucket, "single.dat", None).await;
        assert_eq!(downloaded, stdin_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// stdin → directory bucket, multipart (9 MiB body crosses the
    /// default 8 MiB threshold → 2-part streaming MPU). Express
    /// auto-adds CRC64NVME per part; the upload manager combines them
    /// into the composite that `checksum_verified` keys on.
    #[tokio::test]
    async fn stdin_to_directory_bucket_multipart_auto_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let stdin_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/multi.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "-",
                    &target,
                ],
                stdin_bytes.clone(),
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert!(!stats.has_warning_flag);

        let head = helper.head_object(&bucket, "multi.dat", None).await;
        // The Express ETag for multipart still carries the `-N` suffix
        // even though we don't verify it; pin the upload was actually
        // multipart and not silently coalesced into singlepart.
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "9 MiB / 8 MiB → 2 parts expected on Express, got: {}",
            head.e_tag().unwrap()
        );

        let downloaded = helper.get_object_bytes(&bucket, "multi.dat", None).await;
        assert_eq!(downloaded, stdin_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
    }

    /// directory bucket → stdout, singlepart source (1 KiB). The
    /// dispatcher takes the serial path (size < threshold). With
    /// `--enable-additional-checksum`, the parallel scaffold's
    /// detection picks up the source's auto-stored CRC64NVME and
    /// verifies on read. ETag is skipped on Express.
    #[tokio::test]
    async fn directory_bucket_to_stdout_singlepart_verifies_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/single.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "Express One Zone source ⇒ ETag verification skipped"
        );
        assert_eq!(
            stats.checksum_verified, 1,
            "auto-stored CRC64NVME picked up via --enable-additional-checksum"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// directory bucket → stdout, multipart source (12 MiB > default
    /// 8 MiB threshold ⇒ parallel scaffold runs, splits into 2
    /// chunks). With `--enable-additional-checksum`, the source's
    /// composite CRC64NVME is detected from HEAD, the parallel scaffold
    /// computes per-part CRC64NVME, and `finalize_all` reassembles a
    /// composite that matches the source. ETag still skipped.
    #[tokio::test]
    async fn directory_bucket_to_stdout_multipart_verifies_composite_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/multi.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "multi.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "12 MiB / default 8 MiB → 2-part Express multipart, got: {}",
            head.e_tag().unwrap()
        );
        assert!(
            head.checksum_crc64_nvme().is_some(),
            "Express auto-stores per-part CRC64NVME on multipart"
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "Express One Zone source ⇒ ETag verification skipped on the parallel path too"
        );
        assert_eq!(
            stats.checksum_verified, 1,
            "composite CRC64NVME reassembled from per-part hashes must match source"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// directory bucket → stdout WITHOUT `--enable-additional-checksum`:
    /// the CLI auto-promotes `checksum_mode = Some(Enabled)` for any
    /// Express One Zone source unless
    /// `--disable-express-one-zone-additional-checksum` is set
    /// (src/config/args/common.rs:1024-1030). So even the bare-flags
    /// case for an Express source fires composite CRC64NVME
    /// verification. Pins this auto-promotion: the user gets
    /// checksum protection on Express downloads by default without
    /// having to remember `--enable-additional-checksum`. ETag is
    /// still skipped (Express).
    #[tokio::test]
    async fn directory_bucket_to_stdout_multipart_auto_promotes_checksum_mode() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/multi.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(
            stats.checksum_verified, 1,
            "Express source ⇒ CLI auto-enables checksum_mode ⇒ verification fires \
             even without --enable-additional-checksum"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// directory bucket → stdout with
    /// `--disable-express-one-zone-additional-checksum`: opts out of
    /// the auto-promotion above. With both ETag verification (skipped
    /// on Express) and additional-checksum verification (suppressed
    /// by the flag) off, the download is the truly silent baseline:
    /// bytes round-trip but no verification events of any kind fire.
    #[tokio::test]
    async fn directory_bucket_to_stdout_multipart_with_disable_express_checksum_is_silent() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = directory_bucket_name();
        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/multi.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-express-one-zone-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(
            stats.checksum_verified, 0,
            "--disable-express-one-zone-additional-checksum overrides the auto-promotion"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Chunksize boundary ==: size = N * chunksize exactly (N=2 here).
    /// Pin that an exact-multiple of chunksize produces exactly N parts
    /// (not N+1 with a 0-byte tail), and that ETag + checksum verify.
    /// A boundary-condition bug that emitted a stray empty trailing
    /// part would either produce `-3` here or fail S3's part-must-be
    /// non-empty constraint.
    #[tokio::test]
    async fn stdin_to_s3_size_equals_two_chunks_exact_with_sha256() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // 10 MiB == 2 * 5 MiB chunksize.
        let stdin_bytes = TestHelper::generate_random_bytes(10 * 1024 * 1024).unwrap();
        let target = format!("s3://{}/exact_chunks.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--multipart-threshold",
                    "5MiB",
                    "--multipart-chunksize",
                    "5MiB",
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

        let head = helper.head_object(&bucket, "exact_chunks.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "10 MiB == 2 * 5 MiB ⇒ exactly 2 parts (no stray 3rd), got: {}",
            head.e_tag().unwrap()
        );
        let sha256 = head.checksum_sha256().unwrap();
        assert!(
            sha256.contains("-2"),
            "expected 2-part composite SHA256, got: {sha256}"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // s3 → stdout — `--full-object-checksum` source coverage.
    //
    // `--full-object-checksum` is REJECTED for s3 → stdout at the CLI
    // layer (target must be s3://, see TARGET_LOCAL_STORAGE_SPECIFIED_WITH_FULL_OBJECT_CHECKSUM
    // in src/config/args/common.rs). It can only be set on the upload
    // side. So these tests upload a source object WITH
    // `--full-object-checksum --additional-checksum-algorithm <CRC>`
    // — the source is multipart but its stored additional checksum
    // is a single full-object hash (no `-N` suffix on the checksum,
    // even though the ETag still has `-N` for the multipart upload).
    //
    // On the download side, the parallel scaffold's
    // `is_full_object_checksum(&source_final_checksum)` detection
    // automatically picks the full-object verification path:
    // `checksum_is_multipart == false` ⇒ no per-chunk
    // `AdditionalChecksum::finalize` calls, single `finalize()` at
    // the end ⇒ hash of all chunks concatenated == full-object hash
    // of the body. Crucially, this works WITHOUT the user passing
    // `--full-object-checksum` on the download (which they couldn't
    // anyway). Only `--enable-additional-checksum` is needed.
    //
    // Only CRC32/CRC32C/CRC64NVME support `--full-object-checksum`
    // (FULL_OBJECT_CHECKSUM_NOT_SUPPORTED — SHA1/SHA256 are
    // composite-only on multipart).
    // ---------------------------------------------------------------

    /// Multipart source uploaded with `--full-object-checksum
    /// --additional-checksum-algorithm CRC32`. The source's stored
    /// CRC32 is a single full-object hash (no `-N`). Download via
    /// stdout with `--enable-additional-checksum` triggers the
    /// parallel scaffold; auto-detection picks the full-object
    /// verification path and the recomputed CRC32 over the byte
    /// stream matches.
    #[tokio::test]
    async fn s3_to_stdout_full_object_crc32_multipart_source_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // 9 MiB > default 8 MiB threshold ⇒ 2-part multipart source.
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/foc.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "foc.dat", None).await;
        assert!(
            head.e_tag().unwrap().contains("-2"),
            "9 MiB / default 8 MiB ⇒ 2-part multipart (ETag has `-N` suffix), got: {}",
            head.e_tag().unwrap()
        );
        let crc32 = head.checksum_crc32().unwrap();
        assert!(
            !crc32.contains('-'),
            "--full-object-checksum ⇒ CRC32 has NO `-N` suffix even on multipart, got: {crc32}"
        );
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(
            stats.checksum_verified, 1,
            "auto-detected full-object CRC32 must verify across chunks"
        );
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Same as the previous test but with CRC64NVME. CRC64NVME has a
    /// different rolling-CRC implementation (separate `Checksum` impl)
    /// — pin that the full-object-detection auto-routing works for it
    /// too, not just CRC32.
    #[tokio::test]
    async fn s3_to_stdout_full_object_crc64nvme_multipart_source_verifies() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(9 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/foc64.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--full-object-checksum",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "foc64.dat", None).await;
        let crc64 = head.checksum_crc64_nvme().unwrap();
        assert!(
            !crc64.contains('-'),
            "--full-object-checksum ⇒ CRC64NVME has no `-N` suffix, got: {crc64}"
        );
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
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
        assert!(!stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Cross-chunksize verification: source uploaded with 5 MiB chunks
    /// + full-object CRC32; download with default 8 MiB chunks. The
    /// composite ETag is chunksize-dependent and will mismatch
    /// (warning), but the full-object CRC32 is by definition
    /// independent of chunking and must still verify cleanly. Pins
    /// the property that's the whole reason `--full-object-checksum`
    /// exists.
    #[tokio::test]
    async fn s3_to_stdout_full_object_crc32_verifies_across_different_download_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(16 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/foc_xchunk.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let head = helper.head_object(&bucket, "foc_xchunk.dat", None).await;
        // Source ETag has `-4` (16 MiB / 5 MiB ⇒ 4 parts). Download at
        // default 8 MiB will compute `-2` ⇒ ETag mismatch (warning).
        assert!(
            head.e_tag().unwrap().contains("-4"),
            "expected 4-part source ETag, got: {}",
            head.e_tag().unwrap()
        );

        let (stats, stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        // ETag mismatches because of the chunksize disagreement
        // (composite is chunking-dependent). Composite ETag mismatch
        // from a remote source is a warning, not an error.
        assert_eq!(
            stats.sync_warning, 1,
            "ETag mismatch (composite chunking disagreement) expected as warning"
        );
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(
            stats.checksum_verified, 1,
            "full-object CRC32 is independent of chunking and must verify regardless"
        );
        assert!(stats.has_warning_flag);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
