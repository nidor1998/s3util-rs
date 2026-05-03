#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::ServerSideEncryption;
    use common::*;

    use super::*;

    // ---------------------------------------------------------------
    // stdin → S3 — all SSE modes
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn stdin_to_s3_sse_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--sse",
                    "AES256",
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

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::Aes256
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--sse",
                    "aws:kms",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--sse",
                    "aws:kms:dsse",
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "sse.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    #[tokio::test]
    async fn stdin_to_s3_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let stdin_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let target = format!("s3://{}/sse.dat", bucket);

        let stats = helper
            .cp_test_data_stdin_to_s3(
                vec![
                    "s3util",
                    "cp",
                    "--target-profile",
                    "s3util-e2e-test",
                    "--target-sse-c",
                    "AES256",
                    "--target-sse-c-key",
                    TEST_SSE_C_KEY_1,
                    "--target-sse-c-key-md5",
                    TEST_SSE_C_KEY_1_MD5,
                    "-",
                    &target,
                ],
                stdin_bytes,
            )
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        // SSE-C HeadObject requires the customer key; rather than round-trip,
        // just verify upload succeeded (object exists with SSE-C encryption).
        assert!(helper.is_object_exist(&bucket, "sse.dat", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
    }

    // ---------------------------------------------------------------
    // S3 → stdout — all SSE modes
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn s3_to_stdout_sse_s3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "AES256",
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
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
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
        assert_eq!(stats.e_tag_verified, 0); // KMS ETag not MD5-based
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms:dsse",
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
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_stdout_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Additional SSE-C coverage for s3 → stdout (the existing
    // `s3_to_stdout_sse_c` above covers only singlepart + no
    // additional checksum). The tests below pin:
    //
    //   * Multipart sources (download via the parallel scaffold —
    //     each ranged GET in `ranged_get_into_buffer` must include
    //     the SSE-C customer key, MD5, and algorithm).
    //   * SSE-C + --enable-additional-checksum (verify_e_tag SKIPS
    //     ETag verification when SSE-C is set; additional-checksum
    //     verification is independent and still fires).
    //   * Negative paths: wrong key → S3 rejects → transfer errors;
    //     missing key → S3 rejects.
    // ---------------------------------------------------------------

    /// Multipart SSE-C source download via stdout. 12 MiB > default 8
    /// MiB threshold ⇒ parallel scaffold runs (2 workers, 2 ranged
    /// GETs). Each ranged GET in `ranged_get_into_buffer` must
    /// include `source_sse_c`, `source_sse_c_key`, and
    /// `source_sse_c_key_md5`; if any of them were dropped, S3
    /// would reject with InvalidRequest. Bytes round-trip; ETag
    /// verification is skipped (SSE-C path in `verify_e_tag`).
    #[tokio::test]
    async fn s3_to_stdout_sse_c_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse_multi.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 0,
            "verify_e_tag returns None for any SSE-C source"
        );
        assert_eq!(stats.checksum_verified, 0);
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// SSE-C source uploaded with `--additional-checksum-algorithm
    /// SHA256`; download with SSE-C + `--enable-additional-checksum`.
    /// Per-part composite SHA256 verification fires through the
    /// parallel scaffold (each ranged GET includes the SSE-C key
    /// and the source's per-part SHA256 surfaces via
    /// GetObjectAttributes / per-part HEAD). ETag is skipped (SSE-C);
    /// additional-checksum verification is independent and verifies.
    #[tokio::test]
    async fn s3_to_stdout_sse_c_multipart_with_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(12 * 1024 * 1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse_sha.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
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
            "verify_e_tag short-circuits for SSE-C even with multipart source"
        );
        assert_eq!(
            stats.checksum_verified, 1,
            "additional-checksum verification is independent of SSE-C"
        );
        assert_eq!(stdout_bytes, src_bytes);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Negative path: source uploaded with KEY_1, download attempts
    /// with KEY_2. S3 rejects the GET with InvalidRequest /
    /// AccessDenied (key mismatch). Transfer must surface this as
    /// `sync_error > 0`; no bytes are flushed (or the stream errors
    /// before producing output).
    #[tokio::test]
    async fn s3_to_stdout_sse_c_wrong_key_returns_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse_wrong.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, _stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_2,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_2_MD5,
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(
            stats.sync_complete, 0,
            "wrong key must NOT count as success"
        );
        assert!(
            stats.sync_error > 0,
            "wrong SSE-C key must surface as sync_error"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Negative path: source uploaded with SSE-C, download attempts
    /// without any `--source-sse-c*` flags. S3 rejects the GET
    /// because the object requires a customer-provided key. Pin
    /// that this surfaces as `sync_error`.
    #[tokio::test]
    async fn s3_to_stdout_sse_c_missing_key_returns_error() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let src_bytes = TestHelper::generate_random_bytes(1024).unwrap();
        let upload_file = local_dir.join("upload.dat");
        std::fs::write(&upload_file, &src_bytes).unwrap();

        let s3_path = format!("s3://{}/sse_missing.dat", bucket);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;

        let (stats, _stdout_bytes) = helper
            .cp_test_data_s3_to_stdout(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert!(
            stats.sync_error > 0,
            "missing SSE-C key on a customer-key-encrypted object must error"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
