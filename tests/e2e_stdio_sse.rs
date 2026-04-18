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
                    "s3sync-e2e-test",
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
                    "s3sync-e2e-test",
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
                    "s3sync-e2e-test",
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
                    "s3sync-e2e-test",
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
                "s3sync-e2e-test",
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
                "s3sync-e2e-test",
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
                "s3sync-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0); // KMS ETag not MD5-based
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
                "s3sync-e2e-test",
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
                "s3sync-e2e-test",
                &s3_path,
                "-",
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
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
                "s3sync-e2e-test",
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
                "s3sync-e2e-test",
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
}
