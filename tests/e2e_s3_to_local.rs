#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    #[tokio::test]
    async fn s3_to_local_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an object first
        let test_content = b"hello from s3";
        helper
            .put_object(&bucket, "download.txt", test_content.to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("download.txt");

        let source = format!("s3://{}/download.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        // Verify downloaded content
        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content, test_content);
        TestHelper::verify_downloaded_file_md5(local_file.to_str().unwrap(), b"hello from s3");

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_etag_verified() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(&bucket, "etag_test.dat", vec![0u8; 1024])
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("etag_test.dat");

        let source = format!("s3://{}/etag_test.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);

        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content.len(), 1024);
        TestHelper::verify_downloaded_file_md5(local_file.to_str().unwrap(), &vec![0u8; 1024]);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_disable_etag_verify() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(&bucket, "no_etag.dat", vec![1u8; 512])
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("no_etag.dat");

        let source = format!("s3://{}/no_etag.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);

        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content.len(), 512);
        TestHelper::verify_downloaded_file_md5(local_file.to_str().unwrap(), &vec![1u8; 512]);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_large_file() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload a file larger than multipart threshold
        helper
            .put_sized_object(&bucket, "large_download.bin", 9 * 1024 * 1024)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("large_download.bin");

        let source = format!("s3://{}/large_download.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&local_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(local_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        // First, upload with SSE-C via cp
        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "ssec_upload.txt", b"sse-c download test");

        let s3_path = format!("s3://{}/ssec_dl.txt", bucket);
        let upload_stats = helper
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
        assert_eq!(upload_stats.sync_complete, 1);

        // Download with SSE-C
        let download_file = local_dir.join("ssec_download.txt");
        let stats = helper
            .cp_test_data(vec![
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
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"sse-c download test");

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_empty_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper.put_empty_object(&bucket, "empty.txt").await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("empty.txt");

        let source = format!("s3://{}/empty.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&local_file).unwrap();
        assert_eq!(metadata.len(), 0);

        let content = std::fs::read(&local_file).unwrap();
        assert!(content.is_empty());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_sha256.dat", b"sha256 checksum test");
        let s3_path = format!("s3://{}/checksum_sha256.dat", bucket);

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

        let download_file = local_dir.join("download_sha256.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"sha256 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"sha256 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_crc32.dat", b"crc32 checksum test");
        let s3_path = format!("s3://{}/checksum_crc32.dat", bucket);

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

        let download_file = local_dir.join("download_crc32.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"crc32 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"crc32 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_crc32c.dat", b"crc32c checksum test");
        let s3_path = format!("s3://{}/checksum_crc32c.dat", bucket);

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

        let download_file = local_dir.join("download_crc32c.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"crc32c checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"crc32c checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_sha1.dat", b"sha1 checksum test");
        let s3_path = format!("s3://{}/checksum_sha1.dat", bucket);

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

        let download_file = local_dir.join("download_sha1.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"sha1 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"sha1 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_crc64nvme.dat",
            b"crc64nvme checksum test",
        );
        let s3_path = format!("s3://{}/checksum_crc64nvme.dat", bucket);

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

        let download_file = local_dir.join("download_crc64nvme.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"crc64nvme checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"crc64nvme checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_kms.dat", b"sse-kms download test");
        let s3_path = format!("s3://{}/kms_dl.dat", bucket);

        let upload_stats = helper
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
        assert_eq!(upload_stats.sync_complete, 1);

        let download_file = local_dir.join("download_kms.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"sse-kms download test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"sse-kms download test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_dsse.dat", b"dsse-kms download test");
        let s3_path = format!("s3://{}/dsse_dl.dat", bucket);

        let upload_stats = helper
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
        assert_eq!(upload_stats.sync_complete, 1);

        let download_file = local_dir.join("download_dsse.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"dsse-kms download test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"dsse-kms download test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_kms_sha256.dat",
            b"kms and sha256 test",
        );
        let s3_path = format!("s3://{}/kms_sha256_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let download_file = local_dir.join("download_kms_sha256.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and sha256 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and sha256 test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_sha256.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_sha256.bin", bucket);

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

        let download_file = local_dir.join("download_mp_sha256.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_crc32.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_crc32.bin", bucket);

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

        let download_file = local_dir.join("download_mp_crc32.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_crc64nvme.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_crc64nvme.bin", bucket);

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

        let download_file = local_dir.join("download_mp_crc64nvme.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_auto_chunk.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/auto_chunk.bin", bucket);

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
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        let download_file = local_dir.join("download_auto_chunk.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_disable_etag_verify_large() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_sized_object(&bucket, "no_etag_large.bin", 9 * 1024 * 1024)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("no_etag_large.bin");

        let source = format!("s3://{}/no_etag_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);

        let metadata = std::fs::metadata(&local_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(local_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        // Upload 9MiB with SSE-C via cp (triggers multipart upload)
        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "ssec_mp_upload.bin", 9 * 1024 * 1024);

        let s3_path = format!("s3://{}/ssec_mp.bin", bucket);
        let upload_stats = helper
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
        assert_eq!(upload_stats.sync_complete, 1);

        // Download with SSE-C source args
        let download_file = local_dir.join("ssec_mp_download.bin");
        let stats = helper
            .cp_test_data(vec![
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
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
