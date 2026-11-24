#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    // Composite additional checksums of the 9 MiB zero-filled file
    // (`create_sized_file(dir, name, 9 * 1024 * 1024)`) uploaded as multipart,
    // computed independently of s3util. XXHASH128 / XXHASH3 values are
    // computed at runtime with `TestHelper::compute_composite_additional_checksum`.
    // SHA512, 8 MiB + 1 MiB parts
    const SHA512_9M_ZEROS_8M_CHUNK: &str = "5gHNFwrytRkOHvbjgippRxal7BgRsDFnaswFJdS63mOgmpwtzh+3aXwzelbTU2I7Pd7keP7uWtYLvNvMNZ4nqA==-2";
    // MD5, 8 MiB + 1 MiB parts
    const MD5_9M_ZEROS_8M_CHUNK: &str = "0SbvCIF9BJDiB+RWywrggA==-2";
    // XXHASH64, 8 MiB + 1 MiB parts
    const XXHASH64_9M_ZEROS_8M_CHUNK: &str = "T2YRqk6alD4=-2";
    // SHA512, 5 MiB + 4 MiB parts
    const SHA512_9M_ZEROS_5M_CHUNK: &str = "+1/omnR7hjwN2nPCb60Y1CXfneq2oJTCMJinmAiW8RzxDlE7LbGlsOC4eb+xyHcB4dWXS7tZrOT+71sZXQ2LeA==-2";
    // MD5, 5 MiB + 4 MiB parts
    const MD5_9M_ZEROS_5M_CHUNK: &str = "Dc+0kNe1KtCUfD806Eb2ow==-2";
    // XXHASH64, 5 MiB + 4 MiB parts
    const XXHASH64_9M_ZEROS_5M_CHUNK: &str = "ldoL7OJC/0M=-2";

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

        let source_mtime = helper
            .get_object_last_modified(&bucket, "download.txt", None)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("download.txt");

        let source = format!("s3://{}/download.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // Verify downloaded content
        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content, test_content);
        TestHelper::verify_downloaded_file_md5(local_file.to_str().unwrap(), b"hello from s3");

        // Verify mtime was preserved from S3's last-modified value.
        let local_mtime = TestHelper::get_file_last_modified(local_file.to_str().unwrap());
        assert_eq!(
            local_mtime, source_mtime,
            "downloaded file mtime ({local_mtime}) does not match S3 last-modified ({source_mtime})"
        );

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

        let source_mtime = helper
            .get_object_last_modified(&bucket, "etag_test.dat", None)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("etag_test.dat");

        let source = format!("s3://{}/etag_test.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content.len(), 1024);
        TestHelper::verify_downloaded_file_md5(local_file.to_str().unwrap(), &vec![0u8; 1024]);

        // mtime must be preserved from S3's last-modified value.
        let local_mtime = TestHelper::get_file_last_modified(local_file.to_str().unwrap());
        assert_eq!(local_mtime, source_mtime);

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
                "s3util-e2e-test",
                "--disable-etag-verify",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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

        let source_mtime = helper
            .get_object_last_modified(&bucket, "large_download.bin", None)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("large_download.bin");

        let source = format!("s3://{}/large_download.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let metadata = std::fs::metadata(&local_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(local_file.to_str().unwrap(), SHA256_9M_ZEROS);

        // For the multipart-download path too, mtime must be preserved.
        let local_mtime = TestHelper::get_file_last_modified(local_file.to_str().unwrap());
        assert_eq!(local_mtime, source_mtime);

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
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 0);

        // Download with SSE-C
        let download_file = local_dir.join("ssec_download.txt");
        let stats = helper
            .cp_test_data(vec![
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
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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

        let download_file = local_dir.join("download_sha256.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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
    async fn s3_to_local_with_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_sha512.dat", b"sha512 checksum test");
        let s3_path = format!("s3://{}/checksum_sha512.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "checksum_sha512.dat", None)
            .await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"sha512 checksum test")
        );

        let download_file = local_dir.join("download_sha512.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"sha512 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"sha512 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_md5.dat", b"md5 checksum test");
        let s3_path = format!("s3://{}/checksum_md5.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "checksum_md5.dat", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"md5 checksum test")
        );

        let download_file = local_dir.join("download_md5.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"md5 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"md5 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_xxhash128.dat",
            b"xxhash128 checksum test",
        );
        let s3_path = format!("s3://{}/checksum_xxhash128.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "checksum_xxhash128.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH128", b"xxhash128 checksum test")
        );

        let download_file = local_dir.join("download_xxhash128.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"xxhash128 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"xxhash128 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_xxhash3.dat",
            b"xxhash3 checksum test",
        );
        let s3_path = format!("s3://{}/checksum_xxhash3.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "checksum_xxhash3.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"xxhash3 checksum test")
        );

        let download_file = local_dir.join("download_xxhash3.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"xxhash3 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"xxhash3 checksum test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_xxhash64.dat",
            b"xxhash64 checksum test",
        );
        let s3_path = format!("s3://{}/checksum_xxhash64.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "checksum_xxhash64.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH64", b"xxhash64 checksum test")
        );

        let download_file = local_dir.join("download_xxhash64.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"xxhash64 checksum test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"xxhash64 checksum test",
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

        let download_file = local_dir.join("download_crc32.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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

        let download_file = local_dir.join("download_crc32c.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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

        let download_file = local_dir.join("download_sha1.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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

        let download_file = local_dir.join("download_crc64nvme.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 0);

        let download_file = local_dir.join("download_kms.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--sse",
                "aws:kms:dsse",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 0);

        let download_file = local_dir.join("download_dsse.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA256",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_kms_sha256.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

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
    async fn s3_to_local_with_sse_kms_and_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_kms_sha512.dat",
            b"kms and sha512 test",
        );
        let s3_path = format!("s3://{}/kms_sha512_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA512",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "kms_sha512_dl.dat", None).await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"kms and sha512 test")
        );

        let download_file = local_dir.join("download_kms_sha512.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and sha512 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and sha512 test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms_and_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload_kms_md5.dat", b"kms and md5 test");
        let s3_path = format!("s3://{}/kms_md5_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "MD5",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "kms_md5_dl.dat", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"kms and md5 test")
        );

        let download_file = local_dir.join("download_kms_md5.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and md5 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and md5 test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms_and_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_kms_xxhash128.dat",
            b"kms and xxhash128 test",
        );
        let s3_path = format!("s3://{}/kms_xxhash128_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH128",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "kms_xxhash128_dl.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH128", b"kms and xxhash128 test")
        );

        let download_file = local_dir.join("download_kms_xxhash128.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and xxhash128 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and xxhash128 test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms_and_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_kms_xxhash3.dat",
            b"kms and xxhash3 test",
        );
        let s3_path = format!("s3://{}/kms_xxhash3_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH3",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "kms_xxhash3_dl.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"kms and xxhash3 test")
        );

        let download_file = local_dir.join("download_kms_xxhash3.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and xxhash3 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and xxhash3 test",
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_sse_kms_and_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &local_dir,
            "upload_kms_xxhash64.dat",
            b"kms and xxhash64 test",
        );
        let s3_path = format!("s3://{}/kms_xxhash64_dl.dat", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH64",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "kms_xxhash64_dl.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH64", b"kms and xxhash64 test")
        );

        let download_file = local_dir.join("download_kms_xxhash64.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"kms and xxhash64 test");
        TestHelper::verify_downloaded_file_md5(
            download_file.to_str().unwrap(),
            b"kms and xxhash64 test",
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

        let download_file = local_dir.join("download_mp_sha256.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_sha512.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_sha512.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_sha512.bin", None).await;
        assert_eq!(head.checksum_sha512().unwrap(), SHA512_9M_ZEROS_8M_CHUNK);

        let download_file = local_dir.join("download_mp_sha512.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_md5.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_md5.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_md5.bin", None).await;
        assert_eq!(head.checksum_md5().unwrap(), MD5_9M_ZEROS_8M_CHUNK);

        let download_file = local_dir.join("download_mp_md5.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_xxhash128.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_xxhash128.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_xxhash128.bin", None).await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&upload_file).unwrap(),
                8 * 1024 * 1024
            )
        );

        let download_file = local_dir.join("download_mp_xxhash128.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_xxhash3.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_xxhash3.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_xxhash3.bin", None).await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&upload_file).unwrap(),
                8 * 1024 * 1024
            )
        );

        let download_file = local_dir.join("download_mp_xxhash3.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_xxhash64.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_xxhash64.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "mp_xxhash64.bin", None).await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_9M_ZEROS_8M_CHUNK
        );

        let download_file = local_dir.join("download_mp_xxhash64.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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

        let download_file = local_dir.join("download_mp_crc32.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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

        let download_file = local_dir.join("download_mp_crc64nvme.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_auto_chunk.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_auto_chunk_sha512.bin",
            9 * 1024 * 1024,
        );
        let s3_path = format!("s3://{}/auto_chunk_sha512.bin", bucket);

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
                "--additional-checksum-algorithm",
                "SHA512",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "auto_chunk_sha512.bin", None)
            .await;
        assert_eq!(head.checksum_sha512().unwrap(), SHA512_9M_ZEROS_5M_CHUNK);

        let download_file = local_dir.join("download_auto_chunk_sha512.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_auto_chunk_md5.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/auto_chunk_md5.bin", bucket);

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
                "--additional-checksum-algorithm",
                "MD5",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "auto_chunk_md5.bin", None)
            .await;
        assert_eq!(head.checksum_md5().unwrap(), MD5_9M_ZEROS_5M_CHUNK);

        let download_file = local_dir.join("download_auto_chunk_md5.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_auto_chunk_xxhash128.bin",
            9 * 1024 * 1024,
        );
        let s3_path = format!("s3://{}/auto_chunk_xxhash128.bin", bucket);

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
                "--additional-checksum-algorithm",
                "XXHASH128",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "auto_chunk_xxhash128.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&upload_file).unwrap(),
                5 * 1024 * 1024
            )
        );

        let download_file = local_dir.join("download_auto_chunk_xxhash128.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_auto_chunk_xxhash3.bin",
            9 * 1024 * 1024,
        );
        let s3_path = format!("s3://{}/auto_chunk_xxhash3.bin", bucket);

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
                "--additional-checksum-algorithm",
                "XXHASH3",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "auto_chunk_xxhash3.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&upload_file).unwrap(),
                5 * 1024 * 1024
            )
        );

        let download_file = local_dir.join("download_auto_chunk_xxhash3.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_multipart_with_auto_chunksize_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_auto_chunk_xxhash64.bin",
            9 * 1024 * 1024,
        );
        let s3_path = format!("s3://{}/auto_chunk_xxhash64.bin", bucket);

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
                "--additional-checksum-algorithm",
                "XXHASH64",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "auto_chunk_xxhash64.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_9M_ZEROS_5M_CHUNK
        );

        let download_file = local_dir.join("download_auto_chunk_xxhash64.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

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
                "s3util-e2e-test",
                "--disable-etag-verify",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let metadata = std::fs::metadata(&local_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(local_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Positive case: when the user explicitly chooses a target path
    /// containing `..`, `s3util cp` honors it. Basename semantics in
    /// `extract_keys` append the source basename onto the resolved
    /// target directory, so the file lands where the user asked for.
    ///
    /// Replaces the pre-2026-04-20 test that asserted traversal rejection:
    /// the combined-path regex guard in the local storage layer was
    /// removed, and user-chosen `..` is a legitimate filesystem concept,
    /// not an attack vector. The arg-time guard in `check_source_s3_key`
    /// still rejects malformed *source* URLs — that's unit-covered in
    /// `src/config/args/tests.rs::source_s3_url_*_rejected`.
    #[tokio::test]
    async fn s3_to_local_user_chosen_parent_dir_target_accepted() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let probe_name = format!("parent_dir_probe_{}.dat", uuid::Uuid::new_v4());
        let test_content = b"user chose parent dir";
        helper
            .put_object(&bucket, &probe_name, test_content.to_vec())
            .await;

        // `nested` exists; `..` walks back to `local_dir`, its parent.
        let local_dir = TestHelper::create_temp_dir();
        let nested_dir = local_dir.join("nested");
        std::fs::create_dir_all(&nested_dir).unwrap();
        let target = nested_dir.join("..").to_string_lossy().to_string();

        let source = format!("s3://{}/{}", bucket, probe_name);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                target.as_str(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1, "stats = {stats:?}");
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // File lands in local_dir (parent of nested_dir) with the source basename.
        let expected = local_dir.join(&probe_name);
        assert!(
            TestHelper::is_file_exist(expected.to_str().unwrap()),
            "expected file at {}",
            expected.display()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Companion test: verify the safe path — when the S3 object key
    /// contains `..` segments but the target is a directory, s3util-rs's
    /// basename-based resolution strips the traversal and writes the file
    /// safely inside `local_dir`. No file is created outside `local_dir`.
    #[tokio::test]
    async fn s3_to_local_basename_strips_traversal_when_target_is_dir() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Put an object whose key is a plain filename (the `..` scenario
        // in s3util maps to the target path, since cp is single-object and
        // S3 URL parsing normalizes `..` in the source prefix).
        let test_content = b"safe basename path";
        helper
            .put_object(&bucket, "safe_name.dat", test_content.to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();

        // Target is a directory — s3util appends the source basename.
        let target_dir_with_trailing_sep = format!(
            "{}{}",
            local_dir.to_str().unwrap(),
            std::path::MAIN_SEPARATOR
        );

        let source = format!("s3://{}/safe_name.dat", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                &target_dir_with_trailing_sep,
            ])
            .await;

        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // The file must exist at the expected safe path inside local_dir.
        let expected_file = local_dir.join("safe_name.dat");
        assert!(
            TestHelper::is_file_exist(expected_file.to_str().unwrap()),
            "expected file {} to be written inside local_dir",
            expected_file.display()
        );

        // And nothing should have been written outside local_dir.
        let would_escape_to = local_dir
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join("safe_name.dat");
        assert!(
            !TestHelper::is_file_exist(would_escape_to.to_str().unwrap()),
            "unexpected file created outside local_dir at {}",
            would_escape_to.display()
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Verify that s3util-rs preserves the S3 object's last-modified
    /// timestamp on the downloaded local file. `src/storage/local/mod.rs`
    /// calls `fs_util::set_last_modified_for_path` on both the
    /// single-part and multipart download paths, so the local file's
    /// mtime (seconds resolution) must equal the S3 object's
    /// last-modified (seconds resolution).
    #[tokio::test]
    async fn s3_to_local_preserves_last_modified() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(&bucket, "mtime_check.txt", b"mtime preservation".to_vec())
            .await;

        // Query the S3 object's last-modified BEFORE the local file is
        // created. Any later wall-clock write would show a newer local
        // mtime; preservation must reset it back to source's value.
        let source_mtime = helper
            .get_object_last_modified(&bucket, "mtime_check.txt", None)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("mtime_check.txt");

        let source = format!("s3://{}/mtime_check.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let local_mtime = TestHelper::get_file_last_modified(local_file.to_str().unwrap());
        assert_eq!(
            local_mtime, source_mtime,
            "downloaded file mtime ({local_mtime}) does not match S3 last-modified ({source_mtime})"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_without_slash`: download to a target path
    /// that has no trailing slash (treated as a file path, not a directory).
    #[tokio::test]
    async fn s3_to_local_without_slash() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let test_content = b"without slash download test";
        helper
            .put_object(&bucket, "noslash.txt", test_content.to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        // Target path has no trailing slash — treated as an explicit file path.
        let local_file_path = local_dir.join("noslash.txt");
        let local_file_str = local_file_path.to_string_lossy();
        // Strip trailing separator if any (simulate "no slash" destination).
        let target_path = local_file_str
            .trim_end_matches(std::path::MAIN_SEPARATOR)
            .to_string();

        let source = format!("s3://{}/noslash.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &source,
                &target_path,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let content = std::fs::read(&local_file_path).unwrap();
        assert_eq!(content, test_content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_sha256`:
    /// upload an empty object with SHA256 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with SHA256 additional checksum.
        let s3_path = format!("s3://{}/empty_sha256.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_sha256_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_sha512`:
    /// upload an empty object with SHA512 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with SHA512 additional checksum.
        let s3_path = format!("s3://{}/empty_sha512.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "empty_sha512.dat", None).await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"")
        );

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_sha512_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_md5`:
    /// upload an empty object with MD5 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with MD5 additional checksum.
        let s3_path = format!("s3://{}/empty_md5.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "empty_md5.dat", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"")
        );

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_md5_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_xxhash128`:
    /// upload an empty object with XXHASH128 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with XXHASH128 additional checksum.
        let s3_path = format!("s3://{}/empty_xxhash128.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "empty_xxhash128.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH128", b"")
        );

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_xxhash128_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_xxhash3`:
    /// upload an empty object with XXHASH3 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with XXHASH3 additional checksum.
        let s3_path = format!("s3://{}/empty_xxhash3.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "empty_xxhash3.dat", None).await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"")
        );

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_xxhash3_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_empty_data_checksum_xxhash64`:
    /// upload an empty object with XXHASH64 additional checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_empty_data_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Upload an empty object with XXHASH64 additional checksum.
        let s3_path = format!("s3://{}/empty_xxhash64.dat", bucket);
        let local_dir = TestHelper::create_temp_dir();
        let empty_file = TestHelper::create_test_file(&local_dir, "empty_src.dat", b"");
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                empty_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "empty_xxhash64.dat", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH64", b"")
        );

        // Download with additional checksum verification enabled.
        let download_file = local_dir.join("empty_xxhash64_dl.dat");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_checksum_sha1`:
    /// upload a large (multipart) file with SHA1 additional checksum, then
    /// download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_sha1.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_sha1.bin", bucket);

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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_mp_sha1.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_checksum_crc32c`:
    /// upload a large (multipart) file with CRC32C additional checksum, then
    /// download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_crc32c.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_crc32c.bin", bucket);

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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_mp_crc32c.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_checksum_crc32c_full_object`:
    /// upload a large (multipart) file with CRC32C full-object checksum, then
    /// download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_checksum_crc32c_full_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_crc32c_fo.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_crc32c_fo.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_mp_crc32c_fo.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_disable_multipart_verify`:
    /// upload a large (multipart) file with SHA1 additional checksum, then
    /// download with `--disable-multipart-verify` (skips ETag and per-part
    /// checksum verification).
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_disable_multipart_verify() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_mp_no_verify.bin", 9 * 1024 * 1024);
        let s3_path = format!("s3://{}/mp_no_verify.bin", bucket);

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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_mp_no_verify.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--disable-multipart-verify",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum`:
    /// upload an 8 MiB file with SHA256 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_sha256.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_sha256.bin", bucket);

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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_8mib_sha256.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum_sha512`:
    /// upload an 8 MiB file with SHA512 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_sha512.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_sha512.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "8mib_sha512.bin", None).await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "SHA512",
                &std::fs::read(&upload_file).unwrap(),
                TEST_FILE_SIZE_8MIB
            )
        );

        let download_file = local_dir.join("download_8mib_sha512.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum_md5`:
    /// upload an 8 MiB file with MD5 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&local_dir, "upload_8mib_md5.bin", TEST_FILE_SIZE_8MIB);
        let s3_path = format!("s3://{}/8mib_md5.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "8mib_md5.bin", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "MD5",
                &std::fs::read(&upload_file).unwrap(),
                TEST_FILE_SIZE_8MIB
            )
        );

        let download_file = local_dir.join("download_8mib_md5.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash128`:
    /// upload an 8 MiB file with XXHASH128 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_xxhash128.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_xxhash128.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "8mib_xxhash128.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&upload_file).unwrap(),
                TEST_FILE_SIZE_8MIB
            )
        );

        let download_file = local_dir.join("download_8mib_xxhash128.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash3`:
    /// upload an 8 MiB file with XXHASH3 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_xxhash3.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_xxhash3.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "8mib_xxhash3.bin", None).await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&upload_file).unwrap(),
                TEST_FILE_SIZE_8MIB
            )
        );

        let download_file = local_dir.join("download_8mib_xxhash3.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash64`:
    /// upload an 8 MiB file with XXHASH64 (produces a composite/per-part checksum),
    /// then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_composite_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_xxhash64.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_xxhash64.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "8mib_xxhash64.bin", None).await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH64",
                &std::fs::read(&upload_file).unwrap(),
                TEST_FILE_SIZE_8MIB
            )
        );

        let download_file = local_dir.join("download_8mib_xxhash64.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_crc32_full_object_checksum`:
    /// upload an 8 MiB file with CRC32 full-object checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_crc32_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_crc32_fo.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_crc32_fo.bin", bucket);

        let upload_stats = helper
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
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_8mib_crc32_fo.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_crc32c_full_object_checksum`:
    /// upload an 8 MiB file with CRC32C full-object checksum, then download
    /// with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_crc32c_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_crc32c_fo.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_crc32c_fo.bin", bucket);

        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_8mib_crc32c_fo.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Mirrors s3sync's `s3_to_local_with_multipart_upload_with_8mib_crc64nvme_full_object_checksum`:
    /// upload an 8 MiB file with CRC64NVME additional checksum (full-object by
    /// nature), then download with `--enable-additional-checksum`.
    #[tokio::test]
    async fn s3_to_local_with_multipart_upload_with_8mib_crc64nvme_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_sized_file(
            &local_dir,
            "upload_8mib_crc64nvme.bin",
            TEST_FILE_SIZE_8MIB,
        );
        let s3_path = format!("s3://{}/8mib_crc64nvme.bin", bucket);

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
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let download_file = local_dir.join("download_8mib_crc64nvme.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), TEST_FILE_SIZE_8MIB as u64);

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
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.e_tag_verified, 0);
        assert_eq!(upload_stats.checksum_verified, 0);

        // Download with SSE-C source args
        let download_file = local_dir.join("ssec_mp_download.bin");
        let stats = helper
            .cp_test_data(vec![
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
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let metadata = std::fs::metadata(&download_file).unwrap();
        assert_eq!(metadata.len(), 9 * 1024 * 1024);
        TestHelper::verify_downloaded_file_sha256(download_file.to_str().unwrap(), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_local_with_source_version_id() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let v1_content = b"v1 content";
        let v2_content = b"v2 content";
        let v1_id = helper
            .put_object_with_version(&bucket, "ver_test.txt", v1_content.to_vec())
            .await;
        helper
            .put_object(&bucket, "ver_test.txt", v2_content.to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("ver_test.txt");

        let source = format!("s3://{}/ver_test.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--source-version-id",
                &v1_id,
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content, v1_content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// `--source-version-id` with a SHA512 additional checksum on a
    /// non-current version. Both versions carry a SHA512 checksum of a
    /// different body, so the checksum s3util verifies against (HEAD) and the
    /// bytes it downloads (GET) must both come from v1: reading v2 for either
    /// one fails verification or leaves v2's bytes in the file.
    #[tokio::test]
    async fn s3_to_local_with_source_version_id_and_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "ver_sha512.txt";
        let v1_content = b"v1 content with a SHA512 checksum";
        let v2_content = b"v2 content, also with a SHA512 checksum";
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(&bucket, key, v1_content, "SHA512", None)
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(&bucket, key, v2_content, "SHA512", None)
            .await;
        assert_ne!(v1_id, v2_id);

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join(key);
        let source = format!("s3://{bucket}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--source-version-id",
                &v1_id,
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(std::fs::read(&local_file).unwrap(), v1_content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Multipart `--source-version-id` with an XXHASH3 composite checksum.
    /// v1 (requested, non-current) is 9 MiB in 5 MiB parts; v2 (latest) is a
    /// different 9 MiB body in 8 MiB parts. `--auto-chunksize` reads the part
    /// layout from the source, so the HEAD, the part lookups (HeadObject
    /// partNumber=1, GetObjectAttributes) and every ranged GET must target v1:
    /// any of them reading v2 brings in the wrong part layout or bytes, and the
    /// composite checksum or ETag no longer matches.
    #[tokio::test]
    async fn s3_to_local_multipart_with_source_version_id_and_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;
        helper.enable_bucket_versioning(&bucket).await;

        let key = "ver_mp_xxhash3.bin";
        let v1_content = vec![0x01_u8; 9 * 1024 * 1024];
        let v2_content = vec![0x02_u8; 9 * 1024 * 1024];
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket,
                key,
                &v1_content,
                "XXHASH3",
                Some(5),
            )
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket,
                key,
                &v2_content,
                "XXHASH3",
                Some(8),
            )
            .await;
        assert_ne!(v1_id, v2_id);

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join(key);
        let source = format!("s3://{bucket}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--source-version-id",
                &v1_id,
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&std::fs::read(&local_file).unwrap()),
            TestHelper::get_sha256_from_bytes(&v1_content),
            "the downloaded file must be v1"
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // -----------------------------------------------------------------
    // cp --skip-existing for s3 → local
    // -----------------------------------------------------------------

    fn run_s3util(args: &[&str]) -> std::process::Output {
        std::process::Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .output()
            .expect("spawn s3util")
    }

    #[tokio::test]
    async fn s3_to_local_skip_existing_skips_when_local_file_exists() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Remote object differs from the local file so a buggy "non-skip"
        // would be visible by the local file's contents being overwritten.
        helper
            .put_object(&bucket, "download.txt", b"remote-content".to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file =
            TestHelper::create_test_file(&local_dir, "download.txt", b"existing-content");
        let mtime_before = TestHelper::get_file_last_modified(local_file.to_str().unwrap());
        let bytes_before = std::fs::read(&local_file).unwrap();

        let source = format!("s3://{}/download.txt", bucket);
        let output = run_s3util(&[
            "cp",
            "--skip-existing",
            "--source-profile",
            "s3util-e2e-test",
            "--source-region",
            REGION,
            &source,
            local_file.to_str().unwrap(),
        ]);
        assert!(
            output.status.success(),
            "cp --skip-existing should exit 0 when local file exists; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let bytes_after = std::fs::read(&local_file).unwrap();
        let mtime_after = TestHelper::get_file_last_modified(local_file.to_str().unwrap());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);

        assert_eq!(
            bytes_after, b"existing-content",
            "cp --skip-existing must NOT have downloaded over the existing local file"
        );
        assert_eq!(
            bytes_before, bytes_after,
            "local file contents changed despite --skip-existing"
        );
        assert_eq!(
            mtime_before, mtime_after,
            "local file mtime changed despite --skip-existing"
        );
    }

    #[tokio::test]
    async fn s3_to_local_skip_existing_proceeds_when_local_missing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(&bucket, "download.txt", b"remote-data".to_vec())
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("download.txt");
        // Sanity: the destination must NOT exist before the run.
        assert!(!local_file.exists());

        let source = format!("s3://{}/download.txt", bucket);
        let output = run_s3util(&[
            "cp",
            "--skip-existing",
            "--source-profile",
            "s3util-e2e-test",
            "--source-region",
            REGION,
            &source,
            local_file.to_str().unwrap(),
        ]);
        assert!(
            output.status.success(),
            "cp --skip-existing should exit 0 when local missing; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let exists = local_file.exists();
        let content = if exists {
            std::fs::read(&local_file).unwrap()
        } else {
            Vec::new()
        };

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);

        assert!(
            exists,
            "cp --skip-existing must download when the local file is missing"
        );
        assert_eq!(
            content, b"remote-data",
            "downloaded contents must match the remote object"
        );
    }
}
