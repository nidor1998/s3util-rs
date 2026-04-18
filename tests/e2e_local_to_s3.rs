#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{ServerSideEncryption, StorageClass};

    use super::*;
    use common::ETAG_9M_ZEROS_8M_CHUNK;
    use common::*;

    #[tokio::test]
    async fn local_to_s3_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "test.txt", b"hello world");

        let target = format!("s3://{}/test.txt", bucket);
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

        assert!(helper.is_object_exist(&bucket, "test.txt", None).await);

        helper
            .verify_uploaded_object_etag(&bucket, "test.txt", b"hello world")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "meta.txt", b"metadata test");

        let target = format!("s3://{}/meta.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                "--expires",
                TEST_EXPIRES,
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            helper
                .verify_test_object_metadata(&bucket, "meta.txt", None)
                .await
        );

        helper
            .verify_uploaded_object_etag(&bucket, "meta.txt", b"metadata test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "sc.txt", b"storage class test");

        let target = format!("s3://{}/sc.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--storage-class",
                "STANDARD_IA",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);

        helper
            .verify_uploaded_object_etag(&bucket, "sc.txt", b"storage class test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "kms.txt", b"kms test");

        let target = format!("s3://{}/kms.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "kms.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        helper
            .verify_object_content_md5(&bucket, "kms.txt", b"kms test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "ssec.txt", b"sse-c test");

        let target = format!("s3://{}/ssec.txt", bucket);
        let stats = helper
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
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // Create a file larger than 8 MiB to trigger multipart upload
        let test_file = TestHelper::create_sized_file(&local_dir, "large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/large.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 1);

        let head = helper.head_object(&bucket, "large.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        helper
            .verify_uploaded_object_etag_value(&bucket, "large.bin", ETAG_9M_ZEROS_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "sha256.txt", b"sha256 checksum test");

        let target = format!("s3://{}/sha256.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "sha256.txt", None).await;
        assert!(head.checksum_sha256().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "sha256.txt", b"sha256 checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_additional_checksum_disable_verify() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "sha256_no_verify.txt",
            b"sha256 disable verify test",
        );

        let target = format!("s3://{}/sha256_no_verify.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                "--disable-additional-checksum-verify",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper
            .head_object(&bucket, "sha256_no_verify.txt", None)
            .await;
        assert!(head.checksum_sha256().is_some());

        helper
            .verify_uploaded_object_etag(
                &bucket,
                "sha256_no_verify.txt",
                b"sha256 disable verify test",
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc32.txt", b"crc32 checksum test");

        let target = format!("s3://{}/crc32.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "crc32.txt", None).await;
        assert!(head.checksum_crc32().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "crc32.txt", b"crc32 checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc64.txt", b"crc64nvme checksum test");

        let target = format!("s3://{}/crc64.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "crc64.txt", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "crc64.txt", b"crc64nvme checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_etag_verified() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "etag.txt", b"etag verification test");

        let target = format!("s3://{}/etag.txt", bucket);
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
        assert_eq!(stats.e_tag_verified, 1);

        helper
            .verify_uploaded_object_etag(&bucket, "etag.txt", b"etag verification test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_etag_verify() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "no_etag.txt", b"no etag test");

        let target = format!("s3://{}/no_etag.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-etag-verify",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);

        helper
            .verify_uploaded_object_etag(&bucket, "no_etag.txt", b"no etag test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_website_redirect() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "redir.txt", b"redirect test");

        let target = format!("s3://{}/redir.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--website-redirect",
                TEST_WEBSITE_REDIRECT,
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "redir.txt", None).await;
        assert_eq!(
            head.website_redirect_location().unwrap(),
            TEST_WEBSITE_REDIRECT
        );

        helper
            .verify_uploaded_object_etag(&bucket, "redir.txt", b"redirect test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_content_md5_header() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "nomd5.txt", b"no md5 header test");

        let target = format!("s3://{}/nomd5.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-content-md5-header",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag(&bucket, "nomd5.txt", b"no md5 header test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "sha1.txt", b"sha1 checksum test");

        let target = format!("s3://{}/sha1.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "sha1.txt", None).await;
        assert!(head.checksum_sha1().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "sha1.txt", b"sha1 checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "crc32c.txt", b"crc32c checksum test");

        let target = format!("s3://{}/crc32c.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "crc32c.txt", None).await;
        assert!(head.checksum_crc32_c().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "crc32c.txt", b"crc32c checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "dsse.txt", b"dsse kms test");

        let target = format!("s3://{}/dsse.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms:dsse",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "dsse.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        helper
            .verify_object_content_md5(&bucket, "dsse.txt", b"dsse kms test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_kms_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "kms_sha256.txt", b"kms and sha256 test");

        let target = format!("s3://{}/kms_sha256.txt", bucket);
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

        let head = helper.head_object(&bucket, "kms_sha256.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_sha256().is_some());

        helper
            .verify_object_content_md5(&bucket, "kms_sha256.txt", b"kms and sha256 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_kms_and_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "kms_crc32.txt", b"kms and crc32 test");

        let target = format!("s3://{}/kms_crc32.txt", bucket);
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

        let head = helper.head_object(&bucket, "kms_crc32.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_crc32().is_some());

        helper
            .verify_object_content_md5(&bucket, "kms_crc32.txt", b"kms and crc32 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_dsse_kms_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "dsse_sha256.txt",
            b"dsse kms and sha256 test",
        );

        let target = format!("s3://{}/dsse_sha256.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms:dsse",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "dsse_sha256.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );
        assert!(head.checksum_sha256().is_some());

        helper
            .verify_object_content_md5(&bucket, "dsse_sha256.txt", b"dsse kms and sha256 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_payload_signing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "no_sign.txt",
            b"disable payload signing test",
        );

        let target = format!("s3://{}/no_sign.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-payload-signing",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag(&bucket, "no_sign.txt", b"disable payload signing test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_payload_signing_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "no_sign_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/no_sign_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-payload-signing",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag_value(&bucket, "no_sign_large.bin", ETAG_9M_ZEROS_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_acl() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "acl.txt", b"acl test");

        let target = format!("s3://{}/acl.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--acl",
                "bucket-owner-full-control",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag(&bucket, "acl.txt", b"acl test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_acl_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "acl_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/acl_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--acl",
                "bucket-owner-full-control",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag_value(&bucket, "acl_large.bin", ETAG_9M_ZEROS_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_sha256.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_sha256.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_sha256.bin", None).await;
        assert!(head.checksum_sha256().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_crc32.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_crc32.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_crc32.bin", None).await;
        assert!(head.checksum_crc32().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_crc32c.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_crc32c.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_crc32c.bin", None).await;
        assert!(head.checksum_crc32_c().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_sha1.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_sha1.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_sha1.bin", None).await;
        assert!(head.checksum_sha1().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_crc64.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_crc64.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_crc64.bin", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_kms.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_kms.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_kms.bin", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_dsse.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_dsse.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms:dsse",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_dsse.bin", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_with_sse_kms_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_kms_sha256.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_kms_sha256.bin", bucket);
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

        let head = helper.head_object(&bucket, "mp_kms_sha256.bin", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_sha256().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_content_md5_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "nomd5_sha256.txt",
            b"no md5 with sha256 test",
        );

        let target = format!("s3://{}/nomd5_sha256.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-content-md5-header",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "nomd5_sha256.txt", None).await;
        assert!(head.checksum_sha256().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "nomd5_sha256.txt", b"no md5 with sha256 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_disable_content_md5_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(
            &local_dir,
            "nomd5_crc64.txt",
            b"no md5 with crc64nvme test",
        );

        let target = format!("s3://{}/nomd5_crc64.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-content-md5-header",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "nomd5_crc64.txt", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper
            .verify_uploaded_object_etag(&bucket, "nomd5_crc64.txt", b"no md5 with crc64nvme test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_disable_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_nomd5.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_nomd5.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-content-md5-header",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_nomd5.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_full_object_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_foc_crc32.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_foc_crc32.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_foc_crc32.bin", None).await;
        assert!(head.checksum_crc32().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_full_object_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_foc_crc32c.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_foc_crc32c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_foc_crc32c.bin", None).await;
        assert!(head.checksum_crc32_c().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_multipart_full_object_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_foc_crc64.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/mp_foc_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "mp_foc_crc64.bin", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_c_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "ssec_sha256.txt", b"sse-c with sha256 test");

        let target = format!("s3://{}/ssec_sha256.txt", bucket);
        let stats = helper
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
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_guess_mime_type() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        // Copy the test PNG file
        let png_src = std::path::Path::new("./test_data/img.png");
        let png_dest = local_dir.join("img.png");
        std::fs::copy(png_src, &png_dest).unwrap();

        let target = format!("s3://{}/img.png", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                png_dest.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "img.png", None).await;
        assert_eq!(head.content_type().unwrap(), "image/png");

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_without_guess_mime_type() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let png_src = std::path::Path::new("./test_data/img.png");
        let png_dest = local_dir.join("img.png");
        std::fs::copy(png_src, &png_dest).unwrap();

        let target = format!("s3://{}/img.png", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--no-guess-mime-type",
                png_dest.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "img.png", None).await;
        assert_eq!(head.content_type().unwrap(), "application/octet-stream");

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_metadata2() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_test_file(&local_dir, "meta2.txt", b"metadata2 test");

        // Upload with first set of metadata
        let target = format!("s3://{}/meta2.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-disposition",
                TEST_CONTENT_DISPOSITION,
                "--content-encoding",
                TEST_CONTENT_ENCODING,
                "--content-language",
                TEST_CONTENT_LANGUAGE,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                "--tagging",
                TEST_TAGGING,
                "--expires",
                TEST_EXPIRES,
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            helper
                .verify_test_object_metadata(&bucket, "meta2.txt", None)
                .await
        );

        // Re-upload with second set of metadata
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--cache-control",
                TEST_CACHE_CONTROL2,
                "--content-disposition",
                TEST_CONTENT_DISPOSITION2,
                "--content-encoding",
                TEST_CONTENT_ENCODING2,
                "--content-language",
                TEST_CONTENT_LANGUAGE2,
                "--content-type",
                TEST_CONTENT_TYPE2,
                "--metadata",
                TEST_METADATA_STRING2,
                "--tagging",
                TEST_TAGGING2,
                "--expires",
                TEST_EXPIRES2,
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            helper
                .verify_test_object_metadata2(&bucket, "meta2.txt", None)
                .await
        );

        helper
            .verify_uploaded_object_etag(&bucket, "meta2.txt", b"metadata2 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_if_none_match() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "if_none.txt", b"if-none-match test data");

        let target = format!("s3://{}/if_none.txt", bucket);

        // First upload: object doesn't exist, should succeed
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_uploaded_object_etag(&bucket, "if_none.txt", b"if-none-match test data")
            .await;

        // Second upload: object exists, should fail with precondition error
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert_eq!(stats.sync_error, 1);

        // Original object should still exist
        assert!(helper.is_object_exist(&bucket, "if_none.txt", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_if_none_match_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "if_none_mp.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/if_none_mp.bin", bucket);

        // First upload: object doesn't exist, should succeed
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);

        helper
            .verify_uploaded_object_etag_value(&bucket, "if_none_mp.bin", ETAG_9M_ZEROS_8M_CHUNK)
            .await;

        // Second upload: object exists, should fail with precondition error
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert_eq!(stats.sync_error, 1);

        // Original object should still exist unchanged
        helper
            .verify_uploaded_object_etag_value(&bucket, "if_none_mp.bin", ETAG_9M_ZEROS_8M_CHUNK)
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_put_last_modified_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "plm.txt", b"put last modified test");

        let target = format!("s3://{}/plm.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--put-last-modified-metadata",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket, "plm.txt", None).await;
        let metadata = head.metadata().unwrap();
        assert!(metadata.contains_key("s3sync_origin_last_modified"));
        // Verify the stored value is a parseable RFC3339 timestamp.
        let stored = metadata.get("s3sync_origin_last_modified").unwrap();
        assert!(
            chrono::DateTime::parse_from_rfc3339(stored).is_ok(),
            "expected parseable RFC3339 timestamp, got: {}",
            stored
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_disable_tagging() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "dtag.txt", b"disable tagging test");

        let target = format!("s3://{}/dtag.txt", bucket);
        // --disable-tagging conflicts with --tagging in the CLI, so just upload
        // with --disable-tagging and verify no tag is attached.
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-tagging",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let tagging = helper.get_object_tagging(&bucket, "dtag.txt", None).await;
        assert!(tagging.tag_set().is_empty());

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_target_request_payer() {
        TestHelper::init_dummy_tracing_subscriber();

        // Limitation: Without a Requester-Pays bucket, we can only verify the
        // CLI accepts --target-request-payer and the upload succeeds. For a
        // non-requester-pays bucket the header is ignored server-side.
        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "trp.txt", b"target request payer");

        let target = format!("s3://{}/trp.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--target-request-payer",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket, "trp.txt", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket, REGION)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "ssec_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/ssec_large.bin", bucket);
        let stats = helper
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
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
