#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{ServerSideEncryption, StorageClass};

    use super::*;
    use common::*;

    #[tokio::test]
    async fn roundtrip_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip basic content verification";
        let test_file = TestHelper::create_test_file(&local_dir, "rt.txt", content);

        let target = format!("s3://{}/rt.txt", bucket);
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
        assert_eq!(stats.checksum_verified, 0);

        // Independent verification: download via SDK and compare
        let downloaded = helper.get_object_bytes(&bucket, "rt.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip metadata content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_meta.txt", content);

        let target = format!("s3://{}/rt_meta.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // Verify all metadata (8 assertions inside)
        assert!(
            helper
                .verify_test_object_metadata(&bucket, "rt_meta.txt", None)
                .await
        );

        // Verify content independently
        let downloaded = helper.get_object_bytes(&bucket, "rt_meta.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip storage class content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_sc.txt", content);

        let target = format!("s3://{}/rt_sc.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "rt_sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);

        let downloaded = helper.get_object_bytes(&bucket, "rt_sc.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip sse kms content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_kms.txt", content);

        let target = format!("s3://{}/rt_kms.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "rt_kms.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        let downloaded = helper.get_object_bytes(&bucket, "rt_kms.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip dsse kms content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_dsse.txt", content);

        let target = format!("s3://{}/rt_dsse.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "rt_dsse.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        let downloaded = helper.get_object_bytes(&bucket, "rt_dsse.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "rt_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_large.bin", bucket);
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
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "rt_large.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        // Verify content via SHA256 comparison
        let downloaded = helper.get_object_bytes(&bucket, "rt_large.bin", None).await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_disable_content_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip no md5 header content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_nomd5.txt", content);

        let target = format!("s3://{}/rt_nomd5.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper.get_object_bytes(&bucket, "rt_nomd5.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_disable_payload_signing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip no payload signing content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_nosign.txt", content);

        let target = format!("s3://{}/rt_nosign.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_nosign.txt", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_disable_payload_signing_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_nosign_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_nosign_large.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_nosign_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_acl() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip acl content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_acl.txt", content);

        let target = format!("s3://{}/rt_acl.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper.get_object_bytes(&bucket, "rt_acl.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_acl_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_acl_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_acl_large.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_acl_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_website_redirect() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip website redirect content";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_redir.txt", content);

        let target = format!("s3://{}/rt_redir.txt", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket, "rt_redir.txt", None).await;
        assert_eq!(
            head.website_redirect_location().unwrap(),
            TEST_WEBSITE_REDIRECT
        );

        let downloaded = helper.get_object_bytes(&bucket, "rt_redir.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
