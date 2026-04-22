#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use aws_sdk_s3::types::{ChecksumType, ServerSideEncryption};

    use super::*;
    use common::*;

    // ---------------------------------------------------------------
    // Local to S3: single part, per-algorithm
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_local_to_s3_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip checksum sha256";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_sha256.dat", content);

        let target = format!("s3://{}/rt_sha256.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_sha256.dat", None).await;
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_sha256.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip checksum crc32";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_crc32.dat", content);

        let target = format!("s3://{}/rt_crc32.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_crc32.dat", None).await;
        assert!(head.checksum_crc32().is_some());

        let downloaded = helper.get_object_bytes(&bucket, "rt_crc32.dat", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip checksum crc32c";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_crc32c.dat", content);

        let target = format!("s3://{}/rt_crc32c.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_crc32c.dat", None).await;
        assert!(head.checksum_crc32_c().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_crc32c.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip checksum sha1";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_sha1.dat", content);

        let target = format!("s3://{}/rt_sha1.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_sha1.dat", None).await;
        assert!(head.checksum_sha1().is_some());

        let downloaded = helper.get_object_bytes(&bucket, "rt_sha1.dat", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip checksum crc64nvme";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_crc64.dat", content);

        let target = format!("s3://{}/rt_crc64.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_crc64.dat", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        let downloaded = helper.get_object_bytes(&bucket, "rt_crc64.dat", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Local to S3: multipart, per-algorithm
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_sha256.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_sha256.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_mp_sha256.bin", None).await;
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_sha256.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_crc32.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_crc32.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_mp_crc32.bin", None).await;
        assert!(head.checksum_crc32().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_crc32.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_crc64.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_crc64.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_mp_crc64.bin", None).await;
        assert!(head.checksum_crc64_nvme().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_crc64.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // S3 to S3: checksum verified + presence on target + content
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_s3_to_s3_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with SHA256
        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip s2s checksum sha256";
        TestHelper::create_test_file(&local_dir, "src_sha256.dat", content);
        let local_source = format!("{}/src_sha256.dat", local_dir.display());
        let s3_source = format!("s3://{}/rt_sha256.dat", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/rt_sha256.dat", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "rt_sha256.dat", None).await;
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_sha256.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_s3_to_s3_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip s2s checksum crc32";
        TestHelper::create_test_file(&local_dir, "src_crc32.dat", content);
        let local_source = format!("{}/src_crc32.dat", local_dir.display());
        let s3_source = format!("s3://{}/rt_crc32.dat", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/rt_crc32.dat", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "rt_crc32.dat", None).await;
        assert!(head.checksum_crc32().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_crc32.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Combo: KMS + checksum
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_local_to_s3_kms_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip kms sha256 combo";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_kms_sha256.dat", content);

        let target = format!("s3://{}/rt_kms_sha256.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_kms_sha256.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_kms_sha256.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_kms_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip kms crc32 combo";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_kms_crc32.dat", content);

        let target = format!("s3://{}/rt_kms_crc32.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket, "rt_kms_crc32.dat", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_crc32().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_kms_crc32.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_dsse_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip dsse sha256 combo";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_dsse_sha256.dat", content);

        let target = format!("s3://{}/rt_dsse_sha256.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_dsse_sha256.dat", None)
            .await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_dsse_sha256.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_kms_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_kms_sha256.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_kms_sha256.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_mp_kms_sha256.bin", None)
            .await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_kms_sha256.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Combo: disable-content-md5 + checksum
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_local_to_s3_disable_md5_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip no md5 sha256 combo";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_nomd5_sha256.dat", content);

        let target = format!("s3://{}/rt_nomd5_sha256.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_nomd5_sha256.dat", None)
            .await;
        assert!(head.checksum_sha256().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_nomd5_sha256.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_disable_md5_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"roundtrip no md5 crc64 combo";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_nomd5_crc64.dat", content);

        let target = format!("s3://{}/rt_nomd5_crc64.dat", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_nomd5_crc64.dat", None)
            .await;
        assert!(head.checksum_crc64_nvme().is_some());

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_nomd5_crc64.dat", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // ---------------------------------------------------------------
    // Full-object checksum (multipart)
    // ---------------------------------------------------------------

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_full_object_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_foc_crc32.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_foc_crc32.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_mp_foc_crc32.bin", None)
            .await;
        assert!(head.checksum_crc32().is_some());
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_foc_crc32.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_local_to_s3_multipart_full_object_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_mp_foc_crc64.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_mp_foc_crc64.bin", bucket);
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket, "rt_mp_foc_crc64.bin", None)
            .await;
        assert!(head.checksum_crc64_nvme().is_some());
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_mp_foc_crc64.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
