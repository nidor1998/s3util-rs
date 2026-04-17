#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_sdk_s3::types::{ServerSideEncryption, StorageClass};

    use common::*;

    use super::*;

    #[tokio::test]
    async fn s3_to_s3_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source object
        helper
            .put_object(&bucket1, "copy_test.txt", b"copy me".to_vec())
            .await;

        let source = format!("s3://{}/copy_test.txt", bucket1);
        let target = format!("s3://{}/copy_test.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(
            helper
                .is_object_exist(&bucket2, "copy_test.txt", None)
                .await
        );
        helper
            .verify_object_content_md5(&bucket2, "copy_test.txt", b"copy me")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "ssc_test.txt", b"server side copy".to_vec())
            .await;

        let source = format!("s3://{}/ssc_test.txt", bucket1);
        let target = format!("s3://{}/ssc_test.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);

        assert!(helper.is_object_exist(&bucket2, "ssc_test.txt", None).await);
        helper
            .verify_object_content_md5(&bucket2, "ssc_test.txt", b"server side copy")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "sc.txt", b"storage class copy".to_vec())
            .await;

        let source = format!("s3://{}/sc.txt", bucket1);
        let target = format!("s3://{}/sc.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--storage-class",
                "STANDARD_IA",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);
        helper
            .verify_object_content_md5(&bucket2, "sc.txt", b"storage class copy")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "meta.txt", b"metadata copy test".to_vec())
            .await;

        let source = format!("s3://{}/meta.txt", bucket1);
        let target = format!("s3://{}/meta.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "meta.txt", None).await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);
        assert_eq!(head.metadata().unwrap(), &TEST_METADATA.clone());
        helper
            .verify_object_content_md5(&bucket2, "meta.txt", b"metadata copy test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_large_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "large_copy.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/large_copy.bin", bucket1);
        let target = format!("s3://{}/large_copy.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "large_copy.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "large_copy.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_large_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_large.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_large.bin", bucket1);
        let target = format!("s3://{}/ssc_large.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_large.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_large.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "kms.txt", b"kms copy test".to_vec())
            .await;

        let source = format!("s3://{}/kms.txt", bucket1);
        let target = format!("s3://{}/kms.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "kms.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        helper
            .verify_object_content_md5(&bucket2, "kms.txt", b"kms copy test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "dsse.txt", b"dsse kms copy test".to_vec())
            .await;

        let source = format!("s3://{}/dsse.txt", bucket1);
        let target = format!("s3://{}/dsse.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms:dsse",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "dsse.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );
        helper
            .verify_object_content_md5(&bucket2, "dsse.txt", b"dsse kms copy test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with SHA256 checksum via cp
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "crc32_src.txt", b"crc32 checksum test");
        let local_source = format!("{}/crc32_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/crc32_src.txt", bucket1);
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

        let target = format!("s3://{}/crc32_src.txt", bucket2);
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
        helper
            .verify_object_content_md5(&bucket2, "crc32_src.txt", b"crc32 checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "crc32c_src.txt", b"crc32c checksum test");
        let local_source = format!("{}/crc32c_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/crc32c_src.txt", bucket1);
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

        let target = format!("s3://{}/crc32c_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        helper
            .verify_object_content_md5(&bucket2, "crc32c_src.txt", b"crc32c checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "sha1_src.txt", b"sha1 checksum test");
        let local_source = format!("{}/sha1_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/sha1_src.txt", bucket1);
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

        let target = format!("s3://{}/sha1_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        helper
            .verify_object_content_md5(&bucket2, "sha1_src.txt", b"sha1 checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "crc64_src.txt", b"crc64nvme checksum test");
        let local_source = format!("{}/crc64_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/crc64_src.txt", bucket1);
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

        let target = format!("s3://{}/crc64_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        helper
            .verify_object_content_md5(&bucket2, "crc64_src.txt", b"crc64nvme checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "kms_sha256.txt", b"kms and sha256 test".to_vec())
            .await;

        let source = format!("s3://{}/kms_sha256.txt", bucket1);
        let target = format!("s3://{}/kms_sha256.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA256",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "kms_sha256.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_sha256.txt", b"kms and sha256 test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "ssc_meta.txt", b"ssc metadata test".to_vec())
            .await;

        let source = format!("s3://{}/ssc_meta.txt", bucket1);
        let target = format!("s3://{}/ssc_meta.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_meta.txt", None).await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);
        assert_eq!(head.metadata().unwrap(), &TEST_METADATA.clone());
        helper
            .verify_object_content_md5(&bucket2, "ssc_meta.txt", b"ssc metadata test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "ssc_sc.txt", b"ssc storage class test".to_vec())
            .await;

        let source = format!("s3://{}/ssc_sc.txt", bucket1);
        let target = format!("s3://{}/ssc_sc.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--storage-class",
                "STANDARD_IA",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);
        helper
            .verify_object_content_md5(&bucket2, "ssc_sc.txt", b"ssc storage class test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "ssc_sha256.txt",
                b"ssc sha256 checksum test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/ssc_sha256.txt", bucket1);
        let target = format!("s3://{}/ssc_sha256.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--additional-checksum-algorithm",
                "SHA256",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        helper
            .verify_object_content_md5(&bucket2, "ssc_sha256.txt", b"ssc sha256 checksum test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_auto_chunk.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_auto_chunk.bin", bucket1);
        let target = format!("s3://{}/ssc_auto_chunk.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_auto_chunk.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_disable_payload_signing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "no_sign.txt",
                b"disable payload signing test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/no_sign.txt", bucket1);
        let target = format!("s3://{}/no_sign.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-payload-signing",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket2, "no_sign.txt", None).await);
        helper
            .verify_object_content_md5(&bucket2, "no_sign.txt", b"disable payload signing test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_disable_payload_signing_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "no_sign_large.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/no_sign_large.bin", bucket1);
        let target = format!("s3://{}/no_sign_large.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-payload-signing",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "no_sign_large.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "no_sign_large.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_acl() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "acl.txt", b"acl test".to_vec())
            .await;

        let source = format!("s3://{}/acl.txt", bucket1);
        let target = format!("s3://{}/acl.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--acl",
                "bucket-owner-full-control",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket2, "acl.txt", None).await);
        helper
            .verify_object_content_md5(&bucket2, "acl.txt", b"acl test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_copy_source_if_match() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "if_match.txt",
                b"copy source if match test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/if_match.txt", bucket1);
        let target = format!("s3://{}/if_match.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--copy-source-if-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket2, "if_match.txt", None).await);
        helper
            .verify_object_content_md5(&bucket2, "if_match.txt", b"copy source if match test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_sha256.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_sha256.bin", bucket1);
        let target = format!("s3://{}/mp_sha256.bin", bucket2);
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
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "mp_sha256.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_sha256.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_kms.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_kms.bin", bucket1);
        let target = format!("s3://{}/mp_kms.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "mp_kms.bin", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        let bytes = helper.get_object_bytes(&bucket2, "mp_kms.bin", None).await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MB with 5MiB chunks via cp
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&tmp_dir, "mp_auto_chunk.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/mp_auto_chunk.bin", tmp_dir.display());
        let s3_source = format!("s3://{}/mp_auto_chunk.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-chunksize",
                "5242880",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/mp_auto_chunk.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_auto_chunk.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "ssc_kms.txt", b"ssc kms encryption test".to_vec())
            .await;

        let source = format!("s3://{}/ssc_kms.txt", bucket1);
        let target = format!("s3://{}/ssc_kms.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_kms.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        helper
            .verify_object_content_md5(&bucket2, "ssc_kms.txt", b"ssc kms encryption test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_full_object_checksum_crc32() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_full_crc32.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_full_crc32.bin", bucket1);
        let target = format!("s3://{}/mp_full_crc32.bin", bucket2);
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
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "mp_full_crc32.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_full_crc32.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_full_object_checksum_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_full_crc64.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_full_crc64.bin", bucket1);
        let target = format!("s3://{}/mp_full_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "mp_full_crc64.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_full_crc64.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_if_none_match() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "if_none_s2s.txt",
                b"if-none-match s3 to s3 test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/if_none_s2s.txt", bucket1);
        let target = format!("s3://{}/if_none_s2s.txt", bucket2);

        // First copy: target doesn't exist, should succeed
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        helper
            .verify_object_content_md5(&bucket2, "if_none_s2s.txt", b"if-none-match s3 to s3 test")
            .await;

        // Second copy: target exists, should fail with precondition error
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert_eq!(stats.sync_error, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with SSE-C into bucket1 via cp
        let tmp_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&tmp_dir, "ssec_mp_src.bin", 9 * 1024 * 1024);
        let s3_source = format!("s3://{}/ssec_mp.bin", bucket1);
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
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/ssec_mp.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload small object with SSE-C into bucket1 via cp
        let tmp_dir = TestHelper::create_temp_dir();
        let upload_file = TestHelper::create_test_file(
            &tmp_dir,
            "ssec_ssc_src.txt",
            b"sse-c server side copy test",
        );
        let s3_source = format!("s3://{}/ssec_ssc.txt", bucket1);
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
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/ssec_ssc.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_multipart_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with SSE-C into bucket1 via cp (triggers multipart upload)
        let tmp_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&tmp_dir, "ssec_ssc_mp_src.bin", 9 * 1024 * 1024);
        let s3_source = format!("s3://{}/ssec_ssc_mp.bin", bucket1);
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
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/ssec_ssc_mp.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "ssc_all_meta.txt",
                b"ssc all metadata test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/ssc_all_meta.txt", bucket1);
        let target = format!("s3://{}/ssc_all_meta.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_test_object_metadata(&bucket2, "ssc_all_meta.txt", None)
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_all_metadata() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_all_meta.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_all_meta.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_all_meta.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
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
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper
            .verify_test_object_metadata(&bucket2, "ssc_mp_all_meta.bin", None)
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_tagging() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "tagging.txt", b"tagging test".to_vec())
            .await;

        let source = format!("s3://{}/tagging.txt", bucket1);
        let target = format!("s3://{}/tagging.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--tagging",
                TEST_TAGGING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let tagging = helper
            .get_object_tagging(&bucket2, "tagging.txt", None)
            .await;
        let tag_map = TestHelper::tag_set_to_map(tagging.tag_set());
        let expected = HashMap::from([
            ("tag1".to_string(), "tag_value1".to_string()),
            ("tag2".to_string(), "tag_value2".to_string()),
        ]);
        assert_eq!(tag_map, expected);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_tagging() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "ssc_tagging.txt", b"ssc tagging test".to_vec())
            .await;

        let source = format!("s3://{}/ssc_tagging.txt", bucket1);
        let target = format!("s3://{}/ssc_tagging.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--tagging",
                TEST_TAGGING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let tagging = helper
            .get_object_tagging(&bucket2, "ssc_tagging.txt", None)
            .await;
        let tag_map = TestHelper::tag_set_to_map(tagging.tag_set());
        let expected = HashMap::from([
            ("tag1".to_string(), "tag_value1".to_string()),
            ("tag2".to_string(), "tag_value2".to_string()),
        ]);
        assert_eq!(tag_map, expected);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_special_chars() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "c++☃test";
        helper
            .put_object(&bucket1, key, b"special chars ssc test".to_vec())
            .await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket2, key, None).await);
        helper
            .verify_object_content_md5(&bucket2, key, b"special chars ssc test")
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_special_chars_multipart() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "c++☃test";
        helper
            .put_sized_object(&bucket1, key, 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/{}", bucket1, key);
        let target = format!("s3://{}/{}", bucket2, key);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, key, None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper.get_object_bytes(&bucket2, key, None).await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_meta.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_meta.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_meta.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_mp_meta.bin", None).await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);
        assert_eq!(head.metadata().unwrap(), &TEST_METADATA.clone());
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_meta.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_auto_chunksize_metadata() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_auto_meta.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_auto_meta.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_auto_meta.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                "--cache-control",
                TEST_CACHE_CONTROL,
                "--content-type",
                TEST_CONTENT_TYPE,
                "--metadata",
                TEST_METADATA_STRING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "ssc_mp_auto_meta.bin", None)
            .await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);
        assert_eq!(head.metadata().unwrap(), &TEST_METADATA.clone());
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_auto_meta.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_website_redirect() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(
                &bucket1,
                "ssc_redirect.txt",
                b"ssc website redirect test".to_vec(),
            )
            .await;

        let source = format!("s3://{}/ssc_redirect.txt", bucket1);
        let target = format!("s3://{}/ssc_redirect.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "ssc_redirect.txt", None).await;
        assert_eq!(head.website_redirect_location().unwrap(), "/redirect");

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_with_website_redirect() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_redirect.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_redirect.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_redirect.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "ssc_mp_redirect.bin", None)
            .await;
        assert_eq!(head.website_redirect_location().unwrap(), "/redirect");
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_full_object_crc32() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_full_crc32.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_full_crc32.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_full_crc32.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "ssc_mp_full_crc32.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_full_crc32.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_full_object_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "ssc_mp_full_crc32c.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/ssc_mp_full_crc32c.bin", bucket1);
        let target = format!("s3://{}/ssc_mp_full_crc32c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper
            .head_object(&bucket2, "ssc_mp_full_crc32c.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_full_crc32c.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_with_sse_c_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with SSE-C and 5MiB chunks into bucket1 via cp
        let tmp_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_sized_file(&tmp_dir, "ssec_ssc_auto_src.bin", 9 * 1024 * 1024);
        let s3_source = format!("s3://{}/ssec_ssc_auto.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-chunksize",
                "5242880",
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                upload_file.to_str().unwrap(),
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/ssec_ssc_auto.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                "--source-sse-c",
                "AES256",
                "--source-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--source-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                "--target-sse-c",
                "AES256",
                "--target-sse-c-key",
                TEST_SSE_C_KEY_1,
                "--target-sse-c-key-md5",
                TEST_SSE_C_KEY_1_MD5,
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }
}
