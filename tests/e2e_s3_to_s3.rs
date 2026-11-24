#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aws_sdk_s3::types::{ChecksumType, ServerSideEncryption, StorageClass};

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--storage-class",
                "STANDARD_IA",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
    async fn s3_to_s3_with_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_upload.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_upload.bin", bucket1);
        let target = format!("s3://{}/mp_upload.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket2, "mp_upload.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_upload.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_disable_multipart_verify() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_no_verify.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_no_verify.bin", bucket1);
        let target = format!("s3://{}/mp_no_verify.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--disable-multipart-verify",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms:dsse",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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

        // Upload source with CRC32 checksum via cp
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "crc32_src.txt", b"crc32 checksum test");
        let local_source = format!("{}/crc32_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/crc32_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA1",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
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
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_object_content_md5(&bucket2, "crc64_src.txt", b"crc64nvme checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with SHA256 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &tmp_dir,
            "sha256_add_src.txt",
            b"sha256 additional checksum test",
        );
        let local_source = format!("{}/sha256_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/sha256_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/sha256_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        helper
            .verify_object_content_md5(
                &bucket2,
                "sha256_add_src.txt",
                b"sha256 additional checksum test",
            )
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with SHA512 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &tmp_dir,
            "sha512_add_src.txt",
            b"sha512 additional checksum test",
        );
        let local_source = format!("{}/sha512_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/sha512_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/sha512_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "sha512_add_src.txt", None)
            .await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"sha512 additional checksum test")
        );
        helper
            .verify_object_content_md5(
                &bucket2,
                "sha512_add_src.txt",
                b"sha512 additional checksum test",
            )
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with MD5 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "md5_add_src.txt", b"md5 additional checksum test");
        let local_source = format!("{}/md5_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/md5_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/md5_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "md5_add_src.txt", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"md5 additional checksum test")
        );
        helper
            .verify_object_content_md5(&bucket2, "md5_add_src.txt", b"md5 additional checksum test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with XXHASH128 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &tmp_dir,
            "xxhash128_add_src.txt",
            b"xxhash128 additional checksum test",
        );
        let local_source = format!("{}/xxhash128_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/xxhash128_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/xxhash128_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "xxhash128_add_src.txt", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum(
                "XXHASH128",
                b"xxhash128 additional checksum test"
            )
        );
        helper
            .verify_object_content_md5(
                &bucket2,
                "xxhash128_add_src.txt",
                b"xxhash128 additional checksum test",
            )
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with XXHASH3 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &tmp_dir,
            "xxhash3_add_src.txt",
            b"xxhash3 additional checksum test",
        );
        let local_source = format!("{}/xxhash3_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/xxhash3_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/xxhash3_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "xxhash3_add_src.txt", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"xxhash3 additional checksum test")
        );
        helper
            .verify_object_content_md5(
                &bucket2,
                "xxhash3_add_src.txt",
                b"xxhash3 additional checksum test",
            )
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_additional_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload source with XXHASH64 additional checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &tmp_dir,
            "xxhash64_add_src.txt",
            b"xxhash64 additional checksum test",
        );
        let local_source = format!("{}/xxhash64_add_src.txt", tmp_dir.display());
        let s3_source = format!("s3://{}/xxhash64_add_src.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/xxhash64_add_src.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "xxhash64_add_src.txt", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum(
                "XXHASH64",
                b"xxhash64 additional checksum test"
            )
        );
        helper
            .verify_object_content_md5(
                &bucket2,
                "xxhash64_add_src.txt",
                b"xxhash64 additional checksum test",
            )
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

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_sha256.txt", b"kms and sha256 test");
        let local_source = format!("{}/kms_sha256.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_sha256.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_sha256.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "kms_sha256.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_sha256.txt", b"kms and sha256 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_sha512.txt", b"kms and sha512 test");
        let local_source = format!("{}/kms_sha512.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_sha512.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_sha512.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA512",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "kms_sha512.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"kms and sha512 test")
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_sha512.txt", b"kms and sha512 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_md5.txt", b"kms and md5 test");
        let local_source = format!("{}/kms_md5.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_md5.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_md5.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "MD5",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "kms_md5.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"kms and md5 test")
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_md5.txt", b"kms and md5 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_xxhash128.txt", b"kms and xxhash128 test");
        let local_source = format!("{}/kms_xxhash128.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_xxhash128.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_xxhash128.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "kms_xxhash128.txt", None)
            .await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH128", b"kms and xxhash128 test")
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_xxhash128.txt", b"kms and xxhash128 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_xxhash3.txt", b"kms and xxhash3 test");
        let local_source = format!("{}/kms_xxhash3.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_xxhash3.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_xxhash3.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "kms_xxhash3.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"kms and xxhash3 test")
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_xxhash3.txt", b"kms and xxhash3 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_kms_and_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&tmp_dir, "kms_xxhash64.txt", b"kms and xxhash64 test");
        let local_source = format!("{}/kms_xxhash64.txt", tmp_dir.display());
        let source = format!("s3://{}/kms_xxhash64.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/kms_xxhash64.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "kms_xxhash64.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH64", b"kms and xxhash64 test")
        );
        helper
            .verify_object_content_md5(&bucket2, "kms_xxhash64.txt", b"kms and xxhash64 test")
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--storage-class",
                "STANDARD_IA",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&local_dir, "ssc_sha256.txt", b"ssc sha256 checksum test");
        let local_source = format!("{}/ssc_sha256.txt", local_dir.display());
        let source = format!("s3://{}/ssc_sha256.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_sha256.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        helper
            .verify_object_content_md5(&bucket2, "ssc_sha256.txt", b"ssc sha256 checksum test")
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&local_dir, "ssc_sha512.txt", b"ssc sha512 checksum test");
        let local_source = format!("{}/ssc_sha512.txt", local_dir.display());
        let source = format!("s3://{}/ssc_sha512.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_sha512.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "ssc_sha512.txt", None).await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            TestHelper::compute_additional_checksum("SHA512", b"ssc sha512 checksum test")
        );
        helper
            .verify_object_content_md5(&bucket2, "ssc_sha512.txt", b"ssc sha512 checksum test")
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&local_dir, "ssc_md5.txt", b"ssc md5 checksum test");
        let local_source = format!("{}/ssc_md5.txt", local_dir.display());
        let source = format!("s3://{}/ssc_md5.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_md5.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "ssc_md5.txt", None).await;
        assert_eq!(
            head.checksum_md5().unwrap(),
            TestHelper::compute_additional_checksum("MD5", b"ssc md5 checksum test")
        );
        helper
            .verify_object_content_md5(&bucket2, "ssc_md5.txt", b"ssc md5 checksum test")
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &local_dir,
            "ssc_xxhash128.txt",
            b"ssc xxhash128 checksum test",
        );
        let local_source = format!("{}/ssc_xxhash128.txt", local_dir.display());
        let source = format!("s3://{}/ssc_xxhash128.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_xxhash128.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "ssc_xxhash128.txt", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH128", b"ssc xxhash128 checksum test")
        );
        helper
            .verify_object_content_md5(
                &bucket2,
                "ssc_xxhash128.txt",
                b"ssc xxhash128 checksum test",
            )
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(&local_dir, "ssc_xxhash3.txt", b"ssc xxhash3 checksum test");
        let local_source = format!("{}/ssc_xxhash3.txt", local_dir.display());
        let source = format!("s3://{}/ssc_xxhash3.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_xxhash3.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "ssc_xxhash3.txt", None).await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH3", b"ssc xxhash3 checksum test")
        );
        helper
            .verify_object_content_md5(&bucket2, "ssc_xxhash3.txt", b"ssc xxhash3 checksum test")
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        TestHelper::create_test_file(
            &local_dir,
            "ssc_xxhash64.txt",
            b"ssc xxhash64 checksum test",
        );
        let local_source = format!("{}/ssc_xxhash64.txt", local_dir.display());
        let source = format!("s3://{}/ssc_xxhash64.txt", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &local_source,
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_xxhash64.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "ssc_xxhash64.txt", None).await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            TestHelper::compute_additional_checksum("XXHASH64", b"ssc xxhash64 checksum test")
        );
        helper
            .verify_object_content_md5(&bucket2, "ssc_xxhash64.txt", b"ssc xxhash64 checksum test")
            .await;

        std::fs::remove_dir_all(&local_dir).ok();
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--disable-payload-signing",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--disable-payload-signing",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--acl",
                "bucket-owner-full-control",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        assert!(helper.is_object_exist(&bucket2, "acl.txt", None).await);
        helper
            .verify_object_content_md5(&bucket2, "acl.txt", b"acl test")
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

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_sha256.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_sha256.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_sha256.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_sha256.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_sha256.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_sha512.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_sha512.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_sha512.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_sha512.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(head.checksum_sha512().unwrap(), SHA512_9M_ZEROS_8M_CHUNK);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_sha512.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_md5.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_md5.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_md5.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_md5.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(head.checksum_md5().unwrap(), MD5_9M_ZEROS_8M_CHUNK);
        let bytes = helper.get_object_bytes(&bucket2, "mp_md5.bin", None).await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash128.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash128.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash128.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_xxhash128.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&test_file).unwrap(),
                8 * 1024 * 1024
            )
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash128.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash3.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash3.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash3.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_xxhash3.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&test_file).unwrap(),
                8 * 1024 * 1024
            )
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash3.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash64.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash64.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_xxhash64.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_9M_ZEROS_8M_CHUNK
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash64.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + SHA256 (composite
    /// 2-part SHA256 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// SHA256 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite SHA256 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_sha256_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_sha256_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_sha256_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_sha256_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_sha256_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + SHA512 (composite
    /// 2-part SHA512 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// SHA512 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite SHA512 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_sha512_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_sha512_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_sha512_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA512",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_sha512_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // Source: composite over 5 MiB + 4 MiB parts; target: composite
        // over 8 MiB + 1 MiB parts. Same bytes, different part boundaries.
        let source_head = helper.head_object(&bucket1, "mp_sha512_ng.bin", None).await;
        assert_eq!(
            source_head.checksum_sha512().unwrap(),
            SHA512_9M_ZEROS_5M_CHUNK
        );
        let target_head = helper.head_object(&bucket2, "mp_sha512_ng.bin", None).await;
        assert_eq!(
            target_head.checksum_sha512().unwrap(),
            SHA512_9M_ZEROS_8M_CHUNK
        );

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_sha512_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + MD5 (composite
    /// 2-part MD5 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// MD5 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite MD5 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_md5_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_sized_file(&local_dir, "mp_md5_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_md5_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "MD5",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_md5_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // Source: composite over 5 MiB + 4 MiB parts; target: composite
        // over 8 MiB + 1 MiB parts. Same bytes, different part boundaries.
        let source_head = helper.head_object(&bucket1, "mp_md5_ng.bin", None).await;
        assert_eq!(source_head.checksum_md5().unwrap(), MD5_9M_ZEROS_5M_CHUNK);
        let target_head = helper.head_object(&bucket2, "mp_md5_ng.bin", None).await;
        assert_eq!(target_head.checksum_md5().unwrap(), MD5_9M_ZEROS_8M_CHUNK);

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_md5_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + XXHASH128 (composite
    /// 2-part XXHASH128 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// XXHASH128 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite XXHASH128 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash128_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash128_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash128_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "XXHASH128",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash128_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // Source: composite over 5 MiB + 4 MiB parts; target: composite
        // over 8 MiB + 1 MiB parts. Same bytes, different part boundaries.
        let source_head = helper
            .head_object(&bucket1, "mp_xxhash128_ng.bin", None)
            .await;
        assert_eq!(
            source_head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&test_file).unwrap(),
                5 * 1024 * 1024
            )
        );
        let target_head = helper
            .head_object(&bucket2, "mp_xxhash128_ng.bin", None)
            .await;
        assert_eq!(
            target_head.checksum_xxhash128().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &std::fs::read(&test_file).unwrap(),
                8 * 1024 * 1024
            )
        );

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash128_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + XXHASH3 (composite
    /// 2-part XXHASH3 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// XXHASH3 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite XXHASH3 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash3_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash3_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash3_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "XXHASH3",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash3_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH3",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // Source: composite over 5 MiB + 4 MiB parts; target: composite
        // over 8 MiB + 1 MiB parts. Same bytes, different part boundaries.
        let source_head = helper
            .head_object(&bucket1, "mp_xxhash3_ng.bin", None)
            .await;
        assert_eq!(
            source_head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&test_file).unwrap(),
                5 * 1024 * 1024
            )
        );
        let target_head = helper
            .head_object(&bucket2, "mp_xxhash3_ng.bin", None)
            .await;
        assert_eq!(
            target_head.checksum_xxhash3().unwrap(),
            TestHelper::compute_composite_additional_checksum(
                "XXHASH3",
                &std::fs::read(&test_file).unwrap(),
                8 * 1024 * 1024
            )
        );

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash3_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Source uploaded as multipart with 5 MiB chunks + XXHASH64 (composite
    /// 2-part XXHASH64 over 5+4 MiB). The s3-to-s3 copy uses 8 MiB chunks +
    /// XXHASH64 with no `--auto-chunksize`, so the target ends up as a 2-part
    /// composite XXHASH64 over 8+1 MiB. Composite checksums and multipart
    /// ETags both depend on part boundaries → both mismatch the source.
    /// Per upload_manager: a remote-multipart, non-full-object checksum
    /// mismatch is treated as a warning (not fatal), so the copy still
    /// completes — it just emits ETagMismatch + ChecksumMismatch (both
    /// counted under `sync_warning` by the test helper).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_checksum_xxhash64_chunksize_mismatch_warns() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_xxhash64_ng.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_xxhash64_ng.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "XXHASH64",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_xxhash64_ng.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "8MiB",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);
        // ETagMismatch + ChecksumMismatch — the helper counts both under
        // sync_warning via its SyncStatistics → StatsCount mapping.
        assert_eq!(stats.sync_warning, 2);
        assert!(stats.has_warning_flag);

        // Source: composite over 5 MiB + 4 MiB parts; target: composite
        // over 8 MiB + 1 MiB parts. Same bytes, different part boundaries.
        let source_head = helper
            .head_object(&bucket1, "mp_xxhash64_ng.bin", None)
            .await;
        assert_eq!(
            source_head.checksum_xxhash64().unwrap(),
            XXHASH64_9M_ZEROS_5M_CHUNK
        );
        let target_head = helper
            .head_object(&bucket2, "mp_xxhash64_ng.bin", None)
            .await;
        assert_eq!(
            target_head.checksum_xxhash64().unwrap(),
            XXHASH64_9M_ZEROS_8M_CHUNK
        );

        // The body itself was transferred intact — the warning is purely
        // about the composite-checksum / ETag boundaries differing.
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_xxhash64_ng.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
    async fn s3_to_s3_with_multipart_upload_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_sized_object(&bucket1, "mp_dsse_kms.bin", 9 * 1024 * 1024)
            .await;

        let source = format!("s3://{}/mp_dsse_kms.bin", bucket1);
        let target = format!("s3://{}/mp_dsse_kms.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms:dsse",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket2, "mp_dsse_kms.bin", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_dsse_kms.bin", None)
            .await;
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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_auto_chunk.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_auto_chunksize_disable_payload_signing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with 7MiB chunks to produce a multipart object in source bucket.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&tmp_dir, "mp_nosign_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/mp_nosign_src.bin", tmp_dir.display());
        let s3_source = format!("s3://{}/mp_nosign.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "7340033",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/mp_nosign.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--disable-payload-signing",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket2, "mp_nosign.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_nosign.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_auto_chunksize_sha1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with 7MiB chunks + SHA1 checksum to produce a multipart
        // object with SHA1 additional checksum in the source bucket.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&tmp_dir, "mp_sha1_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/mp_sha1_src.bin", tmp_dir.display());
        let s3_source = format!("s3://{}/mp_sha1.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-chunksize",
                "7340033",
                "--additional-checksum-algorithm",
                "SHA1",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/mp_sha1.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--additional-checksum-algorithm",
                "SHA1",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, "mp_sha1.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper.get_object_bytes(&bucket2, "mp_sha1.bin", None).await;
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--sse",
                "aws:kms",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_full_crc32.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_full_crc32.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_full_crc32.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "mp_full_crc32.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_full_crc32.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
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

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_full_crc64.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_full_crc64.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/mp_full_crc64.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "mp_full_crc64.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_full_crc64.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_crc32c_full_object_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB with CRC32C full-object checksum.
        let tmp_dir = TestHelper::create_temp_dir();
        TestHelper::create_sized_file(&tmp_dir, "mp_full_crc32c_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/mp_full_crc32c_src.bin", tmp_dir.display());
        let s3_source = format!("s3://{}/mp_full_crc32c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                &local_source,
                &s3_source,
            ])
            .await;

        let target = format!("s3://{}/mp_full_crc32c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "mp_full_crc32c.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        assert_eq!(head.checksum_type(), Some(&ChecksumType::FullObject));
        let bytes = helper
            .get_object_bytes(&bucket2, "mp_full_crc32c.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&tmp_dir).ok();
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_object_content_md5(&bucket2, "if_none_s2s.txt", b"if-none-match s3 to s3 test")
            .await;

        // Second copy: target exists, should fail with precondition error
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert_eq!(stats.sync_error, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_multipart_upload_if_none_match() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "mp_if_none.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/mp_if_none.bin", bucket1);
        let target = format!("s3://{}/mp_if_none.bin", bucket2);

        // Upload with explicit multipart chunking so source has a multipart ETag that
        // matches the target ETag produced by the subsequent S3→S3 copy.
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
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        // First copy: target doesn't exist, should succeed.
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // Second copy: target already exists, should fail with precondition error.
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--if-none-match",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 0);
        assert_eq!(stats.sync_error, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn s3_to_s3_with_sse_c_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        helper
            .verify_test_object_metadata(&bucket2, "ssc_mp_all_meta.bin", None)
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_metadata_test_with_multipart_upload() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Upload 9MiB object with full metadata to source bucket.
        let tmp_dir = TestHelper::create_temp_dir();
        let seed_file = TestHelper::create_sized_file(&tmp_dir, "mp_meta_src.bin", 9 * 1024 * 1024);
        helper
            .put_object_with_metadata(&bucket1, "mp_meta.bin", seed_file.to_str().unwrap())
            .await;

        let source = format!("s3://{}/mp_meta.bin", bucket1);
        let target = format!("s3://{}/mp_meta.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        helper
            .verify_test_object_metadata(&bucket2, "mp_meta.bin", None)
            .await;

        std::fs::remove_dir_all(&tmp_dir).ok();
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--tagging",
                TEST_TAGGING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--tagging",
                TEST_TAGGING,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
    async fn s3_to_s3_default_tag_propagation() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object_with_tagging(
                &bucket1,
                "default_tagging.txt",
                b"default tagging test".to_vec(),
                TEST_TAGGING,
            )
            .await;

        let source = format!("s3://{}/default_tagging.txt", bucket1);
        let target = format!("s3://{}/default_tagging.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let tagging = helper
            .get_object_tagging(&bucket2, "default_tagging.txt", None)
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
    async fn s3_to_s3_server_side_copy_default_tag_propagation() {
        TestHelper::init_dummy_tracing_subscriber();
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object_with_tagging(
                &bucket1,
                "ssc_default_tagging.txt",
                b"ssc default tagging test".to_vec(),
                TEST_TAGGING,
            )
            .await;

        let source = format!("s3://{}/ssc_default_tagging.txt", bucket1);
        let target = format!("s3://{}/ssc_default_tagging.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let tagging = helper
            .get_object_tagging(&bucket2, "ssc_default_tagging.txt", None)
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--website-redirect",
                "/redirect",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 1);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

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

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "ssc_mp_full_crc32.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/ssc_mp_full_crc32.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_mp_full_crc32.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC32",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "ssc_mp_full_crc32.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_full_crc32.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
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

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "ssc_mp_full_crc32c.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/ssc_mp_full_crc32c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                test_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/ssc_mp_full_crc32c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC32C",
                "--full-object-checksum",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket2, "ssc_mp_full_crc32c.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);
        let bytes = helper
            .get_object_bytes(&bucket2, "ssc_mp_full_crc32c.bin", None)
            .await;
        assert_eq!(TestHelper::get_sha256_from_bytes(&bytes), SHA256_9M_ZEROS);

        std::fs::remove_dir_all(&local_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_no_sync_system_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Seed source with an object that has full system metadata.
        let tmp_dir = TestHelper::create_temp_dir();
        let seed_file =
            TestHelper::create_test_file(&tmp_dir, "nssm.bin", b"no sync system metadata");
        helper
            .put_object_with_metadata(&bucket1, "nssm.bin", seed_file.to_str().unwrap())
            .await;

        let source = format!("s3://{}/nssm.bin", bucket1);
        let target = format!("s3://{}/nssm.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--no-sync-system-metadata",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        // Target should NOT have the system metadata (cache_control, content_disposition,
        // content_encoding, content_language, content_type, expires, website_redirect).
        let head = helper.head_object(&bucket2, "nssm.bin", None).await;
        assert!(head.cache_control().is_none());
        assert!(head.content_disposition().is_none());
        assert!(head.content_encoding().is_none());
        assert!(head.content_language().is_none());
        // Content-Type is always returned by S3; when cleared it should NOT match the seeded
        // TEST_CONTENT_TYPE (S3 reverts to a default like application/octet-stream).
        assert_ne!(head.content_type().unwrap(), TEST_CONTENT_TYPE);
        assert!(head.expires_string().is_none());
        assert!(head.website_redirect_location().is_none());

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_no_sync_user_defined_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Seed source with user-defined metadata (TEST_METADATA: key1=value1, key2=value2).
        let tmp_dir = TestHelper::create_temp_dir();
        let seed_file =
            TestHelper::create_test_file(&tmp_dir, "nsud.bin", b"no sync user defined metadata");
        helper
            .put_object_with_metadata(&bucket1, "nsud.bin", seed_file.to_str().unwrap())
            .await;

        let source = format!("s3://{}/nsud.bin", bucket1);
        let target = format!("s3://{}/nsud.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--no-sync-user-defined-metadata",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        let head = helper.head_object(&bucket2, "nsud.bin", None).await;
        // Target should have NO user-defined metadata (either None or empty map).
        match head.metadata() {
            None => {}
            Some(m) => assert!(m.is_empty(), "expected empty user metadata, got: {:?}", m),
        }

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_request_payer() {
        TestHelper::init_dummy_tracing_subscriber();

        // Limitation: Without a Requester-Pays-enabled bucket, we can only verify
        // that --source-request-payer / --target-request-payer are accepted by
        // the CLI and the copy succeeds. For non-requester-pays buckets the
        // header is ignored server-side.
        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper
            .put_object(&bucket1, "rp.txt", b"request payer".to_vec())
            .await;

        let source = format!("s3://{}/rp.txt", bucket1);
        let target = format!("s3://{}/rp.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--source-request-payer",
                "--target-request-payer",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        assert!(helper.is_object_exist(&bucket2, "rp.txt", None).await);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_with_sse_c_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper
            .create_bucket_with_sse_c_encryption(&bucket1, REGION)
            .await;
        helper
            .create_bucket_with_sse_c_encryption(&bucket2, REGION)
            .await;

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
                "s3util-e2e-test",
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
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
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
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 0);

        std::fs::remove_dir_all(&tmp_dir).ok();
        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn s3_to_s3_with_source_version_id() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let v1_content = b"v1 content";
        let v2_content = b"v2 content";
        let v1_id = helper
            .put_object_with_version(&bucket1, "ver_test.txt", v1_content.to_vec())
            .await;
        helper
            .put_object(&bucket1, "ver_test.txt", v2_content.to_vec())
            .await;

        let source = format!("s3://{}/ver_test.txt", bucket1);
        let target = format!("s3://{}/ver_test.txt", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--source-version-id",
                &v1_id,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);

        helper
            .verify_object_content_md5(&bucket2, "ver_test.txt", v1_content)
            .await;

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// `--source-version-id` with an MD5 additional checksum on a non-current
    /// version. Both versions carry an MD5 checksum of a different body. The
    /// target must get v1's bytes and checksum, and s3util must verify it
    /// against v1's checksum (HEAD), not v2's.
    #[tokio::test]
    async fn s3_to_s3_with_source_version_id_and_checksum_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "ver_md5.txt";
        let v1_content = b"v1 content with an MD5 checksum";
        let v2_content = b"v2 content, also with an MD5 checksum";
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(&bucket1, key, v1_content, "MD5", None)
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(&bucket1, key, v2_content, "MD5", None)
            .await;
        assert_ne!(v1_id, v2_id);

        let source = format!("s3://{bucket1}/{key}");
        let target = format!("s3://{bucket2}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "MD5",
                "--source-version-id",
                &v1_id,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        helper
            .verify_object_content_md5(&bucket2, key, v1_content)
            .await;
        let head = helper.head_object(&bucket2, key, None).await;
        assert_eq!(
            TestHelper::head_additional_checksum(&head, "MD5"),
            Some(TestHelper::compute_additional_checksum("MD5", v1_content)),
            "the target must carry v1's MD5 checksum"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Multipart `--source-version-id` with an XXHASH64 composite checksum.
    /// v1 (requested, non-current) is 9 MiB in 5 MiB parts; v2 (latest) is a
    /// different 9 MiB body in 8 MiB parts. With `--auto-chunksize` the target
    /// is uploaded in the source's part layout, so it carries v1's composite
    /// checksum only if the HEAD, the part lookups and every ranged GET all
    /// target v1.
    #[tokio::test]
    async fn s3_to_s3_multipart_with_source_version_id_and_checksum_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "ver_mp_xxhash64.bin";
        let v1_content = vec![0x01_u8; 9 * 1024 * 1024];
        let v2_content = vec![0x02_u8; 9 * 1024 * 1024];
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1,
                key,
                &v1_content,
                "XXHASH64",
                Some(5),
            )
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1,
                key,
                &v2_content,
                "XXHASH64",
                Some(8),
            )
            .await;
        assert_ne!(v1_id, v2_id);

        let source = format!("s3://{bucket1}/{key}");
        let target = format!("s3://{bucket2}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH64",
                "--source-version-id",
                &v1_id,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, key, None).await;
        assert_eq!(
            TestHelper::head_additional_checksum(&head, "XXHASH64"),
            Some(TestHelper::compute_composite_additional_checksum(
                "XXHASH64",
                &v1_content,
                5 * 1024 * 1024
            )),
            "the target must carry v1's composite XXHASH64 checksum (5 MiB parts)"
        );
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&helper.get_object_bytes(&bucket2, key, None).await),
            TestHelper::get_sha256_from_bytes(&v1_content),
            "the target must be v1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// `--server-side-copy` with `--source-version-id` and a SHA512
    /// additional checksum. S3 copies `?versionId=v1` and calculates the
    /// target's SHA512 itself; s3util then compares it with the source's
    /// checksum, which must be v1's (both versions carry a SHA512 checksum of
    /// a different body).
    #[tokio::test]
    async fn s3_to_s3_server_side_copy_with_source_version_id_and_checksum_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "ver_ssc_sha512.txt";
        let v1_content = b"v1 content for a server-side copy with SHA512";
        let v2_content = b"v2 content for a server-side copy, also SHA512";
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1, key, v1_content, "SHA512", None,
            )
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1, key, v2_content, "SHA512", None,
            )
            .await;
        assert_ne!(v1_id, v2_id);

        let source = format!("s3://{bucket1}/{key}");
        let target = format!("s3://{bucket2}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA512",
                "--source-version-id",
                &v1_id,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        helper
            .verify_object_content_md5(&bucket2, key, v1_content)
            .await;
        let head = helper.head_object(&bucket2, key, None).await;
        assert_eq!(
            TestHelper::head_additional_checksum(&head, "SHA512"),
            Some(TestHelper::compute_additional_checksum(
                "SHA512", v1_content
            )),
            "the target must carry v1's SHA512 checksum"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// Multipart `--server-side-copy` with `--source-version-id` and an
    /// XXHASH128 composite checksum. v1 (requested, non-current) is 9 MiB in
    /// 5 MiB parts; v2 (latest) is a different 9 MiB body in 8 MiB parts.
    /// `--auto-chunksize` takes the part layout from the source, every
    /// UploadPartCopy names `?versionId=v1` as its copy source, and S3
    /// calculates each target part's XXHASH128 itself. The target therefore
    /// carries v1's composite checksum only if the part lookups and every
    /// part copy target v1; s3util then verifies it against v1's checksum.
    #[tokio::test]
    async fn s3_to_s3_server_side_copy_multipart_with_source_version_id_and_checksum_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.enable_bucket_versioning(&bucket1).await;
        helper.create_bucket(&bucket2, REGION).await;

        let key = "ver_ssc_mp_xxhash128.bin";
        let v1_content = vec![0x01_u8; 9 * 1024 * 1024];
        let v2_content = vec![0x02_u8; 9 * 1024 * 1024];
        let v1_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1,
                key,
                &v1_content,
                "XXHASH128",
                Some(5),
            )
            .await;
        let v2_id = helper
            .put_versioned_object_with_additional_checksum(
                &bucket1,
                key,
                &v2_content,
                "XXHASH128",
                Some(8),
            )
            .await;
        assert_ne!(v1_id, v2_id);

        let source = format!("s3://{bucket1}/{key}");
        let target = format!("s3://{bucket2}/{key}");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--server-side-copy",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "XXHASH128",
                "--source-version-id",
                &v1_id,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let head = helper.head_object(&bucket2, key, None).await;
        assert_eq!(
            TestHelper::head_additional_checksum(&head, "XXHASH128"),
            Some(TestHelper::compute_composite_additional_checksum(
                "XXHASH128",
                &v1_content,
                5 * 1024 * 1024
            )),
            "the target must carry v1's composite XXHASH128 checksum (5 MiB parts)"
        );
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&helper.get_object_bytes(&bucket2, key, None).await),
            TestHelper::get_sha256_from_bytes(&v1_content),
            "the target must be v1"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    /// S3→S3 copy of a multipart object with `--additional-checksum-algorithm`
    /// but WITHOUT `--enable-additional-checksum`. This is the CLI shape that
    /// required `get_final_checksum` to force `ChecksumMode::Enabled` on
    /// head_object: without the fix, final_checksum would be None and the
    /// ranged-GET's range-scoped checksum (no `-N`) would reach
    /// validate_checksum, causing a false mismatch.
    #[tokio::test]
    async fn s3_to_s3_multipart_algorithm_without_mode_crc32c() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let src_file = TestHelper::create_sized_file(&local_dir, "algo.bin", 9 * 1024 * 1024);

        let source = format!("s3://{}/algo.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                src_file.to_str().unwrap(),
                &source,
            ])
            .await;

        let target = format!("s3://{}/algo.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "CRC32C",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    // -----------------------------------------------------------------
    // cp --skip-existing for s3 → s3
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

    /// `--skip-existing` must short-circuit when the target object exists,
    /// regardless of whether the underlying transfer would have been a
    /// client-side roundtrip or a server-side `CopyObject`. We assert both
    /// in one test (the skip branch is upstream of the copy strategy, so
    /// covering both paths in one test keeps the suite small).
    #[tokio::test]
    async fn s3_to_s3_skip_existing_skips_when_target_exists() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        // Source object differs from target so a buggy "non-skip" would
        // overwrite the target's bytes (and thus its ETag).
        helper.put_object(&bucket1, "key", b"source".to_vec()).await;
        helper
            .put_object(&bucket2, "key", b"target-original".to_vec())
            .await;

        let etag_before = helper
            .head_object(&bucket2, "key", None)
            .await
            .e_tag()
            .unwrap()
            .to_string();

        let source = format!("s3://{}/key", bucket1);
        let target = format!("s3://{}/key", bucket2);

        // Pass 1: client-side copy path.
        let output = run_s3util(&[
            "cp",
            "--skip-existing",
            "--source-profile",
            "s3util-e2e-test",
            "--source-region",
            REGION,
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &source,
            &target,
        ]);
        assert!(
            output.status.success(),
            "cp --skip-existing should exit 0 when target exists; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let etag_after_client = helper
            .head_object(&bucket2, "key", None)
            .await
            .e_tag()
            .unwrap()
            .to_string();
        assert_eq!(
            etag_before, etag_after_client,
            "cp --skip-existing (client-side) must NOT have overwritten the target"
        );

        // Pass 2: same scenario, but with --server-side-copy. The early
        // return is upstream of the copy strategy, so this should also
        // skip without issuing the server-side CopyObject.
        let output_ssc = run_s3util(&[
            "cp",
            "--skip-existing",
            "--server-side-copy",
            "--source-profile",
            "s3util-e2e-test",
            "--source-region",
            REGION,
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &source,
            &target,
        ]);
        assert!(
            output_ssc.status.success(),
            "cp --skip-existing --server-side-copy should exit 0 when target exists; stderr: {}",
            String::from_utf8_lossy(&output_ssc.stderr)
        );
        let etag_after_ssc = helper
            .head_object(&bucket2, "key", None)
            .await
            .e_tag()
            .unwrap()
            .to_string();

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;

        assert_eq!(
            etag_before, etag_after_ssc,
            "cp --skip-existing --server-side-copy must NOT have overwritten the target"
        );
    }

    #[tokio::test]
    async fn s3_to_s3_skip_existing_proceeds_when_target_missing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        helper.put_object(&bucket1, "key", b"data".to_vec()).await;

        let source = format!("s3://{}/key", bucket1);
        let target = format!("s3://{}/key", bucket2);
        let output = run_s3util(&[
            "cp",
            "--skip-existing",
            "--source-profile",
            "s3util-e2e-test",
            "--source-region",
            REGION,
            "--target-profile",
            "s3util-e2e-test",
            "--target-region",
            REGION,
            &source,
            &target,
        ]);
        assert!(
            output.status.success(),
            "cp --skip-existing should exit 0 when target missing; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let exists = helper.is_object_exist(&bucket2, "key", None).await;
        let bytes = if exists {
            helper.get_object_bytes(&bucket2, "key", None).await
        } else {
            Vec::new()
        };

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;

        assert!(
            exists,
            "cp --skip-existing must perform the copy when the target is missing"
        );
        assert_eq!(bytes, b"data", "copied object body must match the source");
    }
}
