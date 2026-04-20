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
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s3 to s3 basic";
        helper
            .put_object(&bucket1, "rt_s2s.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_s2s.txt", bucket1);
        let target = format!("s3://{}/rt_s2s.txt", bucket2);
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

        let downloaded = helper.get_object_bytes(&bucket2, "rt_s2s.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_server_side_copy() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip server side copy";
        helper
            .put_object(&bucket1, "rt_ssc.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_ssc.txt", bucket1);
        let target = format!("s3://{}/rt_ssc.txt", bucket2);
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

        let downloaded = helper.get_object_bytes(&bucket2, "rt_ssc.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s storage class";
        helper
            .put_object(&bucket1, "rt_sc.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_sc.txt", bucket1);
        let target = format!("s3://{}/rt_sc.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);

        let downloaded = helper.get_object_bytes(&bucket2, "rt_sc.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s metadata";
        helper
            .put_object(&bucket1, "rt_meta.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_meta.txt", bucket1);
        let target = format!("s3://{}/rt_meta.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_meta.txt", None).await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);

        let downloaded = helper.get_object_bytes(&bucket2, "rt_meta.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_large_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_s2s_large_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/rt_s2s_large_src.bin", local_dir.display());
        let s3_source = format!("s3://{}/rt_s2s_large.bin", bucket1);

        // Upload source to bucket1
        helper
            .put_object_from_file(&bucket1, "rt_s2s_large.bin", &local_source)
            .await;

        let target = format!("s3://{}/rt_s2s_large.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                &s3_source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let head = helper.head_object(&bucket2, "rt_s2s_large.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_s2s_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_server_side_copy_large_object() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_ssc_large_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/rt_ssc_large_src.bin", local_dir.display());

        helper
            .put_object_from_file(&bucket1, "rt_ssc_large.bin", &local_source)
            .await;

        let source = format!("s3://{}/rt_ssc_large.bin", bucket1);
        let target = format!("s3://{}/rt_ssc_large.bin", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_ssc_large.bin", None).await;
        assert_eq!(head.content_length().unwrap(), 9 * 1024 * 1024);

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_ssc_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_sse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s kms";
        helper
            .put_object(&bucket1, "rt_kms.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_kms.txt", bucket1);
        let target = format!("s3://{}/rt_kms.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_kms.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKms
        );

        let downloaded = helper.get_object_bytes(&bucket2, "rt_kms.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_with_dsse_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s dsse kms";
        helper
            .put_object(&bucket1, "rt_dsse.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_dsse.txt", bucket1);
        let target = format!("s3://{}/rt_dsse.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_dsse.txt", None).await;
        assert_eq!(
            head.server_side_encryption().unwrap(),
            &ServerSideEncryption::AwsKmsDsse
        );

        let downloaded = helper.get_object_bytes(&bucket2, "rt_dsse.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_disable_payload_signing() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s no sign";
        helper
            .put_object(&bucket1, "rt_nosign.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_nosign.txt", bucket1);
        let target = format!("s3://{}/rt_nosign.txt", bucket2);
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

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_nosign.txt", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_disable_payload_signing_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_nosign_large_src.bin", 9 * 1024 * 1024);
        let local_source = format!("{}/rt_nosign_large_src.bin", local_dir.display());

        helper
            .put_object_from_file(&bucket1, "rt_nosign_large.bin", &local_source)
            .await;

        let source = format!("s3://{}/rt_nosign_large.bin", bucket1);
        let target = format!("s3://{}/rt_nosign_large.bin", bucket2);
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

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_nosign_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_with_acl() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip s2s acl";
        helper
            .put_object(&bucket1, "rt_acl.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_acl.txt", bucket1);
        let target = format!("s3://{}/rt_acl.txt", bucket2);
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

        let downloaded = helper.get_object_bytes(&bucket2, "rt_acl.txt", None).await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_server_side_copy_with_metadata() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip ssc metadata";
        helper
            .put_object(&bucket1, "rt_ssc_meta.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_ssc_meta.txt", bucket1);
        let target = format!("s3://{}/rt_ssc_meta.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_ssc_meta.txt", None).await;
        assert_eq!(head.cache_control().unwrap(), TEST_CACHE_CONTROL);
        assert_eq!(head.content_type().unwrap(), TEST_CONTENT_TYPE);

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_ssc_meta.txt", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }

    #[tokio::test]
    async fn roundtrip_server_side_copy_with_storage_class() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let content = b"roundtrip ssc storage class";
        helper
            .put_object(&bucket1, "rt_ssc_sc.txt", content.to_vec())
            .await;

        let source = format!("s3://{}/rt_ssc_sc.txt", bucket1);
        let target = format!("s3://{}/rt_ssc_sc.txt", bucket2);
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

        let head = helper.head_object(&bucket2, "rt_ssc_sc.txt", None).await;
        assert_eq!(head.storage_class().unwrap(), &StorageClass::StandardIa);

        let downloaded = helper
            .get_object_bytes(&bucket2, "rt_ssc_sc.txt", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
    }
}
