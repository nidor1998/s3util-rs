#![cfg(e2e_test)]
#[cfg(test)]
mod common;

pub const EXPRESS_ONE_ZONE_BUCKET_SUFFIX: &str = "--apne1-az4--x-s3";

#[cfg(test)]
mod tests {
    use common::*;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn roundtrip_express_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let content = b"express roundtrip content verification";
        let test_file = TestHelper::create_test_file(&local_dir, "rt_express.txt", content);

        let target = format!("s3://{}/rt_express.txt", bucket);
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
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        // Download and verify content
        let downloaded = helper
            .get_object_bytes(&bucket, "rt_express.txt", None)
            .await;
        assert_eq!(downloaded, content);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn roundtrip_express_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "rt_express_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/rt_express_large.bin", bucket);
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
        assert_eq!(stats.e_tag_verified, 0);
        assert_eq!(stats.checksum_verified, 1);

        let downloaded = helper
            .get_object_bytes(&bucket, "rt_express_large.bin", None)
            .await;
        let original_sha256 = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let downloaded_sha256 = TestHelper::get_sha256_from_bytes(&downloaded);
        assert_eq!(original_sha256, downloaded_sha256);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
