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
    async fn local_to_express_one_zone_basic() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "express.txt", b"express one zone test");

        let target = format!("s3://{}/express.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn local_to_express_one_zone_multipart() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "express_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/express_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    #[tokio::test]
    async fn express_one_zone_to_local() {
        TestHelper::init_dummy_tracing_subscriber();

        let bucket = format!("s3e2e-{}{}", Uuid::new_v4(), EXPRESS_ONE_ZONE_BUCKET_SUFFIX);
        let helper = TestHelper::new().await;

        helper
            .create_directory_bucket(&bucket, EXPRESS_ONE_ZONE_AZ)
            .await;

        // Upload first
        let local_dir = TestHelper::create_temp_dir();
        let upload_file =
            TestHelper::create_test_file(&local_dir, "upload.txt", b"express download test");
        let s3_path = format!("s3://{}/download_test.txt", bucket);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                upload_file.to_str().unwrap(),
                &s3_path,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);

        // Download
        let download_file = local_dir.join("downloaded.txt");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--disable-stalled-stream-protection",
                &s3_path,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&download_file).unwrap();
        assert_eq!(content, b"express download test");

        helper.delete_directory_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
