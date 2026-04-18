#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    /// Test that --show-progress flag does not break the transfer.
    #[tokio::test]
    async fn upload_with_progress() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_test_file(&local_dir, "progress.txt", b"progress bar test data");

        let target = format!("s3://{}/progress.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--show-progress",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        assert!(helper.is_object_exist(&bucket, "progress.txt", None).await);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Test progress with a multipart upload.
    #[tokio::test]
    async fn upload_multipart_with_progress() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_sized_file(&local_dir, "progress_large.bin", 9 * 1024 * 1024);

        let target = format!("s3://{}/progress_large.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--show-progress",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.e_tag_verified, 1);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Test download with progress.
    #[tokio::test]
    async fn download_with_progress() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        helper
            .put_object(
                &bucket,
                "dl_progress.txt",
                b"download progress test".to_vec(),
            )
            .await;

        let local_dir = TestHelper::create_temp_dir();
        let local_file = local_dir.join("dl_progress.txt");

        let source = format!("s3://{}/dl_progress.txt", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--show-progress",
                &source,
                local_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);

        let content = std::fs::read(&local_file).unwrap();
        assert_eq!(content, b"download progress test");

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
