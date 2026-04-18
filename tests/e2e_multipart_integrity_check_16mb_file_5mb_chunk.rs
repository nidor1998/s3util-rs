#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_16M_FILE_5M_CHUNK: &str = "usH4aHyuoRO0zyxwVyuWJE7kS+ZJZ/8HXY/+/Ltct/4=-4";
    const CRC64NVME_16M_FILE_5M_CHUNK: &str = "0k/JgmAhfhc=";
    const ETAG_16M_FILE_5M_CHUNK: &str = "\"db5daa6fb02e1c6b2063c5469b99e096-4\"";
    const SHA256_16M_PLUS_1_FILE_5M_CHUNK: &str = "31zMNBPu9QSPzGZCAD/SVu5Ln5CWYxX3eCj2VMMwsuY=-4";
    const CRC64NVME_16M_PLUS_1_FILE_5M_CHUNK: &str = "EkI8PNeCSro=";
    const ETAG_16M_PLUS_1_FILE_5M_CHUNK: &str = "\"5e7e959b1416576b46fe9a7b3dea4c5e-4\"";
    const SHA256_16M_MINUS_1_FILE_5M_CHUNK: &str = "MLVHZy/Po6lUhNzthT8oDCg8wLDhVhMNtkU1/dE+gEo=-4";
    const CRC64NVME_16M_MINUS_1_FILE_5M_CHUNK: &str = "G5Y2N0Yfx+o=";
    const ETAG_16M_MINUS_1_FILE_5M_CHUNK: &str = "\"cd769ef00f81a6d450848efda5e8870d-4\"";
    const SHA256_16M_FILE_WHOLE: &str =
        "23bf32cdfd60784647663a160aee7c46ca7941173d48ad37db52713fda4562e1";
    const SHA256_16M_PLUS_1_FILE_WHOLE: &str =
        "0fbb2466d100013b3716965c89ac0c1375bba2c8f126e63ee6bc5ffff68ef33b";
    const SHA256_16M_MINUS_1_FILE_WHOLE: &str =
        "cf674acbd51c8c0e3c08ba06cb8b2bcfa871b2193399cca34d3915b8312f57cb";

    /// Upload a 16 MiB file with 5 MiB chunk and verify.
    #[tokio::test]
    async fn test_multipart_upload_16mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_5c.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(&bucket, "16mb_5c.bin", ETAG_16M_FILE_5M_CHUNK)
            .await;

        let download_dir = TestHelper::create_temp_dir();
        let download_file = download_dir.join("16mb_5c.bin");
        let download_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(download_stats.sync_complete, 1);
        assert_eq!(download_stats.sync_error, 0);
        assert_eq!(download_stats.sync_warning, 0);
        assert_eq!(download_stats.e_tag_verified, 1);
        assert_eq!(download_stats.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
        let _ = std::fs::remove_dir_all(&download_dir);
    }

    /// 16 MiB file with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_5c_sha.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
        helper
            .verify_uploaded_object_etag_value(&bucket, "16mb_5c_sha.bin", ETAG_16M_FILE_5M_CHUNK)
            .await;
        let head = helper.head_object(&bucket, "16mb_5c_sha.bin", None).await;
        assert_eq!(head.checksum_sha256.unwrap(), SHA256_16M_FILE_5M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_5c_crc64.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_5c_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
        helper
            .verify_uploaded_object_etag_value(&bucket, "16mb_5c_crc64.bin", ETAG_16M_FILE_5M_CHUNK)
            .await;
        let head = helper.head_object(&bucket, "16mb_5c_crc64.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme.unwrap(),
            CRC64NVME_16M_FILE_5M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 file, ETag only, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_5c.bin", 16, 1).unwrap();

        let target = format!("s3://{}/16mb_plus1_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_plus1_5c.bin",
                ETAG_16M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 file, ETag only, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_5c.bin", 16, -1).unwrap();

        let target = format!("s3://{}/16mb_minus1_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_minus1_5c.bin",
                ETAG_16M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;

        let download_dir = TestHelper::create_temp_dir();
        let download_file = download_dir.join("16mb_minus1_5c.bin");
        let download_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(download_stats.sync_complete, 1);
        assert_eq!(download_stats.sync_error, 0);
        assert_eq!(download_stats.sync_warning, 0);
        assert_eq!(download_stats.e_tag_verified, 1);
        assert_eq!(download_stats.checksum_verified, 0);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_MINUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
        let _ = std::fs::remove_dir_all(&download_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and SHA256, threshold=5MiB, chunksize=5MiB.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_sha256_5c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_sha256_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Upload 16 MiB+1 to bucket1 (threshold=5MiB, chunksize=5MiB), then S3-to-S3 copy to bucket2
    /// with --auto-chunksize. Assert e_tag_verified==1 on copy.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_autochunk_5c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_autochunk_5c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        helper
            .verify_uploaded_object_etag_value(
                &bucket1,
                "16mb_plus1_autochunk_5c.bin",
                ETAG_16M_PLUS_1_FILE_5M_CHUNK,
            )
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_autochunk_5c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_autochunk_5c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with SHA256, threshold=5MiB, chunksize=5MiB. Verifies ETag and checksum on upload
    /// and S3-to-S3 copy.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_sha_5c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_sha_5c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);
        let head1 = helper
            .head_object(&bucket1, "16mb_plus1_sha_5c.bin", None)
            .await;
        assert_eq!(head1.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head1.checksum_sha256.unwrap(),
            SHA256_16M_PLUS_1_FILE_5M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_sha_5c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head2 = helper
            .head_object(&bucket2, "16mb_plus1_sha_5c.bin", None)
            .await;
        assert_eq!(head2.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head2.checksum_sha256.unwrap(),
            SHA256_16M_PLUS_1_FILE_5M_CHUNK
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_sha_5c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side-copy (bucket1 → bucket2)
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 1);
        let head3 = helper
            .head_object(&bucket2, "16mb_plus1_sha_5c.bin", None)
            .await;
        assert_eq!(head3.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head3.checksum_sha256.unwrap(),
            SHA256_16M_PLUS_1_FILE_5M_CHUNK
        );

        // Scope 4: S3 → local download and verify whole-file SHA256
        let download_dir = TestHelper::create_temp_dir();
        let download_file = download_dir.join("16mb_plus1_sha_5c.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(
            TestHelper::get_sha256_from_file(download_file.to_str().unwrap()),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
        let _ = std::fs::remove_dir_all(&download_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + SHA256.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha256_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_sha_5c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_sha_5c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_sha_5c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_sha_5c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with CRC64NVME, threshold=5MiB, chunksize=5MiB. Verifies ETag and checksum on
    /// upload and S3-to-S3 copy.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_crc64_5c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_crc64_5c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);
        let head1 = helper
            .head_object(&bucket1, "16mb_plus1_crc64_5c.bin", None)
            .await;
        assert_eq!(head1.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head1.checksum_crc64_nvme.unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_5M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_crc64_5c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);
        let head2 = helper
            .head_object(&bucket2, "16mb_plus1_crc64_5c.bin", None)
            .await;
        assert_eq!(head2.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head2.checksum_crc64_nvme.unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_5M_CHUNK
        );
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_crc64_5c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side-copy (bucket1 → bucket2)
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--server-side-copy",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                "--enable-additional-checksum",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 1);
        let head3 = helper
            .head_object(&bucket2, "16mb_plus1_crc64_5c.bin", None)
            .await;
        assert_eq!(head3.e_tag.unwrap(), ETAG_16M_PLUS_1_FILE_5M_CHUNK);
        assert_eq!(
            head3.checksum_crc64_nvme.unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_5M_CHUNK
        );

        // Scope 4: S3 → local download and verify whole-file SHA256
        let download_dir = TestHelper::create_temp_dir();
        let download_file = download_dir.join("16mb_plus1_crc64_5c.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.e_tag_verified, 1);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(
            TestHelper::get_sha256_from_file(download_file.to_str().unwrap()),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
        let _ = std::fs::remove_dir_all(&download_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_crc64nvme_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_crc64_5c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_crc64_5c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_crc64_5c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3sync-e2e-test",
                "--target-profile",
                "s3sync-e2e-test",
                "--auto-chunksize",
                "--enable-additional-checksum",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_crc64_5c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms (ETag only).
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_5c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms + CRC64NVME.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_crc64_5c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_crc64_5c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
                "--sse",
                "aws:kms",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with SHA256 checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_5c_sha.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_5c_sha.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_minus1_5c_sha.bin",
                ETAG_16M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;
        let head = helper
            .head_object(&bucket, "16mb_minus1_5c_sha.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256.unwrap(),
            SHA256_16M_MINUS_1_FILE_5M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with CRC64NVME checksum, 5 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_5c_crc64.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_5c_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3sync-e2e-test",
                "--multipart-threshold",
                "5MiB",
                "--multipart-chunksize",
                "5MiB",
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
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_minus1_5c_crc64.bin",
                ETAG_16M_MINUS_1_FILE_5M_CHUNK,
            )
            .await;
        let head = helper
            .head_object(&bucket, "16mb_minus1_5c_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme.unwrap(),
            CRC64NVME_16M_MINUS_1_FILE_5M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
