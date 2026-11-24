#![cfg(e2e_test)]
#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use common::*;

    use super::*;

    const SHA256_16M_FILE_8M_CHUNK: &str = "LVOdCFd7WEX67+8xvcScVKkhkmmSweO04ZpwoISRidg=-2";
    const SHA512_16M_FILE_8M_CHUNK: &str = "56kFHlp/xXlP8YYqLyFGILA/NzIDu/Ei+R0dZk4zJkXRKKXDKww3lDB3tGE41pzxRRfElzyxvQXCXt6MLphl4A==-2";
    const MD5_16M_FILE_8M_CHUNK: &str = "k3JNkYRTScFpXyJJlfpo6g==-2";
    const XXHASH128_16M_FILE_8M_CHUNK: &str = "ywObHx3DRGT4JLGZ0qmvsw==-2";
    const XXHASH3_16M_FILE_8M_CHUNK: &str = "LlzJIeGJMBk=-2";
    const XXHASH64_16M_FILE_8M_CHUNK: &str = "C3MPgGrPE7M=-2";
    const CRC64NVME_16M_FILE_8M_CHUNK: &str = "0k/JgmAhfhc=";
    const ETAG_16M_FILE_8M_CHUNK: &str = "\"93724d91845349c1695f224995fa68ea-2\"";
    const SHA256_16M_PLUS_1_FILE_8M_CHUNK: &str = "Iit+zZRe9Uyo6R+uoTqiMb54ZurDpJwvDjHj1pogXqI=-3";
    const SHA512_16M_PLUS_1_FILE_8M_CHUNK: &str = "TuZlKBF4tIDRVaFkOs7xxr6ExbjhHaKWbGrTDllz3RP/M6ximI3XUIwENqMSv0/OeCygQd7YQGN7eF9YPlQ1tA==-3";
    const MD5_16M_PLUS_1_FILE_8M_CHUNK: &str = "THLtVDmGOcj9vzj5RVNTyQ==-3";
    const XXHASH128_16M_PLUS_1_FILE_8M_CHUNK: &str = "tV7rCY3B8grOcKDpvVsw8Q==-3";
    const XXHASH3_16M_PLUS_1_FILE_8M_CHUNK: &str = "m/KAyCSP6bc=-3";
    const XXHASH64_16M_PLUS_1_FILE_8M_CHUNK: &str = "n3t3dSU17zo=-3";
    const CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK: &str = "EkI8PNeCSro=";
    const ETAG_16M_PLUS_1_FILE_8M_CHUNK: &str = "\"4c72ed54398639c8fdbf38f9455353c9-3\"";
    const SHA256_16M_MINUS_1_FILE_8M_CHUNK: &str = "SH8JKa5JGy6vdNjmK6uC91GcqnWasSH+mNdfZZG2/6k=-2";
    const SHA512_16M_MINUS_1_FILE_8M_CHUNK: &str = "ZxieDWTNdgE+Heamq/RImMCCyeCwlmQi1EFX1EHG6Ytx9T+vexRsHLZycS1yrSF7xTx3aq/WarccvL98+Vw6hQ==-2";
    const MD5_16M_MINUS_1_FILE_8M_CHUNK: &str = "o0oY+rPocVs7Htw8uaUmRw==-2";
    const XXHASH128_16M_MINUS_1_FILE_8M_CHUNK: &str = "+ZyRiR4upiUQqNrGqeqeMA==-2";
    const XXHASH3_16M_MINUS_1_FILE_8M_CHUNK: &str = "o/f6F95a67A=-2";
    const XXHASH64_16M_MINUS_1_FILE_8M_CHUNK: &str = "dwE/7Z1yBNg=-2";
    const CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK: &str = "G5Y2N0Yfx+o=";
    const ETAG_16M_MINUS_1_FILE_8M_CHUNK: &str = "\"a34a18fab3e8715b3b1edc3cb9a52647-2\"";
    const SHA256_16M_FILE_WHOLE: &str =
        "23bf32cdfd60784647663a160aee7c46ca7941173d48ad37db52713fda4562e1";
    const SHA256_16M_PLUS_1_FILE_WHOLE: &str =
        "0fbb2466d100013b3716965c89ac0c1375bba2c8f126e63ee6bc5ffff68ef33b";
    const SHA256_16M_MINUS_1_FILE_WHOLE: &str =
        "cf674acbd51c8c0e3c08ba06cb8b2bcfa871b2193399cca34d3915b8312f57cb";

    /// Upload a 16 MiB file with 8 MiB chunk (2 parts) and verify.
    #[tokio::test]
    async fn test_multipart_upload_16mb() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(&local_dir, "16mb.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket, "16mb.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb.bin", None).await;
        assert_eq!(head.content_length().unwrap(), (16 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with SHA256 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_sha256.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_sha256.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_sha256.bin", None).await;
        assert_eq!(head.checksum_sha256().unwrap(), SHA256_16M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with SHA512 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_sha512.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_sha512.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "SHA512",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_sha512.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_sha512.bin", None).await;
        assert_eq!(head.checksum_sha512().unwrap(), SHA512_16M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with MD5 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_md5.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_md5.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "MD5",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_md5.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_md5.bin", None).await;
        assert_eq!(head.checksum_md5().unwrap(), MD5_16M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with XXHASH128 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_xxhash128.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_xxhash128.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
                "16mb_xxhash128.bin",
                ETAG_16M_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_xxhash128.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            XXHASH128_16M_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with XXHASH3 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_xxhash3.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_xxhash3.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_xxhash3.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_xxhash3.bin", None).await;
        assert_eq!(head.checksum_xxhash3().unwrap(), XXHASH3_16M_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with XXHASH64 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_xxhash64.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_xxhash64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_xxhash64.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_xxhash64.bin", None).await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_16M_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB file with CRC64NVME checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_crc64.bin", 16, 0).unwrap();

        let target = format!("s3://{}/16mb_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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
            .verify_uploaded_object_etag_value(&bucket, "16mb_crc64.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        let head = helper.head_object(&bucket, "16mb_crc64.bin", None).await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_16M_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Roundtrip: local->S3->S3->local, 16 MiB, verify content matches.
    #[tokio::test]
    async fn test_multipart_16mb_roundtrip() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_rt.bin", 16, 0).unwrap();

        // Local to S3
        let s3_path1 = format!("s3://{}/16mb_rt.bin", bucket1);
        let stats1 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &s3_path1,
            ])
            .await;
        assert_eq!(stats1.sync_complete, 1);
        assert_eq!(stats1.sync_error, 0);
        assert_eq!(stats1.sync_warning, 0);
        assert_eq!(stats1.e_tag_verified, 1);
        assert_eq!(stats1.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket1, "16mb_rt.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        // S3 to S3
        let s3_path2 = format!("s3://{}/16mb_rt.bin", bucket2);
        let stats2 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                &s3_path1,
                &s3_path2,
            ])
            .await;
        assert_eq!(stats2.sync_complete, 1);
        assert_eq!(stats2.sync_error, 0);
        assert_eq!(stats2.sync_warning, 0);
        assert_eq!(stats2.e_tag_verified, 1);
        assert_eq!(stats2.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(&bucket2, "16mb_rt.bin", ETAG_16M_FILE_8M_CHUNK)
            .await;

        // S3 to local
        let download_file = local_dir.join("16mb_dl.bin");
        let stats3 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &s3_path2,
                download_file.to_str().unwrap(),
            ])
            .await;
        assert_eq!(stats3.sync_complete, 1);
        assert_eq!(stats3.sync_error, 0);
        assert_eq!(stats3.sync_warning, 0);
        assert_eq!(stats3.e_tag_verified, 1);
        assert_eq!(stats3.checksum_verified, 0);

        let orig_sha = TestHelper::get_sha256_from_file(test_file.to_str().unwrap());
        let dl_sha = TestHelper::get_sha256_from_file(download_file.to_str().unwrap());
        assert_eq!(orig_sha, dl_sha);
        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 file, ETag only, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1.bin", 16, 1).unwrap();

        let target = format!("s3://{}/16mb_plus1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_plus1.bin",
                ETAG_16M_PLUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper.head_object(&bucket, "16mb_plus1.bin", None).await;
        assert_eq!(
            head.content_length().unwrap(),
            (16 * 1024 * 1024 + 1) as i64
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 file, ETag only, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1.bin", 16, -1).unwrap();

        let target = format!("s3://{}/16mb_minus1.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                test_file.to_str().unwrap(),
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        helper
            .verify_uploaded_object_etag_value(
                &bucket,
                "16mb_minus1.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let download_file = local_dir.join("16mb_minus1_dl.bin");
        let dl_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                &target,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(dl_stats.sync_complete, 1);
        assert_eq!(dl_stats.sync_error, 0);
        assert_eq!(dl_stats.sync_warning, 0);
        assert_eq!(dl_stats.e_tag_verified, 1);
        assert_eq!(dl_stats.checksum_verified, 0);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_MINUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and SHA256, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha256_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_sha256.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and SHA512, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha512_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_sha512.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_sha512.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "SHA512",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_sha512.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            SHA512_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and MD5, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_md5_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_md5.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_md5.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "MD5",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_md5.bin", None)
            .await;
        assert_eq!(head.checksum_md5().unwrap(), MD5_16M_PLUS_1_FILE_8M_CHUNK,);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and XXHASH128, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash128_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_xxhash128.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_xxhash128.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_xxhash128.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            XXHASH128_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and XXHASH3, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash3_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_xxhash3.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_xxhash3.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_xxhash3.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            XXHASH3_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms and XXHASH64, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash64_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_xxhash64.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_xxhash64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--sse",
                "aws:kms",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_xxhash64.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize (ETag only), default 8 MiB threshold.
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
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_8c.bin", 16, 1).unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;
        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 0);

        let target_s3 = format!("s3://{}/16mb_plus1_ac_8c.bin", bucket2);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 0);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with SHA256 checksum, 8 MiB threshold and 8 MiB chunksize.
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
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_sha256_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_sha256_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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

        let head = helper
            .head_object(&bucket1, "16mb_plus1_sha256_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_sha256_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_sha256_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with SHA256 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_sha256_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_sha256().unwrap(),
            SHA256_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_sha256_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with SHA512 checksum, 8 MiB threshold and 8 MiB chunksize.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_sha512_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_sha512_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA512",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket1, "16mb_plus1_sha512_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_sha512().unwrap(),
            SHA512_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_sha512_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_sha512_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with SHA512 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_sha512_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_sha512().unwrap(),
            SHA512_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_sha512_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with MD5 checksum, 8 MiB threshold and 8 MiB chunksize.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_md5_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_md5_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "MD5",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket1, "16mb_plus1_md5_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(head.checksum_md5().unwrap(), MD5_16M_PLUS_1_FILE_8M_CHUNK);

        let target_s3 = format!("s3://{}/16mb_plus1_md5_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_md5_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with MD5 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_md5_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(head3.checksum_md5().unwrap(), MD5_16M_PLUS_1_FILE_8M_CHUNK);

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_md5_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with XXHASH128 checksum, 8 MiB threshold and 8 MiB chunksize.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_xxhash128_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_xxhash128_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH128",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket1, "16mb_plus1_xxhash128_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            XXHASH128_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_xxhash128_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_xxhash128_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with XXHASH128 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_xxhash128_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_xxhash128().unwrap(),
            XXHASH128_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_xxhash128_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with XXHASH3 checksum, 8 MiB threshold and 8 MiB chunksize.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_xxhash3_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_xxhash3_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH3",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket1, "16mb_plus1_xxhash3_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            XXHASH3_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_xxhash3_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_xxhash3_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with XXHASH3 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_xxhash3_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_xxhash3().unwrap(),
            XXHASH3_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_xxhash3_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with XXHASH64 checksum, 8 MiB threshold and 8 MiB chunksize.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_xxhash64_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_xxhash64_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH64",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        assert_eq!(upload_stats.sync_complete, 1);
        assert_eq!(upload_stats.sync_error, 0);
        assert_eq!(upload_stats.sync_warning, 0);
        assert_eq!(upload_stats.e_tag_verified, 1);
        assert_eq!(upload_stats.checksum_verified, 1);

        let head = helper
            .head_object(&bucket1, "16mb_plus1_xxhash64_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_xxhash64_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_xxhash64_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with XXHASH64 checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
            .head_object(&bucket2, "16mb_plus1_xxhash64_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_xxhash64().unwrap(),
            XXHASH64_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_xxhash64_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with CRC64NVME checksum, 8 MiB threshold and 8 MiB chunksize.
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
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_crc64_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_crc64_8c.bin", bucket1);
        let upload_stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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

        let head = helper
            .head_object(&bucket1, "16mb_plus1_crc64_8c.bin", None)
            .await;
        assert_eq!(head.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
        );

        let target_s3 = format!("s3://{}/16mb_plus1_crc64_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.e_tag_verified, 1);
        assert_eq!(stats.checksum_verified, 1);

        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_crc64_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        // Scope 3: S3 → S3 server-side copy with CRC64NVME checksum.
        helper.delete_all_objects(&bucket2).await;

        let stats3 = helper
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
                "CRC64NVME",
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
            .head_object(&bucket2, "16mb_plus1_crc64_8c.bin", None)
            .await;
        assert_eq!(head3.e_tag().unwrap(), ETAG_16M_PLUS_1_FILE_8M_CHUNK);
        assert_eq!(
            head3.checksum_crc64_nvme().unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK
        );

        // Scope 4: S3 → Local download and verify whole-file SHA256.
        let download_file = local_dir.join("16mb_plus1_crc64_8c_dl.bin");
        let stats4 = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--enable-additional-checksum",
                &target_s3,
                download_file.to_str().unwrap(),
            ])
            .await;

        assert_eq!(stats4.sync_complete, 1);
        assert_eq!(stats4.sync_error, 0);
        assert_eq!(stats4.sync_warning, 0);
        assert_eq!(stats4.checksum_verified, 1);
        assert_eq!(stats4.e_tag_verified, 1);

        TestHelper::verify_downloaded_file_sha256(
            download_file.to_str().unwrap(),
            SHA256_16M_PLUS_1_FILE_WHOLE,
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + SHA256, default 8 MiB threshold.
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
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_sha_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_sha_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_sha_8c.bin", bucket2);
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
                "SHA256",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_sha_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + SHA512, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_sha512_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_sha512_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_sha512_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA512",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_sha512_8c.bin", bucket2);
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
                "SHA512",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_sha512_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + MD5, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_md5_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_md5_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_md5_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "MD5",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_md5_8c.bin", bucket2);
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
                "MD5",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_md5_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + XXHASH128, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash128_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file = TestHelper::create_random_data_file(
            &local_dir,
            "16mb_plus1_ac_xxhash128_8c.bin",
            16,
            1,
        )
        .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_xxhash128_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH128",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_xxhash128_8c.bin", bucket2);
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
                "XXHASH128",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_xxhash128_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + XXHASH3, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash3_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_xxhash3_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_xxhash3_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH3",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_xxhash3_8c.bin", bucket2);
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
                "XXHASH3",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_xxhash3_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + XXHASH64, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_xxhash64_auto_chunksize() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_xxhash64_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_xxhash64_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH64",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_xxhash64_8c.bin", bucket2);
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
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_xxhash64_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --auto-chunksize + CRC64NVME, default 8 MiB threshold.
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
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_ac_crc64_8c.bin", 16, 1)
                .unwrap();

        let source_s3 = format!("s3://{}/16mb_plus1_ac_crc64_8c.bin", bucket1);
        helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "CRC64NVME",
                test_file.to_str().unwrap(),
                &source_s3,
            ])
            .await;

        let target_s3 = format!("s3://{}/16mb_plus1_ac_crc64_8c.bin", bucket2);
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
                "CRC64NVME",
                &source_s3,
                &target_s3,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(stats.checksum_verified, 1);
        assert_eq!(stats.e_tag_verified, 1);
        let bytes = helper
            .get_object_bytes(&bucket2, "16mb_plus1_ac_crc64_8c.bin", None)
            .await;
        assert_eq!(
            TestHelper::get_sha256_from_bytes(&bytes),
            SHA256_16M_PLUS_1_FILE_WHOLE
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms (ETag only), default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_8c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_8c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB+1 with --sse aws:kms + CRC64NVME, default 8 MiB threshold.
    #[tokio::test]
    async fn test_multipart_upload_16mb_plus_1_crc64nvme_kms() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_plus1_kms_crc64_8c.bin", 16, 1)
                .unwrap();

        let target = format!("s3://{}/16mb_plus1_kms_crc64_8c.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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
        // Skip ETag verification for SSE-KMS (ETag is not MD5-based)
        let head = helper
            .head_object(&bucket, "16mb_plus1_kms_crc64_8c.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_16M_PLUS_1_FILE_8M_CHUNK,
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with SHA256 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_sha256() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_sha256.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_sha256.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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
                "16mb_minus1_sha256.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_sha256.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha256().unwrap(),
            SHA256_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with SHA512 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_sha512() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_sha512.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_sha512.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA512",
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
                "16mb_minus1_sha512.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_sha512.bin", None)
            .await;
        assert_eq!(
            head.checksum_sha512().unwrap(),
            SHA512_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with MD5 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_md5() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_md5.bin", 16, -1).unwrap();

        let target = format!("s3://{}/16mb_minus1_md5.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "MD5",
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
                "16mb_minus1_md5.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_md5.bin", None)
            .await;
        assert_eq!(head.checksum_md5().unwrap(), MD5_16M_MINUS_1_FILE_8M_CHUNK);

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with XXHASH128 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_xxhash128() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_xxhash128.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_xxhash128.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH128",
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
                "16mb_minus1_xxhash128.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_xxhash128.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash128().unwrap(),
            XXHASH128_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with XXHASH3 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_xxhash3() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_xxhash3.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_xxhash3.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH3",
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
                "16mb_minus1_xxhash3.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_xxhash3.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash3().unwrap(),
            XXHASH3_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with XXHASH64 checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_xxhash64() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_xxhash64.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_xxhash64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
                "--additional-checksum-algorithm",
                "XXHASH64",
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
                "16mb_minus1_xxhash64.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_xxhash64.bin", None)
            .await;
        assert_eq!(
            head.checksum_xxhash64().unwrap(),
            XXHASH64_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// 16 MiB-1 with CRC64NVME checksum, 8 MiB chunk.
    #[tokio::test]
    async fn test_multipart_upload_16mb_minus_1_crc64nvme() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "16mb_minus1_crc64.bin", 16, -1)
                .unwrap();

        let target = format!("s3://{}/16mb_minus1_crc64.bin", bucket);
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--target-profile",
                "s3util-e2e-test",
                "--multipart-threshold",
                "8MiB",
                "--multipart-chunksize",
                "8MiB",
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
                "16mb_minus1_crc64.bin",
                ETAG_16M_MINUS_1_FILE_8M_CHUNK,
            )
            .await;

        let head = helper
            .head_object(&bucket, "16mb_minus1_crc64.bin", None)
            .await;
        assert_eq!(
            head.checksum_crc64_nvme().unwrap(),
            CRC64NVME_16M_MINUS_1_FILE_8M_CHUNK
        );

        helper.delete_bucket_with_cascade(&bucket).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }
}
