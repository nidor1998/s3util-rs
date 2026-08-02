#![cfg(e2e_test)]

//! Multipart-copy verification variants that only real AWS can prove
//! end-to-end: `--expires` conversion on the auto-chunksize path,
//! `--disable-additional-checksum-verify` on a multipart s3→s3 copy,
//! the chunk-size-mismatch + `--disable-multipart-verify` silent skip,
//! and the verified outcome for a KMS-encrypted annotation stored without
//! any client checksum (S3's automatically computed CRC64NVME takes over).

#[cfg(test)]
mod common;

#[cfg(test)]
mod tests {
    use super::*;
    use common::*;

    use std::process::{Command, Stdio};

    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::types::{
        ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
        ServerSideEncryptionRule,
    };

    fn run_s3util(args: &[&str]) -> std::process::Output {
        Command::new(env!("CARGO_BIN_EXE_s3util"))
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .expect("spawn s3util")
    }

    /// Upload a 10 MiB object as 2×5 MiB parts so the source carries a
    /// multipart-style ETag (and, when `sha256` is set, a composite
    /// SHA256 tied to that part layout).
    async fn seed_multipart_source(
        helper: &TestHelper,
        bucket: &str,
        key: &str,
        local_file: &str,
        sha256: bool,
    ) {
        let target = format!("s3://{bucket}/{key}");
        let mut args = vec![
            "s3util",
            "cp",
            "--target-profile",
            "s3util-e2e-test",
            "--multipart-threshold",
            "5MiB",
            "--multipart-chunksize",
            "5MiB",
        ];
        if sha256 {
            args.extend(["--additional-checksum-algorithm", "SHA256"]);
        }
        args.push(local_file);
        args.push(&target);
        let stats = helper.cp_test_data(args).await;
        assert_eq!(stats.sync_complete, 1, "seeding the source must succeed");
        assert_eq!(stats.sync_error, 0);
    }

    /// `--auto-chunksize` copy with `--expires`: the configured expires is
    /// converted while building the CreateMultipartUpload attributes on the
    /// auto-chunksize path and must round-trip onto the target object.
    #[tokio::test]
    async fn s3_to_s3_auto_chunksize_multipart_preserves_configured_expires() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "auto_expires.bin", 10, 0).unwrap();
        seed_multipart_source(
            &helper,
            &bucket1,
            "auto_expires.bin",
            test_file.to_str().unwrap(),
            false,
        )
        .await;

        let source = format!("s3://{bucket1}/auto_expires.bin");
        let target = format!("s3://{bucket2}/auto_expires.bin");
        let stats = helper
            .cp_test_data(vec![
                "s3util",
                "cp",
                "--source-profile",
                "s3util-e2e-test",
                "--target-profile",
                "s3util-e2e-test",
                "--auto-chunksize",
                "--expires",
                TEST_EXPIRES,
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 1,
            "auto-chunksize replicates the part layout"
        );

        let head = helper.head_object(&bucket2, "auto_expires.bin", None).await;
        let expires = head.expires_string().unwrap_or_default().to_string();
        assert!(
            expires.contains("2055"),
            "--expires must round-trip onto the copied object; got: {expires:?}"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// `--disable-additional-checksum-verify` on a multipart s3→s3 copy:
    /// the copy completes, the ETag still verifies, and the checksum
    /// verification is skipped (not failed).
    #[tokio::test]
    async fn s3_to_s3_multipart_with_additional_checksum_verify_disabled() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "no_checksum_verify.bin", 10, 0)
                .unwrap();
        seed_multipart_source(
            &helper,
            &bucket1,
            "no_checksum_verify.bin",
            test_file.to_str().unwrap(),
            true,
        )
        .await;

        let source = format!("s3://{bucket1}/no_checksum_verify.bin");
        let target = format!("s3://{bucket2}/no_checksum_verify.bin");
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
                "--additional-checksum-algorithm",
                "SHA256",
                "--disable-additional-checksum-verify",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(stats.sync_warning, 0);
        assert_eq!(
            stats.e_tag_verified, 1,
            "same chunk size keeps the ETag verifiable"
        );
        assert_eq!(
            stats.checksum_verified, 0,
            "checksum verification must be skipped when disabled"
        );

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// Copying with a different chunk size than the source layout makes both
    /// the multipart ETag and the composite SHA256 incomparable. With
    /// `--disable-multipart-verify` the mismatch must be skipped silently:
    /// no warning, no verified counters, and an intact 10 MiB target.
    #[tokio::test]
    async fn s3_to_s3_chunksize_mismatch_with_disable_multipart_verify_skips_silently() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket1 = TestHelper::generate_bucket_name();
        let bucket2 = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket1, REGION).await;
        helper.create_bucket(&bucket2, REGION).await;

        let local_dir = TestHelper::create_temp_dir();
        let test_file =
            TestHelper::create_random_data_file(&local_dir, "chunk_mismatch.bin", 10, 0).unwrap();
        seed_multipart_source(
            &helper,
            &bucket1,
            "chunk_mismatch.bin",
            test_file.to_str().unwrap(),
            true,
        )
        .await;

        let source = format!("s3://{bucket1}/chunk_mismatch.bin");
        let target = format!("s3://{bucket2}/chunk_mismatch.bin");
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
                "8MiB",
                "--additional-checksum-algorithm",
                "SHA256",
                "--disable-multipart-verify",
                &source,
                &target,
            ])
            .await;

        assert_eq!(stats.sync_complete, 1);
        assert_eq!(stats.sync_error, 0);
        assert_eq!(
            stats.sync_warning, 0,
            "--disable-multipart-verify must skip the mismatch without warning"
        );
        assert_eq!(
            stats.e_tag_verified, 0,
            "multipart ETags are incomparable here"
        );
        assert_eq!(
            stats.checksum_verified, 0,
            "composite checksums are incomparable here"
        );

        let head = helper
            .head_object(&bucket2, "chunk_mismatch.bin", None)
            .await;
        assert_eq!(head.content_length().unwrap(), (10 * 1024 * 1024) as i64);

        helper.delete_bucket_with_cascade(&bucket1).await;
        helper.delete_bucket_with_cascade(&bucket2).await;
        let _ = std::fs::remove_dir_all(&local_dir);
    }

    /// An annotation stored WITHOUT any client checksum (raw SDK put) on a
    /// bucket whose default encryption is SSE-KMS: the ETag is not an MD5,
    /// but S3's default data-integrity protections compute and store a
    /// CRC64NVME checksum for uploads that carry none — so
    /// get-object-annotation still verifies the payload via that checksum
    /// and exits 0 with "written and verified". (The truly-unverifiable
    /// outcome needs a service that returns neither an AES256 MD5 ETag nor
    /// any checksum; real AWS cannot produce that, so it is pinned by the
    /// stub test in `cli_stub_server.rs` instead.)
    #[tokio::test]
    async fn get_object_annotation_on_kms_bucket_verifies_via_s3_computed_checksum() {
        TestHelper::init_dummy_tracing_subscriber();

        let helper = TestHelper::new().await;
        let bucket = TestHelper::generate_bucket_name();
        helper.create_bucket(&bucket, REGION).await;

        // Switch the bucket's default encryption to SSE-KMS (aws/s3 key).
        let default_encryption = ServerSideEncryptionByDefault::builder()
            .sse_algorithm(ServerSideEncryption::AwsKms)
            .build()
            .unwrap();
        let rule = ServerSideEncryptionRule::builder()
            .apply_server_side_encryption_by_default(default_encryption)
            .build();
        let encryption_config = ServerSideEncryptionConfiguration::builder()
            .rules(rule)
            .build()
            .unwrap();
        helper
            .client
            .put_bucket_encryption()
            .bucket(&bucket)
            .server_side_encryption_configuration(encryption_config)
            .send()
            .await
            .unwrap();

        let key = "kms-annotated-object.txt";
        helper
            .put_object(&bucket, key, b"kms object body".to_vec())
            .await;

        // Seed the annotation with the raw SDK — deliberately WITHOUT the
        // client-side CRC64NVME the s3util CLI always attaches — so the only
        // checksum left is the one S3 computes on its own.
        let payload = b"kms annotation payload";
        helper
            .client
            .put_object_annotation()
            .bucket(&bucket)
            .key(key)
            .annotation_name("kms-note")
            .annotation_payload(ByteStream::from(payload.to_vec()))
            .send()
            .await
            .unwrap();

        let tmp_dir = TestHelper::create_temp_dir();
        let outfile = tmp_dir.join("kms-annotation.bin");
        let outfile_path = outfile.to_str().unwrap();
        let object_arg = format!("s3://{bucket}/{key}");
        let out = run_s3util(&[
            "get-object-annotation",
            "--target-profile",
            "s3util-e2e-test",
            "--annotation-name",
            "kms-note",
            "-v",
            &object_arg,
            outfile_path,
        ]);

        helper.delete_bucket_with_cascade(&bucket).await;

        assert_eq!(
            out.status.code(),
            Some(0),
            "the get must succeed; stderr: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("written and verified"),
            "S3's auto-computed checksum must verify the payload; got: {stderr}"
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("aws:kms"),
            "the annotation must actually be KMS-encrypted (no MD5 ETag available); got: {stdout}"
        );
        assert!(
            stdout.contains("ChecksumCRC64NVME"),
            "the S3-computed CRC64NVME must be present — it is what verified the payload; got: {stdout}"
        );
        assert_eq!(
            std::fs::read(&outfile).unwrap(),
            payload,
            "the payload must be written"
        );
        let _ = std::fs::remove_dir_all(&tmp_dir);
    }
}
