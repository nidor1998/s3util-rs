use std::path::Path;

use crate::storage::checksum::AdditionalChecksum;
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use anyhow::{Result, anyhow};
use aws_sdk_s3::types::ChecksumAlgorithm;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio::time::Instant;
use tracing::debug;

const UNKNOWN_CHECKSUM_VALUE: &str = "UNKNOWN";

pub fn is_multipart_upload_checksum(checksum: &Option<String>) -> bool {
    if checksum.is_none() {
        return false;
    }

    let find_result = checksum.as_ref().unwrap().find('-');
    find_result.is_some()
}

pub async fn generate_checksum_from_path(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    object_parts: Vec<i64>,
    multipart_threshold: usize,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }

    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path() start. It will take a time if the file is large."
    );

    let checksum_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;
    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);
    let parts_count = object_parts.len();
    let file_size = file.metadata().await?.len();

    let multipart =
        parts_count > 1 || (parts_count == 1 && multipart_threshold as i64 <= object_parts[0]);
    let mut read_bytes: usize = 0;
    let mut last_hash = "".to_string();
    for chunksize in object_parts {
        // `object_parts` comes from the source's own object-attributes response,
        // so a hostile or non-compliant endpoint controls these sizes. A
        // negative value wraps to a huge `usize`, and any value larger than the
        // bytes still unread cannot be a real part — either would size the buffer
        // far past the file's actual content. Reject both before allocating, and
        // fail closed with UNKNOWN exactly like the `read_bytes != file_size`
        // check below (an inconsistent part list can't reproduce a valid checksum).
        let remaining_bytes = file_size.saturating_sub(read_bytes as u64);
        if chunksize < 0 || chunksize as u64 > remaining_bytes {
            return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
        }
        let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
        buffer.resize_with(chunksize as usize, Default::default);
        let read_result = file.read_exact(buffer.as_mut_slice()).await;
        if let Err(e) = read_result {
            return if e.kind() != std::io::ErrorKind::UnexpectedEof {
                Err(anyhow!("Failed to read: {:?}", e))
            } else {
                Ok(UNKNOWN_CHECKSUM_VALUE.to_string())
            };
        }
        read_bytes += read_result?;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path() end."
        );
        return Ok(last_hash);
    }

    let final_checksum = Ok(checksum.finalize_all());
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path() end."
    );

    final_checksum
}

pub async fn generate_checksum_from_path_for_check(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart: bool,
    object_parts: Vec<i64>,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    if object_parts.is_empty() {
        panic!("parts_size is empty");
    }
    if !multipart && 2 <= object_parts.len() {
        panic!("multipart is false but object_parts has more than 1 element");
    }

    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path_for_check() start. It will take a time if the file is large."
    );

    let checksum_generate_start_time = Instant::now();

    let mut file = File::open(path).await?;
    let file_size = file.metadata().await?.len();

    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);
    let mut read_bytes: usize = 0;
    let mut last_hash = "".to_string();
    for chunksize in object_parts {
        // `object_parts` comes from the source's own object-attributes response,
        // so a hostile or non-compliant endpoint controls these sizes. A
        // negative value wraps to a huge `usize`, and any value larger than the
        // bytes still unread cannot be a real part — either would size the buffer
        // far past the file's actual content. Reject both before allocating, and
        // fail closed with UNKNOWN exactly like the `read_bytes != file_size`
        // check below (an inconsistent part list can't reproduce a valid checksum).
        let remaining_bytes = file_size.saturating_sub(read_bytes as u64);
        if chunksize < 0 || chunksize as u64 > remaining_bytes {
            return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
        }
        let mut buffer = Vec::<u8>::with_capacity(chunksize as usize);
        buffer.resize_with(chunksize as usize, Default::default);
        let read_result = file.read_exact(buffer.as_mut_slice()).await;
        if read_result.is_err() {
            if let Err(e) = read_result {
                if e.kind() != std::io::ErrorKind::UnexpectedEof {
                    return Err(anyhow!("Failed to read: {:?}", e));
                } else {
                    return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
                }
            }
        }
        read_bytes += read_result?;

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path_for_check() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        checksum.update(buffer.as_slice());
        last_hash = checksum.finalize()
    }

    if read_bytes != file_size as usize {
        return Ok(UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    if !multipart {
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path_for_check() end."
        );
        return Ok(last_hash);
    }

    let final_checksum = Ok(checksum.finalize_all());
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path_for_check() end."
    );

    final_checksum
}

pub async fn generate_checksum_from_path_with_chunksize(
    path: &Path,
    checksum_algorithm: ChecksumAlgorithm,
    multipart_chunksize: usize,
    multipart_threshold: usize,
    full_object_checksum: bool,
    cancellation_token: PipelineCancellationToken,
) -> Result<String> {
    let checksum_generate_start_time = Instant::now();
    debug!(
        path = path.to_str().unwrap(),
        "generate_checksum_from_path_with_chunksize() start. It will take a time if the file is large."
    );

    let mut file = File::open(path).await?;
    let mut remaining_bytes = file.metadata().await?.len();

    let mut checksum = AdditionalChecksum::new(checksum_algorithm, full_object_checksum);

    if remaining_bytes < multipart_threshold as u64 {
        let mut buffer = vec![0u8; remaining_bytes as usize];
        file.read_exact(buffer.as_mut_slice()).await?;
        checksum.update(buffer.as_slice());

        let final_checksum = Ok(checksum.finalize());
        debug!(
            path = path.to_str().unwrap(),
            duration_ms = checksum_generate_start_time.elapsed().as_millis(),
            "generate_checksum_from_path_with_chunksize() end."
        );
        return final_checksum;
    }

    while 0 < remaining_bytes {
        let real_chunksize: usize = if remaining_bytes < multipart_chunksize as u64 {
            remaining_bytes as usize
        } else {
            multipart_chunksize
        };

        let mut buffer = Vec::<u8>::with_capacity(real_chunksize);
        buffer.resize_with(real_chunksize, Default::default);
        file.read_exact(buffer.as_mut_slice()).await?;
        checksum.update(buffer.as_slice());
        // A full object checksum covers the whole object, so the file must not
        // be split into parts. Hash algorithms (SHA, MD5, XXHASH) ignore the
        // full object flag and would otherwise produce a composite "xxx-N"
        // checksum.
        if !full_object_checksum {
            let _ = checksum.finalize();
        }

        if cancellation_token.is_cancelled() {
            debug!(
                path = path.to_str().unwrap(),
                "generate_checksum_from_path_with_chunksize() is cancelled."
            );
            return Err(anyhow!(S3syncError::Cancelled));
        }

        remaining_bytes -= real_chunksize as u64;
    }

    let final_checksum = if full_object_checksum {
        Ok(checksum.finalize())
    } else {
        Ok(checksum.finalize_all())
    };
    debug!(
        path = path.to_str().unwrap(),
        duration_ms = checksum_generate_start_time.elapsed().as_millis(),
        "generate_checksum_from_path_with_chunksize() end."
    );

    final_checksum
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::storage::additional_checksum_verify::{
        UNKNOWN_CHECKSUM_VALUE, generate_checksum_from_path, generate_checksum_from_path_for_check,
        generate_checksum_from_path_with_chunksize, is_multipart_upload_checksum,
    };
    use crate::storage::test_support::create_large_file;
    use crate::types::token::create_pipeline_cancellation_token;
    use aws_sdk_s3::types::ChecksumAlgorithm;
    use tracing_subscriber::EnvFilter;

    const TEST_SHA256_BASE64_DIGEST: &str = "WZRHGrsBESr8wYFZ9sx0tPURuZgG2lmzyvWpwXPKz8U=";

    pub const LARGE_FILE_PATH: &str = "./tests/fixtures/large_data/9MiB";
    pub const LARGE_FILE_DIR: &str = "./tests/fixtures/large_data/";
    pub const LARGE_FILE_SIZE: usize = 9 * 1024 * 1024;

    const LARGE_FILE_SHA256_BASE64_FINAL_DIGEST: &str =
        "zWifJvli3SaQ9LZtHxzpOjkUE9x4ovgJZ+34As/NMwc=-2";
    const THRESHOLD_SHA256_BASE64_FINAL_DIGEST: &str =
        "CPUzmvYGEjhKR5UARYyMgSkXZMUvVQ2BMxs1zpgY97g=-1";

    const LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST: &str = "YyFLrQ==";
    const LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST: &str = "Gs/MBw==";
    const LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST: &str = "1q6/IYHP8XY=";

    const TEST_CRC32_BASE64_DIGEST: &str = "y/U6HA==";

    // Digests of the whole 9MiB zero-filled file as a single part, computed
    // independently with Python (hashlib, and a from-the-specification XXH64).
    const LARGE_FILE_SHA256_SINGLE_PART_BASE64_DIGEST: &str =
        "0u5HA82WmJRcp7n+FonqMJVZfqwaCv2NugDKx4lP3EM=";
    const LARGE_FILE_SHA1_SINGLE_PART_BASE64_DIGEST: &str = "j3ZZsPo5lPzOK+Biu+oNGD6bxE4=";
    const LARGE_FILE_SHA512_SINGLE_PART_BASE64_DIGEST: &str =
        "CBSQPS16xHGAnhdRo54040XWfiCzG/W8BE8ZC+pES/4ccMj8TSnl1cIMZX1611g8uh2BzXcH94/xVENxEwuTgQ==";
    const LARGE_FILE_MD5_SINGLE_PART_BASE64_DIGEST: &str = "uCtKuH5El2Akq8FKFnDawA==";
    const LARGE_FILE_XXHASH64_SINGLE_PART_BASE64_DIGEST: &str = "gYFOKDSXcG4=";

    // Composite checksums of the 9MiB zero-filled file uploaded in 8MiB + 1MiB
    // parts, computed independently with Python: base64(H(H(part1) || H(part2))) + "-2".
    const LARGE_FILE_MD5_BASE64_FINAL_DIGEST: &str = "0SbvCIF9BJDiB+RWywrggA==-2";
    const LARGE_FILE_SHA512_BASE64_FINAL_DIGEST: &str = "5gHNFwrytRkOHvbjgippRxal7BgRsDFnaswFJdS63mOgmpwtzh+3aXwzelbTU2I7Pd7keP7uWtYLvNvMNZ4nqA==-2";
    const LARGE_FILE_XXHASH64_BASE64_FINAL_DIGEST: &str = "T2YRqk6alD4=-2";

    // The same, uploaded in 5MiB + 4MiB parts.
    const LARGE_FILE_MD5_5MIB_PARTS_BASE64_FINAL_DIGEST: &str = "Dc+0kNe1KtCUfD806Eb2ow==-2";
    const LARGE_FILE_SHA512_5MIB_PARTS_BASE64_FINAL_DIGEST: &str = "+1/omnR7hjwN2nPCb60Y1CXfneq2oJTCMJinmAiW8RzxDlE7LbGlsOC4eb+xyHcB4dWXS7tZrOT+71sZXQ2LeA==-2";
    const LARGE_FILE_XXHASH64_5MIB_PARTS_BASE64_FINAL_DIGEST: &str = "ldoL7OJC/0M=-2";

    // The XXH3 variants have no independently computed values. Their expected
    // checksums are built here from twox-hash's one-shot API on each part, not
    // with `AdditionalChecksum`, so s3util's part splitting and composite
    // construction are checked against an independent construction.
    const XXH3_ALGORITHMS: [ChecksumAlgorithm; 2] =
        [ChecksumAlgorithm::Xxhash3, ChecksumAlgorithm::Xxhash128];

    /// The raw XXH3 digest of `data`, big-endian as S3 encodes it.
    fn xxh3_raw_digest(algorithm: &ChecksumAlgorithm, data: &[u8]) -> Vec<u8> {
        match algorithm {
            ChecksumAlgorithm::Xxhash3 => {
                twox_hash::XxHash3_64::oneshot(data).to_be_bytes().to_vec()
            }
            ChecksumAlgorithm::Xxhash128 => {
                twox_hash::XxHash3_128::oneshot(data).to_be_bytes().to_vec()
            }
            _ => unreachable!("{algorithm:?}"),
        }
    }

    /// The base64 single-part XXH3 checksum of `data`.
    fn xxh3_single_part(algorithm: &ChecksumAlgorithm, data: &[u8]) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        STANDARD.encode(xxh3_raw_digest(algorithm, data))
    }

    /// `base64(H(H(part1) || ... || H(partN)))-N` of `data` split into `part_sizes`.
    fn xxh3_composite(algorithm: &ChecksumAlgorithm, data: &[u8], part_sizes: &[usize]) -> String {
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        assert_eq!(part_sizes.iter().sum::<usize>(), data.len());
        let mut offset = 0;
        let mut concatenated = Vec::new();
        for size in part_sizes {
            concatenated.extend(xxh3_raw_digest(algorithm, &data[offset..offset + size]));
            offset += size;
        }
        format!(
            "{}-{}",
            STANDARD.encode(xxh3_raw_digest(algorithm, &concatenated)),
            part_sizes.len()
        )
    }

    #[test]
    fn is_multipart_upload_checksum_test() {
        init_dummy_tracing_subscriber();

        assert!(!is_multipart_upload_checksum(&None));
        assert!(!is_multipart_upload_checksum(&Some(
            "G2nS8dFcgMbpp8z9aYGNm97xnMGIf9BRjqs7z7hskVk=".to_string()
        )));

        assert!(is_multipart_upload_checksum(&Some(
            "wCfoo1d9Hfd+fHyNC38fUcN2GClCUsbud3NqVCs9Vww=-1".to_string()
        )));
        assert!(is_multipart_upload_checksum(&Some(
            "wCfoo1d9Hfd+fHyNC38fUcN2GClCUsbud3NqVCs9Vww=-2".to_string()
        )));
    }

    #[tokio::test]
    async fn generate_checksum_from_path_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![5],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_rejects_bogus_part_sizes() {
        // The part sizes come from the source's own object-attributes response,
        // so a hostile or non-compliant endpoint controls them. A negative size
        // wraps to a huge `usize` and an oversized size dwarfs the file; before
        // the guard, either sized the read buffer past the file and aborted the
        // allocation (OOM). Both must now fail closed with UNKNOWN.
        init_dummy_tracing_subscriber();

        for parts in [vec![-1], vec![i64::MAX], vec![5, -1], vec![5, i64::MAX]] {
            let checksum = generate_checksum_from_path(
                PathBuf::from("test_data/5byte.dat").as_path(),
                ChecksumAlgorithm::Sha256,
                parts,
                8 * 1024 * 1024,
                false,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
        }
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_rejects_bogus_part_sizes() {
        // Same guard on the `_for_check` variant.
        init_dummy_tracing_subscriber();

        for parts in [vec![-1], vec![i64::MAX]] {
            let checksum = generate_checksum_from_path_for_check(
                PathBuf::from("test_data/5byte.dat").as_path(),
                ChecksumAlgorithm::Sha256,
                false,
                parts,
                false,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
        }
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048575],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048577],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![8 * 1024 * 1024, 1048576, 5],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![7 * 1024 * 1024],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                vec![8 * 1024 * 1024, 1048576],
                8 * 1024 * 1024,
                false,
                cancel_token,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            vec![8 * 1024 * 1024, 1048576],
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            vec![8 * 1024 * 1024, 1048575],
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            vec![9 * 1024 * 1024],
            9 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, THRESHOLD_SHA256_BASE64_FINAL_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                false,
                cancel_token,
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_full_object_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Crc32,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_CRC32_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_multipart_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(
            checksum,
            LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_multipart_chunksize_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());
    }

    // A full object (single part) checksum of a file at or above the multipart
    // threshold: hash algorithms must digest the whole file at once instead of
    // producing a composite "xxx-N" checksum.
    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_single_part_hash_over_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, expected) in [
            (
                ChecksumAlgorithm::Sha256,
                LARGE_FILE_SHA256_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha1,
                LARGE_FILE_SHA1_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                LARGE_FILE_SHA512_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Md5,
                LARGE_FILE_MD5_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                LARGE_FILE_XXHASH64_SINGLE_PART_BASE64_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert_eq!(checksum, expected.to_string(), "{algorithm}");
        }

        // XXH3 variants: compare against a single digest of the whole file.
        let data = tokio::fs::read(LARGE_FILE_PATH).await.unwrap();
        for algorithm in XXH3_ALGORITHMS {
            let expected = xxh3_single_part(&algorithm, &data);

            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert!(!checksum.contains('-'), "{algorithm}: {checksum}");
            assert_eq!(checksum, expected, "{algorithm}");
        }
    }

    // CRC full object checksums must stay the same when the file is larger than the threshold.
    #[tokio::test]
    async fn generate_checksum_from_path_chunksize_crc_full_object_over_threshold_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, expected) in [
            (
                ChecksumAlgorithm::Crc32,
                LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Crc32C,
                LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Crc64Nvme,
                LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                8 * 1024 * 1024,
                8 * 1024 * 1024,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert_eq!(checksum, expected.to_string(), "{algorithm}");
        }
    }

    // The composite checksum that a local-to-S3 upload compares with the one
    // S3 calculates, for the new hash algorithms.
    #[tokio::test]
    async fn generate_checksum_from_path_multipart_chunksize_new_algorithms_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, chunksize, expected) in [
            (
                ChecksumAlgorithm::Md5,
                8 * 1024 * 1024,
                LARGE_FILE_MD5_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                8 * 1024 * 1024,
                LARGE_FILE_SHA512_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                8 * 1024 * 1024,
                LARGE_FILE_XXHASH64_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Md5,
                5 * 1024 * 1024,
                LARGE_FILE_MD5_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                5 * 1024 * 1024,
                LARGE_FILE_SHA512_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                5 * 1024 * 1024,
                LARGE_FILE_XXHASH64_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path_with_chunksize(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                chunksize,
                chunksize,
                false,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();

            assert_eq!(checksum, expected.to_string(), "{algorithm} {chunksize}");
        }

        let data = tokio::fs::read(LARGE_FILE_PATH).await.unwrap();
        for algorithm in XXH3_ALGORITHMS {
            for chunksize in [8 * 1024 * 1024, 5 * 1024 * 1024] {
                let expected =
                    xxh3_composite(&algorithm, &data, &[chunksize, LARGE_FILE_SIZE - chunksize]);

                let checksum = generate_checksum_from_path_with_chunksize(
                    PathBuf::from(LARGE_FILE_PATH).as_path(),
                    algorithm.clone(),
                    chunksize,
                    chunksize,
                    false,
                    create_pipeline_cancellation_token(),
                )
                .await
                .unwrap();

                assert_eq!(checksum, expected, "{algorithm} {chunksize}");
            }
        }
    }

    // The composite checksum that an S3-to-local download recomputes from the
    // part sizes of the source object, for the new hash algorithms.
    #[tokio::test]
    async fn generate_checksum_from_path_multipart_new_algorithms_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        for (algorithm, parts, expected) in [
            (
                ChecksumAlgorithm::Md5,
                vec![8 * 1024 * 1024, 1024 * 1024],
                LARGE_FILE_MD5_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                vec![8 * 1024 * 1024, 1024 * 1024],
                LARGE_FILE_SHA512_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                vec![8 * 1024 * 1024, 1024 * 1024],
                LARGE_FILE_XXHASH64_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Md5,
                vec![5 * 1024 * 1024, 4 * 1024 * 1024],
                LARGE_FILE_MD5_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                vec![5 * 1024 * 1024, 4 * 1024 * 1024],
                LARGE_FILE_SHA512_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                vec![5 * 1024 * 1024, 4 * 1024 * 1024],
                LARGE_FILE_XXHASH64_5MIB_PARTS_BASE64_FINAL_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                parts.clone(),
                5 * 1024 * 1024,
                false,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, expected.to_string(), "{algorithm} {parts:?}");

            let checksum = generate_checksum_from_path_for_check(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                true,
                parts.clone(),
                false,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, expected.to_string(), "{algorithm} {parts:?}");
        }

        // A single-part object (no "-N" suffix) is verified with the digest of
        // the whole file, even though the file is larger than 8MiB.
        for (algorithm, expected) in [
            (
                ChecksumAlgorithm::Md5,
                LARGE_FILE_MD5_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Sha512,
                LARGE_FILE_SHA512_SINGLE_PART_BASE64_DIGEST,
            ),
            (
                ChecksumAlgorithm::Xxhash64,
                LARGE_FILE_XXHASH64_SINGLE_PART_BASE64_DIGEST,
            ),
        ] {
            let checksum = generate_checksum_from_path(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                vec![LARGE_FILE_SIZE as i64],
                LARGE_FILE_SIZE + 1,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, expected.to_string(), "{algorithm}");
        }

        let data = tokio::fs::read(LARGE_FILE_PATH).await.unwrap();
        for algorithm in XXH3_ALGORITHMS {
            for parts in [
                vec![8 * 1024 * 1024, 1024 * 1024],
                vec![5 * 1024 * 1024, 4 * 1024 * 1024],
            ] {
                let expected = xxh3_composite(&algorithm, &data, &parts);
                let part_sizes: Vec<i64> = parts.iter().map(|size| *size as i64).collect();

                let checksum = generate_checksum_from_path(
                    PathBuf::from(LARGE_FILE_PATH).as_path(),
                    algorithm.clone(),
                    part_sizes.clone(),
                    5 * 1024 * 1024,
                    false,
                    create_pipeline_cancellation_token(),
                )
                .await
                .unwrap();
                assert_eq!(checksum, expected, "{algorithm} {parts:?}");

                let checksum = generate_checksum_from_path_for_check(
                    PathBuf::from(LARGE_FILE_PATH).as_path(),
                    algorithm.clone(),
                    true,
                    part_sizes,
                    false,
                    create_pipeline_cancellation_token(),
                )
                .await
                .unwrap();
                assert_eq!(checksum, expected, "{algorithm} {parts:?}");
            }

            // The single-part counterpart, as above.
            let checksum = generate_checksum_from_path(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                algorithm.clone(),
                vec![LARGE_FILE_SIZE as i64],
                LARGE_FILE_SIZE + 1,
                true,
                create_pipeline_cancellation_token(),
            )
            .await
            .unwrap();
            assert_eq!(checksum, xxh3_single_part(&algorithm, &data), "{algorithm}");
        }
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_test() {
        init_dummy_tracing_subscriber();

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![5],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();

        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }
    #[tokio::test]
    async fn generate_checksum_from_path_for_check_multipart_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048576],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, LARGE_FILE_SHA256_BASE64_FINAL_DIGEST.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048575],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048577],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![8 * 1024 * 1024, 1048576, 5],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Sha256,
            true,
            vec![7 * 1024 * 1024],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, UNKNOWN_CHECKSUM_VALUE.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_multipart_cancel_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let cancel_token = create_pipeline_cancellation_token();
        cancel_token.cancel();

        assert!(
            generate_checksum_from_path_for_check(
                PathBuf::from(LARGE_FILE_PATH).as_path(),
                ChecksumAlgorithm::Sha256,
                true,
                vec![8 * 1024 * 1024, 1048576],
                false,
                cancel_token
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_full_object_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc32C,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC32C_FULL_OBJECT_BASE64_DIGEST.to_string()
        );

        let checksum = generate_checksum_from_path_for_check(
            PathBuf::from(LARGE_FILE_PATH).as_path(),
            ChecksumAlgorithm::Crc64Nvme,
            true,
            vec![8 * 1024 * 1024, 1048576],
            true,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(
            checksum,
            LARGE_FILE_CRC64NVME_FULL_OBJECT_BASE64_DIGEST.to_string()
        );
    }

    #[tokio::test]
    #[should_panic(expected = "parts_size is empty")]
    async fn generate_checksum_from_path_panics_on_empty_parts() {
        // Empty object_parts is unreachable in normal flow but the helper
        // panics defensively to surface caller bugs.
        let _ = generate_checksum_from_path(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "parts_size is empty")]
    async fn generate_checksum_from_path_for_check_panics_on_empty_parts() {
        let _ = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[tokio::test]
    #[should_panic(expected = "multipart is false")]
    async fn generate_checksum_from_path_for_check_panics_on_multi_parts_with_multipart_false() {
        // multipart=false implies caller intends a single-part hash, so passing
        // multiple parts is a programming error and must not be silently accepted.
        let _ = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![3, 2],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
    }

    #[tokio::test]
    async fn generate_checksum_from_path_with_chunksize_below_threshold_returns_single_hash() {
        // File smaller than multipart_threshold ⇒ single-shot path that returns
        // the per-call finalize() rather than finalize_all(). 5-byte file with
        // SHA-256 must produce the well-known fixed digest.
        let checksum = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/5byte.dat").as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap();
        assert_eq!(checksum, TEST_SHA256_BASE64_DIGEST.to_string());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_open_error_propagates() {
        // Non-existent path → File::open fails, error bubbles out before any
        // hashing happens.
        let result = generate_checksum_from_path(
            PathBuf::from("test_data/__never_exists__.bin").as_path(),
            ChecksumAlgorithm::Sha256,
            vec![1],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_open_error_propagates() {
        let result = generate_checksum_from_path_for_check(
            PathBuf::from("test_data/__never_exists__.bin").as_path(),
            ChecksumAlgorithm::Sha256,
            false,
            vec![1],
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn generate_checksum_from_path_with_chunksize_open_error_propagates() {
        let result = generate_checksum_from_path_with_chunksize(
            PathBuf::from("test_data/__never_exists__.bin").as_path(),
            ChecksumAlgorithm::Sha256,
            8 * 1024 * 1024,
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await;
        assert!(result.is_err());
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }

    #[tokio::test]
    async fn generate_checksum_from_path_read_failure_is_an_error() {
        init_dummy_tracing_subscriber();

        // On Unix a directory opens fine but read(2) fails with a non-EOF
        // error (EISDIR), driving the hard-error arm of the read loop. On
        // Windows File::open on a directory already fails (ERROR_ACCESS_DENIED),
        // so the read loop is unreachable and only the error surface is
        // asserted there.
        let dir = tempfile::tempdir().unwrap();
        let err = generate_checksum_from_path(
            &PathBuf::from(dir.path()),
            ChecksumAlgorithm::Sha256,
            vec![1],
            8 * 1024 * 1024,
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap_err();
        if cfg!(not(windows)) {
            assert!(
                err.to_string().contains("Failed to read"),
                "unexpected error: {err:#}"
            );
        }
    }

    #[tokio::test]
    async fn generate_checksum_from_path_for_check_read_failure_is_an_error() {
        init_dummy_tracing_subscriber();

        let dir = tempfile::tempdir().unwrap();
        let err = generate_checksum_from_path_for_check(
            &PathBuf::from(dir.path()),
            ChecksumAlgorithm::Sha256,
            false,
            vec![1],
            false,
            create_pipeline_cancellation_token(),
        )
        .await
        .unwrap_err();
        // See generate_checksum_from_path_read_failure_is_an_error: the read
        // loop's error arm is only reachable on non-Windows platforms.
        if cfg!(not(windows)) {
            assert!(
                err.to_string().contains("Failed to read"),
                "unexpected error: {err:#}"
            );
        }
    }
}
