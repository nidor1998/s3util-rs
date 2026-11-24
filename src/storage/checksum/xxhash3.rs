use base64::{Engine as _, engine::general_purpose};
use std::hash::Hasher as _;
use twox_hash::XxHash3_64;

use crate::storage::checksum::Checksum;

/// XXHASH3 is the 64-bit variant of XXH3 with the default seed and secret.
pub struct ChecksumXXHash3 {
    hasher: XxHash3_64,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 8;

impl Default for ChecksumXXHash3 {
    fn default() -> Self {
        ChecksumXXHash3 {
            hasher: XxHash3_64::new(),
            total_hash: vec![],
        }
    }
}

impl Checksum for ChecksumXXHash3 {
    // S3 does not support the full object checksum type for XXHASH3.
    // The flag is ignored, as for SHA1/SHA256.
    fn new(_full_object_checksum: bool) -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.hasher.write(data);
    }

    fn finalize(&mut self) -> String {
        // The canonical representation of an XXHash value is big-endian.
        let digest = self.hasher.finish().to_be_bytes();
        self.total_hash.extend_from_slice(digest.as_slice());
        self.hasher = XxHash3_64::new();
        general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn finalize_all(&mut self) -> String {
        let digest = XxHash3_64::oneshot(self.total_hash.as_slice()).to_be_bytes();
        self.hasher = XxHash3_64::new();
        format!(
            "{}-{}",
            general_purpose::STANDARD.encode(digest.as_slice()),
            self.total_hash.len() / HASH_SIZE_BYTE
        )
    }
}

#[cfg(test)]
mod tests {
    use tokio::fs::File;
    use tokio::io::AsyncReadExt;

    use super::*;
    use crate::storage::test_support::create_large_file;

    const LARGE_FILE_PATH: &str = "./tests/fixtures/large_data/50MiB";
    const LARGE_FILE_DIR: &str = "./tests/fixtures/large_data/";
    const LARGE_FILE_SIZE: usize = 50 * 1024 * 1024;

    // Regression values (the same as in s3sync's unit tests). The encoding is
    // pinned by the reference test vector below, and the composite construction
    // by `checksum_xxhash3_composite_matches_oneshot`.
    const CHECKSUM_PART_1_TO_3: &str = "g0NO1/Pil8o=";
    const CHECKSUM_LAST_PART: &str = "KbMzTVILtyI=";
    const CHECKSUM_TOTAL: &str = "UP876eHGiuo=-4";

    // XXH3_64bits("") = 0x2D06800538D394C2 (xxHash reference test vector).
    const CHECKSUM_EMPTY: &str = "LQaABTjTlMI=";

    #[tokio::test]
    async fn checksum_xxhash3_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumXXHash3::default();

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(889190);
        buffer.resize_with(889190, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_LAST_PART.to_string());

        assert_eq!(checksum.finalize_all(), CHECKSUM_TOTAL.to_string());
    }

    #[tokio::test]
    async fn checksum_xxhash3_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        // The full object flag must not change the result.
        let mut checksum = ChecksumXXHash3::new(true);

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(17179870);
        buffer.resize_with(17179870, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_PART_1_TO_3.to_string());

        let mut buffer = Vec::<u8>::with_capacity(889190);
        buffer.resize_with(889190, Default::default);
        file.read_exact(buffer.as_mut_slice()).await.unwrap();
        checksum.update(buffer.as_slice());
        assert_eq!(checksum.finalize(), CHECKSUM_LAST_PART.to_string());

        assert_eq!(checksum.finalize_all(), CHECKSUM_TOTAL.to_string());
    }

    #[test]
    fn checksum_xxhash3_known_answer_test() {
        init_dummy_tracing_subscriber();

        let mut checksum = ChecksumXXHash3::default();
        assert_eq!(checksum.finalize(), CHECKSUM_EMPTY);
    }

    // The composite checksum is the XXH3 of the concatenated big-endian part
    // digests. A part digest must not depend on how update() splits the data:
    // the split sizes fall on and around XXH3's 64-byte stripes, 256-byte
    // internal buffer and 1024-byte blocks, so the streaming state is carried
    // across every kind of boundary.
    #[test]
    fn checksum_xxhash3_composite_matches_oneshot() {
        init_dummy_tracing_subscriber();

        let parts: [Vec<u8>; 3] = [
            // Many XXH3 blocks, so the long-input streaming path is exercised.
            (0..300_000u32).map(|i| (i % 251) as u8).collect(),
            (0..1_234u32).map(|i| (i % 7) as u8).collect(),
            // At most 240 bytes: XXH3's separate short-input path.
            (0..200u32).map(|i| (i % 13) as u8).collect(),
        ];
        let digests: Vec<[u8; 8]> = parts
            .iter()
            .map(|part| XxHash3_64::oneshot(part).to_be_bytes())
            .collect();
        let expected_total = format!(
            "{}-3",
            general_purpose::STANDARD.encode(XxHash3_64::oneshot(&digests.concat()).to_be_bytes())
        );

        for split in [
            1, 63, 64, 65, 255, 256, 257, 1023, 1024, 1025, 4093, 300_000,
        ] {
            let mut checksum = ChecksumXXHash3::default();
            for (part, digest) in parts.iter().zip(&digests) {
                for chunk in part.chunks(split) {
                    checksum.update(chunk);
                }
                assert_eq!(
                    checksum.finalize(),
                    general_purpose::STANDARD.encode(digest),
                    "split {split}, part of {} bytes",
                    part.len()
                );
            }
            assert_eq!(checksum.finalize_all(), expected_total, "split {split}");
        }
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
