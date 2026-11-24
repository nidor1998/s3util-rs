use base64::{Engine as _, engine::general_purpose};
use std::hash::Hasher as _;
use twox_hash::XxHash64;

use crate::storage::checksum::Checksum;

// S3 computes XXHASH64 with the default seed.
const SEED: u64 = 0;

pub struct ChecksumXXHash64 {
    hasher: XxHash64,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 8;

impl Default for ChecksumXXHash64 {
    fn default() -> Self {
        ChecksumXXHash64 {
            hasher: XxHash64::with_seed(SEED),
            total_hash: vec![],
        }
    }
}

impl Checksum for ChecksumXXHash64 {
    // S3 does not support the full object checksum type for XXHASH64.
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
        self.hasher = XxHash64::with_seed(SEED);
        general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn finalize_all(&mut self) -> String {
        let digest = XxHash64::oneshot(SEED, self.total_hash.as_slice()).to_be_bytes();
        self.hasher = XxHash64::with_seed(SEED);
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

    // Independently computed with a from-the-specification Python implementation
    // of XXH64 (seed 0) over zero-filled buffers, encoded as base64 of the
    // big-endian hash: 17179870 zero bytes, 889190 zero bytes and the
    // concatenation of the four part digests (+ "-4").
    const CHECKSUM_PART_1_TO_3: &str = "q6TmMlqFqls=";
    const CHECKSUM_LAST_PART: &str = "SVdoea44HMY=";
    const CHECKSUM_TOTAL: &str = "ivM+ESXvuqM=-4";

    // XXH64("", seed = 0) = 0xEF46DB3751D8E999 (xxHash reference test vector).
    const CHECKSUM_EMPTY: &str = "70bbN1HY6Zk=";

    #[tokio::test]
    async fn checksum_xxhash64_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumXXHash64::default();

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
    async fn checksum_xxhash64_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        // The full object flag must not change the result.
        let mut checksum = ChecksumXXHash64::new(true);

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
    fn checksum_xxhash64_known_answer_test() {
        init_dummy_tracing_subscriber();

        let mut checksum = ChecksumXXHash64::default();
        assert_eq!(checksum.finalize(), CHECKSUM_EMPTY);
    }

    #[test]
    fn checksum_xxhash64_finalize_starts_a_new_part() {
        init_dummy_tracing_subscriber();

        // Data of a finalized part must not leak into the next part, and the
        // per-part value must not depend on how update() splits the data.
        let mut checksum = ChecksumXXHash64::default();
        checksum.update(b"xyz");
        let _ = checksum.finalize();
        checksum.update(b"a");
        checksum.update(b"bc");
        let split = checksum.finalize();

        let mut fresh = ChecksumXXHash64::default();
        fresh.update(b"abc");
        assert_eq!(split, fresh.finalize());
        assert_ne!(split, CHECKSUM_EMPTY);
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
