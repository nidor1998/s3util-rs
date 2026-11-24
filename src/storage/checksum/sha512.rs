use base64::{Engine as _, engine::general_purpose};
use sha2;
use sha2::Digest;

use crate::storage::checksum::Checksum;

pub struct ChecksumSha512 {
    hasher: sha2::Sha512,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 64;

impl Default for ChecksumSha512 {
    fn default() -> Self {
        ChecksumSha512 {
            hasher: sha2::Sha512::new(),
            total_hash: vec![],
        }
    }
}

// sha2 uses generic-array v0.x internally, which is deprecated.
// Suppress warnings until the underlying library is updated.
#[allow(deprecated)]
impl Checksum for ChecksumSha512 {
    // SHA512 is a hash algorithm, so S3 never stores it as a full object
    // checksum for multipart uploads. The flag is ignored, as for SHA1/SHA256.
    fn new(_full_object_checksum: bool) -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.hasher.update(data);
    }

    fn finalize(&mut self) -> String {
        let digest = self.hasher.finalize_reset();
        self.total_hash.extend_from_slice(digest.as_slice());
        general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn finalize_all(&mut self) -> String {
        self.hasher.reset();
        self.hasher.update(self.total_hash.as_slice());
        let digest = self.hasher.finalize_reset();
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

    // Independently computed with Python's hashlib over zero-filled buffers:
    // base64(sha512(17179870 zero bytes)), base64(sha512(889190 zero bytes)) and
    // base64(sha512(part * 3 + last)) + "-4".
    const CHECKSUM_PART_1_TO_3: &str =
        "007C5Od555bSYB4tw0iayLg/OBJfOV0BVh4qwLZtdVkIy1+vezx+LIELM83SCl7nISkzbXe5yrbve6Ull+paqA==";
    const CHECKSUM_LAST_PART: &str =
        "5CcjjEfTOfVWYGi6pTk0lcB437YtnBQe17kEiXlfG+6nsc+TcUS8L+41sxSsWw7Vo3ArkCIL2n4sPqbvaqF1/g==";
    const CHECKSUM_TOTAL: &str = "5qRYilX4F5jQqYoodGngocR+JSTI/8/r90eTYCrWH4sd6gfRDkLlxMAitTbkbzFoJL7Q8xg+Sk5Z4XVumthJ4Q==-4";

    // FIPS 180-4 test vectors, base64-encoded.
    const CHECKSUM_EMPTY: &str =
        "z4PhNX7vuL3xVChQ1m2AB9Yg5AULVxXcg/SpIdNs6c5H0NE8XYXysP+DGNKHfuwvY7kxvUdBeoGlODJ6+SfaPg==";
    const CHECKSUM_ABC: &str =
        "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw==";

    #[tokio::test]
    async fn checksum_sha512_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumSha512::default();

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
    async fn checksum_sha512_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        // The full object flag must not change the result of a hash algorithm.
        let mut checksum = ChecksumSha512::new(true);

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
    fn checksum_sha512_known_answer_test() {
        init_dummy_tracing_subscriber();

        let mut checksum = ChecksumSha512::default();
        assert_eq!(checksum.finalize(), CHECKSUM_EMPTY);

        // update() may be called several times for one part.
        checksum.update(b"a");
        checksum.update(b"bc");
        assert_eq!(checksum.finalize(), CHECKSUM_ABC);
    }

    #[test]
    fn checksum_sha512_finalize_starts_a_new_part() {
        init_dummy_tracing_subscriber();

        // Data of a finalized part must not leak into the next part.
        let mut checksum = ChecksumSha512::default();
        checksum.update(b"xyz");
        let _ = checksum.finalize();
        checksum.update(b"abc");
        assert_eq!(checksum.finalize(), CHECKSUM_ABC);
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
