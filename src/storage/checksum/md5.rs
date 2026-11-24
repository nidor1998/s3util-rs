use base64::{Engine as _, engine::general_purpose};

use crate::storage::checksum::Checksum;

pub struct ChecksumMd5 {
    hasher: md5::Context,
    total_hash: Vec<u8>,
}

const HASH_SIZE_BYTE: usize = 16;

impl Default for ChecksumMd5 {
    fn default() -> Self {
        ChecksumMd5 {
            hasher: md5::Context::new(),
            total_hash: vec![],
        }
    }
}

impl Checksum for ChecksumMd5 {
    // MD5 is a hash algorithm, so S3 never stores it as a full object checksum
    // for multipart uploads. The flag is ignored, as for SHA1/SHA256.
    fn new(_full_object_checksum: bool) -> Self {
        Self::default()
    }

    fn update(&mut self, data: &[u8]) {
        self.hasher.consume(data);
    }

    fn finalize(&mut self) -> String {
        let digest = std::mem::replace(&mut self.hasher, md5::Context::new()).finalize();
        self.total_hash.extend_from_slice(digest.as_slice());
        general_purpose::STANDARD.encode(digest.as_slice())
    }

    fn finalize_all(&mut self) -> String {
        let mut hasher = md5::Context::new();
        hasher.consume(self.total_hash.as_slice());
        let digest = hasher.finalize();
        self.hasher = md5::Context::new();
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
    // base64(md5(17179870 zero bytes)), base64(md5(889190 zero bytes)) and
    // base64(md5(part * 3 + last)) + "-4".
    const CHECKSUM_PART_1_TO_3: &str = "g6GGHKcEBCaDJ3oEWdqz1Q==";
    const CHECKSUM_LAST_PART: &str = "q/S4Q07QcvKbBGJ1nnYBMg==";
    const CHECKSUM_TOTAL: &str = "6E0aXvH2r9sNLDByrfX7pQ==-4";

    // RFC 1321 test vectors, base64-encoded.
    const CHECKSUM_EMPTY: &str = "1B2M2Y8AsgTpgAmY7PhCfg==";
    const CHECKSUM_ABC: &str = "kAFQmDzST7DWlj99KOF/cg==";

    #[tokio::test]
    async fn checksum_md5_test() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        let mut checksum = ChecksumMd5::default();

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
    async fn checksum_md5_test_with_new() {
        init_dummy_tracing_subscriber();

        create_large_file(LARGE_FILE_PATH, LARGE_FILE_DIR, LARGE_FILE_SIZE).await;
        let mut file = File::open(LARGE_FILE_PATH).await.unwrap();

        // The full object flag must not change the result of a hash algorithm.
        let mut checksum = ChecksumMd5::new(true);

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
    fn checksum_md5_known_answer_test() {
        init_dummy_tracing_subscriber();

        let mut checksum = ChecksumMd5::default();
        assert_eq!(checksum.finalize(), CHECKSUM_EMPTY);

        // update() may be called several times for one part.
        checksum.update(b"a");
        checksum.update(b"bc");
        assert_eq!(checksum.finalize(), CHECKSUM_ABC);
    }

    #[test]
    fn checksum_md5_finalize_starts_a_new_part() {
        init_dummy_tracing_subscriber();

        // Data of a finalized part must not leak into the next part.
        let mut checksum = ChecksumMd5::default();
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
