use aws_sdk_s3::types::ChecksumAlgorithm;

use crate::storage::checksum::crc32::ChecksumCRC32;
use crate::storage::checksum::crc32_c::ChecksumCRC32c;
use crate::storage::checksum::crc64_nvme::ChecksumCRC64NVMe;
use crate::storage::checksum::md5::ChecksumMd5;
use crate::storage::checksum::sha1::ChecksumSha1;
use crate::storage::checksum::sha256::ChecksumSha256;
use crate::storage::checksum::sha512::ChecksumSha512;
use crate::storage::checksum::xxhash3::ChecksumXXHash3;
use crate::storage::checksum::xxhash64::ChecksumXXHash64;
use crate::storage::checksum::xxhash128::ChecksumXXHash128;

pub mod crc32;
pub mod crc32_c;
pub mod crc64_nvme;
pub mod md5;
pub mod sha1;
pub mod sha256;
pub mod sha512;
pub mod xxhash128;
pub mod xxhash3;
pub mod xxhash64;

pub trait Checksum {
    fn new(full_object_checksum: bool) -> Self
    where
        Self: Sized;
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> String;
    fn finalize_all(&mut self) -> String;
}

pub struct AdditionalChecksum {
    checksum: Box<dyn Checksum + Send + Sync + 'static>,
}

impl AdditionalChecksum {
    pub fn new(algorithm: ChecksumAlgorithm, full_object_checksum: bool) -> Self {
        match algorithm {
            ChecksumAlgorithm::Sha1 => AdditionalChecksum {
                checksum: Box::<ChecksumSha1>::default(),
            },
            ChecksumAlgorithm::Sha256 => AdditionalChecksum {
                checksum: Box::<ChecksumSha256>::default(),
            },
            ChecksumAlgorithm::Crc32 => AdditionalChecksum {
                checksum: Box::new(ChecksumCRC32::new(full_object_checksum)),
            },
            ChecksumAlgorithm::Crc32C => AdditionalChecksum {
                checksum: Box::new(ChecksumCRC32c::new(full_object_checksum)),
            },
            ChecksumAlgorithm::Crc64Nvme => AdditionalChecksum {
                checksum: Box::<ChecksumCRC64NVMe>::default(),
            },
            ChecksumAlgorithm::Sha512 => AdditionalChecksum {
                checksum: Box::<ChecksumSha512>::default(),
            },
            ChecksumAlgorithm::Md5 => AdditionalChecksum {
                checksum: Box::<ChecksumMd5>::default(),
            },
            ChecksumAlgorithm::Xxhash64 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash64>::default(),
            },
            ChecksumAlgorithm::Xxhash3 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash3>::default(),
            },
            ChecksumAlgorithm::Xxhash128 => AdditionalChecksum {
                checksum: Box::<ChecksumXXHash128>::default(),
            },
            _ => {
                panic!("Unknown ChecksumAlgorithm")
            }
        }
    }

    /// Whether [`AdditionalChecksum::new`] can construct a checksum for
    /// `algorithm` — i.e. s3util can recompute and verify it locally. Kept in
    /// sync with the `match` in `new`: any algorithm not handled there (an
    /// `Unknown` variant, or an algorithm added to the SDK later) returns
    /// `false` and would otherwise hit the `panic!` in `new`. Check this before
    /// constructing an `AdditionalChecksum` from a server-provided algorithm.
    pub fn is_supported(algorithm: &ChecksumAlgorithm) -> bool {
        matches!(
            algorithm,
            ChecksumAlgorithm::Sha1
                | ChecksumAlgorithm::Sha256
                | ChecksumAlgorithm::Crc32
                | ChecksumAlgorithm::Crc32C
                | ChecksumAlgorithm::Crc64Nvme
                | ChecksumAlgorithm::Sha512
                | ChecksumAlgorithm::Md5
                | ChecksumAlgorithm::Xxhash64
                | ChecksumAlgorithm::Xxhash3
                | ChecksumAlgorithm::Xxhash128
        )
    }

    pub fn update(&mut self, data: &[u8]) {
        self.checksum.update(data)
    }
    pub fn finalize(&mut self) -> String {
        self.checksum.finalize()
    }
    pub fn finalize_all(&mut self) -> String {
        self.checksum.finalize_all()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha1_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha1, false);
    }

    #[test]
    fn sha256_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha256, false);
    }

    #[test]
    fn crc32_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32, true);
    }

    #[test]
    fn crc32c_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32C, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc32C, true);
    }

    #[test]
    fn crc64nvme_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Crc64Nvme, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Crc64Nvme, true);
    }

    #[test]
    fn sha512_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Sha512, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Sha512, true);
    }

    #[test]
    fn md5_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Md5, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Md5, true);
    }

    #[test]
    fn xxhash64_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash64, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash64, true);
    }

    #[test]
    fn xxhash3_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash3, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash3, true);
    }

    #[test]
    fn xxhash128_test() {
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash128, false);
        AdditionalChecksum::new(ChecksumAlgorithm::Xxhash128, true);
    }

    // `AdditionalChecksum::new` must dispatch every algorithm to its own
    // implementation, so a wrong arm (e.g. MD5 built as SHA1) is detected.
    // The SHA/MD5/CRC32 values are base64 of the digest of "abc", computed
    // independently with Python's hashlib/zlib. The XXHash values are taken
    // from twox-hash's one-shot API (their encoding is pinned by the reference
    // test vectors in each module).
    #[test]
    fn dispatches_each_algorithm_to_its_own_implementation() {
        use base64::{Engine as _, engine::general_purpose};

        let base64 = |digest: &[u8]| general_purpose::STANDARD.encode(digest);
        let expected: Vec<(ChecksumAlgorithm, String)> = vec![
            (
                ChecksumAlgorithm::Sha1,
                "qZk+NkcGgWq6PiVxeFDCbJzQ2J0=".to_string(),
            ),
            (
                ChecksumAlgorithm::Sha256,
                "ungWv48Bz+pBQUDeXa4iI7ADYaOWF3qctBD/YfIAFa0=".to_string(),
            ),
            (
                ChecksumAlgorithm::Sha512,
                "3a81oZNherrMQXNJriBBMRLm+k6JqX6iCp7u5ktV05ohkpkqJ0/BqDa6PCOj/uu9RU1EI2Q86A4qmslPpUyknw=="
                    .to_string(),
            ),
            (
                ChecksumAlgorithm::Md5,
                "kAFQmDzST7DWlj99KOF/cg==".to_string(),
            ),
            (ChecksumAlgorithm::Crc32, "NSRBwg==".to_string()),
            (
                ChecksumAlgorithm::Xxhash64,
                base64(&twox_hash::XxHash64::oneshot(0, b"abc").to_be_bytes()),
            ),
            (
                ChecksumAlgorithm::Xxhash3,
                base64(&twox_hash::XxHash3_64::oneshot(b"abc").to_be_bytes()),
            ),
            (
                ChecksumAlgorithm::Xxhash128,
                base64(&twox_hash::XxHash3_128::oneshot(b"abc").to_be_bytes()),
            ),
        ];

        for (algorithm, expected) in expected {
            let mut checksum = AdditionalChecksum::new(algorithm.clone(), false);
            checksum.update(b"abc");
            assert_eq!(checksum.finalize(), expected, "{algorithm:?}");
        }

        // The three XXHash variants must differ from each other.
        let values: std::collections::HashSet<String> = [
            ChecksumAlgorithm::Xxhash64,
            ChecksumAlgorithm::Xxhash3,
            ChecksumAlgorithm::Xxhash128,
        ]
        .into_iter()
        .map(|algorithm| {
            let mut checksum = AdditionalChecksum::new(algorithm, false);
            checksum.update(b"abc");
            checksum.finalize()
        })
        .collect();
        assert_eq!(values.len(), 3);
    }

    #[test]
    #[should_panic(expected = "Unknown ChecksumAlgorithm")]
    fn unknown_algorithm_panics() {
        // ChecksumAlgorithm is #[non_exhaustive] and its From<&str> impl returns
        // ChecksumAlgorithm::Unknown(_) for unrecognized inputs, which lands on
        // the `_ =>` arm of AdditionalChecksum::new.
        let unknown = ChecksumAlgorithm::from("bogus");
        AdditionalChecksum::new(unknown, false);
    }

    #[test]
    fn is_supported_true_for_computable_algorithms() {
        for algo in [
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Crc32C,
            ChecksumAlgorithm::Crc64Nvme,
            ChecksumAlgorithm::Sha512,
            ChecksumAlgorithm::Md5,
            ChecksumAlgorithm::Xxhash64,
            ChecksumAlgorithm::Xxhash3,
            ChecksumAlgorithm::Xxhash128,
        ] {
            assert!(
                AdditionalChecksum::is_supported(&algo),
                "{algo:?} should be supported"
            );
            // `is_supported` must agree with `new`: constructing must not panic.
            AdditionalChecksum::new(algo, false);
        }
    }

    #[test]
    fn is_supported_true_for_every_known_sdk_algorithm() {
        // Every algorithm the SDK knows must be supported; a new SDK variant
        // shows up here (and in `new`) instead of silently skipping verification.
        for algo in ChecksumAlgorithm::values() {
            let algo = ChecksumAlgorithm::from(*algo);
            assert!(
                AdditionalChecksum::is_supported(&algo),
                "{algo:?} should be supported"
            );
        }
    }

    #[test]
    fn is_supported_false_for_unknown_algorithms() {
        // An algorithm `AdditionalChecksum::new` cannot build (it would hit
        // its `panic!`). `is_supported` must gate it out so callers can error
        // in advance instead.
        for raw in ["bogus", "SHA3", "sha256"] {
            let algo = ChecksumAlgorithm::from(raw);
            assert!(
                !AdditionalChecksum::is_supported(&algo),
                "{raw} must not be reported as supported"
            );
        }
    }
}
