use aws_sdk_s3::types::ChecksumAlgorithm;

use crate::storage::checksum::crc32::ChecksumCRC32;
use crate::storage::checksum::crc32_c::ChecksumCRC32c;
use crate::storage::checksum::crc64_nvme::ChecksumCRC64NVMe;
use crate::storage::checksum::sha1::ChecksumSha1;
use crate::storage::checksum::sha256::ChecksumSha256;

pub mod crc32;
pub mod crc32_c;
pub mod crc64_nvme;
pub mod sha1;
pub mod sha256;

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
            _ => {
                panic!("Unknown ChecksumAlgorithm")
            }
        }
    }

    /// Whether [`AdditionalChecksum::new`] can construct a checksum for
    /// `algorithm` — i.e. s3util can recompute and verify it locally. Kept in
    /// sync with the `match` in `new`: any algorithm not handled there (e.g.
    /// `SHA512`, `MD5`, the `XXHASH*` family, or an `Unknown` variant) returns
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
        ] {
            assert!(
                AdditionalChecksum::is_supported(&algo),
                "{algo:?} should be supported"
            );
        }
    }

    #[test]
    fn is_supported_false_for_uncomputable_algorithms() {
        // Algorithms the SDK exposes but `AdditionalChecksum::new` cannot build
        // (they would hit its `panic!`). `is_supported` must gate them out so
        // callers can error in advance instead.
        for raw in ["SHA512", "MD5", "XXHASH64", "XXHASH3", "XXHASH128", "bogus"] {
            let algo = ChecksumAlgorithm::from(raw);
            assert!(
                !AdditionalChecksum::is_supported(&algo),
                "{raw} must not be reported as supported"
            );
        }
    }
}
