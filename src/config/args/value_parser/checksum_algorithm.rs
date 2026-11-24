const INVALID_CHECKSUM_ALGORITHM: &str = "invalid checksum_algorithm. valid choices: CRC32 | CRC32C | CRC64NVME | SHA1 | SHA256 | SHA512 | MD5 | XXHASH128 | XXHASH3 | XXHASH64 .";

pub fn parse_checksum_algorithm(checksum_algorithm: &str) -> Result<String, String> {
    match checksum_algorithm {
        "CRC32" | "CRC32C" | "CRC64NVME" | "SHA1" | "SHA256" | "SHA512" | "MD5" | "XXHASH128"
        | "XXHASH3" | "XXHASH64" => Ok(checksum_algorithm.to_string()),
        _ => Err(INVALID_CHECKSUM_ALGORITHM.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::types::ChecksumAlgorithm;

    const ALL_ALGORITHMS: [&str; 10] = [
        "CRC32",
        "CRC32C",
        "CRC64NVME",
        "SHA1",
        "SHA256",
        "SHA512",
        "MD5",
        "XXHASH128",
        "XXHASH3",
        "XXHASH64",
    ];

    #[test]
    fn accepts_known_checksum_algorithms() {
        for algo in ALL_ALGORITHMS {
            assert_eq!(parse_checksum_algorithm(algo).unwrap(), algo);
        }
    }

    #[test]
    fn accepted_values_are_the_sdk_algorithm_names() {
        // The accepted value is converted with `ChecksumAlgorithm::from` later.
        // It must map to a known variant, not to `Unknown`.
        for algo in ALL_ALGORITHMS {
            let parsed = ChecksumAlgorithm::from(parse_checksum_algorithm(algo).unwrap().as_str());
            assert!(
                ChecksumAlgorithm::values().contains(&parsed.as_str()),
                "{algo} must be a known SDK algorithm"
            );
            assert_eq!(parsed.as_str(), algo);
        }

        // Every algorithm the SDK knows is accepted.
        for algo in ChecksumAlgorithm::values() {
            assert!(parse_checksum_algorithm(algo).is_ok(), "{algo}");
        }
    }

    #[test]
    fn rejects_unknown_checksum_algorithm() {
        for algo in ["SHA3", "XXHASH32", "CRC64", "", "sha256", "md5", "XxHash64"] {
            let err = parse_checksum_algorithm(algo).unwrap_err();
            assert!(err.contains("invalid checksum_algorithm"), "{algo}");
        }
    }

    #[test]
    fn error_message_lists_every_valid_choice() {
        let err = parse_checksum_algorithm("bogus").unwrap_err();
        for algo in ALL_ALGORITHMS {
            assert!(
                err.split(['|', ' ', ':']).any(|choice| choice == algo),
                "{algo} is missing from: {err}"
            );
        }
    }
}
