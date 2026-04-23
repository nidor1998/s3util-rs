const INVALID_CHECKSUM_ALGORITHM: &str =
    "invalid checksum_algorithm. valid choices: CRC32 | CRC32C | CRC64NVME | SHA1 | SHA256 .";

pub fn parse_checksum_algorithm(checksum_algorithm: &str) -> Result<String, String> {
    match checksum_algorithm {
        "CRC32" | "CRC32C" | "CRC64NVME" | "SHA1" | "SHA256" => Ok(checksum_algorithm.to_string()),
        _ => Err(INVALID_CHECKSUM_ALGORITHM.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_known_checksum_algorithms() {
        for algo in ["CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"] {
            assert_eq!(parse_checksum_algorithm(algo).unwrap(), algo);
        }
    }

    #[test]
    fn rejects_unknown_checksum_algorithm() {
        let err = parse_checksum_algorithm("MD5").unwrap_err();
        assert!(err.contains("invalid checksum_algorithm"));
    }
}
