use aws_sdk_s3::types::StorageClass;

const INVALID_STORAGE_CLASS: &str = "invalid storage class. valid choices: STANDARD | REDUCED_REDUNDANCY | STANDARD_IA | ONE-ZONE_IA | INTELLIGENT_TIERING | GLACIER | DEEP_ARCHIVE | GLACIER_IR | EXPRESS_ONEZONE.";

pub fn parse_storage_class(class: &str) -> Result<String, String> {
    #[allow(deprecated)]
    if matches!(StorageClass::from(class), StorageClass::Unknown(_)) {
        return Err(INVALID_STORAGE_CLASS.to_string());
    }

    Ok(class.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_known_storage_classes() {
        for class in [
            "STANDARD",
            "REDUCED_REDUNDANCY",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "GLACIER",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
            "EXPRESS_ONEZONE",
        ] {
            assert_eq!(parse_storage_class(class).unwrap(), class);
        }
    }

    #[test]
    fn rejects_unknown_storage_class() {
        let err = parse_storage_class("INVALID_CLASS").unwrap_err();
        assert!(err.contains("invalid storage class"));
    }
}
