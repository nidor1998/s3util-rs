use byte_unit::Byte;
use std::str::FromStr;

const UNDER_MIN_VALUE: &str = "must be greater than or equal to 5MiB";
const OVER_MAX_VALUE: &str = "must be smaller than or equal to 5GiB";

const MIN_VALUE: u128 = 5 * 1024 * 1024;
const MAX_VALUE: u128 = 5 * 1024 * 1024 * 1024;

const UNDER_MIN_BANDWIDTH: &str = "must be greater than or equal to 1MiB";
const OVER_MAX_BANDWIDTH: &str = "must be smaller than or equal to 100GiB";

const MIN_BANDWIDTH: u128 = 1024 * 1024;
const MAX_BANDWIDTH: u128 = 100 * 1024 * 1024 * 1024;

pub fn check_human_bytes(value: &str) -> Result<String, String> {
    let result = Byte::from_str(value).map_err(|e| e.to_string())?;

    if result.as_u128() < MIN_VALUE {
        return Err(UNDER_MIN_VALUE.to_string());
    }
    if result.as_u128() > MAX_VALUE {
        return Err(OVER_MAX_VALUE.to_string());
    }

    Ok(value.to_string())
}

pub fn parse_human_bytes(value: &str) -> Result<u64, String> {
    check_human_bytes(value)?;

    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    Ok(result.as_u128().try_into().unwrap())
}

pub fn check_human_bandwidth(value: &str) -> Result<String, String> {
    let result = Byte::from_str(value).map_err(|e| e.to_string())?;

    if result.as_u128() < MIN_BANDWIDTH {
        return Err(UNDER_MIN_BANDWIDTH.to_string());
    }
    if result.as_u128() > MAX_BANDWIDTH {
        return Err(OVER_MAX_BANDWIDTH.to_string());
    }

    Ok(value.to_string())
}

pub fn parse_human_bandwidth(value: &str) -> Result<u64, String> {
    check_human_bandwidth(value)?;

    let result = Byte::from_str(value).map_err(|e| e.to_string())?;
    Ok(result.as_u128().try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_valid_value() {
        check_human_bytes("5MiB").unwrap();
        check_human_bytes("5242880").unwrap();
        check_human_bytes("5GiB").unwrap();
        check_human_bytes("8MiB").unwrap();
    }

    #[test]
    fn check_invalid_value() {
        assert!(check_human_bytes("524287a").is_err());
        assert!(check_human_bytes("5Zib").is_err());
    }

    #[test]
    fn check_under_min_value() {
        let result = check_human_bytes("5242879");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), UNDER_MIN_VALUE);
    }

    #[test]
    fn check_over_max_value() {
        let result = check_human_bytes("5368709121");
        assert!(result.is_err());
        assert_eq!(result.err().unwrap(), OVER_MAX_VALUE);
    }

    #[test]
    fn parse_valid_value() {
        assert_eq!(8 * 1024 * 1024, parse_human_bytes("8MiB").unwrap());
        assert_eq!(5 * 1024 * 1024, parse_human_bytes("5242880").unwrap());
    }
}
