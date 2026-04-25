use regex::Regex;

const INVALID_TAGGING: &str = "invalid tagging.";

pub fn parse_tagging(tagging: &str) -> Result<String, String> {
    let regex = Regex::new(r"(&?([a-zA-Z0-9+%._\-~]+)=([a-zA-Z0-9+%._\-~]*))+").unwrap();

    let mat = regex.find(tagging);
    if mat.is_none() {
        return Err(INVALID_TAGGING.to_string());
    }

    if mat.unwrap().as_str() != tagging {
        return Err(INVALID_TAGGING.to_string());
    }

    Ok(tagging.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tagging_test() {
        parse_tagging("key=value").unwrap();
        parse_tagging("key1=value1&key2=value2").unwrap();

        assert!(parse_tagging("key").is_err());
        assert!(parse_tagging("key=value,key2=value2").is_err());
    }
}
