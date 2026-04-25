use url::Url;

const INVALID_SCHEME: &str = "scheme must be https:// or http:// .";

pub fn check_scheme(url: &str) -> Result<String, String> {
    let parsed = Url::parse(url).map_err(|e| e.to_string())?;

    if parsed.scheme() != "https" && parsed.scheme() != "http" {
        return Err(INVALID_SCHEME.to_string());
    }

    Ok(url.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_https_url() {
        assert_eq!(
            check_scheme("https://example.com").unwrap(),
            "https://example.com"
        );
    }

    #[test]
    fn accepts_http_url() {
        assert_eq!(
            check_scheme("http://example.com").unwrap(),
            "http://example.com"
        );
    }

    #[test]
    fn rejects_non_http_scheme() {
        let err = check_scheme("ftp://example.com").unwrap_err();
        assert!(err.contains("scheme must be"));
    }

    #[test]
    fn rejects_malformed_url() {
        // No scheme at all → url::parse returns an error before scheme check runs.
        assert!(check_scheme("not-a-url").is_err());
    }
}
