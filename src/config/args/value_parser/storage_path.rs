use percent_encoding::percent_decode_str;
use regex::Regex;
use url::{ParseError, Url};

use crate::types::StoragePath;

const INVALID_SCHEME: &str = "scheme must be s3:// .";
const INVALID_PATH: &str = "path must be a valid URL or a local path.";
const NO_BUCKET_NAME_SPECIFIED: &str = "bucket name must be specified.";
const NO_PATH_SPECIFIED: &str = "path must be specified.";
const MULTI_REGION_ARN_REGEX: &str = r"^s3://arn:aws:s3::?.+:accesspoint/.+";

const STDIO_PATH: &str = "-";

pub fn check_storage_path(path: &str) -> Result<String, String> {
    if path == STDIO_PATH {
        return Ok(path.to_string());
    }

    if is_multi_region_arn(path) {
        return Ok(path.to_string());
    }

    let result = Url::parse(path);
    if result == Err(ParseError::RelativeUrlWithoutBase) {
        if path.is_empty() {
            return Err(NO_PATH_SPECIFIED.to_string());
        }

        return Ok(path.to_string());
    }

    if result.is_err() {
        return Err(INVALID_PATH.to_string());
    }

    let parsed = result.unwrap();
    match parsed.scheme() {
        "s3" => {
            if parsed.host_str().is_none() {
                return Err(NO_BUCKET_NAME_SPECIFIED.to_string());
            }
        }
        _ => {
            if !is_windows_absolute_path(path) {
                return Err(INVALID_SCHEME.to_string());
            }
        }
    }

    Ok(path.to_string())
}

fn is_multi_region_arn(path: &str) -> bool {
    Regex::new(MULTI_REGION_ARN_REGEX).unwrap().is_match(path)
}

pub fn parse_storage_path(path: &str) -> StoragePath {
    check_storage_path(path).unwrap();

    if path == STDIO_PATH {
        return StoragePath::Stdio;
    }

    if is_multi_region_arn(path) {
        return StoragePath::S3 {
            bucket: extract_multi_region_arn(path),
            prefix: extract_prefix(path),
        };
    }

    let result = Url::parse(path);
    if result == Err(ParseError::RelativeUrlWithoutBase) {
        return parse_local_path(path);
    }
    if is_windows_absolute_path(path) {
        return parse_local_path(path);
    }

    parse_s3_path(path)
}

fn extract_multi_region_arn(path: &str) -> String {
    let mut iter = path.match_indices('/');
    let third_slash = iter.nth(3);
    match third_slash {
        Some((idx, _)) => {
            let arn = &path[..=idx].to_string();
            arn.replace("s3://", "")
                .to_string()
                .strip_suffix('/')
                .map_or(arn.to_string(), |s| s.to_string())
                .to_string()
        }
        None => path.replace("s3://", "").to_string(),
    }
}

fn extract_prefix(path: &str) -> String {
    // skipcq: RS-W1201
    path.char_indices()
        .filter(|&(_, c)| c == '/')
        .nth(3)
        .map(|(i, _)| &path[i + 1..])
        .map_or("", |s| s)
        .to_string()
}

pub fn is_both_storage_local(source: &StoragePath, target: &StoragePath) -> bool {
    let source_local = matches!(source, StoragePath::Local(_));
    let target_local = matches!(target, StoragePath::Local(_));
    source_local && target_local
}

pub fn is_both_storage_s3(source: &StoragePath, target: &StoragePath) -> bool {
    let source_s3 = matches!(source, StoragePath::S3 { .. });
    let target_s3 = matches!(target, StoragePath::S3 { .. });
    source_s3 && target_s3
}

fn parse_local_path(path: &str) -> StoragePath {
    StoragePath::Local(path.into())
}

fn parse_s3_path(path: &str) -> StoragePath {
    let bucket = Url::parse(path).unwrap().host_str().unwrap().to_string();
    let mut prefix = Url::parse(path).unwrap().path().to_string();

    // remove first '/'
    if !prefix.is_empty() {
        prefix.remove(0);
    }

    prefix = percent_decode_str(&prefix)
        .decode_utf8()
        .unwrap()
        .to_string();

    StoragePath::S3 { bucket, prefix }
}

fn is_windows_absolute_path(path: &str) -> bool {
    if !cfg!(windows) {
        return false;
    }

    let re = Regex::new(r"^[a-zA-Z]:\\").unwrap();
    re.is_match(path)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn check_valid_url() {
        check_storage_path("/etc/").unwrap();
        check_storage_path("etc/dir").unwrap();
        check_storage_path("s3://my-bucket").unwrap();
        check_storage_path("s3://my-bucket/").unwrap();
        check_storage_path("s3://my-bucket/xyz.dat").unwrap();
        check_storage_path("-").unwrap();
    }

    #[test]
    fn check_valid_url_error() {
        assert!(check_storage_path("s3://arn:aws").is_err());
    }

    #[test]
    fn parse_stdio_path() {
        assert!(matches!(parse_storage_path("-"), StoragePath::Stdio));
    }

    #[test]
    fn parse_local() {
        let local_path = "dir1/";
        if let StoragePath::Local(path) = parse_storage_path(local_path) {
            assert_eq!(path, PathBuf::from("dir1/"));
        } else {
            panic!("local path not found");
        }
    }

    #[test]
    fn parse_s3_url_with_key() {
        let s3_url = "s3://test-bucket/my_key";
        if let StoragePath::S3 { bucket, prefix } = parse_storage_path(s3_url) {
            assert_eq!(bucket, "test-bucket");
            assert_eq!(prefix, "my_key");
        } else {
            panic!("s3 url not found");
        }
    }

    #[test]
    fn empty_local_path() {
        let result = check_storage_path("");
        assert!(result.is_err());
    }

    #[test]
    fn invalid_scheme() {
        let result = check_storage_path("https://my-bucket");
        assert!(result.is_err());
    }

    #[test]
    fn no_bucket_name() {
        let result = check_storage_path("s3://");
        assert!(result.is_err());
    }

    #[test]
    fn both_storage_local() {
        assert!(is_both_storage_local(
            &StoragePath::Local("/source".into()),
            &StoragePath::Local("/target".into()),
        ));
    }

    #[test]
    fn both_storage_s3() {
        assert!(is_both_storage_s3(
            &StoragePath::S3 {
                bucket: "source-bucket".to_string(),
                prefix: "source-prefix".to_string(),
            },
            &StoragePath::S3 {
                bucket: "target-bucket".to_string(),
                prefix: "target-prefix".to_string(),
            },
        ));
    }
}
