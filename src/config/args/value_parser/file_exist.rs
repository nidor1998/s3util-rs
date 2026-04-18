use std::path::PathBuf;

#[allow(dead_code)]
pub fn is_file_exist(file_path: &str) -> Result<String, String> {
    let file_path = PathBuf::from(file_path);

    if file_path.exists() && file_path.is_file() {
        Ok(file_path.to_string_lossy().to_string())
    } else {
        Err(format!("File does not exist: {}", file_path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn accepts_existing_file() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"data").unwrap();
        let path = tmp.path().to_string_lossy().to_string();
        assert_eq!(is_file_exist(&path).unwrap(), path);
    }

    #[test]
    fn rejects_nonexistent_path() {
        let err = is_file_exist("/nonexistent/path/that/should/not/exist/abc123").unwrap_err();
        assert!(err.contains("File does not exist"));
    }

    #[test]
    fn rejects_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let err = is_file_exist(&path).unwrap_err();
        assert!(err.contains("File does not exist"));
    }
}
