use std::path::{MAIN_SEPARATOR, Path};

const STDIO_PATH: &str = "-";
const NO_PATH_SPECIFIED: &str = "output file path must be specified.";

/// clap `value_parser` for the `get-object-annotation` output file destination.
///
/// A destination must denote a (possibly not-yet-existing) *file*, so a path
/// that names a directory is rejected up front rather than failing later when
/// the payload cannot be persisted onto it. Rejects:
/// - an empty path,
/// - a path that ends with a path separator (e.g. `foo/`), which denotes a
///   directory even when it does not exist yet,
/// - a path that already exists and is a directory (covers `.`, `..`, and
///   existing directories).
///
/// The `-` sentinel (write payload to stdout) is accepted verbatim.
pub fn check_outfile_path(path: &str) -> Result<String, String> {
    if path == STDIO_PATH {
        return Ok(path.to_string());
    }

    if path.is_empty() {
        return Err(NO_PATH_SPECIFIED.to_string());
    }

    // A trailing path separator denotes a directory even when the path does not
    // exist yet (e.g. `foo/`), so reject it before touching the filesystem.
    if path.ends_with('/') || path.ends_with(MAIN_SEPARATOR) {
        return Err(dir_error(path));
    }

    // An existing directory (including `.` and `..`) can never be written to as
    // a file; reject it here instead of failing later at persist time.
    if Path::new(path).is_dir() {
        return Err(dir_error(path));
    }

    Ok(path.to_string())
}

fn dir_error(path: &str) -> String {
    format!("output file path must not be a directory: {path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_stdio_dash() {
        assert_eq!(check_outfile_path("-").unwrap(), "-");
    }

    #[test]
    fn accepts_nonexistent_file_path() {
        let path = "/nonexistent/dir123/out.bin";
        assert_eq!(check_outfile_path(path).unwrap(), path);
    }

    #[test]
    fn rejects_empty_path() {
        assert!(check_outfile_path("").is_err());
    }

    #[test]
    fn rejects_current_dir() {
        let err = check_outfile_path(".").unwrap_err();
        assert!(err.contains("directory"), "unexpected err: {err}");
    }

    #[test]
    fn rejects_parent_dir() {
        let err = check_outfile_path("..").unwrap_err();
        assert!(err.contains("directory"), "unexpected err: {err}");
    }

    #[test]
    fn rejects_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().to_string_lossy().to_string();
        let err = check_outfile_path(&path).unwrap_err();
        assert!(err.contains("directory"), "unexpected err: {err}");
    }

    #[test]
    fn rejects_trailing_separator_even_when_nonexistent() {
        let path = format!("/nonexistent/dir123{MAIN_SEPARATOR}");
        let err = check_outfile_path(&path).unwrap_err();
        assert!(err.contains("directory"), "unexpected err: {err}");
    }

    #[test]
    fn rejects_trailing_forward_slash() {
        let err = check_outfile_path("some/dir/").unwrap_err();
        assert!(err.contains("directory"), "unexpected err: {err}");
    }
}
