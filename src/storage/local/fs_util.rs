use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use aws_sdk_s3::primitives::DateTime;
use filetime::{FileTime, set_file_mtime};
use regex::Regex;
use tempfile::NamedTempFile;
use tokio::fs::File;

pub fn check_directory_traversal(key: &str) -> bool {
    let re = Regex::new(r"\.\.[/\\]").unwrap();
    re.is_match(key)
}

pub async fn get_file_size(path: &PathBuf) -> Result<u64> {
    Ok(File::open(path).await?.metadata().await?.len())
}

pub async fn get_last_modified(path: &PathBuf) -> Result<DateTime> {
    Ok(DateTime::from(
        File::open(path).await?.metadata().await?.modified()?,
    ))
}

pub fn set_last_modified(
    path: PathBuf,
    key: &str,
    seconds: i64,
    nanos: u32,
) -> std::io::Result<()> {
    set_file_mtime(
        key_to_file_path(path, key),
        FileTime::from_unix_time(seconds, nanos),
    )
}

pub fn set_last_modified_for_path(path: &PathBuf, seconds: i64, nanos: u32) -> std::io::Result<()> {
    set_file_mtime(path, FileTime::from_unix_time(seconds, nanos))
}

pub fn is_key_a_directory(key: &str) -> bool {
    if cfg!(windows) && key.ends_with('\\') {
        return true;
    }

    key.ends_with('/')
}

pub async fn create_temp_file_from_key(path: &Path, key: &str) -> Result<NamedTempFile> {
    require_directory_for_key(path.to_path_buf(), key).await?;

    let temp_directory_path = key_to_directory_without_filename(path.to_path_buf(), key);
    let file =
        NamedTempFile::new_in(temp_directory_path).context("NamedTempFile::new_in failed.")?;
    Ok(file)
}

pub async fn create_temp_file_for_key(key: &str) -> Result<NamedTempFile> {
    require_parent_directory(key).await?;

    let path = PathBuf::from(key);
    let parent = path.parent().unwrap_or(Path::new("."));
    let file = NamedTempFile::new_in(parent).context("NamedTempFile::new_in failed.")?;
    Ok(file)
}

pub async fn require_parent_directory(key: &str) -> Result<()> {
    let path = PathBuf::from(key);
    let parent = path.parent().unwrap_or(Path::new("."));

    if parent.try_exists().unwrap_or(false) {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "parent directory does not exist: '{}'. \
         Please create it before running this command.",
        parent.to_string_lossy()
    ))
}

pub async fn require_directory_for_key(path: PathBuf, key: &str) -> Result<()> {
    let directory_path = key_to_directory_without_filename(path, key);

    if directory_path.try_exists().unwrap_or(false) {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "destination directory does not exist: '{}'. \
         Please create it before running this command.",
        directory_path.to_string_lossy()
    ))
}

pub fn remove_root_slash(key: &str) -> String {
    let re = Regex::new(r"^/+").unwrap();
    re.replace(key, "").to_string()
}

pub fn key_to_file_path(path: PathBuf, key: &str) -> PathBuf {
    let file = convert_os_specific_directory_char(&remove_root_slash(key));
    let lossy_path = path.to_string_lossy();

    format!("{lossy_path}{file}").into()
}

fn key_to_directory_without_filename(path: PathBuf, key: &str) -> PathBuf {
    let lossy_path = path.to_string_lossy();
    let directory_from_key = remove_file_name_if_exist(
        convert_os_specific_directory_char(&remove_root_slash(key)).into(),
    )
    .to_string_lossy()
    .to_string();
    format!("{lossy_path}{directory_from_key}").into()
}

pub fn remove_file_name_if_exist(path: PathBuf) -> PathBuf {
    let mut path_str = path.to_str().unwrap().to_string();
    if path_str.ends_with(std::path::MAIN_SEPARATOR) {
        path_str.pop();
        return PathBuf::from(path_str);
    }

    path.parent().unwrap().to_path_buf()
}

fn convert_os_specific_directory_char(key: &str) -> String {
    key.replace('/', std::path::MAIN_SEPARATOR_STR)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    const TEST_DATA_SIZE: u64 = 5;

    #[test]
    fn check_directory_traversal_test() {
        init_dummy_tracing_subscriber();

        assert!(check_directory_traversal("../etc/passwd"));
        assert!(check_directory_traversal("dir1/dir2/../../etc/passwd"));
        assert!(check_directory_traversal("/xyz/data/../../etc/passwd"));

        assert!(check_directory_traversal("..\\etc\\passwd"));
        assert!(check_directory_traversal("dir1\\dir2\\..\\..\\etc\\passwd"));
        assert!(check_directory_traversal(
            "\\xyz\\data\\..\\..\\etc\\passwd"
        ));
        assert!(check_directory_traversal(
            "c:\\xyz\\data\\..\\..\\etc\\passwd"
        ));

        assert!(!check_directory_traversal("/etc/passwd"));
        assert!(!check_directory_traversal("etc/passwd"));
        assert!(!check_directory_traversal("passwd"));
        assert!(!check_directory_traversal("/xyz/test.jpg"));
        assert!(!check_directory_traversal("/xyz/test..jpg"));

        assert!(!check_directory_traversal("\\etc\\passwd"));
        assert!(!check_directory_traversal("etc\\passwd"));
        assert!(!check_directory_traversal("\\xyz\\test.jpg"));
        assert!(!check_directory_traversal("\\xyz\\test..jpg"));
    }

    #[tokio::test]
    async fn get_file_size_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            get_file_size(&PathBuf::from("test_data/5byte.dat"))
                .await
                .unwrap(),
            TEST_DATA_SIZE
        );
    }

    #[tokio::test]
    async fn get_file_last_modified_test() {
        init_dummy_tracing_subscriber();

        get_last_modified(&PathBuf::from("test_data/5byte.dat"))
            .await
            .unwrap();
    }

    #[test]
    fn is_key_directory_test() {
        init_dummy_tracing_subscriber();

        assert!(is_key_a_directory("/dir/"));
        assert!(is_key_a_directory("dir/"));
        assert!(is_key_a_directory("/dir1/dir2/"));

        assert!(!is_key_a_directory("/dir"));
        assert!(!is_key_a_directory("dir"));
        assert!(!is_key_a_directory("/dir1/dir2"));
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn is_key_directory_windows_test() {
        init_dummy_tracing_subscriber();

        assert!(is_key_a_directory("\\dir\\"));
        assert!(is_key_a_directory("dir\\"));
        assert!(is_key_a_directory("\\dir1\\dir\\"));

        assert!(!is_key_a_directory("\\dir"));
        assert!(!is_key_a_directory("dir"));
        assert!(!is_key_a_directory("\\dir1\\dir2"));
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn create_temp_file_from_key_errors_when_dir_missing() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();
        let base = format!("{}/", temp.path().display());
        assert!(
            create_temp_file_from_key(Path::new(&base), "missing_subdir/filename")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn require_directory_for_key_unix() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();
        let base: PathBuf = format!("{}/", temp.path().display()).into();

        // Missing directory → error.
        assert!(
            require_directory_for_key(base.clone(), "missing_dir/filename")
                .await
                .is_err()
        );

        // Pre-create the directory; subsequent call succeeds.
        tokio::fs::create_dir_all(base.join("existing_dir"))
            .await
            .unwrap();
        require_directory_for_key(base.clone(), "existing_dir/filename")
            .await
            .unwrap();
        require_directory_for_key(base, "existing_dir/")
            .await
            .unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn require_directory_for_key_windows() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();
        let base: PathBuf = format!("{}\\", temp.path().display()).into();

        assert!(
            require_directory_for_key(base.clone(), "missing_dir/filename")
                .await
                .is_err()
        );

        tokio::fs::create_dir_all(base.join("existing_dir"))
            .await
            .unwrap();
        require_directory_for_key(base.clone(), "existing_dir/filename")
            .await
            .unwrap();
        require_directory_for_key(base, "existing_dir/")
            .await
            .unwrap();
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn key_to_local_directory_path_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1"), "key/")
                .to_str()
                .unwrap(),
            "dir1key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "key/")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "key/file1")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "key/")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "key/file1")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("./dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "./dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "key/")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "/key/")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("/dir1/"), "/key/file1")
                .to_str()
                .unwrap(),
            "/dir1/key".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn key_to_local_path_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("/xyz/dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "/xyz/dir1/data/".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("./dir1/"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "./dir1/data/".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn key_to_local_path_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from("c:\\xyz\\dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            "c:\\xyz\\dir1\\data\\".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "data")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "/data")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data".to_string()
        );

        assert_eq!(
            key_to_file_path(PathBuf::from(".\\dir1\\"), "/data/")
                .to_str()
                .unwrap()
                .to_string(),
            ".\\dir1\\data\\".to_string()
        );
    }

    #[test]
    #[cfg(target_family = "unix")]
    fn remove_file_name_if_exist_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1/file1"))
                .to_str()
                .unwrap(),
            "dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1/dir2/"))
                .to_str()
                .unwrap(),
            "dir1/dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("/dir1/file1"))
                .to_str()
                .unwrap(),
            "/dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("/dir1/dir2/"))
                .to_str()
                .unwrap(),
            "/dir1/dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("./dir1/file1"))
                .to_str()
                .unwrap(),
            "./dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("./dir1/dir2/"))
                .to_str()
                .unwrap(),
            "./dir1/dir2"
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn remove_file_name_if_exist_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1\\file1"))
                .to_str()
                .unwrap(),
            "dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            "dir1\\dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("c:\\dir1\\file1"))
                .to_str()
                .unwrap(),
            "c:\\dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from("c:\\dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            "c:\\dir1\\dir2"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from(".\\dir1\\file1"))
                .to_str()
                .unwrap(),
            ".\\dir1"
        );

        assert_eq!(
            remove_file_name_if_exist(PathBuf::from(".\\dir1\\dir2\\"))
                .to_str()
                .unwrap(),
            ".\\dir1\\dir2"
        );
    }

    #[test]
    #[cfg(target_family = "windows")]
    fn key_to_local_directory_path_windows() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "key/")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "key/file1")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            "dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "key/")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "key/file1")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from(".\\dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            ".\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "key/")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "/key/")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );

        assert_eq!(
            key_to_directory_without_filename(PathBuf::from("c:\\dir1\\"), "/key/file1")
                .to_str()
                .unwrap(),
            "c:\\dir1\\key".to_string()
        );
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn set_last_modification_time_unix() {
        init_dummy_tracing_subscriber();

        set_last_modified("./test_data/".into(), "5byte.dat", 0, 0).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into())
            .await
            .unwrap();
        assert_eq!(mtime.secs(), 0);
        assert_eq!(mtime.subsec_nanos(), 0);

        set_last_modified("./test_data/".into(), "5byte.dat", 777, 999).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into())
            .await
            .unwrap();
        assert_eq!(mtime.secs(), 777);
        assert_eq!(mtime.subsec_nanos(), 999);
    }

    #[tokio::test]
    #[cfg(target_family = "windows")]
    async fn set_last_modification_time_windows() {
        init_dummy_tracing_subscriber();

        set_last_modified(".\\test_data\\".into(), "5byte.dat", 0, 0).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into())
            .await
            .unwrap();
        assert_eq!(mtime.secs(), 0);
        assert_eq!(mtime.subsec_nanos(), 0);

        set_last_modified(".\\test_data\\".into(), "5byte.dat", 777, 999).unwrap();
        let mtime = get_last_modified(&"./test_data/5byte.dat".into())
            .await
            .unwrap();
        assert_eq!(mtime.secs(), 777);
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn require_parent_directory_errors_on_missing_and_succeeds_on_existing() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();

        // Parent doesn't exist → error.
        let missing_key = format!("{}/nested/deeper/file", temp.path().display());
        assert!(require_parent_directory(&missing_key).await.is_err());

        // Parent exists (the tempdir itself) → ok.
        let existing_key = format!("{}/file", temp.path().display());
        require_parent_directory(&existing_key).await.unwrap();
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn create_temp_file_for_key_errors_when_parent_missing() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();
        let key = format!("{}/new/subdir/tempfile", temp.path().display());
        assert!(create_temp_file_for_key(&key).await.is_err());
    }

    #[tokio::test]
    #[cfg(target_family = "unix")]
    async fn create_temp_file_for_key_succeeds_when_parent_exists() {
        init_dummy_tracing_subscriber();

        let temp = tempfile::tempdir().unwrap();
        let key = format!("{}/tempfile", temp.path().display());

        let file = create_temp_file_for_key(&key).await.unwrap();
        assert!(file.path().exists());
        assert_eq!(
            file.path().parent().unwrap(),
            PathBuf::from(&key).parent().unwrap()
        );
    }

    #[test]
    fn remove_root_slash_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_root_slash("/"), "");
        assert_eq!(remove_root_slash("//"), "");
        assert_eq!(remove_root_slash("/dir1"), "dir1");
        assert_eq!(remove_root_slash("//dir1"), "dir1");
        assert_eq!(remove_root_slash("///dir1"), "dir1");
        assert_eq!(remove_root_slash("//dir1/"), "dir1/");

        assert_eq!(remove_root_slash("/dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("//dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("///dir1/dir2/dir3"), "dir1/dir2/dir3");

        assert_eq!(remove_root_slash("dir1/dir2/dir3"), "dir1/dir2/dir3");
        assert_eq!(remove_root_slash("dir1/dir2/dir3/"), "dir1/dir2/dir3/");

        assert_eq!(remove_root_slash("key1"), "key1");
        assert_eq!(remove_root_slash("key1/"), "key1/");
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}
