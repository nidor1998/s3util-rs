use anyhow::{Result, anyhow};

pub mod local_to_s3;
pub mod progress;
pub mod s3_to_local;
pub mod s3_to_s3;
pub mod s3_to_stdio;
pub mod stdio_to_s3;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferDirection {
    LocalToS3,
    S3ToLocal,
    S3ToS3,
    StdioToS3,
    S3ToStdio,
}

fn is_s3_path(path: &str) -> bool {
    path.starts_with("s3://")
}

fn is_stdio(path: &str) -> bool {
    path == "-"
}

pub fn detect_direction(source: &str, target: &str) -> Result<TransferDirection> {
    match (
        is_stdio(source),
        is_stdio(target),
        is_s3_path(source),
        is_s3_path(target),
    ) {
        (true, true, _, _) => Err(anyhow!("both source and target cannot be stdio")),
        (true, _, _, true) => Ok(TransferDirection::StdioToS3),
        (_, true, true, _) => Ok(TransferDirection::S3ToStdio),
        (true, _, _, false) => Err(anyhow!("stdio source requires S3 target")),
        (_, true, false, _) => Err(anyhow!("stdio target requires S3 source")),
        (_, _, true, true) => Ok(TransferDirection::S3ToS3),
        (_, _, true, false) => Ok(TransferDirection::S3ToLocal),
        (_, _, false, true) => Ok(TransferDirection::LocalToS3),
        (_, _, false, false) => Err(anyhow!("both source and target cannot be local paths")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_local_to_s3() {
        assert_eq!(
            detect_direction("/tmp/file.txt", "s3://bucket/key").unwrap(),
            TransferDirection::LocalToS3
        );
    }

    #[test]
    fn detect_s3_to_local() {
        assert_eq!(
            detect_direction("s3://bucket/key", "/tmp/file.txt").unwrap(),
            TransferDirection::S3ToLocal
        );
    }

    #[test]
    fn detect_s3_to_s3() {
        assert_eq!(
            detect_direction("s3://bucket1/key", "s3://bucket2/key").unwrap(),
            TransferDirection::S3ToS3
        );
    }

    #[test]
    fn detect_stdio_to_s3() {
        assert_eq!(
            detect_direction("-", "s3://bucket/key").unwrap(),
            TransferDirection::StdioToS3
        );
    }

    #[test]
    fn detect_s3_to_stdio() {
        assert_eq!(
            detect_direction("s3://bucket/key", "-").unwrap(),
            TransferDirection::S3ToStdio
        );
    }

    #[test]
    fn reject_both_stdio() {
        assert!(detect_direction("-", "-").is_err());
    }

    #[test]
    fn reject_both_local() {
        assert!(detect_direction("/tmp/a", "/tmp/b").is_err());
    }

    #[test]
    fn reject_local_to_stdio() {
        assert!(detect_direction("/tmp/a", "-").is_err());
    }

    #[test]
    fn reject_stdio_to_local() {
        assert!(detect_direction("-", "/tmp/a").is_err());
    }
}
