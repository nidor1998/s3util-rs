use anyhow::Error;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum S3syncError {
    #[error("cancelled")]
    Cancelled,
    #[error("an error occurred while downloading an object")]
    DownloadForceRetryableError,
}

pub fn is_cancelled_error(e: &Error) -> bool {
    if let Some(err) = e.downcast_ref::<S3syncError>() {
        return *err == S3syncError::Cancelled;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    #[test]
    fn is_cancelled_error_test() {
        assert!(is_cancelled_error(&anyhow!(S3syncError::Cancelled)));
        assert!(!is_cancelled_error(&anyhow!(
            S3syncError::DownloadForceRetryableError
        )));
    }

    #[test]
    fn is_cancelled_error_false_for_non_s3sync_error() {
        assert!(!is_cancelled_error(&anyhow!("unrelated error")));
    }

    #[test]
    fn is_cancelled_error_through_context_wrap() {
        use anyhow::Context;
        let err = Err::<(), _>(anyhow!(S3syncError::Cancelled))
            .context("failed to upload to target: foo")
            .unwrap_err();
        assert!(is_cancelled_error(&err));
    }
}
