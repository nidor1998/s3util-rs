pub type PipelineCancellationToken = tokio_util::sync::CancellationToken;

pub fn create_pipeline_cancellation_token() -> PipelineCancellationToken {
    tokio_util::sync::CancellationToken::new()
}

/// How long an input EOF is held back to let a concurrent Ctrl+C reach the
/// token. See `is_cancelled_within_grace`.
const CANCELLATION_GRACE: std::time::Duration = std::time::Duration::from_millis(100);

/// Whether the token is cancelled now or becomes so within a short grace
/// period.
///
/// A terminal Ctrl+C signals the whole foreground process group: the process
/// feeding stdin dies (so stdin reads EOF) while the SIGINT handler task
/// cancels the token. Those two race, so an EOF alone does not prove the
/// input is complete. Callers about to commit data read up to an EOF use
/// this instead of a bare `is_cancelled()` to give the handler time to run.
pub async fn is_cancelled_within_grace(token: &PipelineCancellationToken) -> bool {
    tokio::time::timeout(CANCELLATION_GRACE, token.cancelled())
        .await
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn grace_check_sees_an_already_cancelled_token() {
        let token = create_pipeline_cancellation_token();
        token.cancel();
        assert!(is_cancelled_within_grace(&token).await);
    }

    #[tokio::test]
    async fn grace_check_sees_a_cancel_that_lands_during_the_grace_period() {
        let token = create_pipeline_cancellation_token();
        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            canceller.cancel();
        });
        assert!(is_cancelled_within_grace(&token).await);
    }

    #[tokio::test]
    async fn grace_check_is_false_when_never_cancelled() {
        let token = create_pipeline_cancellation_token();
        assert!(!is_cancelled_within_grace(&token).await);
    }

    #[test]
    fn create_cancellation_token() {
        create_pipeline_cancellation_token();
    }
}
