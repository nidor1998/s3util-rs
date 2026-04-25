use anyhow::{Result, anyhow};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::Response;
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::error::display::DisplayErrorContext;

pub mod first_chunk;
pub mod local_to_s3;
pub mod progress;
pub mod s3_to_local;
pub mod s3_to_s3;
pub mod s3_to_stdio;
pub mod stdio_to_s3;

/// Rewrite a source-side `head_object` error for user display.
///
/// - Service error `HeadObjectError::NotFound` → `"object not found: <key>"`.
/// - Other service errors → AWS error code/message (e.g. `"AccessDenied: ..."`),
///   falling back to `"HTTP <status>"` when the response carries no metadata
///   (e.g. a 301 redirect).
/// - Non-service `SdkError` (dispatch, timeout, ...) → `DisplayErrorContext`
///   chain so the underlying IO/TLS/... cause is surfaced.
/// - Anything else (not an `SdkError<HeadObjectError>`) is returned unchanged.
pub(crate) fn translate_source_head_object_error(e: anyhow::Error, key: &str) -> anyhow::Error {
    if let Some(sdk) = e.downcast_ref::<SdkError<HeadObjectError, Response<SdkBody>>>() {
        if let SdkError::ServiceError(svc) = sdk {
            let err = svc.err();
            if matches!(err, HeadObjectError::NotFound(_)) {
                return anyhow!("object not found: {key}");
            }
            let status = svc.raw().status().as_u16();
            return match (err.code(), err.message()) {
                (Some(code), Some(message)) => anyhow!("{code}: {message}"),
                (Some(code), None) => anyhow!("{code} (HTTP {status})"),
                (None, Some(message)) => anyhow!("{message} (HTTP {status})"),
                (None, None) => anyhow!("S3 returned HTTP {status} without error details"),
            };
        }
        return anyhow!("{}", DisplayErrorContext(sdk));
    }
    e
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferDirection {
    LocalToS3,
    S3ToLocal,
    S3ToS3,
    StdioToS3,
    S3ToStdio,
}

/// Information surfaced by a transfer call that downstream subcommands may need.
///
/// Today only `mv` consumes this. `cp` ignores it. Designed as a struct (rather
/// than `Option<String>`) so future fields (e.g. `etag`, `last_modified`) can
/// be added without churning every call site.
#[derive(Debug, Default, Clone)]
pub struct TransferOutcome {
    /// The source object's version-id observed at copy time.
    /// `None` for Local/stdio sources or non-versioned buckets.
    pub source_version_id: Option<String>,
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
    fn transfer_outcome_default_has_no_version_id() {
        let outcome = TransferOutcome::default();
        assert_eq!(outcome.source_version_id, None);
    }

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

    mod translate_source_head_object_error {
        use super::*;
        use aws_smithy_runtime_api::http::StatusCode;

        fn not_found_error() -> SdkError<HeadObjectError, Response<SdkBody>> {
            SdkError::service_error(
                HeadObjectError::NotFound(aws_sdk_s3::types::error::NotFound::builder().build()),
                Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from("")),
            )
        }

        #[test]
        fn not_found_becomes_object_not_found_message() {
            // Both storage backends wrap the SdkError with anyhow!(), so mimic
            // that here to exercise the downcast path.
            let input = anyhow!(not_found_error());
            let out = translate_source_head_object_error(input, "my/key");
            assert_eq!(out.to_string(), "object not found: my/key");
        }

        #[test]
        fn not_found_survives_context_wrapping() {
            // S3Storage wraps the SdkError with `.context(...)`. The helper
            // must still downcast through that outer context layer.
            let sdk = not_found_error();
            let wrapped = anyhow::Error::new(sdk).context("wrapper");
            let out = translate_source_head_object_error(wrapped, "k");
            assert_eq!(out.to_string(), "object not found: k");
        }

        #[test]
        fn unrelated_error_passes_through_unchanged() {
            let input = anyhow!("some other failure");
            let out = translate_source_head_object_error(input, "k");
            assert_eq!(out.to_string(), "some other failure");
        }

        fn generic_service_error(
            code: Option<&str>,
            message: Option<&str>,
            status: u16,
        ) -> SdkError<HeadObjectError, Response<SdkBody>> {
            let mut builder = aws_smithy_types::error::metadata::ErrorMetadata::builder();
            if let Some(c) = code {
                builder = builder.code(c);
            }
            if let Some(m) = message {
                builder = builder.message(m);
            }
            SdkError::service_error(
                HeadObjectError::generic(builder.build()),
                Response::new(StatusCode::try_from(status).unwrap(), SdkBody::from("")),
            )
        }

        #[test]
        fn service_error_with_code_and_message_uses_code_colon_message() {
            let input = anyhow!(generic_service_error(
                Some("AccessDenied"),
                Some("User is not authorized"),
                403,
            ));
            let out = translate_source_head_object_error(input, "k");
            assert_eq!(out.to_string(), "AccessDenied: User is not authorized");
        }

        #[test]
        fn service_error_with_code_only_appends_http_status() {
            let input = anyhow!(generic_service_error(Some("AccessDenied"), None, 403));
            let out = translate_source_head_object_error(input, "k");
            assert_eq!(out.to_string(), "AccessDenied (HTTP 403)");
        }

        #[test]
        fn service_error_with_message_only_appends_http_status() {
            let input = anyhow!(generic_service_error(None, Some("redirect"), 301));
            let out = translate_source_head_object_error(input, "k");
            assert_eq!(out.to_string(), "redirect (HTTP 301)");
        }

        #[test]
        fn service_error_without_code_or_message_falls_back_to_status() {
            let input = anyhow!(generic_service_error(None, None, 301));
            let out = translate_source_head_object_error(input, "k");
            assert_eq!(
                out.to_string(),
                "S3 returned HTTP 301 without error details"
            );
        }

        #[test]
        fn non_service_sdk_error_uses_display_error_context() {
            // A construction-failure SdkError routes through the final
            // DisplayErrorContext arm rather than the ServiceError match.
            let sdk: SdkError<HeadObjectError, Response<SdkBody>> =
                SdkError::construction_failure(anyhow!("cannot build request"));
            let input = anyhow!(sdk);
            let out = translate_source_head_object_error(input, "k");
            // DisplayErrorContext walks the source chain; the inner "cannot
            // build request" must be surfaced somewhere in the rendered string.
            assert!(
                out.to_string().contains("cannot build request"),
                "unexpected output: {out}"
            );
        }
    }
}
