use anyhow::{Result, anyhow};
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingError;
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

/// How a failed source `get_object_tagging` call should be treated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaggingUnavailable {
    /// The store reports that the object has no tag set (NoSuchTagSet).
    NoTags,
    /// The tags exist or may exist but cannot be read: AccessDenied / 403, or
    /// another client error that is neither "not found" nor transient. The
    /// object may still have tags, so copying without them can lose data.
    Unreadable(String),
    /// The store has no object-tagging API (NotImplemented / 501, as S3
    /// Express directory buckets return, MethodNotAllowed / 405, NotSupported
    /// and similar). Such an object cannot have tags, so nothing is lost
    /// without them. AWS S3 also answers 405 for a delete marker, but HEAD has
    /// just succeeded on the same object, so that is not the case here.
    Unsupported(String),
}

/// Classifies a source `get_object_tagging` failure. Returns `None` for any
/// failure that might be transient or means the object is gone (network,
/// throttling, timeouts, 5xx, NoSuchKey, NoSuchVersion), which must fail the copy.
pub(crate) fn tagging_unavailable_reason(e: &anyhow::Error) -> Option<TaggingUnavailable> {
    let sdk = e.downcast_ref::<SdkError<GetObjectTaggingError, Response<SdkBody>>>()?;
    let SdkError::ServiceError(svc) = sdk else {
        return None;
    };
    let status = svc.raw().status().as_u16();
    let label = || {
        svc.err()
            .code()
            .map_or_else(|| format!("HTTP {status}"), str::to_string)
    };
    match (svc.err().code(), status) {
        (Some("NoSuchTagSet"), _) => Some(TaggingUnavailable::NoTags),
        (
            Some("NotImplemented" | "NotSupported" | "UnsupportedOperation" | "XNotImplemented"),
            _,
        )
        | (Some("MethodNotAllowed"), _)
        | (None, 405 | 501) => Some(TaggingUnavailable::Unsupported(label())),
        (
            Some(
                "NoSuchKey"
                | "NoSuchVersion"
                | "NoSuchBucket"
                | "RequestTimeout"
                | "SlowDown"
                | "Throttling"
                | "ThrottlingException"
                | "RequestTimeTooSkewed",
            ),
            _,
        )
        | (_, 404 | 408 | 429) => None,
        (_, 400..=499) => Some(TaggingUnavailable::Unreadable(label())),
        _ => None,
    }
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

/// The `s3://` scheme is case-insensitive, matching the path parser.
fn is_s3_path(path: &str) -> bool {
    path.get(..5)
        .is_some_and(|scheme| scheme.eq_ignore_ascii_case("s3://"))
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
pub(crate) mod tests {
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

    pub(crate) fn get_object_tagging_service_error(
        code: Option<&str>,
        status: u16,
    ) -> SdkError<GetObjectTaggingError, Response<SdkBody>> {
        let mut builder = aws_smithy_types::error::metadata::ErrorMetadata::builder();
        if let Some(c) = code {
            builder = builder.code(c);
        }
        SdkError::service_error(
            GetObjectTaggingError::generic(builder.build()),
            Response::new(
                aws_smithy_runtime_api::http::StatusCode::try_from(status).unwrap(),
                SdkBody::from(""),
            ),
        )
    }

    mod tagging_unavailable_reason {
        use super::*;

        fn reason(code: Option<&str>, status: u16) -> Option<TaggingUnavailable> {
            tagging_unavailable_reason(&anyhow!(get_object_tagging_service_error(code, status)))
        }

        #[test]
        fn access_denied_is_unreadable_through_context() {
            // S3Storage wraps the SdkError with `.context(...)`.
            let e = anyhow::Error::new(get_object_tagging_service_error(Some("AccessDenied"), 403))
                .context("wrapper");
            assert_eq!(
                tagging_unavailable_reason(&e),
                Some(TaggingUnavailable::Unreadable("AccessDenied".to_string()))
            );
            assert_eq!(
                reason(None, 403),
                Some(TaggingUnavailable::Unreadable("HTTP 403".to_string()))
            );
        }

        #[test]
        fn other_client_errors_are_unreadable() {
            for (code, status) in [
                (Some("InvalidRequest"), 400),
                (Some("InvalidArgument"), 400),
                (None, 400),
            ] {
                assert!(
                    matches!(
                        reason(code, status),
                        Some(TaggingUnavailable::Unreadable(_))
                    ),
                    "{code:?} {status}"
                );
            }
        }

        #[test]
        fn no_such_tag_set_means_no_tags() {
            assert_eq!(
                reason(Some("NoSuchTagSet"), 404),
                Some(TaggingUnavailable::NoTags)
            );
        }

        #[test]
        fn unsupported_tagging_api_is_unsupported() {
            for (code, status) in [
                (Some("NotImplemented"), 501),
                (Some("NotImplemented"), 400),
                (Some("NotSupported"), 400),
                (Some("UnsupportedOperation"), 400),
                (Some("XNotImplemented"), 501),
                (Some("MethodNotAllowed"), 405),
                (None, 405),
                (None, 501),
            ] {
                assert!(
                    matches!(
                        reason(code, status),
                        Some(TaggingUnavailable::Unsupported(_))
                    ),
                    "{code:?} {status}"
                );
            }
        }

        #[test]
        fn transient_or_missing_object_failures_are_not_unavailable() {
            for (code, status) in [
                (Some("NoSuchKey"), 404),
                (Some("NoSuchVersion"), 404),
                (Some("RequestTimeout"), 400),
                (Some("SlowDown"), 503),
                (Some("InternalError"), 500),
                (None, 404),
                (None, 429),
                (None, 500),
            ] {
                assert_eq!(reason(code, status), None, "{code:?} {status}");
            }
            assert_eq!(tagging_unavailable_reason(&anyhow!("network down")), None);
            let sdk: SdkError<GetObjectTaggingError, Response<SdkBody>> =
                SdkError::construction_failure(anyhow!("cannot build request"));
            assert_eq!(tagging_unavailable_reason(&anyhow!(sdk)), None);
        }
    }

    #[test]
    fn s3_scheme_is_detected_case_insensitively() {
        assert_eq!(
            detect_direction("./f", "S3://bucket/key").unwrap(),
            TransferDirection::LocalToS3
        );
        assert_eq!(
            detect_direction("S3://a/k", "s3://b/k").unwrap(),
            TransferDirection::S3ToS3
        );
        assert!(!is_s3_path("s3:"));
    }
}
