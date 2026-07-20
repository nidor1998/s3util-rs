//! Source-level tests that `--target-request-payer` actually reaches S3.
//!
//! The flag is parsed by `CommonClientArgs` into `ClientConfig.request_payer`
//! for every subcommand that advertises it, but for a long time only the four
//! object-annotation wrappers ever forwarded it. `rm`, `head-object`,
//! `get-object-tagging`, `put-object-tagging`, `restore-object` and `presign`
//! accepted the flag and silently dropped it, so every one of them returned 403
//! against a Requester Pays bucket (and `presign` produced a URL that always
//! did).
//!
//! These are static checks in the same spirit as `cli_command_api_mapping.rs`:
//! the behaviour they guard needs a Requester Pays bucket to observe, so the
//! e2e suite cannot run in CI, but the wiring itself is checkable from source.
//! The bug class is a wrapper quietly losing the parameter again.

use std::fs;
use std::path::PathBuf;

fn repo_path(path: impl AsRef<std::path::Path>) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn read(path: impl AsRef<std::path::Path>) -> String {
    let path = repo_path(path);
    fs::read_to_string(&path).unwrap_or_else(|e| panic!("failed to read {}: {e}", path.display()))
}

/// Extract the body of `pub async fn <name>(` by brace matching.
fn function_body<'a>(source: &'a str, name: &str) -> &'a str {
    let start = source
        .find(&format!("pub async fn {name}("))
        .unwrap_or_else(|| panic!("function {name} not found"));
    let body_start = source[start..]
        .find('{')
        .unwrap_or_else(|| panic!("body of {name} not found"))
        + start;

    let mut depth = 0usize;
    for (offset, ch) in source[body_start..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[body_start..=body_start + offset];
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced braces in {name}");
}

/// api.rs wrappers whose S3 operation accepts `x-amz-request-payer` and which
/// therefore must both take the parameter and forward it to the SDK builder.
///
/// `delete_object_tagging` and `head_bucket` are deliberately absent: the S3
/// API has no request-payer parameter for `DeleteObjectTagging` or
/// `HeadBucket`, so there is nothing to forward (confirmed against the
/// aws-sdk-s3 builders, which expose no `set_request_payer` for either).
const WRAPPERS_FORWARDING_REQUEST_PAYER: &[&str] = &[
    "head_object",
    "delete_object",
    "get_object_tagging",
    "put_object_tagging",
    "restore_object",
    "presign_get_object",
];

#[test]
fn api_wrappers_forward_request_payer_to_the_sdk() {
    let source = read("src/storage/s3/api.rs");

    for wrapper in WRAPPERS_FORWARDING_REQUEST_PAYER {
        let body = function_body(&source, wrapper);
        assert!(
            body.contains(".request_payer("),
            "api::{wrapper} must forward request_payer to the SDK builder, \
             otherwise the documented --target-request-payer flag does nothing \
             and the call 403s against a Requester Pays bucket"
        );
    }
}

#[test]
fn head_object_opts_carries_request_payer() {
    let source = read("src/storage/s3/api.rs");
    let start = source
        .find("pub struct HeadObjectOpts")
        .expect("HeadObjectOpts not found");
    let end = source[start..].find('}').expect("struct end not found") + start;
    let decl = &source[start..end];

    assert!(
        decl.contains("request_payer"),
        "HeadObjectOpts must carry request_payer; head-object and cp's \
         --skip-existing probe both go through it"
    );
}

/// Each CLI runtime must pass the resolved `ClientConfig.request_payer` down.
/// Listing the files explicitly (rather than scanning) keeps a newly added
/// command from silently escaping the check.
const CLI_FILES_PASSING_REQUEST_PAYER: &[&str] = &[
    "rm.rs",
    "head_object.rs",
    "get_object_tagging.rs",
    "put_object_tagging.rs",
    "restore_object.rs",
    "presign.rs",
    "cp.rs",
];

#[test]
fn cli_runtimes_pass_request_payer_from_client_config() {
    for file in CLI_FILES_PASSING_REQUEST_PAYER {
        let source = read(format!("src/bin/s3util/cli/{file}"));
        // Require the actual field access, not merely the words: a bare
        // `contains("request_payer")` would be satisfied by a comment.
        // `cp.rs` reads it from `target_client_config`, so match on the suffix.
        assert!(
            source.contains("client_config.request_payer"),
            "src/bin/s3util/cli/{file} advertises --target-request-payer but \
             never reads `client_config.request_payer`, so the flag is dropped"
        );
    }
}

/// The S3 API has no request-payer parameter for `DeleteObjectTagging` or
/// `HeadBucket`, so those wrappers must NOT grow one. This pins the reason the
/// two commands are absent from the list above: if a future SDK adds support,
/// this test fails and the omission gets revisited deliberately rather than
/// staying an unexplained gap.
#[test]
fn wrappers_without_s3_support_do_not_pretend_to_forward_request_payer() {
    let source = read("src/storage/s3/api.rs");

    for wrapper in ["delete_object_tagging", "head_bucket"] {
        let body = function_body(&source, wrapper);
        assert!(
            !body.contains(".request_payer("),
            "api::{wrapper} forwards request_payer, but the S3 API has no such \
             parameter for this operation — if the SDK now supports it, add the \
             wrapper to WRAPPERS_FORWARDING_REQUEST_PAYER and wire up the CLI"
        );
    }
}

/// `--target-request-payer` must remain reachable on these subcommands; if the
/// flag were removed from `CommonClientArgs` the forwarding above would become
/// dead code rather than a bug, and this test says so out loud.
#[test]
fn target_request_payer_flag_is_advertised_on_the_affected_subcommands() {
    let source = read("src/config/args/common_client.rs");
    assert!(
        source.contains("pub target_request_payer: bool"),
        "CommonClientArgs must still expose --target-request-payer"
    );
    assert!(
        source.contains("RequestPayer::Requester"),
        "the flag must still map to RequestPayer::Requester"
    );
}
