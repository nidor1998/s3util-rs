//! Source-level routing tests for the thin-wrapper command path.
//!
//! These tests pin the hand-written chain:
//! clap subcommand -> `src/bin/s3util/main.rs` runtime -> `api::*` wrapper
//! -> AWS SDK operation. They are intentionally static checks; the bug class
//! they guard against is a valid command accidentally invoking a sibling
//! operation such as `get-bucket-policy` calling `get_bucket_versioning`.

use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;

use clap::CommandFactory;
use s3util_rs::config::args::Cli;

#[derive(Debug)]
struct CommandRoute {
    variant: &'static str,
    cli_name: &'static str,
    runner: &'static str,
    runtime_file: Option<&'static str>,
    api_calls: &'static [&'static str],
}

const COMMAND_ROUTES: &[CommandRoute] = &[
    CommandRoute {
        variant: "Cp",
        cli_name: "cp",
        runner: "run_cp",
        runtime_file: None,
        api_calls: &[],
    },
    CommandRoute {
        variant: "Mv",
        cli_name: "mv",
        runner: "run_mv",
        runtime_file: None,
        api_calls: &[],
    },
    CommandRoute {
        variant: "CreateBucket",
        cli_name: "create-bucket",
        runner: "run_create_bucket",
        runtime_file: Some("create_bucket.rs"),
        api_calls: &["head_bucket", "create_bucket", "put_bucket_tagging"],
    },
    CommandRoute {
        variant: "DeleteBucket",
        cli_name: "delete-bucket",
        runner: "run_delete_bucket",
        runtime_file: Some("delete_bucket.rs"),
        api_calls: &["delete_bucket"],
    },
    CommandRoute {
        variant: "Rm",
        cli_name: "rm",
        runner: "run_rm",
        runtime_file: Some("rm.rs"),
        api_calls: &["delete_object"],
    },
    CommandRoute {
        variant: "HeadObject",
        cli_name: "head-object",
        runner: "run_head_object",
        runtime_file: Some("head_object.rs"),
        api_calls: &["head_object"],
    },
    CommandRoute {
        variant: "HeadBucket",
        cli_name: "head-bucket",
        runner: "run_head_bucket",
        runtime_file: Some("head_bucket.rs"),
        api_calls: &["head_bucket"],
    },
    CommandRoute {
        variant: "GetObjectTagging",
        cli_name: "get-object-tagging",
        runner: "run_get_object_tagging",
        runtime_file: Some("get_object_tagging.rs"),
        api_calls: &["get_object_tagging"],
    },
    CommandRoute {
        variant: "PutObjectTagging",
        cli_name: "put-object-tagging",
        runner: "run_put_object_tagging",
        runtime_file: Some("put_object_tagging.rs"),
        api_calls: &["put_object_tagging"],
    },
    CommandRoute {
        variant: "DeleteBucketTagging",
        cli_name: "delete-bucket-tagging",
        runner: "run_delete_bucket_tagging",
        runtime_file: Some("delete_bucket_tagging.rs"),
        api_calls: &["delete_bucket_tagging"],
    },
    CommandRoute {
        variant: "DeleteObjectTagging",
        cli_name: "delete-object-tagging",
        runner: "run_delete_object_tagging",
        runtime_file: Some("delete_object_tagging.rs"),
        api_calls: &["delete_object_tagging"],
    },
    CommandRoute {
        variant: "GetBucketTagging",
        cli_name: "get-bucket-tagging",
        runner: "run_get_bucket_tagging",
        runtime_file: Some("get_bucket_tagging.rs"),
        api_calls: &["get_bucket_tagging"],
    },
    CommandRoute {
        variant: "PutBucketVersioning",
        cli_name: "put-bucket-versioning",
        runner: "run_put_bucket_versioning",
        runtime_file: Some("put_bucket_versioning.rs"),
        api_calls: &["put_bucket_versioning"],
    },
    CommandRoute {
        variant: "PutBucketPolicy",
        cli_name: "put-bucket-policy",
        runner: "run_put_bucket_policy",
        runtime_file: Some("put_bucket_policy.rs"),
        api_calls: &["put_bucket_policy"],
    },
    CommandRoute {
        variant: "GetBucketPolicy",
        cli_name: "get-bucket-policy",
        runner: "run_get_bucket_policy",
        runtime_file: Some("get_bucket_policy.rs"),
        api_calls: &["get_bucket_policy"],
    },
    CommandRoute {
        variant: "DeleteBucketPolicy",
        cli_name: "delete-bucket-policy",
        runner: "run_delete_bucket_policy",
        runtime_file: Some("delete_bucket_policy.rs"),
        api_calls: &["delete_bucket_policy"],
    },
    CommandRoute {
        variant: "PutBucketLifecycleConfiguration",
        cli_name: "put-bucket-lifecycle-configuration",
        runner: "run_put_bucket_lifecycle_configuration",
        runtime_file: Some("put_bucket_lifecycle_configuration.rs"),
        api_calls: &["put_bucket_lifecycle_configuration"],
    },
    CommandRoute {
        variant: "GetBucketLifecycleConfiguration",
        cli_name: "get-bucket-lifecycle-configuration",
        runner: "run_get_bucket_lifecycle_configuration",
        runtime_file: Some("get_bucket_lifecycle_configuration.rs"),
        api_calls: &["get_bucket_lifecycle_configuration"],
    },
    CommandRoute {
        variant: "DeleteBucketLifecycleConfiguration",
        cli_name: "delete-bucket-lifecycle-configuration",
        runner: "run_delete_bucket_lifecycle_configuration",
        runtime_file: Some("delete_bucket_lifecycle_configuration.rs"),
        api_calls: &["delete_bucket_lifecycle_configuration"],
    },
    CommandRoute {
        variant: "PutBucketEncryption",
        cli_name: "put-bucket-encryption",
        runner: "run_put_bucket_encryption",
        runtime_file: Some("put_bucket_encryption.rs"),
        api_calls: &["put_bucket_encryption"],
    },
    CommandRoute {
        variant: "GetBucketEncryption",
        cli_name: "get-bucket-encryption",
        runner: "run_get_bucket_encryption",
        runtime_file: Some("get_bucket_encryption.rs"),
        api_calls: &["get_bucket_encryption"],
    },
    CommandRoute {
        variant: "DeleteBucketEncryption",
        cli_name: "delete-bucket-encryption",
        runner: "run_delete_bucket_encryption",
        runtime_file: Some("delete_bucket_encryption.rs"),
        api_calls: &["delete_bucket_encryption"],
    },
    CommandRoute {
        variant: "PutBucketCors",
        cli_name: "put-bucket-cors",
        runner: "run_put_bucket_cors",
        runtime_file: Some("put_bucket_cors.rs"),
        api_calls: &["put_bucket_cors"],
    },
    CommandRoute {
        variant: "GetBucketCors",
        cli_name: "get-bucket-cors",
        runner: "run_get_bucket_cors",
        runtime_file: Some("get_bucket_cors.rs"),
        api_calls: &["get_bucket_cors"],
    },
    CommandRoute {
        variant: "DeleteBucketCors",
        cli_name: "delete-bucket-cors",
        runner: "run_delete_bucket_cors",
        runtime_file: Some("delete_bucket_cors.rs"),
        api_calls: &["delete_bucket_cors"],
    },
    CommandRoute {
        variant: "PutPublicAccessBlock",
        cli_name: "put-public-access-block",
        runner: "run_put_public_access_block",
        runtime_file: Some("put_public_access_block.rs"),
        api_calls: &["put_public_access_block"],
    },
    CommandRoute {
        variant: "GetPublicAccessBlock",
        cli_name: "get-public-access-block",
        runner: "run_get_public_access_block",
        runtime_file: Some("get_public_access_block.rs"),
        api_calls: &["get_public_access_block"],
    },
    CommandRoute {
        variant: "DeletePublicAccessBlock",
        cli_name: "delete-public-access-block",
        runner: "run_delete_public_access_block",
        runtime_file: Some("delete_public_access_block.rs"),
        api_calls: &["delete_public_access_block"],
    },
    CommandRoute {
        variant: "GetBucketVersioning",
        cli_name: "get-bucket-versioning",
        runner: "run_get_bucket_versioning",
        runtime_file: Some("get_bucket_versioning.rs"),
        api_calls: &["get_bucket_versioning"],
    },
    CommandRoute {
        variant: "PutBucketTagging",
        cli_name: "put-bucket-tagging",
        runner: "run_put_bucket_tagging",
        runtime_file: Some("put_bucket_tagging.rs"),
        api_calls: &["put_bucket_tagging"],
    },
    CommandRoute {
        variant: "PutBucketWebsite",
        cli_name: "put-bucket-website",
        runner: "run_put_bucket_website",
        runtime_file: Some("put_bucket_website.rs"),
        api_calls: &["put_bucket_website"],
    },
    CommandRoute {
        variant: "GetBucketWebsite",
        cli_name: "get-bucket-website",
        runner: "run_get_bucket_website",
        runtime_file: Some("get_bucket_website.rs"),
        api_calls: &["get_bucket_website"],
    },
    CommandRoute {
        variant: "DeleteBucketWebsite",
        cli_name: "delete-bucket-website",
        runner: "run_delete_bucket_website",
        runtime_file: Some("delete_bucket_website.rs"),
        api_calls: &["delete_bucket_website"],
    },
    CommandRoute {
        variant: "PutBucketLogging",
        cli_name: "put-bucket-logging",
        runner: "run_put_bucket_logging",
        runtime_file: Some("put_bucket_logging.rs"),
        api_calls: &["put_bucket_logging"],
    },
    CommandRoute {
        variant: "GetBucketLogging",
        cli_name: "get-bucket-logging",
        runner: "run_get_bucket_logging",
        runtime_file: Some("get_bucket_logging.rs"),
        api_calls: &["get_bucket_logging"],
    },
    CommandRoute {
        variant: "PutBucketNotificationConfiguration",
        cli_name: "put-bucket-notification-configuration",
        runner: "run_put_bucket_notification_configuration",
        runtime_file: Some("put_bucket_notification_configuration.rs"),
        api_calls: &["put_bucket_notification_configuration"],
    },
    CommandRoute {
        variant: "GetBucketNotificationConfiguration",
        cli_name: "get-bucket-notification-configuration",
        runner: "run_get_bucket_notification_configuration",
        runtime_file: Some("get_bucket_notification_configuration.rs"),
        api_calls: &["get_bucket_notification_configuration"],
    },
];

#[test]
fn route_table_matches_clap_subcommands() {
    let cli = Cli::command();
    let routed_names: BTreeSet<&str> = COMMAND_ROUTES.iter().map(|route| route.cli_name).collect();

    for route in COMMAND_ROUTES {
        assert!(
            cli.find_subcommand(route.cli_name).is_some(),
            "route table references unknown CLI subcommand {:?}",
            route.cli_name
        );
    }

    for subcommand in cli.get_subcommands() {
        let name = subcommand.get_name();
        if name == "help" {
            continue;
        }
        assert!(
            routed_names.contains(name),
            "CLI subcommand {name:?} is missing from COMMAND_ROUTES"
        );
    }
}

#[test]
fn main_dispatches_each_command_variant_to_expected_runtime() {
    let main_rs = read_repo_file("src/bin/s3util/main.rs");

    for route in COMMAND_ROUTES {
        let branch = match_branch(&main_rs, route.variant);
        let actual_runners = runner_calls_in(branch);
        assert_eq!(
            actual_runners,
            [route.runner],
            "Commands::{} must dispatch to exactly cli::{}",
            route.variant,
            route.runner
        );
    }
}

#[test]
fn each_runtime_calls_only_its_expected_api_wrappers() {
    for route in COMMAND_ROUTES
        .iter()
        .filter(|route| route.runtime_file.is_some())
    {
        let runtime = read_repo_file(format!(
            "src/bin/s3util/cli/{}",
            route.runtime_file.unwrap()
        ));
        let actual_calls = api_calls_in(&runtime);
        assert_eq!(
            actual_calls, route.api_calls,
            "`{}` must call exactly the expected api::* wrappers",
            route.cli_name
        );
    }
}

#[test]
fn api_wrappers_call_the_expected_sdk_operations() {
    let api_rs = read_repo_file("src/storage/s3/api.rs");
    let known_sdk_operations = all_expected_sdk_operations();

    for api_call in expected_api_calls() {
        let body = function_body(&api_rs, api_call);
        let expected_sdk = expected_sdk_operation(api_call);
        let actual_sdk_calls = sdk_operation_calls_in(body, &known_sdk_operations);
        assert_eq!(
            actual_sdk_calls,
            [expected_sdk],
            "api::{api_call} must call the expected AWS SDK operation"
        );
    }
}

fn read_repo_file(path: impl AsRef<std::path::Path>) -> String {
    fs::read_to_string(repo_path(path)).unwrap()
}

fn repo_path(path: impl AsRef<std::path::Path>) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(path)
}

fn match_branch<'a>(source: &'a str, variant: &str) -> &'a str {
    let branch_start = source
        .find(&format!("Commands::{variant}("))
        .unwrap_or_else(|| panic!("Commands::{variant} branch not found"));
    let after_branch_start = &source[branch_start..];

    let body_start = after_branch_start
        .find("=> {")
        .unwrap_or_else(|| panic!("Commands::{variant} branch body not found"))
        + branch_start
        + "=> ".len();

    brace_block(source, body_start)
}

fn function_body<'a>(source: &'a str, function_name: &str) -> &'a str {
    let function_start = source
        .find(&format!("pub async fn {function_name}("))
        .unwrap_or_else(|| panic!("api::{function_name} function not found"));
    let after_function_start = &source[function_start..];
    let body_start = after_function_start
        .find('{')
        .unwrap_or_else(|| panic!("api::{function_name} function body not found"))
        + function_start;

    brace_block(source, body_start)
}

fn brace_block(source: &str, opening_brace: usize) -> &str {
    let mut depth = 0usize;
    let mut body_end = None;

    for (offset, ch) in source[opening_brace..].char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    body_end = Some(opening_brace + offset + ch.len_utf8());
                    break;
                }
            }
            _ => {}
        }
    }

    &source[opening_brace..body_end.expect("matching closing brace not found")]
}

fn api_calls_in(source: &str) -> Vec<&str> {
    prefixed_identifiers_in(source, "api::")
}

fn runner_calls_in(source: &str) -> Vec<&str> {
    prefixed_identifiers_in(source, "cli::")
        .into_iter()
        .filter(|name| name.starts_with("run_"))
        .collect()
}

fn prefixed_identifiers_in<'a>(source: &'a str, prefix: &str) -> Vec<&'a str> {
    let mut calls = Vec::new();
    let mut remainder = source;

    while let Some(offset) = remainder.find(prefix) {
        let after_prefix = &remainder[offset + prefix.len()..];
        let ident_len = after_prefix
            .find(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
            .unwrap_or(after_prefix.len());
        if ident_len > 0 {
            calls.push(&after_prefix[..ident_len]);
        }
        remainder = &after_prefix[ident_len..];
    }

    calls
}

fn expected_api_calls() -> BTreeSet<&'static str> {
    COMMAND_ROUTES
        .iter()
        .flat_map(|route| route.api_calls.iter().copied())
        .collect()
}

fn all_expected_sdk_operations() -> BTreeSet<&'static str> {
    expected_api_calls()
        .into_iter()
        .map(expected_sdk_operation)
        .collect()
}

fn expected_sdk_operation(api_call: &str) -> &str {
    match api_call {
        "delete_bucket_lifecycle_configuration" => "delete_bucket_lifecycle",
        other => other,
    }
}

fn sdk_operation_calls_in<'a>(
    function_body: &str,
    known_sdk_operations: &'a BTreeSet<&'static str>,
) -> Vec<&'a str> {
    known_sdk_operations
        .iter()
        .copied()
        .filter(|operation| function_body.contains(&format!(".{operation}()")))
        .collect()
}
