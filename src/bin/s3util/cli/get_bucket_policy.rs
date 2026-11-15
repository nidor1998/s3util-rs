use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::get_bucket_policy::GetBucketPolicyArgs;
use s3util_rs::output::json::get_bucket_policy_to_json;
use s3util_rs::storage::s3::api::{self, HeadError};

use crate::pipe_safe::println_pipe_safe;

use super::ExitStatus;

/// Runtime entry for `s3util get-bucket-policy s3://<BUCKET>`.
///
/// Builds the SDK client from `client_config`, issues `GetBucketPolicy`,
/// and prints the response as pretty-printed JSON followed by a newline.
/// Default output mirrors `aws s3api get-bucket-policy --output json`:
/// `{"Policy": "<escaped-JSON-string>"}` where `Policy` is the raw policy
/// JSON double-encoded as a JSON string. With `--policy-only`, prints just
/// the policy JSON itself, re-indented but otherwise verbatim (key order,
/// duplicate keys, and number spelling are preserved).
///
/// Returns `ExitStatus::NotFound` (exit code 4) when S3 reports
/// `NoSuchBucket` (logged as "bucket … not found") or `NoSuchBucketPolicy`
/// (logged as "policy for … not found").
pub async fn run_get_bucket_policy(
    args: GetBucketPolicyArgs,
    client_config: ClientConfig,
) -> Result<ExitStatus> {
    let bucket = args
        .bucket_name()
        .map_err(|e| anyhow::anyhow!("{}", e.trim_end()))?;
    let client = client_config.create_client().await;
    match api::get_bucket_policy(&client, &bucket).await {
        Ok(out) => {
            let pretty = if args.policy_only {
                render_policy_only(out.policy())?
            } else {
                serde_json::to_string_pretty(&get_bucket_policy_to_json(&out))?
            };
            println_pipe_safe(&pretty)?;
            Ok(ExitStatus::Success)
        }
        Err(HeadError::BucketNotFound) => {
            tracing::error!("bucket s3://{bucket} not found");
            Ok(ExitStatus::NotFound)
        }
        Err(HeadError::NotFound) => {
            tracing::error!("policy for s3://{bucket} not found");
            Ok(ExitStatus::NotFound)
        }
        Err(HeadError::Other(e)) => Err(e),
    }
}

/// Render the inner policy for `--policy-only`. Parses the policy string as
/// JSON and pretty-prints it; falls back to the raw string verbatim if S3
/// somehow returned non-JSON. Returns `{}` when no policy field is present
/// (a 200-OK with empty body — should not occur in practice; S3 returns
/// `NoSuchBucketPolicy` instead, which the caller maps to NotFound).
fn render_policy_only(policy: Option<&str>) -> Result<String> {
    let Some(policy) = policy else {
        return Ok("{}".to_string());
    };
    match serde_json::from_str::<serde::de::IgnoredAny>(policy) {
        Ok(_) => Ok(reindent_json(policy)),
        Err(_) => Ok(policy.to_string()),
    }
}

/// Pretty-print already-validated JSON text by re-flowing whitespace only.
///
/// Going through `serde_json::Value` would normalise the policy: without the
/// `preserve_order` feature its maps are sorted by key, duplicate keys collapse
/// to the last one, and numbers are re-spelled (`1.50` -> `1.5`). Here every
/// token is copied verbatim; only the whitespace between tokens is replaced,
/// using `serde_json::to_string_pretty`'s layout (two-space indent, `": "`,
/// empty containers kept as `{}` / `[]`).
fn reindent_json(src: &str) -> String {
    fn newline(out: &mut String, depth: usize) {
        out.push('\n');
        for _ in 0..depth {
            out.push_str("  ");
        }
    }

    let mut out = String::with_capacity(src.len() * 2);
    let mut depth = 0usize;
    let mut chars = src.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                out.push('"');
                while let Some(c) = chars.next() {
                    out.push(c);
                    match c {
                        '\\' => {
                            if let Some(escaped) = chars.next() {
                                out.push(escaped);
                            }
                        }
                        '"' => break,
                        _ => {}
                    }
                }
            }
            '{' | '[' => {
                out.push(c);
                while chars.peek().is_some_and(|c| c.is_ascii_whitespace()) {
                    chars.next();
                }
                if matches!(chars.peek(), Some('}' | ']')) {
                    out.push(chars.next().unwrap());
                } else {
                    depth += 1;
                    newline(&mut out, depth);
                }
            }
            '}' | ']' => {
                depth = depth.saturating_sub(1);
                newline(&mut out, depth);
                out.push(c);
            }
            ',' => {
                out.push(',');
                newline(&mut out, depth);
            }
            ':' => out.push_str(": "),
            c if c.is_ascii_whitespace() => {}
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_policy_only_pretty_prints_valid_json() {
        let raw = r#"{"Version":"2012-10-17","Statement":[]}"#;
        let got = render_policy_only(Some(raw)).unwrap();
        // Must be pretty-printed (newlines, indentation), and re-parse identically.
        assert!(got.contains('\n'), "expected pretty-printed output: {got}");
        let reparsed: serde_json::Value = serde_json::from_str(&got).unwrap();
        let original: serde_json::Value = serde_json::from_str(raw).unwrap();
        assert_eq!(reparsed, original);
    }

    #[test]
    fn render_policy_only_falls_back_to_raw_for_invalid_json() {
        let raw = "not-json";
        let got = render_policy_only(Some(raw)).unwrap();
        assert_eq!(got, "not-json");
    }

    #[test]
    fn render_policy_only_emits_empty_object_when_policy_missing() {
        let got = render_policy_only(None).unwrap();
        assert_eq!(got, "{}");
    }

    /// `--policy-only` must show the policy S3 stored, not a normalised copy:
    /// key order, number spelling, and duplicate keys all survive.
    #[test]
    fn render_policy_only_preserves_policy_text_exactly() {
        let raw = r#"{"Version":"2012-10-17","Statement":[{"Sid":"a","Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*","Condition":{"NumericLessThan":{"s3:max-keys":1.50,"k":1e3},"NumericEquals":{"x":"1"},"NumericEquals":{"x":"2"}}}]}"#;
        let got = render_policy_only(Some(raw)).unwrap();
        let expected = r#"{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "a",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::b/*",
      "Condition": {
        "NumericLessThan": {
          "s3:max-keys": 1.50,
          "k": 1e3
        },
        "NumericEquals": {
          "x": "1"
        },
        "NumericEquals": {
          "x": "2"
        }
      }
    }
  ]
}"#;
        assert_eq!(got, expected);
    }

    #[test]
    fn render_policy_only_keeps_string_contents_verbatim() {
        // Structural characters, whitespace, and escapes inside strings must
        // not be touched by the re-indenter.
        let raw = "{ \"Sid\" : \"a, b: {c} [d]\\\"  \\\\\" ,\n\t\"S\": [ ], \"O\": { } }";
        let got = render_policy_only(Some(raw)).unwrap();
        let expected = "{\n  \"Sid\": \"a, b: {c} [d]\\\"  \\\\\",\n  \"S\": [],\n  \"O\": {}\n}";
        assert_eq!(got, expected);
    }
}
