//! Categorized top-level `--help` for s3util.
//!
//! Clap has no built-in feature for grouping subcommands under multiple
//! help headings, so we intercept top-level help requests before clap
//! parses arguments and render our own format. Subcommand descriptions
//! are read from the `clap::Command` (built from the `Commands` enum) so
//! the help text stays in sync with the doc comments — only the
//! category-to-subcommand mapping is hand-maintained below.

use std::io::Write;

use clap::CommandFactory;

use s3util_rs::config::args::Cli;

/// Subcommand → category. Order within a category is the display order.
/// Subcommands not listed here fall under "Other".
const CATEGORIES: &[(&str, &[&str])] = &[
    ("Object Operations", &["cp", "mv", "rm"]),
    (
        "Object Metadata",
        &[
            "head-object",
            "get-object-tagging",
            "put-object-tagging",
            "delete-object-tagging",
        ],
    ),
    (
        "Bucket Operations",
        &["create-bucket", "head-bucket", "delete-bucket"],
    ),
    (
        "Bucket Tagging",
        &[
            "get-bucket-tagging",
            "put-bucket-tagging",
            "delete-bucket-tagging",
        ],
    ),
    (
        "Bucket Policy",
        &[
            "get-bucket-policy",
            "put-bucket-policy",
            "delete-bucket-policy",
        ],
    ),
    (
        "Bucket Versioning",
        &["get-bucket-versioning", "put-bucket-versioning"],
    ),
    (
        "Bucket Lifecycle Configuration",
        &[
            "get-bucket-lifecycle-configuration",
            "put-bucket-lifecycle-configuration",
            "delete-bucket-lifecycle-configuration",
        ],
    ),
    (
        "Bucket Encryption",
        &[
            "get-bucket-encryption",
            "put-bucket-encryption",
            "delete-bucket-encryption",
        ],
    ),
    (
        "Bucket CORS",
        &["get-bucket-cors", "put-bucket-cors", "delete-bucket-cors"],
    ),
    (
        "Bucket Public Access Block",
        &[
            "get-public-access-block",
            "put-public-access-block",
            "delete-public-access-block",
        ],
    ),
    (
        "Bucket Website",
        &[
            "get-bucket-website",
            "put-bucket-website",
            "delete-bucket-website",
        ],
    ),
    (
        "Bucket Logging",
        &["get-bucket-logging", "put-bucket-logging"],
    ),
    (
        "Bucket Notification Configuration",
        &[
            "get-bucket-notification-configuration",
            "put-bucket-notification-configuration",
        ],
    ),
];

/// True when `args` (typically `std::env::args().collect()`) requests
/// top-level help: `s3util -h`, `s3util --help`, or bare `s3util help`.
/// Argv with no subcommand at all is *not* treated as help — it preserves
/// the existing "missing subcommand" error path.
pub fn is_top_level_help_request(args: &[String]) -> bool {
    if args.len() < 2 {
        return false;
    }
    let first = args[1].as_str();
    first == "-h" || first == "--help" || (first == "help" && args.len() == 2)
}

/// Print categorized top-level help to `out`.
pub fn print_categorized_help<W: Write>(out: &mut W) -> std::io::Result<()> {
    let cmd = Cli::command();
    let bin = cmd.get_name();
    let about = cmd.get_about().map(|s| s.to_string()).unwrap_or_default();

    if !about.is_empty() {
        writeln!(out, "{about}")?;
        writeln!(out)?;
    }
    writeln!(out, "Usage: {bin} [OPTIONS] [COMMAND]")?;
    writeln!(out)?;

    let max_name_len = cmd
        .get_subcommands()
        .map(|s| s.get_name().len())
        .max()
        .unwrap_or(0);

    let mut printed: std::collections::HashSet<String> = std::collections::HashSet::new();

    for (heading, names) in CATEGORIES {
        writeln!(out, "{heading}:")?;
        for name in *names {
            if let Some(sub) = cmd.find_subcommand(name) {
                let desc = sub.get_about().map(|s| s.to_string()).unwrap_or_default();
                writeln!(out, "  {name:<max_name_len$}  {desc}")?;
                printed.insert(name.to_string());
            }
        }
        writeln!(out)?;
    }

    let other_subs: Vec<_> = cmd
        .get_subcommands()
        .filter(|s| !printed.contains(s.get_name()))
        .collect();
    let mut other_lines: Vec<(String, String)> = other_subs
        .iter()
        .map(|s| {
            (
                s.get_name().to_string(),
                s.get_about().map(|x| x.to_string()).unwrap_or_default(),
            )
        })
        .collect();
    if !other_lines.iter().any(|(n, _)| n == "help") {
        other_lines.push((
            "help".to_string(),
            "Print this message or the help of the given subcommand(s)".to_string(),
        ));
    }
    if !other_lines.is_empty() {
        writeln!(out, "Other:")?;
        for (name, desc) in &other_lines {
            writeln!(out, "  {name:<max_name_len$}  {desc}")?;
        }
        writeln!(out)?;
    }

    writeln!(out, "Options:")?;
    writeln!(out, "  -h, --help     Print help")?;
    writeln!(out, "  -V, --version  Print version")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_help_flag() {
        assert!(is_top_level_help_request(&[
            "s3util".into(),
            "--help".into(),
        ]));
        assert!(is_top_level_help_request(&["s3util".into(), "-h".into()]));
        assert!(is_top_level_help_request(
            &["s3util".into(), "help".into(),]
        ));
    }

    #[test]
    fn does_not_treat_bare_invocation_as_help() {
        // Preserves the existing "missing subcommand exits non-zero" behaviour.
        assert!(!is_top_level_help_request(&["s3util".into()]));
        assert!(!is_top_level_help_request(&[]));
    }

    #[test]
    fn does_not_intercept_help_with_subcommand() {
        // `s3util help cp` -> let clap render cp's own help.
        assert!(!is_top_level_help_request(&[
            "s3util".into(),
            "help".into(),
            "cp".into(),
        ]));
    }

    #[test]
    fn does_not_intercept_subcommand_help() {
        // `s3util cp --help` -> let clap render cp's own help.
        assert!(!is_top_level_help_request(&[
            "s3util".into(),
            "cp".into(),
            "--help".into(),
        ]));
    }

    #[test]
    fn every_categorized_name_is_a_real_subcommand() {
        let cmd = Cli::command();
        for (heading, names) in CATEGORIES {
            for name in *names {
                assert!(
                    cmd.find_subcommand(name).is_some(),
                    "category {heading:?} references unknown subcommand {name:?}"
                );
            }
        }
    }

    #[test]
    fn every_subcommand_is_categorized() {
        // New subcommands must be added to CATEGORIES (or explicitly accepted
        // here as "Other") so the categorized help stays exhaustive.
        let cmd = Cli::command();
        let mut categorized = std::collections::HashSet::new();
        for (_, names) in CATEGORIES {
            for n in *names {
                categorized.insert(n.to_string());
            }
        }
        let allowed_other = ["help"];
        for sub in cmd.get_subcommands() {
            let name = sub.get_name();
            if categorized.contains(name) || allowed_other.contains(&name) {
                continue;
            }
            panic!(
                "subcommand {name:?} is not in CATEGORIES — add it to a category in src/bin/s3util/help.rs"
            );
        }
    }

    #[test]
    fn rendered_help_contains_category_headings() {
        let mut buf = Vec::new();
        print_categorized_help(&mut buf).unwrap();
        let s = String::from_utf8(buf).unwrap();
        assert!(s.contains("Object Operations:"));
        assert!(s.contains("Bucket Operations:"));
        assert!(s.contains("Bucket Lifecycle Configuration:"));
        assert!(s.contains("Other:"));
        assert!(s.contains("Options:"));
        assert!(s.contains("cp"));
        assert!(s.contains("get-bucket-cors"));
    }
}
