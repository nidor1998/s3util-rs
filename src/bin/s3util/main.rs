use anyhow::Result;
use clap::{CommandFactory, Parser};
use clap_complete::generate;

use s3util_rs::Config;
use s3util_rs::config::args::{Cli, Commands};

mod cli;
mod tracing_init;

#[tokio::main]
async fn main() -> Result<()> {
    let cli_args = Cli::parse();

    match cli_args.command {
        Commands::Cp(cp_args) => {
            // Short-circuit before Config::try_from — source/target aren't required
            // for shell-completion generation, and try_from would otherwise reject
            // the missing paths.
            if let Some(shell) = cp_args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let config = match Config::try_from(cp_args) {
                Ok(config) => config,
                Err(error_message) => {
                    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
                }
            };

            start_tracing_if_necessary(&config);
            trace_config_summary(&config);

            let exit_code = match cli::run_cp(config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::Mv(mv_args) => {
            if let Some(shell) = mv_args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }
            let config = match Config::try_from(mv_args) {
                Ok(config) => config,
                Err(error_message) => {
                    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
                }
            };
            start_tracing_if_necessary(&config);
            trace_config_summary(&config);

            let exit_code = match cli::run_mv(config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::CreateBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_create_bucket(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::DeleteBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::Rm(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_rm(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::HeadObject(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_head_object(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::HeadBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_head_bucket(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::GetObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_object_tagging(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::PutObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_object_tagging(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::DeleteBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_tagging(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::DeleteObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_object_tagging(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::GetBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_tagging(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::PutBucketVersioning(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_versioning(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::PutBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_policy(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::GetBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_policy(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::DeleteBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_policy(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::GetBucketVersioning(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_versioning(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
        Commands::PutBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_tagging(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            std::process::exit(exit_code);
        }
    }
}

fn start_tracing_if_necessary(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return false;
    }
    tracing_init::init_tracing(config.tracing_config.as_ref().unwrap());
    true
}

// Trace only non-sensitive summary fields. Avoids `{:?}` on the full Config,
// which would risk leaking credentials or SSE-C key material if a future field
// is added without a redacting Debug impl.
fn trace_config_summary(config: &Config) {
    tracing::trace!(
        "config = {{ source: {:?}, target: {:?}, transfer_config: {:?}, server_side_copy: {}, version_id: {:?} }}",
        config.source,
        config.target,
        config.transfer_config,
        config.server_side_copy,
        config.version_id,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3util_rs::config::args::parse_from_args;

    fn build_config(args: Vec<&str>) -> Config {
        let cli = parse_from_args(args).unwrap();
        let Commands::Cp(cp_args) = cli.command else {
            panic!("expected Cp variant");
        };
        Config::try_from(cp_args).unwrap()
    }

    #[test]
    fn start_tracing_returns_false_when_silenced() {
        // -qqq drives clap-verbosity-flag below Error → log_level() = None →
        // tracing_config = None → start_tracing_if_necessary returns false
        // without touching the global subscriber.
        let config = build_config(vec!["s3util", "cp", "-qqq", "/tmp/a", "s3://b/k"]);
        assert!(config.tracing_config.is_none());
        assert!(!start_tracing_if_necessary(&config));
    }

    // The Some-branch of start_tracing_if_necessary calls init_tracing which
    // installs a *global* subscriber via .init() (not .try_init()). That makes
    // it impossible to assert the call's effect from multiple tests in the same
    // process without leaking state into other tests. The branching logic is
    // covered by the test above; the install-side effect is exercised end-to-end
    // by the binary path in tests/e2e_*.rs (cp uses default WarnLevel verbosity).
}
