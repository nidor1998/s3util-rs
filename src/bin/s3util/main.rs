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
            tracing::trace!("config = {:?}", config);

            let exit_code = match cli::run_cp(config).await {
                Ok(status) => status.code(),
                Err(_) => cli::EXIT_CODE_ERROR,
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
