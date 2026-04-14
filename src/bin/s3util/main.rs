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
            let config = match Config::try_from(cp_args) {
                Ok(config) => config,
                Err(error_message) => {
                    clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message).exit();
                }
            };

            if let Some(shell) = config.auto_complete_shell {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return Ok(());
            }

            start_tracing_if_necessary(&config);
            tracing::trace!("config = {:?}", config);

            if let Err(_e) = cli::run_cp(config).await {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn start_tracing_if_necessary(config: &Config) -> bool {
    if config.tracing_config.is_none() {
        return false;
    }
    tracing_init::init_tracing(config.tracing_config.as_ref().unwrap());
    true
}
