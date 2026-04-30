use std::process::ExitCode;

use clap::{CommandFactory, Parser};
use clap_complete::generate;

use s3util_rs::Config;
use s3util_rs::config::args::{Cli, Commands};

mod cli;
mod help;
mod tracing_init;

#[tokio::main]
async fn main() -> ExitCode {
    let raw_args: Vec<String> = std::env::args().collect();
    if help::is_top_level_help_request(&raw_args) {
        let mut stdout = std::io::stdout().lock();
        let _ = help::print_categorized_help(&mut stdout);
        return ExitCode::SUCCESS;
    }
    // No subcommand: render the same categorized help, but to stderr with
    // exit 2 to preserve the "missing subcommand is an error" semantic.
    if raw_args.len() <= 1 {
        let mut stderr = std::io::stderr().lock();
        let _ = help::print_categorized_help(&mut stderr);
        return ExitCode::from(2);
    }

    let cli_args = Cli::parse();

    match cli_args.command {
        Commands::Cp(cp_args) => {
            // Short-circuit before Config::try_from — source/target aren't required
            // for shell-completion generation, and try_from would otherwise reject
            // the missing paths.
            if let Some(shell) = cp_args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let config = match Config::try_from(cp_args) {
                Ok(config) => config,
                Err(error_message) => {
                    let _ =
                        clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message)
                            .print();
                    return ExitCode::from(2);
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::Mv(mv_args) => {
            if let Some(shell) = mv_args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }
            let config = match Config::try_from(mv_args) {
                Ok(config) => config,
                Err(error_message) => {
                    let _ =
                        clap::Error::raw(clap::error::ErrorKind::ValueValidation, error_message)
                            .print();
                    return ExitCode::from(2);
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::CreateBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::Rm(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::HeadObject(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::HeadBucket(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteObjectTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketVersioning(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketPolicy(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code =
                match cli::run_put_bucket_lifecycle_configuration(args, client_config).await {
                    Ok(()) => cli::EXIT_CODE_SUCCESS,
                    Err(e) => {
                        tracing::error!(error = format!("{e:#}"));
                        cli::EXIT_CODE_ERROR
                    }
                };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code =
                match cli::run_get_bucket_lifecycle_configuration(args, client_config).await {
                    Ok(status) => status.code(),
                    Err(e) => {
                        tracing::error!(error = format!("{e:#}"));
                        cli::EXIT_CODE_ERROR
                    }
                };
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketLifecycleConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code =
                match cli::run_delete_bucket_lifecycle_configuration(args, client_config).await {
                    Ok(()) => cli::EXIT_CODE_SUCCESS,
                    Err(e) => {
                        tracing::error!(error = format!("{e:#}"));
                        cli::EXIT_CODE_ERROR
                    }
                };
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketEncryption(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_encryption(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketEncryption(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_encryption(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketEncryption(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_encryption(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketCors(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_cors(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketCors(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_cors(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketCors(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_cors(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutPublicAccessBlock(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_public_access_block(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetPublicAccessBlock(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_public_access_block(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeletePublicAccessBlock(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_public_access_block(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketVersioning(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketTagging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
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
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketWebsite(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_website(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketWebsite(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_website(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::DeleteBucketWebsite(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_delete_bucket_website(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketLogging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_put_bucket_logging(args, client_config).await {
                Ok(()) => cli::EXIT_CODE_SUCCESS,
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketLogging(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code = match cli::run_get_bucket_logging(args, client_config).await {
                Ok(status) => status.code(),
                Err(e) => {
                    tracing::error!(error = format!("{e:#}"));
                    cli::EXIT_CODE_ERROR
                }
            };
            return ExitCode::from(exit_code as u8);
        }
        Commands::PutBucketNotificationConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code =
                match cli::run_put_bucket_notification_configuration(args, client_config).await {
                    Ok(()) => cli::EXIT_CODE_SUCCESS,
                    Err(e) => {
                        tracing::error!(error = format!("{e:#}"));
                        cli::EXIT_CODE_ERROR
                    }
                };
            return ExitCode::from(exit_code as u8);
        }
        Commands::GetBucketNotificationConfiguration(args) => {
            if let Some(shell) = args.auto_complete_shell() {
                generate(shell, &mut Cli::command(), "s3util", &mut std::io::stdout());
                return ExitCode::SUCCESS;
            }

            let tracing_config = args.common.build_tracing_config();
            if let Some(tc) = &tracing_config {
                tracing_init::init_tracing(tc);
            }

            let client_config = args.common.build_client_config();

            let exit_code =
                match cli::run_get_bucket_notification_configuration(args, client_config).await {
                    Ok(status) => status.code(),
                    Err(e) => {
                        tracing::error!(error = format!("{e:#}"));
                        cli::EXIT_CODE_ERROR
                    }
                };
            return ExitCode::from(exit_code as u8);
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
