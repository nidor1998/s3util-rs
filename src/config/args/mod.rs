use crate::config::Config;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

#[cfg(feature = "version")]
use shadow_rs::shadow;

#[cfg(feature = "version")]
shadow!(build);

pub mod common;
pub mod common_client;
pub mod cp;
pub mod head_bucket;
pub mod mv;
pub mod value_parser;

#[cfg(test)]
mod tests;

pub use cp::CpArgs;
pub use head_bucket::HeadBucketArgs;
pub use mv::MvArgs;

// Re-exports kept here so existing callers that reference
// `crate::config::args::TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX`
// (notably the in-module tests) keep compiling unchanged.
#[cfg(test)]
pub(crate) use common::TARGET_LOCAL_DIRECTORY_DOES_NOT_EXIST_PREFIX;

const EXPRESS_ONEZONE_STORAGE_SUFFIX: &str = "--x-s3";

#[derive(Parser, Clone, Debug)]
#[cfg_attr(
    feature = "version",
    command(version = format!(
        "{} ({} {}), {}",
        build::PKG_VERSION,
        build::SHORT_COMMIT,
        build::BUILD_TARGET,
        build::RUST_VERSION
    ))
)]
#[cfg_attr(not(feature = "version"), command(version))]
#[command(name = "s3util", about = "S3 utility commands")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Clone, Debug)]
pub enum Commands {
    /// Copy objects from/to S3
    Cp(CpArgs),
    /// Move objects from/to S3 (copy then delete source)
    Mv(MvArgs),
}

pub fn parse_from_args<I, T>(args: I) -> Result<Cli, clap::Error>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    Cli::try_parse_from(args)
}

pub fn build_config_from_args<I, T>(args: I) -> Result<Config, String>
where
    I: IntoIterator<Item = T>,
    T: Into<OsString> + Clone,
{
    let cli = Cli::try_parse_from(args).map_err(|e| e.to_string())?;
    match cli.command {
        Commands::Cp(cp_args) => Config::try_from(cp_args),
        Commands::Mv(mv_args) => Config::try_from(mv_args),
    }
}

pub(crate) fn is_express_onezone_storage(bucket: &str) -> bool {
    bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
}
