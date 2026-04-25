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
pub mod create_bucket;
pub mod delete_bucket;
pub mod delete_object_tagging;
pub mod get_bucket_versioning;
pub mod get_object_tagging;
pub mod head_bucket;
pub mod head_object;
pub mod mv;
pub mod put_bucket_versioning;
pub mod put_object_tagging;
pub mod rm;
pub mod value_parser;

#[cfg(test)]
mod tests;

pub use cp::CpArgs;
pub use create_bucket::CreateBucketArgs;
pub use delete_bucket::DeleteBucketArgs;
pub use delete_object_tagging::DeleteObjectTaggingArgs;
pub use get_bucket_versioning::GetBucketVersioningArgs;
pub use get_object_tagging::GetObjectTaggingArgs;
pub use head_bucket::HeadBucketArgs;
pub use head_object::HeadObjectArgs;
pub use mv::MvArgs;
pub use put_bucket_versioning::PutBucketVersioningArgs;
pub use put_object_tagging::PutObjectTaggingArgs;
pub use rm::RmArgs;

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
    /// Create an S3 bucket
    CreateBucket(CreateBucketArgs),
    /// Delete an S3 bucket (must be empty)
    DeleteBucket(DeleteBucketArgs),
    /// Delete all tags from an S3 object
    DeleteObjectTagging(DeleteObjectTaggingArgs),
    /// Retrieve the versioning state of an S3 bucket and print it as JSON
    GetBucketVersioning(GetBucketVersioningArgs),
    /// Retrieve the tags of an S3 object and print them as JSON
    GetObjectTagging(GetObjectTaggingArgs),
    /// Head an S3 bucket and print its metadata as JSON
    HeadBucket(HeadBucketArgs),
    /// Head an S3 object and print its metadata as JSON
    HeadObject(HeadObjectArgs),
    /// Move objects from/to S3 (copy then delete source)
    Mv(MvArgs),
    /// Set the versioning state of an S3 bucket (Enabled or Suspended)
    PutBucketVersioning(PutBucketVersioningArgs),
    /// Replace all tags on an S3 object
    PutObjectTagging(PutObjectTaggingArgs),
    /// Delete a single S3 object
    Rm(RmArgs),
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
        Commands::CreateBucket(_) => Err(
            "build_config_from_args is for cp/mv only; create-bucket is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucket(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; delete-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketVersioning(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-versioning is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; get-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::HeadBucket(_) => Err(
            "build_config_from_args is for cp/mv only; head-bucket is dispatched in main.rs"
                .to_string(),
        ),
        Commands::HeadObject(_) => Err(
            "build_config_from_args is for cp/mv only; head-object is dispatched in main.rs"
                .to_string(),
        ),
        Commands::Mv(mv_args) => Config::try_from(mv_args),
        Commands::PutBucketVersioning(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-versioning is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; put-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::Rm(_) => {
            Err("build_config_from_args is for cp/mv only; rm is dispatched in main.rs".to_string())
        }
    }
}

pub(crate) fn is_express_onezone_storage(bucket: &str) -> bool {
    bucket.ends_with(EXPRESS_ONEZONE_STORAGE_SUFFIX)
}
