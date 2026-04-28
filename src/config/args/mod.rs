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
pub mod delete_bucket_cors;
pub mod delete_bucket_encryption;
pub mod delete_bucket_lifecycle_configuration;
pub mod delete_bucket_policy;
pub mod delete_bucket_tagging;
pub mod delete_bucket_website;
pub mod delete_object_tagging;
pub mod delete_public_access_block;
pub mod get_bucket_cors;
pub mod get_bucket_encryption;
pub mod get_bucket_lifecycle_configuration;
pub mod get_bucket_logging;
pub mod get_bucket_notification_configuration;
pub mod get_bucket_policy;
pub mod get_bucket_tagging;
pub mod get_bucket_versioning;
pub mod get_bucket_website;
pub mod get_object_tagging;
pub mod get_public_access_block;
pub mod head_bucket;
pub mod head_object;
pub mod mv;
pub mod put_bucket_cors;
pub mod put_bucket_encryption;
pub mod put_bucket_lifecycle_configuration;
pub mod put_bucket_logging;
pub mod put_bucket_notification_configuration;
pub mod put_bucket_policy;
pub mod put_bucket_tagging;
pub mod put_bucket_versioning;
pub mod put_bucket_website;
pub mod put_object_tagging;
pub mod put_public_access_block;
pub mod rm;
pub mod value_parser;

#[cfg(test)]
mod tests;

pub use cp::CpArgs;
pub use create_bucket::CreateBucketArgs;
pub use delete_bucket::DeleteBucketArgs;
pub use delete_bucket_cors::DeleteBucketCorsArgs;
pub use delete_bucket_encryption::DeleteBucketEncryptionArgs;
pub use delete_bucket_lifecycle_configuration::DeleteBucketLifecycleConfigurationArgs;
pub use delete_bucket_policy::DeleteBucketPolicyArgs;
pub use delete_bucket_tagging::DeleteBucketTaggingArgs;
pub use delete_bucket_website::DeleteBucketWebsiteArgs;
pub use delete_object_tagging::DeleteObjectTaggingArgs;
pub use delete_public_access_block::DeletePublicAccessBlockArgs;
pub use get_bucket_cors::GetBucketCorsArgs;
pub use get_bucket_encryption::GetBucketEncryptionArgs;
pub use get_bucket_lifecycle_configuration::GetBucketLifecycleConfigurationArgs;
pub use get_bucket_logging::GetBucketLoggingArgs;
pub use get_bucket_notification_configuration::GetBucketNotificationConfigurationArgs;
pub use get_bucket_policy::GetBucketPolicyArgs;
pub use get_bucket_tagging::GetBucketTaggingArgs;
pub use get_bucket_versioning::GetBucketVersioningArgs;
pub use get_bucket_website::GetBucketWebsiteArgs;
pub use get_object_tagging::GetObjectTaggingArgs;
pub use get_public_access_block::GetPublicAccessBlockArgs;
pub use head_bucket::HeadBucketArgs;
pub use head_object::HeadObjectArgs;
pub use mv::MvArgs;
pub use put_bucket_cors::PutBucketCorsArgs;
pub use put_bucket_encryption::PutBucketEncryptionArgs;
pub use put_bucket_lifecycle_configuration::PutBucketLifecycleConfigurationArgs;
pub use put_bucket_logging::PutBucketLoggingArgs;
pub use put_bucket_notification_configuration::PutBucketNotificationConfigurationArgs;
pub use put_bucket_policy::PutBucketPolicyArgs;
pub use put_bucket_tagging::PutBucketTaggingArgs;
pub use put_bucket_versioning::PutBucketVersioningArgs;
pub use put_bucket_website::PutBucketWebsiteArgs;
pub use put_object_tagging::PutObjectTaggingArgs;
pub use put_public_access_block::PutPublicAccessBlockArgs;
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
    #[command(display_order = 1)]
    Cp(CpArgs),
    /// Create an S3 bucket (general-purpose or directory bucket)
    ///
    /// Creates an S3 general-purpose bucket, or a directory bucket
    /// (S3 Express One Zone) when the name ends with `--<zone-id>--x-s3`
    /// (e.g. `s3://my-bucket--apne1-az4--x-s3` for an Availability Zone,
    /// `s3://my-bucket--usw2-lax1-az1--x-s3` for a Local Zone).
    ///
    /// For general-purpose buckets, the location constraint is derived from the
    /// resolved region (--target-region, AWS_REGION, or the active profile's region).
    #[command(display_order = 8)]
    CreateBucket(CreateBucketArgs),
    /// Delete an S3 bucket (must be empty)
    #[command(display_order = 10)]
    DeleteBucket(DeleteBucketArgs),
    /// Delete the CORS configuration from an S3 bucket
    #[command(display_order = 27)]
    DeleteBucketCors(DeleteBucketCorsArgs),
    /// Delete the encryption configuration from an S3 bucket
    #[command(display_order = 24)]
    DeleteBucketEncryption(DeleteBucketEncryptionArgs),
    /// Delete the lifecycle configuration from an S3 bucket
    #[command(display_order = 21)]
    DeleteBucketLifecycleConfiguration(DeleteBucketLifecycleConfigurationArgs),
    /// Delete the bucket policy from an S3 bucket
    #[command(display_order = 13)]
    DeleteBucketPolicy(DeleteBucketPolicyArgs),
    /// Delete all tags from an S3 bucket
    #[command(display_order = 16)]
    DeleteBucketTagging(DeleteBucketTaggingArgs),
    /// Delete the website configuration from an S3 bucket
    #[command(display_order = 33)]
    DeleteBucketWebsite(DeleteBucketWebsiteArgs),
    /// Delete all tags from an S3 object
    #[command(display_order = 7)]
    DeleteObjectTagging(DeleteObjectTaggingArgs),
    /// Delete the public-access-block configuration from an S3 bucket
    #[command(display_order = 30)]
    DeletePublicAccessBlock(DeletePublicAccessBlockArgs),
    /// Retrieve the CORS configuration of an S3 bucket and print it as JSON
    #[command(display_order = 26)]
    GetBucketCors(GetBucketCorsArgs),
    /// Retrieve the encryption configuration of an S3 bucket and print it as JSON
    #[command(display_order = 23)]
    GetBucketEncryption(GetBucketEncryptionArgs),
    /// Retrieve the lifecycle configuration of an S3 bucket and print it as JSON
    #[command(display_order = 20)]
    GetBucketLifecycleConfiguration(GetBucketLifecycleConfigurationArgs),
    /// Retrieve the logging configuration of an S3 bucket and print it as JSON
    #[command(display_order = 35)]
    GetBucketLogging(GetBucketLoggingArgs),
    /// Retrieve the notification configuration of an S3 bucket and print it as JSON
    #[command(display_order = 37)]
    GetBucketNotificationConfiguration(GetBucketNotificationConfigurationArgs),
    /// Retrieve the bucket policy of an S3 bucket and print it as JSON
    #[command(display_order = 12)]
    GetBucketPolicy(GetBucketPolicyArgs),
    /// Retrieve the tags of an S3 bucket and print them as JSON
    #[command(display_order = 15)]
    GetBucketTagging(GetBucketTaggingArgs),
    /// Retrieve the versioning state of an S3 bucket and print it as JSON
    #[command(display_order = 18)]
    GetBucketVersioning(GetBucketVersioningArgs),
    /// Retrieve the website configuration of an S3 bucket and print it as JSON
    #[command(display_order = 32)]
    GetBucketWebsite(GetBucketWebsiteArgs),
    /// Retrieve the tags of an S3 object and print them as JSON
    #[command(display_order = 6)]
    GetObjectTagging(GetObjectTaggingArgs),
    /// Retrieve the public-access-block configuration of an S3 bucket and print it as JSON
    #[command(display_order = 29)]
    GetPublicAccessBlock(GetPublicAccessBlockArgs),
    /// Head an S3 bucket and print its metadata as JSON
    #[command(display_order = 9)]
    HeadBucket(HeadBucketArgs),
    /// Head an S3 object and print its metadata as JSON
    #[command(display_order = 4)]
    HeadObject(HeadObjectArgs),
    /// Move objects from/to S3 (copy then delete source)
    #[command(display_order = 2)]
    Mv(MvArgs),
    /// Set the CORS configuration on an S3 bucket
    #[command(display_order = 25)]
    PutBucketCors(PutBucketCorsArgs),
    /// Set the encryption configuration on an S3 bucket
    #[command(display_order = 22)]
    PutBucketEncryption(PutBucketEncryptionArgs),
    /// Set the lifecycle configuration on an S3 bucket
    #[command(display_order = 19)]
    PutBucketLifecycleConfiguration(PutBucketLifecycleConfigurationArgs),
    /// Set the logging configuration on an S3 bucket
    ///
    /// To disable logging, provide an empty configuration (`{}`). There is no
    /// `delete-bucket-logging` API in S3.
    #[command(display_order = 34)]
    PutBucketLogging(PutBucketLoggingArgs),
    /// Set the notification configuration on an S3 bucket
    ///
    /// To remove every notification, provide an empty configuration (`{}`).
    /// There is no `delete-bucket-notification-configuration` API in S3.
    #[command(display_order = 36)]
    PutBucketNotificationConfiguration(PutBucketNotificationConfigurationArgs),
    /// Set the bucket policy on an S3 bucket
    #[command(display_order = 11)]
    PutBucketPolicy(PutBucketPolicyArgs),
    /// Replace all tags on an S3 bucket
    #[command(display_order = 14)]
    PutBucketTagging(PutBucketTaggingArgs),
    /// Set the versioning state of an S3 bucket (Enabled or Suspended)
    #[command(display_order = 17)]
    PutBucketVersioning(PutBucketVersioningArgs),
    /// Set the website configuration on an S3 bucket
    #[command(display_order = 31)]
    PutBucketWebsite(PutBucketWebsiteArgs),
    /// Replace all tags on an S3 object
    #[command(display_order = 5)]
    PutObjectTagging(PutObjectTaggingArgs),
    /// Set the public-access-block configuration on an S3 bucket
    #[command(display_order = 28)]
    PutPublicAccessBlock(PutPublicAccessBlockArgs),
    /// Delete a single S3 object
    #[command(display_order = 3)]
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
        Commands::DeleteBucketCors(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-cors is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucketEncryption(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-encryption is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucketPolicy(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-policy is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucketTagging(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteBucketWebsite(_) => Err(
            "build_config_from_args is for cp/mv only; delete-bucket-website is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeleteObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; delete-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::DeletePublicAccessBlock(_) => Err(
            "build_config_from_args is for cp/mv only; delete-public-access-block is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketCors(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-cors is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketEncryption(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-encryption is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketLogging(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-logging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketNotificationConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-notification-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketPolicy(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-policy is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketTagging(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketVersioning(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-versioning is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetBucketWebsite(_) => Err(
            "build_config_from_args is for cp/mv only; get-bucket-website is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; get-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::GetPublicAccessBlock(_) => Err(
            "build_config_from_args is for cp/mv only; get-public-access-block is dispatched in main.rs"
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
        Commands::PutBucketCors(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-cors is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketEncryption(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-encryption is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketLifecycleConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-lifecycle-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketLogging(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-logging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketNotificationConfiguration(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-notification-configuration is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketPolicy(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-policy is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketTagging(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketVersioning(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-versioning is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutBucketWebsite(_) => Err(
            "build_config_from_args is for cp/mv only; put-bucket-website is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutObjectTagging(_) => Err(
            "build_config_from_args is for cp/mv only; put-object-tagging is dispatched in main.rs"
                .to_string(),
        ),
        Commands::PutPublicAccessBlock(_) => Err(
            "build_config_from_args is for cp/mv only; put-public-access-block is dispatched in main.rs"
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
