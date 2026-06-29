use anyhow::Result;

use s3util_rs::config::ClientConfig;
use s3util_rs::config::args::list_object_annotations::ListObjectAnnotationsArgs;

use super::ExitStatus;

/// Stub implementation for list-object-annotations.
/// This will be implemented in a later task.
pub async fn run_list_object_annotations(
    _args: ListObjectAnnotationsArgs,
    _client_config: ClientConfig,
) -> Result<ExitStatus> {
    unimplemented!("list-object-annotations is not yet implemented")
}
