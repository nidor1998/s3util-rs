use anyhow::Result;
use tracing::error;

use s3util_rs::Config;

use crate::cli::{ExitStatus, run_copy_phase};

pub async fn run_cp(config: Config) -> Result<ExitStatus> {
    let phase = run_copy_phase(config).await?;
    if phase.cancelled {
        return Ok(ExitStatus::Cancelled);
    }
    if let Err(e) = phase.transfer_result {
        error!(error = format!("{e:#}"), "copy failed.");
        return Err(e);
    }
    if phase.has_warning {
        return Ok(ExitStatus::Warning);
    }
    Ok(ExitStatus::Success)
}
