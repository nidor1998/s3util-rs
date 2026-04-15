use anyhow::{Context, Result, anyhow};
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::types::builders::ObjectPartBuilder;
use aws_sdk_s3::types::{ChecksumAlgorithm, ChecksumMode, ObjectPart};
use tracing::{error, warn};

use crate::config::Config;
use crate::storage::e_tag_verify;
use crate::storage::{StorageTrait, get_range_from_content_range, parse_range_header_string};
use crate::types::{
    ObjectChecksum, get_additional_checksum, get_additional_checksum_with_head_object,
    is_full_object_checksum,
};

/// Minimum S3 multipart part size (5 MiB). Objects smaller than this are always fetched in full.
/// Matches s3sync/src/types/mod.rs:49.
const MINIMUM_CHUNKSIZE: i64 = 5 * 1024 * 1024;

/// Default max_parts for get_object_parts_attributes. Matches s3sync default of 1000.
const MAX_PARTS_DEFAULT: i32 = 1000;

/// Calculate the range header for fetching only the first chunk.
/// Returns None if the full object should be fetched.
/// Ported from s3sync/src/pipeline/syncer.rs:1270-1352.
pub async fn get_first_chunk_range(
    source: &dyn StorageTrait,
    config: &Config,
    source_size: i64,
    source_key: &str,
    version_id: Option<String>,
) -> Result<Option<String>> {
    // If the object size is less than the minimum chunk size, no need to get the first chunk range.
    if source_size < MINIMUM_CHUNKSIZE {
        return Ok(None);
    }

    if source.is_local_storage() {
        if config
            .transfer_config
            .is_multipart_upload_required(source_size as u64)
        {
            let first_chunk_size =
                if source_size < config.transfer_config.multipart_chunksize as i64 {
                    source_size as u64
                } else {
                    config.transfer_config.multipart_chunksize
                };
            return Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)));
        }
        return Ok(None);
    }

    // If auto_chunksize is enabled, we need to get the first chunk size from the head object.
    // Without auto_chunksize, we do not need to get the first chunk range, even if the object has a additional checksum,
    if config.transfer_config.auto_chunksize {
        let head_object_result = source
            .head_object_first_part(
                source_key,
                version_id,
                Some(ChecksumMode::Enabled),
                config.source_sse_c.clone(),
                config.source_sse_c_key.clone(),
                config.source_sse_c_key_md5.clone(),
            )
            .await
            .context("get_first_chunk_range() failed.");

        if head_object_result.is_err() {
            error!(key = source_key, "get_first_chunk_range() failed.");

            return Err(anyhow!("get_first_chunk_range() failed."));
        }

        return Ok(Some(format!(
            "bytes=0-{}",
            head_object_result?.content_length.unwrap() - 1
        )));
    }

    if !config
        .transfer_config
        .is_multipart_upload_required(source_size as u64)
    {
        return Ok(None);
    }

    let first_chunk_size = if source_size < config.transfer_config.multipart_chunksize as i64 {
        source_size as u64
    } else {
        config.transfer_config.multipart_chunksize
    };
    Ok(Some(format!("bytes=0-{}", first_chunk_size - 1)))
}

/// Validate that the response content-range matches the requested range.
/// Ported from s3sync/src/pipeline/syncer.rs:623-646.
pub fn validate_content_range(get_object_output: &GetObjectOutput, range: &str) -> Result<()> {
    if get_object_output.content_range().is_none() {
        return Err(anyhow!(
            "get_object() returned no content range. This is unexpected."
        ));
    }
    let (request_start, request_end) =
        parse_range_header_string(range).context("failed to parse request range header")?;
    let (response_start, response_end) = get_range_from_content_range(get_object_output)
        .context("get_object() returned no content range")?;
    if (request_start != response_start) || (request_end != response_end) {
        return Err(anyhow!(
            "get_object() returned unexpected content range. \
            expected: {}-{}, actual: {}-{}",
            request_start,
            request_end,
            response_start,
            response_end,
        ));
    }
    Ok(())
}

/// Get object parts from source if needed (for auto-chunksize or checksum verification).
/// Ported from s3sync/src/pipeline/syncer.rs:1355-1492.
#[allow(clippy::too_many_arguments)]
pub async fn get_object_parts_if_necessary(
    source: &dyn StorageTrait,
    config: &Config,
    key: &str,
    version_id: Option<&str>,
    e_tag: Option<&str>,
    first_chunk_content_length: i64,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    full_object_checksum: bool,
    range: Option<&str>,
) -> Result<Option<Vec<ObjectPart>>> {
    if !e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
        && range.is_none()
    {
        return Ok(None);
    }

    let key = key.to_string();

    // If auto_chunksize is disabled, we still need to get the object parts if the checksum algorithm is specified.
    if let Some(algorithm) = checksum_algorithm {
        // A full object checksum has no object parts.
        if !algorithm.is_empty() && !full_object_checksum {
            let object_parts = source
                .get_object_parts_attributes(
                    &key,
                    version_id.map(|version_id| version_id.to_string()),
                    MAX_PARTS_DEFAULT,
                    config.source_sse_c.clone(),
                    config.source_sse_c_key.clone(),
                    config.source_sse_c_key_md5.clone(),
                )
                .await
                .context("get_object_parts_if_necessary() failed.")?;

            if object_parts.is_empty()
                && e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string()))
            {
                error!(
                    key = key,
                    "failed to get object attributes information. \
                        Please remove --auto-chunksize option and retry."
                );

                return Err(anyhow!("failed to get object attributes information."));
            }

            if config.transfer_config.auto_chunksize
                && object_parts[0].size.unwrap() != first_chunk_content_length
            {
                error!(
                    key = key,
                    "object parts(attribute) size does not match content length. \
                    This is unexpected. Please remove --auto-chunksize option and retry."
                );

                return Err(anyhow!(
                    "object parts(attribute) size does not match content length."
                ));
            }

            return Ok(Some(object_parts));
        }
    }

    if config.transfer_config.auto_chunksize && !source.is_local_storage() {
        let object_parts = source
            .get_object_parts(
                &key,
                version_id.map(|version_id| version_id.to_string()),
                config.source_sse_c.clone(),
                config.source_sse_c_key.clone(),
                config.source_sse_c_key_md5.clone(),
            )
            .await
            .context("get_object_parts_if_necessary() failed.")?;

        if e_tag_verify::is_multipart_upload_e_tag(&e_tag.map(|e_tag| e_tag.to_string())) {
            // If the object is a multipart upload, and the object parts are empty, it should be a error.
            if object_parts.is_empty() {
                error!(
                    key = key,
                    "failed to get object parts information. \
                    this is most likely due to the lack of HeadObject support for partNumber parameter. \
                    Please remove --auto-chunksize option and retry."
                );

                return Err(anyhow!("failed to get object parts information."));
            }
        } else {
            // Even if the object is not a multipart upload, we need to return the object parts for auto-chunksize.
            let object_parts = vec![
                ObjectPartBuilder::default()
                    .size(first_chunk_content_length)
                    .build(),
            ];
            return Ok(Some(object_parts));
        }

        if object_parts[0].size.unwrap() != first_chunk_content_length {
            error!(
                key = key,
                "object parts size does not match content length. \
                This is unexpected. Please remove --auto-chunksize option and retry."
            );

            return Err(anyhow!("object parts size does not match content length."));
        }

        Ok(Some(object_parts))
    } else {
        Ok(None)
    }
}

/// Retrieve the final checksum. When a range download was used, the first chunk doesn't
/// contain the final checksum, so a head_object call is needed.
/// Ported from s3sync/src/pipeline/syncer.rs:1176-1268.
#[allow(clippy::too_many_arguments)]
pub async fn get_final_checksum(
    source: &dyn StorageTrait,
    target: &dyn StorageTrait,
    config: &Config,
    get_object_output: &GetObjectOutput,
    range: Option<&str>,
    source_key: &str,
    version_id: Option<String>,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
) -> Option<String> {
    let _additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
        if algorithm.is_empty()
            || (config.additional_checksum_mode.is_none() && !target.is_local_storage())
        {
            None
        } else {
            // Only one algorithm supported
            Some(algorithm[0].clone())
        }
    } else {
        None
    };

    // If the object is from local storage, we can get the additional checksum directly.
    if source.is_local_storage() {
        return get_additional_checksum(
            get_object_output,
            config.additional_checksum_algorithm.clone(),
        );
    }

    // If additional_checksum_mode is not set in remote storage, we cannot get the final checksum.
    config.additional_checksum_mode.as_ref()?;

    // If range option is not specified, the final checksum is already calculated.
    if range.is_none() {
        return get_additional_checksum(
            get_object_output,
            config.additional_checksum_algorithm.clone(),
        );
    }

    // if range option is specified, we need to get the final checksum from the head object.
    let head_object_result = source
        .head_object(
            source_key,
            version_id,
            config.additional_checksum_mode.clone(),
            None,
            config.source_sse_c.clone(),
            config.source_sse_c_key.clone(),
            config.source_sse_c_key_md5.clone(),
        )
        .await
        .context("get_final_checksum() failed.");

    if head_object_result.is_err() {
        let message = "failed to get object parts information. \
            checksum verification may fail. This is most likely due to the lack of HeadObject support for partNumber parameter.";
        warn!(key = source_key, message);

        return None;
    }

    get_additional_checksum_with_head_object(
        &head_object_result.unwrap(),
        _additional_checksum_algorithm,
    )
}

/// Build ObjectChecksum with correct object_parts populated.
/// Ported from s3sync/src/pipeline/syncer.rs:1494-1547.
#[allow(clippy::too_many_arguments)]
pub async fn build_object_checksum(
    source: &dyn StorageTrait,
    target: &dyn StorageTrait,
    config: &Config,
    key: &str,
    get_object_output: &GetObjectOutput,
    checksum_algorithm: Option<&[ChecksumAlgorithm]>,
    final_checksum: Option<String>,
) -> Result<Option<ObjectChecksum>> {
    let additional_checksum_algorithm = if let Some(algorithm) = checksum_algorithm {
        if algorithm.is_empty()
            || (config.additional_checksum_mode.is_none() && !target.is_local_storage())
        {
            None
        } else {
            // Only one algorithm supported
            Some(algorithm[0].clone())
        }
    } else {
        None
    };

    // If additional_checksum_mode is not set, do not build object checksum regardless of the existence of checksum.
    let checksum_algorithm = if config.additional_checksum_mode.is_none() {
        None
    } else {
        checksum_algorithm
    };

    let object_parts = get_object_parts_if_necessary(
        source,
        config,
        key,
        get_object_output.version_id(),
        get_object_output.e_tag(),
        get_object_output.content_length.unwrap(),
        checksum_algorithm,
        is_full_object_checksum(&final_checksum),
        get_object_output.content_range(),
    )
    .await?;

    Ok(Some(ObjectChecksum {
        key: key.to_string(),
        version_id: get_object_output
            .version_id()
            .map(|version_id| version_id.to_string()),
        checksum_algorithm: additional_checksum_algorithm.clone(),
        checksum_type: get_object_output.checksum_type().cloned(),
        object_parts,
        final_checksum: get_additional_checksum(get_object_output, additional_checksum_algorithm),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_minimum_chunksize_constant() {
        assert_eq!(MINIMUM_CHUNKSIZE, 5 * 1024 * 1024);
    }

    #[test]
    fn test_max_parts_default_constant() {
        assert_eq!(MAX_PARTS_DEFAULT, 1000);
    }

    #[test]
    fn test_validate_content_range_matching() {
        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 0-8388607/30000000")
            .build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_content_range_missing() {
        let get_object_output = GetObjectOutput::builder().build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no content range"));
    }

    #[test]
    fn test_validate_content_range_mismatch() {
        let get_object_output = GetObjectOutput::builder()
            .content_range("bytes 0-4194303/30000000")
            .build();
        let result = validate_content_range(&get_object_output, "bytes=0-8388607");
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("unexpected content range")
        );
    }
}
