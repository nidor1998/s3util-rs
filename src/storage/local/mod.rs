use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::get_object::builders::GetObjectOutputBuilder;
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::builders::HeadObjectOutputBuilder;
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumMode, ObjectPart, RequestPayer, ServerSideEncryption, StorageClass,
    Tagging,
};
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_runtime_api::http::{Response, StatusCode};
use aws_smithy_types::body::SdkBody;
use aws_smithy_types::byte_stream::Length;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use leaky_bucket::RateLimiter;
use std::error::Error;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::BufReader;
use tokio::io::{AsyncBufReadExt, AsyncSeekExt, AsyncWriteExt};
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::ClientConfig;
use crate::storage::additional_checksum_verify::{
    generate_checksum_from_path, generate_checksum_from_path_with_chunksize,
    is_multipart_upload_checksum,
};
use crate::storage::e_tag_verify::{
    generate_e_tag_hash_from_path, generate_e_tag_hash_from_path_with_auto_chunksize,
    is_multipart_upload_e_tag, verify_e_tag,
};
use crate::storage::{
    Storage, StorageFactory, StorageTrait, convert_to_buf_byte_stream_with_callback,
};
use crate::types::SyncStatistics::{ChecksumVerified, ETagVerified, SyncWarning};
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::{
    ObjectChecksum, SseCustomerKey, StoragePath, SyncStatistics, is_full_object_checksum,
};
use crate::{Config, storage};

pub mod fs_util;

const MISMATCH_WARNING_WITH_HELP: &str = "mismatch. object in the local storage may be corrupted. \
 or the current multipart_threshold or multipart_chunksize may be different when uploading to the source. \
 To suppress this warning, please add --disable-multipart-verify command line option. \
 To resolve this issue, please add --auto-chunksize command line option(but extra API overheads).";

pub struct LocalStorageFactory {}

#[async_trait]
impl StorageFactory for LocalStorageFactory {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        _client_config: Option<ClientConfig>,
        _request_payer: Option<RequestPayer>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
        _object_to_list: Option<String>,
    ) -> Storage {
        LocalStorage::create(
            config,
            path,
            cancellation_token,
            stats_sender,
            rate_limit_bandwidth,
            has_warning,
        )
        .await
    }
}

#[derive(Clone)]
struct LocalStorage {
    config: Config,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    has_warning: Arc<AtomicBool>,
}

impl LocalStorage {
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
    ) -> Storage {
        if !matches!(path, StoragePath::Local(_)) {
            panic!("local path not found")
        }

        let storage = LocalStorage {
            config,
            cancellation_token,
            stats_sender,
            rate_limit_bandwidth,
            has_warning,
        };

        Box::new(storage)
    }

    // I can't find a way to simplify this function.
    #[allow(clippy::too_many_arguments)]
    async fn verify_local_file(
        &self,
        key: &str,
        object_checksum: Option<ObjectChecksum>,
        source_sse: &Option<ServerSideEncryption>,
        source_e_tag: &Option<String>,
        source_content_length: u64,
        source_final_checksum: Option<String>,
        source_checksum_algorithm: Option<ChecksumAlgorithm>,
        real_path: &PathBuf,
        target_object_parts: Option<Vec<ObjectPart>>,
        target_content_length: u64,
        source_express_onezone_storage: bool,
    ) -> Result<()> {
        let key = key.to_string();
        if !self.config.disable_etag_verify && !source_express_onezone_storage {
            debug!(
                key = key,
                size = source_content_length,
                "download completed. start to etag verify. depends on the size, this may take a while.",
            );

            let target_sse = None;
            let target_e_tag = if let Some(parts) = target_object_parts.as_ref() {
                // If the source object is a multipart upload, we need to calculate the ETag from the parts.
                if is_multipart_upload_e_tag(source_e_tag) {
                    Some(
                        generate_e_tag_hash_from_path_with_auto_chunksize(
                            real_path,
                            parts.iter().map(|part| part.size().unwrap()).collect(),
                            self.cancellation_token.clone(),
                        )
                        .await?,
                    )
                } else {
                    // If the source object is not a multipart upload, we need to calculate the ETag from the whole file.
                    Some(
                        generate_e_tag_hash_from_path(
                            real_path,
                            source_content_length as usize + 1,
                            source_content_length as usize + 1,
                            self.cancellation_token.clone(),
                        )
                        .await?,
                    )
                }
            } else if self.config.transfer_config.auto_chunksize
                && !is_multipart_upload_e_tag(source_e_tag)
            {
                // --auto-chunksize with a single-part source: match S3's single-part ETag
                // via a whole-file hash. Without this branch, the fallback below would
                // use default multipart settings and synthesize a chunked local ETag,
                // even though the user explicitly asked us to match the source's chunking.
                Some(
                    generate_e_tag_hash_from_path(
                        real_path,
                        source_content_length as usize + 1,
                        source_content_length as usize + 1,
                        self.cancellation_token.clone(),
                    )
                    .await?,
                )
            } else {
                Some(
                    generate_e_tag_hash_from_path(
                        real_path,
                        self.config.transfer_config.multipart_chunksize as usize,
                        self.config.transfer_config.multipart_threshold as usize,
                        self.cancellation_token.clone(),
                    )
                    .await?,
                )
            };

            let verify_result = verify_e_tag(
                !self.config.disable_multipart_verify,
                &self.config.source_sse_c,
                &self.config.target_sse_c,
                source_sse,
                source_e_tag,
                &target_sse,
                &target_e_tag,
            );

            if let Some(e_tag_match) = verify_result {
                if !e_tag_match {
                    if (source_content_length == target_content_length)
                        && (is_multipart_upload_e_tag(source_e_tag)
                            && self.config.disable_multipart_verify)
                    {
                        debug!(
                            key = &key,
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            "skip e_tag verification."
                        );
                    } else {
                        let message = if source_content_length
                            == fs_util::get_file_size(real_path).await?
                            && is_multipart_upload_e_tag(source_e_tag)
                            && object_checksum
                                .clone()
                                .unwrap_or_default()
                                .object_parts
                                .is_none()
                        {
                            format!("{} {}", "e_tag", MISMATCH_WARNING_WITH_HELP)
                        } else {
                            "e_tag mismatch. file in the local storage may be corrupted."
                                .to_string()
                        };

                        let source_e_tag = source_e_tag.clone().unwrap();
                        let target_e_tag = target_e_tag.clone().unwrap();
                        warn!(
                            key = key,
                            source_e_tag = source_e_tag,
                            target_e_tag = target_e_tag,
                            message
                        );

                        self.send_stats(SyncWarning { key: key.clone() }).await;
                        self.set_warning();
                    }
                } else {
                    let source_e_tag = source_e_tag.clone().unwrap();
                    let target_e_tag = target_e_tag.clone().unwrap();

                    debug!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        "e_tag verified."
                    );

                    self.send_stats(ETagVerified { key: key.clone() }).await;
                }
            }
        } else if source_content_length != target_content_length {
            let message = "content length mismatch. file in the local storage may be corrupted.";
            warn!(
                key = &key,
                source_content_length = source_content_length,
                target_content_length = target_content_length,
                message
            );

            self.send_stats(SyncWarning { key: key.clone() }).await;
            self.set_warning();
        }

        // Since aws-sdk-s3 1.69.0, the checksum mode is always enabled,
        // and cannot be disabled(maybe).
        // So, s3sync check the checksum mode is enabled by the user.
        if self.config.additional_checksum_mode.is_none() {
            return Ok(());
        }

        if let Some(source_final_checksum) = source_final_checksum {
            debug!(
                key = &key,
                size = source_content_length,
                "start to additional checksum verify. depends on the size, this may take a while.",
            );

            let parts = if let Some(parts) = target_object_parts.as_ref() {
                parts
                    .iter()
                    .map(|part| part.size().unwrap())
                    .collect::<Vec<i64>>()
            } else {
                vec![source_content_length as i64]
            };

            // If the source object is not a multipart upload, we need to calculate the checksum whole the file.
            let multipart_threshold =
                if !is_multipart_upload_checksum(&Some(source_final_checksum.clone())) {
                    source_content_length as usize + 1
                } else {
                    // If the source object is a multipart upload, and first chunk size is equal to the first part size,
                    // We adjust the multipart threshold to the first part size.
                    if source_content_length == (*parts.first().unwrap() as u64) {
                        source_content_length as usize
                    } else {
                        self.config.transfer_config.multipart_threshold as usize
                    }
                };

            let target_final_checksum = generate_checksum_from_path(
                real_path,
                source_checksum_algorithm.as_ref().unwrap().clone(),
                parts,
                multipart_threshold,
                is_full_object_checksum(&Some(source_final_checksum.clone())),
                self.cancellation_token.clone(),
            )
            .await?;

            let additional_checksum_algorithm =
                source_checksum_algorithm.as_ref().unwrap().as_str();

            if source_final_checksum != target_final_checksum {
                // full_object_checksum covers the whole object; a mismatch cannot be
                // explained by chunksize differences and always indicates corruption.
                if is_full_object_checksum(&Some(source_final_checksum.clone())) {
                    return Err(anyhow!(
                        "additional checksum mismatch. file in the local storage may be corrupted. \
                         key={}, algorithm={}, source_final_checksum={}, target_final_checksum={}",
                        key,
                        additional_checksum_algorithm,
                        source_final_checksum,
                        target_final_checksum
                    ));
                }

                warn!(
                    key = key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum mismatch. file in the local storage may be corrupted."
                );

                self.send_stats(SyncWarning { key }).await;
                self.set_warning();
            } else {
                debug!(
                    key = &key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    source_final_checksum = source_final_checksum,
                    target_final_checksum = target_final_checksum,
                    "additional checksum verified."
                );

                self.send_stats(ChecksumVerified { key: key.clone() }).await;
            }
        }
        Ok(())
    }

    async fn put_object_single_part(
        &self,
        key: &str,
        source: Storage,
        get_object_output: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output.server_side_encryption().cloned();
        let source_e_tag = get_object_output.e_tag().map(|e_tag| e_tag.to_string());
        let source_content_length = get_object_output.content_length().unwrap() as u64;
        let source_final_checksum = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.final_checksum.clone()
        } else {
            None
        };
        let source_checksum_algorithm = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.checksum_algorithm.clone()
        } else {
            None
        };
        let source_storage_class = get_object_output.storage_class().cloned();

        if fs_util::is_key_a_directory(key) {
            fs_util::require_parent_directory(key).await?;

            return Ok(PutObjectOutput::builder().build());
        }

        let mut temp_file = fs_util::create_temp_file_for_key(key).await?;
        let mut file = tokio::fs::File::from_std(temp_file.as_file_mut().try_clone()?);

        let seconds = get_object_output.last_modified().as_ref().unwrap().secs();
        let nanos = get_object_output
            .last_modified()
            .as_ref()
            .unwrap()
            .subsec_nanos();

        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output.body.into_async_read(),
            None,
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );

        let mut buf_reader = BufReader::new(byte_stream.into_async_read());

        let mut chunked_remaining: u64 = 0;
        loop {
            let result = buf_reader.fill_buf().await;
            if let Err(e) = result {
                warn!(key = &key, "Failed to read data from the body: {e:?}");
                return Err(anyhow!(S3syncError::DownloadForceRetryableError));
            }
            let buffer = result?;
            if buffer.is_empty() {
                break;
            }

            let buffer_len = buffer.len();
            file.write_all(buffer).await?;
            let _ = self
                .get_stats_sender()
                .send(SyncStatistics::SyncBytes(buffer_len as u64))
                .await;
            buf_reader.consume(buffer_len);

            // make it easy to cancel
            chunked_remaining += buffer_len as u64;
            if chunked_remaining > self.config.transfer_config.multipart_chunksize {
                chunked_remaining = 0;

                if self.cancellation_token.is_cancelled() {
                    warn!(key = key, "sync cancelled.",);
                    return Err(anyhow!(S3syncError::Cancelled));
                }
            }
        }

        file.flush().await?;
        drop(file);

        let real_path = PathBuf::from(key);
        temp_file.persist(&real_path)?;

        fs_util::set_last_modified_for_path(&real_path, seconds, nanos)?;

        let target_object_parts = if let Some(object_checksum) = &object_checksum {
            object_checksum.object_parts.clone()
        } else {
            None
        };

        let target_content_length = fs_util::get_file_size(&real_path).await?;

        self.verify_local_file(
            key,
            object_checksum,
            &source_sse,
            &source_e_tag,
            source_content_length,
            source_final_checksum,
            source_checksum_algorithm,
            &real_path,
            target_object_parts,
            target_content_length,
            source_storage_class == Some(StorageClass::ExpressOnezone),
        )
        .await?;

        let lossy_path = real_path.to_string_lossy().to_string();
        debug!(
            key = key,
            real_path = lossy_path,
            size = source_content_length,
            "sync completed.",
        );

        Ok(PutObjectOutput::builder().build())
    }

    #[allow(clippy::too_many_arguments)]
    // skipcq: RS-R1000
    async fn put_object_multipart(
        &self,
        key: &str,
        source_key: &str,
        source: Storage,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
    ) -> Result<PutObjectOutput> {
        let source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());
        let source_sse = get_object_output_first_chunk
            .server_side_encryption()
            .cloned();
        let source_e_tag = get_object_output_first_chunk
            .e_tag()
            .map(|e_tag| e_tag.to_string());
        let source_checksum_algorithm = if let Some(object_checksum) = object_checksum.as_ref() {
            object_checksum.checksum_algorithm.clone()
        } else {
            None
        };
        let source_storage_class = get_object_output_first_chunk.storage_class().cloned();
        let source_last_modified_seconds = get_object_output_first_chunk
            .last_modified()
            .as_ref()
            .unwrap()
            .secs();
        let source_last_modified_nanos = get_object_output_first_chunk
            .last_modified()
            .as_ref()
            .unwrap()
            .subsec_nanos();

        if fs_util::is_key_a_directory(key) {
            fs_util::require_parent_directory(key).await?;

            return Ok(PutObjectOutput::builder().build());
        }

        let shared_total_upload_size = Arc::new(Mutex::new(Vec::new()));

        let mut temp_file = fs_util::create_temp_file_for_key(key).await?;
        let mut file = tokio::fs::File::from_std(temp_file.as_file_mut().try_clone()?);

        let config_chunksize = self.config.transfer_config.multipart_chunksize as usize;

        let byte_stream = convert_to_buf_byte_stream_with_callback(
            get_object_output_first_chunk.body.into_async_read(),
            None,
            source.get_rate_limit_bandwidth(),
            None,
            None,
        );

        let first_chunk_content_length =
            get_object_output_first_chunk.content_length.unwrap() as usize;
        let mut chunked_remaining: u64 = 0;
        let mut first_chunk_data = Vec::<u8>::with_capacity(first_chunk_content_length);
        let mut buf_reader = BufReader::new(byte_stream.into_async_read());
        let mut read_data_size = 0;
        loop {
            let result = buf_reader.fill_buf().await;
            if let Err(e) = result {
                warn!(key = &key, "Failed to read data from the body: {e:?}");
                return Err(anyhow!(S3syncError::DownloadForceRetryableError));
            }
            let tmp_buffer = result?;

            if tmp_buffer.is_empty() {
                if read_data_size != first_chunk_content_length {
                    return Err(anyhow!(
                        "Invalid first chunk data size. Expected: {first_chunk_content_length}, Actual: {read_data_size}"
                    ));
                }
                break;
            }
            let buffer_len = tmp_buffer.len();
            first_chunk_data.append(tmp_buffer.to_vec().as_mut());
            buf_reader.consume(buffer_len);

            read_data_size += buffer_len;

            // make it easy to cancel
            chunked_remaining += buffer_len as u64;
            if chunked_remaining > config_chunksize as u64 {
                chunked_remaining = 0;

                if self.cancellation_token.is_cancelled() {
                    warn!(key = key, "sync cancelled.",);
                    return Err(anyhow!(S3syncError::Cancelled));
                }
            }
        }

        let mut offset = 0;
        let mut part_number = 1;
        let mut upload_parts_join_handles = FuturesUnordered::new();
        let target_stats_sender = self.get_stats_sender();
        loop {
            let chunksize = if part_number == 1 {
                first_chunk_content_length
            } else if offset + config_chunksize as u64 > source_size {
                (source_size - offset) as usize
            } else {
                config_chunksize
            };

            let mut cloned_file = tokio::fs::File::from_std(temp_file.reopen()?);

            let cloned_source = dyn_clone::clone_box(&*(source));
            let source_key = source_key.to_string();
            let source_version_id = source_version_id.clone();
            let source_sse_c = self.config.source_sse_c.clone();
            let source_sse_c_key = self.config.source_sse_c_key.clone();
            let source_sse_c_key_md5 = self.config.source_sse_c_key_md5.clone();

            let additional_checksum_mode = self.config.additional_checksum_mode.clone();

            let total_upload_size = Arc::clone(&shared_total_upload_size);

            let cancellation_token = self.cancellation_token.clone();
            let target_stats_sender = target_stats_sender.clone();
            let mut chunk_whole_data = Vec::<u8>::with_capacity(chunksize);
            chunk_whole_data.resize_with(chunksize, Default::default);

            if part_number == 1 {
                chunk_whole_data.copy_from_slice(&first_chunk_data);
            }

            let permit = self
                .config
                .clone()
                .target_client_config
                .unwrap()
                .parallel_upload_semaphore
                .acquire_owned()
                .await?;

            let task: JoinHandle<Result<()>> = task::spawn(async move {
                let _permit = permit; // Keep the semaphore permit in scope

                debug!(
                    key = &source_key,
                    part_number = part_number,
                    offset = offset,
                    chunksize = chunksize,
                    "LocalStorage: write to local file",
                );

                // If the part number is greater than 1, we need to get the object from the source storage.
                if part_number > 1 {
                    let range = Some(format!(
                        "bytes={}-{}",
                        offset,
                        offset + chunksize as u64 - 1
                    ));
                    debug!(
                        key = &source_key,
                        part_number = part_number,
                        "LocalStorage: source get_object() start. range = {range:?}",
                    );

                    let get_object_output = cloned_source
                        .get_object(
                            &source_key,
                            source_version_id.clone(),
                            additional_checksum_mode,
                            range,
                            source_sse_c,
                            source_sse_c_key,
                            source_sse_c_key_md5,
                        )
                        .await;
                    let chunk_content_length =
                        get_object_output.as_ref().unwrap().content_length.unwrap() as usize;
                    let body = convert_to_buf_byte_stream_with_callback(
                        get_object_output
                            .context("get_object() failed.")?
                            .body
                            .into_async_read(),
                        None,
                        cloned_source.get_rate_limit_bandwidth(),
                        None,
                        None,
                    )
                    .into_async_read();

                    let mut chunked_remaining: u64 = 0;
                    let mut chunk_data = Vec::<u8>::with_capacity(chunk_content_length);
                    let mut buf_reader = BufReader::new(body);
                    let mut read_data_size = 0;
                    loop {
                        let result = buf_reader.fill_buf().await;
                        if let Err(e) = result {
                            warn!(
                                key = &source_key,
                                "Failed to read data from the body: {e:?}"
                            );
                            return Err(anyhow!(S3syncError::DownloadForceRetryableError));
                        }
                        let tmp_buffer = result.unwrap();

                        if tmp_buffer.is_empty() {
                            if read_data_size != chunk_content_length {
                                return Err(anyhow!(
                                    "Invalid chunk data size. Expected: {chunk_content_length}, Actual: {read_data_size}"
                                ));
                            }
                            break;
                        }

                        let buffer_len = tmp_buffer.len();
                        chunk_data.append(tmp_buffer.to_vec().as_mut());
                        buf_reader.consume(buffer_len);

                        read_data_size += buffer_len;

                        chunked_remaining += buffer_len as u64;
                        if chunked_remaining > config_chunksize as u64 {
                            chunked_remaining = 0;

                            if cancellation_token.is_cancelled() {
                                warn!(key = &source_key, "sync cancelled.",);
                                return Err(anyhow!(S3syncError::Cancelled));
                            }
                        }
                    }

                    chunk_whole_data.copy_from_slice(&chunk_data);
                } else {
                    debug!(
                        key = &source_key,
                        part_number = part_number,
                        first_chunk_content_length = first_chunk_content_length,
                        "LocalStorage: source get_object() skipped for first part.",
                    );
                }

                let chunk_whole_data_size = chunk_whole_data.len();
                cloned_file.seek(io::SeekFrom::Start(offset)).await?;
                cloned_file.write_all(&chunk_whole_data).await?;
                cloned_file.flush().await?;

                let _ = target_stats_sender
                    .send(SyncStatistics::SyncBytes(chunk_whole_data_size as u64))
                    .await;

                let mut upload_size_vec = total_upload_size.lock().unwrap();
                upload_size_vec.push(chunk_whole_data_size as u64);

                debug!(
                    key = &source_key,
                    part_number = part_number,
                    "LocalStorage: write_all() completed",
                );

                Ok(())
            });

            upload_parts_join_handles.push(task);

            part_number += 1;
            offset += chunksize as u64;

            if offset >= source_size {
                break;
            }
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result??;
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }
        }

        file.flush().await?;
        drop(file);

        let real_path = PathBuf::from(key);
        temp_file.persist(&real_path)?;

        fs_util::set_last_modified_for_path(
            &real_path,
            source_last_modified_seconds,
            source_last_modified_nanos,
        )?;

        let target_object_parts = if let Some(object_checksum) = &object_checksum {
            object_checksum.object_parts.clone()
        } else {
            None
        };

        let total_upload_size: u64 = shared_total_upload_size.lock().unwrap().iter().sum();
        if total_upload_size == source_size {
            debug!(
                key,
                total_upload_size, "multipart upload(local) completed successfully."
            );
        } else {
            return Err(anyhow!(format!(
                "multipart upload(local) size mismatch: key={key}, expected = {0}, actual {total_upload_size}",
                source_size
            )));
        }

        let target_content_length = fs_util::get_file_size(&real_path).await?;

        self.verify_local_file(
            key,
            object_checksum,
            &source_sse,
            &source_e_tag,
            source_size,
            source_additional_checksum,
            source_checksum_algorithm,
            &real_path,
            target_object_parts,
            target_content_length,
            source_storage_class == Some(StorageClass::ExpressOnezone),
        )
        .await?;

        let lossy_path = real_path.to_string_lossy().to_string();
        debug!(
            key = key,
            real_path = lossy_path,
            size = source_size,
            "sync completed.",
        );

        Ok(PutObjectOutput::builder().build())
    }
}

#[async_trait]
impl StorageTrait for LocalStorage {
    fn is_local_storage(&self) -> bool {
        true
    }

    fn is_express_onezone_storage(&self) -> bool {
        false
    }

    // skipcq: RS-R1000
    async fn get_object(
        &self,
        key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput> {
        let path = PathBuf::from(key);

        if !path.clone().try_exists()? {
            let (get_object_error, response) = build_no_such_key_response();

            return Err(anyhow!(SdkError::service_error(get_object_error, response)));
        }

        let content_type = if self.config.no_guess_mime_type {
            None
        } else {
            Some(
                mime_guess::from_path(&path)
                    .first_or_octet_stream()
                    .to_string(),
            )
        };

        let mut need_checksum = true;
        let body;
        let content_length;
        let content_range;
        if range.is_some() {
            let file_range = storage::parse_range_header(&range.unwrap())?;
            body = Some(
                ByteStream::read_from()
                    .path(path.clone())
                    .offset(file_range.offset)
                    .length(Length::Exact(file_range.size))
                    .buffer_size(self.config.transfer_config.multipart_chunksize as usize)
                    .build()
                    .await?,
            );
            // For performance, if the range is specified, we need to calculate the checksum only if the offset is 0.
            need_checksum = file_range.offset == 0;
            content_length = file_range.size as i64;
            content_range = Some(format!(
                "bytes {}-{}/{}",
                file_range.offset,
                file_range.offset + file_range.size - 1,
                fs_util::get_file_size(&path).await?
            ));
        } else {
            body = Some(ByteStream::from_path(path.clone()).await?);
            content_length = fs_util::get_file_size(&path).await? as i64;
            content_range = None;
        }

        if self.config.disable_additional_checksum_verify {
            need_checksum = false;
        }

        let checksum = if self.config.additional_checksum_algorithm.is_some() && need_checksum {
            Some(
                generate_checksum_from_path_with_chunksize(
                    &path,
                    self.config
                        .additional_checksum_algorithm
                        .as_ref()
                        .unwrap()
                        .clone(),
                    self.config.transfer_config.multipart_chunksize as usize,
                    self.config.transfer_config.multipart_threshold as usize,
                    self.config.full_object_checksum,
                    self.cancellation_token.clone(),
                )
                .await?,
            )
        } else {
            None
        };

        let checksum_sha256 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha256
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_sha1 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Sha1
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32 = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc32_c = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc32C
            ) {
            checksum.clone()
        } else {
            None
        };

        let checksum_crc64_nvme = if self.config.additional_checksum_algorithm.is_some()
            && need_checksum
            && matches!(
                self.config.additional_checksum_algorithm.as_ref().unwrap(),
                ChecksumAlgorithm::Crc64Nvme
            ) {
            checksum.clone()
        } else {
            None
        };

        Ok(GetObjectOutputBuilder::default()
            .set_content_length(Some(content_length))
            .set_content_type(content_type)
            .set_content_range(content_range)
            .last_modified(fs_util::get_last_modified(&path).await?)
            .set_body(body)
            .set_checksum_sha256(checksum_sha256)
            .set_checksum_sha1(checksum_sha1)
            .set_checksum_crc32(checksum_crc32)
            .set_checksum_crc32_c(checksum_crc32_c)
            .set_checksum_crc64_nvme(checksum_crc64_nvme)
            .build())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn get_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput> {
        // local storage does not support tagging.
        unimplemented!();
    }

    async fn head_object(
        &self,
        key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        _range: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        let path = PathBuf::from(key);

        let result = path.try_exists();
        if let Err(e) = result {
            self.send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;
            self.set_warning();

            let error = e.to_string();

            let message = "failed to access local file.";

            let source = e.source();
            warn!(error = error, source = source, message);

            return Err(anyhow!("failed to path.try_exists()."));
        }

        if !result? {
            let (head_object_error, response) = build_not_found_response();

            return Err(anyhow!(SdkError::service_error(
                head_object_error,
                response
            )));
        }

        if path.is_dir() {
            return Ok(HeadObjectOutputBuilder::default()
                .set_content_length(Some(0))
                .build());
        }

        Ok(HeadObjectOutputBuilder::default()
            .set_content_length(Some(fs_util::get_file_size(&path).await? as i64))
            .last_modified(fs_util::get_last_modified(&path).await?)
            .build())
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn head_object_first_part(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _checksum_mode: Option<ChecksumMode>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput> {
        unimplemented!();
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn get_object_parts(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        // local storage does not support multipart upload.
        unimplemented!();
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn get_object_parts_attributes(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _max_parts: i32,
        _sse_c: Option<String>,
        _sse_c_key: SseCustomerKey,
        _sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>> {
        // local storage does not support multipart upload.
        unimplemented!();
    }

    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_key: &str,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        _tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        _if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        if get_object_output_first_chunk.content_range.is_none() {
            // with --dry-run, it always goes to a single part upload.
            self.put_object_single_part(
                key,
                source,
                get_object_output_first_chunk,
                _tagging,
                object_checksum,
            )
            .await
        } else {
            // Pass source_key through — the multipart path fetches remaining
            // chunks from the source via ranged GETs, which need the source
            // key (not the local destination path).
            self.put_object_multipart(
                key,
                source_key,
                source,
                source_size,
                source_additional_checksum,
                get_object_output_first_chunk,
                _tagging,
                object_checksum,
            )
            .await
        }
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    async fn put_object_tagging(
        &self,
        _key: &str,
        _version_id: Option<String>,
        _tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput> {
        // local storage does not support tagging.
        unimplemented!();
    }

    async fn delete_object(
        &self,
        key: &str,
        _version_id: Option<String>,
    ) -> Result<DeleteObjectOutput> {
        let file_to_delete = PathBuf::from(key);
        let lossy_path = file_to_delete.to_string_lossy().to_string();

        tokio::fs::remove_file(&file_to_delete).await?;

        info!(key = key, real_path = lossy_path, "delete completed.",);

        Ok(DeleteObjectOutput::builder().build())
    }

    fn get_client(&self) -> Option<Arc<Client>> {
        None
    }

    fn get_stats_sender(&self) -> Sender<SyncStatistics> {
        self.stats_sender.clone()
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }
    fn get_local_path(&self) -> PathBuf {
        PathBuf::new()
    }

    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
        self.rate_limit_bandwidth.clone()
    }

    #[cfg_attr(coverage_nightly, coverage(off))]
    fn generate_copy_source_key(&self, _: &str, _: Option<String>) -> String {
        unimplemented!()
    }

    fn set_warning(&self) {
        self.has_warning.store(true, Ordering::SeqCst);
    }
}

#[cfg(all(test, target_family = "unix"))]
fn remove_local_path_prefix(path: &str, prefix: &str) -> String {
    if path == prefix {
        let path = PathBuf::from(path);
        return path.file_name().unwrap().to_str().unwrap().to_string();
    }

    let mut without_prefix_path = path.replacen(prefix, "", 1);

    if without_prefix_path.starts_with(std::path::MAIN_SEPARATOR) {
        without_prefix_path = without_prefix_path.replacen(std::path::MAIN_SEPARATOR, "", 1);
    }
    without_prefix_path
}

fn build_not_found_response() -> (HeadObjectError, Response<SdkBody>) {
    let head_object_error =
        HeadObjectError::NotFound(aws_sdk_s3::types::error::NotFound::builder().build());
    let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));
    (head_object_error, response)
}

fn build_no_such_key_response() -> (GetObjectError, Response<SdkBody>) {
    let get_object_error =
        GetObjectError::NoSuchKey(aws_sdk_s3::types::error::NoSuchKey::builder().build());
    let response = Response::new(StatusCode::try_from(404).unwrap(), SdkBody::from(r#""#));
    (get_object_error, response)
}

#[allow(dead_code)]
fn convert_windows_directory_char_to_slash(path: &str) -> String {
    path.replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(target_family = "unix")]
    use tracing_subscriber::EnvFilter;

    #[test]
    #[cfg(target_family = "unix")]
    fn remove_path_prefix_test_unix() {
        init_dummy_tracing_subscriber();

        assert_eq!(remove_local_path_prefix("./dir1/data1", "./dir1"), "data1");
        assert_eq!(remove_local_path_prefix("./dir1/data1", "./dir1/"), "data1");
        assert_eq!(remove_local_path_prefix("dir1/data1", "dir1"), "data1");
        assert_eq!(remove_local_path_prefix("dir1/data1", "dir1/"), "data1");
        assert_eq!(remove_local_path_prefix("/dir1/data1", "/dir1"), "data1");
        assert_eq!(remove_local_path_prefix("/dir1/data1", "/dir1/"), "data1");
        assert_eq!(
            remove_local_path_prefix("/dir1/data1", "/dir1/data1"),
            "data1"
        );
        assert_eq!(
            remove_local_path_prefix("/dir1/data1/dir1/", "dir1/"),
            "data1/dir1/"
        );
        assert_eq!(
            remove_local_path_prefix("/dir1/data1/dir1/data1", "dir1/"),
            "data1/dir1/data1"
        );
    }

    #[cfg(target_family = "unix")]
    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }

    #[test]
    fn convert_windows_directory_char_to_slash_replaces_backslashes() {
        assert_eq!(convert_windows_directory_char_to_slash("a\\b\\c"), "a/b/c");
        assert_eq!(convert_windows_directory_char_to_slash("a/b"), "a/b");
        assert_eq!(convert_windows_directory_char_to_slash(""), "");
    }

    #[test]
    fn build_not_found_response_returns_404_not_found() {
        let (err, response) = build_not_found_response();
        assert!(matches!(err, HeadObjectError::NotFound(_)));
        assert_eq!(response.status().as_u16(), 404);
    }

    #[test]
    fn build_no_such_key_response_returns_404_no_such_key() {
        let (err, response) = build_no_such_key_response();
        assert!(matches!(err, GetObjectError::NoSuchKey(_)));
        assert_eq!(response.status().as_u16(), 404);
    }
}
