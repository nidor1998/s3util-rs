use anyhow::{Context, Result, anyhow};
use async_channel::Sender;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::abort_multipart_upload::AbortMultipartUploadOutput;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::primitives::{DateTime, DateTimeFormat};
use aws_sdk_s3::types::{
    ChecksumAlgorithm, ChecksumType, CompletedMultipartUpload, CompletedPart, MetadataDirective,
    ObjectCannedAcl, ObjectPart, RequestPayer, ServerSideEncryption, StorageClass,
    TaggingDirective,
};
use aws_smithy_types_convert::date_time::DateTimeExt;
use base64::{Engine as _, engine::general_purpose};
use chrono::SecondsFormat;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, error, trace, warn};

use crate::config::Config;
use crate::storage;
use crate::storage::e_tag_verify::{generate_e_tag_hash, is_multipart_upload_e_tag};
use crate::storage::{
    Storage, convert_copy_to_put_object_output, convert_copy_to_upload_part_output,
    get_range_from_content_range, parse_range_header_string,
};
use crate::types::SyncStatistics::{ChecksumVerified, ETagVerified, SyncWarning};
use crate::types::error::S3syncError;
use crate::types::token::PipelineCancellationToken;
use crate::types::{S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY, SyncStatistics};

const MISMATCH_WARNING_WITH_HELP: &str = "mismatch. object in the target storage may be corrupted. \
 or the current multipart_threshold or multipart_chunksize may be different when uploading to the source. \
 To suppress this warning, please add --disable-multipart-verify command line option. \
 To resolve this issue, please add --auto-chunksize command line option(but extra API overheads).";

#[derive(Default, Debug, Clone)]
struct UploadMetadata {
    pub acl: Option<ObjectCannedAcl>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_type: Option<String>,
    pub expires: Option<DateTime>,
    pub metadata: Option<HashMap<String, String>>,
    pub request_payer: Option<RequestPayer>,
    pub storage_class: Option<StorageClass>,
    pub website_redirect_location: Option<String>,
    pub tagging: Option<String>,
}

pub struct MutipartEtags {
    pub digest: Vec<u8>,
    pub part_number: i32,
}
pub struct UploadManager {
    client: Arc<Client>,
    config: Config,
    request_payer: Option<RequestPayer>,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
    tagging: Option<String>,
    object_parts: Option<Vec<ObjectPart>>,
    concatnated_md5_hash: Vec<u8>,
    express_onezone_storage: bool,
    source: Storage,
    source_key: String,
    source_total_size: Option<u64>,
    source_additional_checksum: Option<String>,
    if_none_match: Option<String>,
    has_warning: Arc<AtomicBool>,
}

/// Read bytes into `buf` until it is completely full or the reader signals EOF.
/// Returns the number of bytes actually read (0 <= n <= buf.len()).
///
/// `AsyncReadExt::read_exact` can't distinguish "EOF at a clean boundary" from
/// "EOF mid-read"; this helper treats any short read that terminates with
/// `read() -> Ok(0)` as a clean EOF and returns the accumulated count.
async fn read_exact_or_eof<R: tokio::io::AsyncRead + Unpin + ?Sized>(
    reader: &mut R,
    buf: &mut [u8],
) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        let n = reader
            .read(&mut buf[total..])
            .await
            .context("read_exact_or_eof: reader returned an error")?;
        if n == 0 {
            break;
        }
        total += n;
    }
    Ok(total)
}

impl UploadManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client: Arc<Client>,
        config: Config,
        request_payer: Option<RequestPayer>,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        tagging: Option<String>,
        object_parts: Option<Vec<ObjectPart>>,
        express_onezone_storage: bool,
        source: Storage,
        source_key: String,
        source_total_size: Option<u64>,
        source_additional_checksum: Option<String>,
        if_none_match: Option<String>,
        has_warning: Arc<AtomicBool>,
    ) -> Self {
        UploadManager {
            client,
            config,
            request_payer,
            cancellation_token,
            stats_sender,
            tagging,
            object_parts,
            concatnated_md5_hash: vec![],
            express_onezone_storage,
            source,
            source_key,
            source_total_size,
            source_additional_checksum,
            if_none_match,
            has_warning,
        }
    }

    pub async fn upload(
        &mut self,
        bucket: &str,
        key: &str,
        mut get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        get_object_output_first_chunk = self.modify_metadata(get_object_output_first_chunk);

        if self.is_auto_chunksize_enabled() {
            if is_multipart_upload_e_tag(
                &get_object_output_first_chunk
                    .e_tag()
                    .map(|e_tag| e_tag.to_string()),
            ) {
                return self
                    .upload_with_auto_chunksize(bucket, key, get_object_output_first_chunk)
                    .await;
            }

            let first_chunk_size = self
                .object_parts
                .as_ref()
                .unwrap()
                .first()
                .unwrap()
                .size()
                .unwrap();
            let source_total_size = self
                .source_total_size
                .expect("source_total_size is Some in non-streaming upload path");
            if source_total_size != first_chunk_size as u64 {
                return Err(anyhow!(format!(
                    "source_total_size does not match the first object part size: \
                     source_total_size = {}, first object part size = {}",
                    source_total_size, first_chunk_size
                )));
            }

            // If auto-chunksize is enabled but the ETag is not a multipart upload ETag, it should be a single part upload.
            let put_object_output = self
                .singlepart_upload(bucket, key, get_object_output_first_chunk)
                .await?;
            trace!(key = key, "{put_object_output:?}");
            return Ok(put_object_output);
        }

        let put_object_output = if self.config.transfer_config.is_multipart_upload_required(
            self.source_total_size
                .expect("source_total_size is Some in non-streaming upload path"),
        ) {
            self.multipart_upload(bucket, key, get_object_output_first_chunk)
                .await?
        } else {
            self.singlepart_upload(bucket, key, get_object_output_first_chunk)
                .await?
        };

        trace!(key = key, "{put_object_output:?}");
        Ok(put_object_output)
    }

    fn modify_metadata(&self, mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        if self.config.no_sync_system_metadata {
            get_object_output = Self::clear_system_meta_data(get_object_output);
        }

        if self.config.no_sync_user_defined_metadata {
            get_object_output.metadata = None
        }

        if self.config.metadata.is_some() {
            get_object_output.metadata = Some(self.config.metadata.as_ref().unwrap().clone());
        }

        if self.config.put_last_modified_metadata {
            get_object_output = Self::modify_last_modified_metadata(get_object_output);
        }

        get_object_output
    }

    fn modify_last_modified_metadata(mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        let mut metadata = get_object_output.metadata().cloned().unwrap_or_default();
        let last_modified = DateTime::from_millis(
            get_object_output
                .last_modified()
                .unwrap()
                .to_millis()
                .unwrap(),
        )
        .to_chrono_utc()
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, false);

        metadata.insert(
            S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY.to_string(),
            last_modified,
        );
        get_object_output.metadata = Some(metadata);

        get_object_output
    }

    fn clear_system_meta_data(mut get_object_output: GetObjectOutput) -> GetObjectOutput {
        get_object_output.content_disposition = None;
        get_object_output.content_encoding = None;
        get_object_output.content_language = None;
        get_object_output.content_type = None;
        get_object_output.cache_control = None;
        get_object_output.expires_string = None;
        get_object_output.website_redirect_location = None;

        get_object_output
    }

    pub async fn upload_with_auto_chunksize(
        &mut self,
        bucket: &str,
        key: &str,
        get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        if self.object_parts.as_ref().unwrap().is_empty() {
            panic!("Illegal object_parts state");
        }

        let put_object_output = self
            .multipart_upload(bucket, key, get_object_output_first_chunk)
            .await?;

        trace!(key = key, "{put_object_output:?}");

        Ok(put_object_output)
    }

    async fn multipart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let storage_class = if self.config.storage_class.is_none() {
            get_object_output_first_chunk.storage_class().cloned()
        } else {
            self.config.storage_class.clone()
        };

        let checksum_type = if self.config.full_object_checksum {
            Some(ChecksumType::FullObject)
        } else {
            None
        };

        let upload_metadata = UploadMetadata {
            acl: self.config.canned_acl.clone(),
            cache_control: if self.config.cache_control.is_none() {
                get_object_output_first_chunk
                    .cache_control()
                    .map(|value| value.to_string())
            } else {
                self.config.cache_control.clone()
            },
            content_disposition: if self.config.content_disposition.is_none() {
                get_object_output_first_chunk
                    .content_disposition()
                    .map(|value| value.to_string())
            } else {
                self.config.content_disposition.clone()
            },
            content_encoding: if self.config.content_encoding.is_none() {
                get_object_output_first_chunk
                    .content_encoding()
                    .map(|value| value.to_string())
            } else {
                self.config.content_encoding.clone()
            },
            content_language: if self.config.content_language.is_none() {
                get_object_output_first_chunk
                    .content_language()
                    .map(|value| value.to_string())
            } else {
                self.config.content_language.clone()
            },
            content_type: if self.config.content_type.is_none() {
                get_object_output_first_chunk
                    .content_type()
                    .map(|value| value.to_string())
            } else {
                self.config.content_type.clone()
            },
            expires: if self.config.expires.is_none() {
                get_object_output_first_chunk
                    .expires_string()
                    .map(|expires_string| {
                        DateTime::from_str(expires_string, DateTimeFormat::HttpDate).unwrap()
                    })
            } else {
                Some(DateTime::from_str(
                    &self.config.expires.unwrap().to_rfc3339(),
                    DateTimeFormat::DateTimeWithOffset,
                )?)
            },
            metadata: if self.config.metadata.is_none() {
                get_object_output_first_chunk.metadata().cloned()
            } else {
                self.config.metadata.clone()
            },
            request_payer: self.request_payer.clone(),
            storage_class: storage_class.clone(),
            website_redirect_location: if self.config.website_redirect.is_none() {
                get_object_output_first_chunk
                    .website_redirect_location()
                    .map(|value| value.to_string())
            } else {
                self.config.website_redirect.clone()
            },
            tagging: self.tagging.clone(),
        };

        let create_multipart_upload_output = self
            .client
            .create_multipart_upload()
            .set_request_payer(upload_metadata.request_payer)
            .set_storage_class(upload_metadata.storage_class)
            .bucket(bucket)
            .key(key)
            .set_metadata(upload_metadata.metadata)
            .set_tagging(upload_metadata.tagging)
            .set_website_redirect_location(upload_metadata.website_redirect_location)
            .set_content_type(upload_metadata.content_type)
            .set_content_encoding(upload_metadata.content_encoding)
            .set_cache_control(upload_metadata.cache_control)
            .set_content_disposition(upload_metadata.content_disposition)
            .set_content_language(upload_metadata.content_language)
            .set_expires(upload_metadata.expires)
            .set_server_side_encryption(self.config.sse.clone())
            .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_acl(upload_metadata.acl)
            .set_checksum_algorithm(self.config.additional_checksum_algorithm.as_ref().cloned())
            .set_checksum_type(checksum_type)
            .send()
            .await
            .context("aws_sdk_s3::client::Client create_multipart_upload() failed.")?;
        let upload_id = create_multipart_upload_output.upload_id().unwrap();

        let upload_result = self
            .upload_parts_and_complete(bucket, key, upload_id, get_object_output_first_chunk)
            .await
            .context("upload_parts() failed.");
        if upload_result.is_err() {
            self.abort_multipart_upload(bucket, key, upload_id).await?;
            return Err(upload_result.err().unwrap());
        }

        upload_result
    }

    #[rustfmt::skip] // For coverage tool incorrectness
    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<AbortMultipartUploadOutput> {
        self.client.abort_multipart_upload().set_request_payer(self.request_payer.clone()).bucket(bucket).key(key).upload_id(upload_id).send().await.context("aws_sdk_s3::client::Client abort_multipart_upload() failed.")
    }

    async fn upload_parts_and_complete(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output_first_chunk
            .server_side_encryption()
            .cloned();
        let source_remote_storage = get_object_output_first_chunk.e_tag().is_some();
        let source_content_length = self
            .source_total_size
            .expect("source_total_size is Some in non-streaming upload path");
        let source_e_tag = get_object_output_first_chunk
            .e_tag()
            .map(|e_tag| e_tag.to_string());
        let source_local_storage = source_e_tag.is_none();
        let source_checksum = self.source_additional_checksum.clone();
        let source_storage_class = get_object_output_first_chunk.storage_class().cloned();

        let upload_parts = if self.is_auto_chunksize_enabled() {
            self.upload_parts_with_auto_chunksize(
                bucket,
                key,
                upload_id,
                get_object_output_first_chunk,
            )
            .await
            .context("upload_parts_with_auto_chunksize() failed.")?
        } else {
            self.upload_parts(bucket, key, upload_id, get_object_output_first_chunk)
                .await
                .context("upload_parts() failed.")?
        };

        let checksum_type = if self.config.full_object_checksum {
            Some(ChecksumType::FullObject)
        } else {
            None
        };

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(upload_parts))
            .build();

        let complete_multipart_upload_output = self
            .client
            .complete_multipart_upload()
            .set_request_payer(self.request_payer.clone())
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .multipart_upload(completed_multipart_upload)
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_checksum_type(checksum_type)
            .set_if_none_match(self.if_none_match.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client complete_multipart_upload() failed.")?;

        trace!(
            key = key,
            upload_id = upload_id,
            "{complete_multipart_upload_output:?}"
        );

        let source_e_tag = if source_local_storage {
            Some(self.generate_e_tag_hash(self.calculate_parts_count(source_content_length as i64)))
        } else {
            source_e_tag
        };

        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && !self.config.disable_content_md5_header
            && source_storage_class != Some(StorageClass::ExpressOnezone)
        {
            let target_sse = complete_multipart_upload_output
                .server_side_encryption()
                .cloned();
            let target_e_tag = complete_multipart_upload_output
                .e_tag()
                .map(|e| e.to_string());

            self.verify_e_tag(
                key,
                &source_sse,
                source_remote_storage,
                &source_e_tag,
                &target_sse,
                &target_e_tag,
            )
            .await;
        }

        if !self.config.disable_additional_checksum_verify {
            let target_checksum = get_additional_checksum_from_multipart_upload_result(
                &complete_multipart_upload_output,
                self.config.additional_checksum_algorithm.clone(),
            );

            self.validate_checksum(
                key,
                source_checksum,
                target_checksum,
                &source_e_tag,
                source_remote_storage,
            )
            .await;
        }

        Ok(PutObjectOutput::builder()
            .e_tag(complete_multipart_upload_output.e_tag().unwrap())
            .build())
    }

    async fn verify_e_tag(
        &mut self,
        key: &str,
        source_sse: &Option<ServerSideEncryption>,
        source_remote_storage: bool,
        source_e_tag: &Option<String>,
        target_sse: &Option<ServerSideEncryption>,
        target_e_tag: &Option<String>,
    ) {
        let verify_result = storage::e_tag_verify::verify_e_tag(
            !self.config.disable_multipart_verify,
            &self.config.source_sse_c,
            &self.config.target_sse_c,
            source_sse,
            source_e_tag,
            target_sse,
            target_e_tag,
        );

        if let Some(e_tag_match) = verify_result {
            if !e_tag_match {
                if source_remote_storage
                    && is_multipart_upload_e_tag(source_e_tag)
                    && self.config.disable_multipart_verify
                {
                    debug!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        "skip e_tag verification"
                    );
                } else {
                    let message = if source_remote_storage
                        && is_multipart_upload_e_tag(source_e_tag)
                        && !self.is_auto_chunksize_enabled()
                    {
                        format!("{} {}", "e_tag", MISMATCH_WARNING_WITH_HELP)
                    } else {
                        "e_tag mismatch. file in the target storage may be corrupted.".to_string()
                    };

                    self.send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;
                    self.has_warning.store(true, Ordering::SeqCst);

                    let source_e_tag = source_e_tag.clone().unwrap();
                    let target_e_tag = target_e_tag.clone().unwrap();

                    warn!(
                        key = &key,
                        source_e_tag = source_e_tag,
                        target_e_tag = target_e_tag,
                        message
                    );
                }
            } else {
                self.send_stats(ETagVerified {
                    key: key.to_string(),
                })
                .await;

                debug!(
                    key = &key,
                    source_e_tag = source_e_tag,
                    target_e_tag = target_e_tag,
                    "e_tag verified."
                );
            }
        }
    }

    // skipcq: RS-R1000
    async fn upload_parts(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<Vec<CompletedPart>> {
        let shared_source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());
        let shared_multipart_etags = Arc::new(Mutex::new(Vec::new()));
        let shared_upload_parts = Arc::new(Mutex::new(Vec::new()));
        let shared_total_upload_size = Arc::new(Mutex::new(Vec::new()));

        let config_chunksize = self.config.transfer_config.multipart_chunksize as usize;
        let source_total_size = self
            .source_total_size
            .expect("source_total_size is Some in non-streaming upload path")
            as usize;
        let source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());

        // If content_range is None, the full body is available (e.g. stdin or pre-buffered data).
        // In this case, read all parts sequentially from the body stream.
        // If content_range is set, only the first chunk is in the body (first-chunk optimization),
        // and parts 2+ must be fetched from source via get_object() with ranges.
        let full_body_available = get_object_output_first_chunk.content_range.is_none();

        // first_chunk_size represents the size of data in the body for part 1.
        // With first-chunk optimization (content_range set): content_length = first chunk size.
        // With full body (stdin): content_length = total size, but part 1 only reads config_chunksize.
        let first_chunk_size = if full_body_available {
            std::cmp::min(
                get_object_output_first_chunk.content_length().unwrap(),
                config_chunksize as i64,
            )
        } else {
            get_object_output_first_chunk.content_length().unwrap()
        };

        let mut body = get_object_output_first_chunk.body.into_async_read();

        let mut upload_parts_join_handles = FuturesUnordered::new();
        let mut part_number = 1;
        for offset in (0..source_total_size).step_by(config_chunksize) {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }

            let source = dyn_clone::clone_box(&*(self.source));
            let source_key = self.source_key.clone();
            let copy_source = if self.config.server_side_copy {
                self.source
                    .generate_copy_source_key(source_key.as_ref(), source_version_id.clone())
            } else {
                "".to_string()
            };
            let source_version_id = shared_source_version_id.clone();
            let source_sse_c = self.config.source_sse_c.clone();
            let source_sse_c_key = self.config.source_sse_c_key.clone();
            let source_sse_c_key_string = self.config.source_sse_c_key.clone().key.clone();
            let source_sse_c_key_md5 = self.config.source_sse_c_key_md5.clone();

            let target = dyn_clone::clone_box(&*(self.client));
            let target_bucket = bucket.to_string();
            let target_key = key.to_string();
            let target_upload_id = upload_id.to_string();
            let target_sse_c = self.config.target_sse_c.clone();
            let target_sse_c_key = self.config.target_sse_c_key.clone().key.clone();
            let target_sse_c_key_md5 = self.config.target_sse_c_key_md5.clone();

            let chunksize = if offset + config_chunksize > source_total_size {
                source_total_size - offset
            } else {
                config_chunksize
            };

            let upload_parts = Arc::clone(&shared_upload_parts);
            let multipart_etags = Arc::clone(&shared_multipart_etags);
            let total_upload_size = Arc::clone(&shared_total_upload_size);

            let additional_checksum_mode = self.config.additional_checksum_mode.clone();
            let additional_checksum_algorithm = self.config.additional_checksum_algorithm.clone();
            let disable_payload_signing = self.config.disable_payload_signing;
            let multipart_chunksize = self.config.transfer_config.multipart_chunksize;
            let express_onezone_storage = self.express_onezone_storage;
            let disable_content_md5_header = self.config.disable_content_md5_header;
            let request_payer = self.request_payer.clone();
            let server_side_copy = self.config.server_side_copy;

            let stats_sender = self.stats_sender.clone();

            let mut buffer = if !server_side_copy {
                let mut buffer = Vec::<u8>::with_capacity(multipart_chunksize as usize);
                buffer.resize_with(chunksize, Default::default);
                buffer
            } else {
                Vec::new() // For server-side copy, we do not need to read the body.
            };

            // Read data from the body stream before spawning the upload task.
            // - Part 1 always reads from body (it contains the first chunk).
            // - Parts 2+ read from body only when full_body_available is true
            //   (e.g. stdin where source is not re-readable via get_object with ranges).
            if (part_number == 1 || full_body_available) && !server_side_copy {
                let result = body.read_exact(buffer.as_mut_slice()).await;
                if let Err(e) = result {
                    warn!(
                        key = &source_key,
                        part_number = part_number,
                        "Failed to read data from the body: {e:?}"
                    );
                    return Err(anyhow!(S3syncError::DownloadForceRetryableError));
                }
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
                let range = format!("bytes={}-{}", offset, offset + chunksize - 1);

                debug!(
                    key = &target_key,
                    part_number = part_number,
                    "upload_part() start. range = {range:?}",
                );

                let upload_size;
                // If the part number is greater than 1, we need to get the object from the source storage.
                // Skip fetching from source when full_body_available — data was already read above.
                if part_number > 1 && !full_body_available {
                    if !server_side_copy {
                        let get_object_output = source
                            .get_object(
                                &source_key,
                                source_version_id.clone(),
                                additional_checksum_mode,
                                Some(range.clone()),
                                source_sse_c.clone(),
                                source_sse_c_key,
                                source_sse_c_key_md5.clone(),
                            )
                            .await
                            .context("source.get_object() failed.")?;
                        upload_size = get_object_output.content_length().unwrap();

                        if get_object_output.content_range().is_none() {
                            error!("get_object() returned no content range. This is unexpected.");
                            return Err(anyhow!(
                                "get_object() returned no content range. This is unexpected. key={}.",
                                &target_key
                            ));
                        }
                        let (request_start, request_end) = parse_range_header_string(&range)
                            .context("failed to parse request range header")?;
                        let (response_start, response_end) =
                            get_range_from_content_range(&get_object_output)
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

                        // Read body directly — SyncBytes are sent after upload_part completes,
                        // not when reading from source.
                        let mut body = get_object_output.body.into_async_read();

                        let result = body.read_exact(buffer.as_mut_slice()).await;
                        if let Err(e) = result {
                            warn!(
                                key = &source_key,
                                part_number = part_number,
                                "Failed to read data from the body: {e:?}"
                            );
                            return Err(anyhow!(S3syncError::DownloadForceRetryableError));
                        }
                    } else {
                        upload_size = chunksize as i64;
                    }
                } else if part_number == 1 {
                    upload_size = first_chunk_size;
                } else {
                    // full_body_available: data was read from body before spawning
                    upload_size = chunksize as i64;
                }

                let md5_digest;
                let md5_digest_base64 =
                    if !express_onezone_storage && !disable_content_md5_header && !server_side_copy
                    {
                        let md5_digest_raw = md5::compute(&buffer);
                        md5_digest = Some(md5_digest_raw);
                        Some(general_purpose::STANDARD.encode(md5_digest_raw.as_slice()))
                    } else {
                        md5_digest = None;
                        None
                    };

                let upload_part_output;
                if !server_side_copy {
                    let builder = target
                        .upload_part()
                        .set_request_payer(request_payer)
                        .bucket(&target_bucket)
                        .key(&target_key)
                        .upload_id(target_upload_id.clone())
                        .part_number(part_number)
                        .set_content_md5(md5_digest_base64)
                        .content_length(chunksize as i64)
                        .set_checksum_algorithm(additional_checksum_algorithm)
                        .set_sse_customer_algorithm(target_sse_c)
                        .set_sse_customer_key(target_sse_c_key)
                        .set_sse_customer_key_md5(target_sse_c_key_md5)
                        .body(ByteStream::from(buffer));

                    upload_part_output = if disable_payload_signing {
                        builder
                            .customize()
                            .disable_payload_signing()
                            .send()
                            .await
                            .context("aws_sdk_s3::client::Client upload_part() failed.")?
                    } else {
                        builder
                            .send()
                            .await
                            .context("aws_sdk_s3::client::Client upload_part() failed.")?
                    };

                    debug!(
                        key = &target_key,
                        part_number = part_number,
                        "upload_part() complete",
                    );

                    trace!(key = &target_key, "{upload_part_output:?}");

                    let _ = stats_sender.send_blocking(SyncStatistics::SyncBytes(chunksize as u64));

                    #[allow(clippy::unnecessary_unwrap)]
                    if md5_digest.is_some() {
                        let mut locked_multipart_etags = multipart_etags.lock().unwrap();
                        locked_multipart_etags.push(MutipartEtags {
                            digest: md5_digest.as_ref().unwrap().as_slice().to_vec(),
                            part_number,
                        });
                    }
                } else {
                    let upload_part_copy_output = target
                        .upload_part_copy()
                        .copy_source(copy_source)
                        .set_request_payer(request_payer)
                        .set_copy_source_range(Some(range))
                        .bucket(&target_bucket)
                        .key(&target_key)
                        .upload_id(target_upload_id.clone())
                        .part_number(part_number)
                        .set_copy_source_sse_customer_algorithm(source_sse_c)
                        .set_copy_source_sse_customer_key(source_sse_c_key_string)
                        .set_copy_source_sse_customer_key_md5(source_sse_c_key_md5)
                        .set_sse_customer_algorithm(target_sse_c)
                        .set_sse_customer_key(target_sse_c_key)
                        .set_sse_customer_key_md5(target_sse_c_key_md5)
                        .send()
                        .await?;

                    debug!(
                        key = &target_key,
                        part_number = part_number,
                        "upload_part_copy() complete",
                    );

                    trace!(key = &target_key, "{upload_part_copy_output:?}");

                    let _ =
                        stats_sender.send_blocking(SyncStatistics::SyncBytes(upload_size as u64));

                    upload_part_output =
                        convert_copy_to_upload_part_output(upload_part_copy_output);
                }

                let mut upload_size_vec = total_upload_size.lock().unwrap();
                upload_size_vec.push(upload_size);

                let mut locked_upload_parts = upload_parts.lock().unwrap();
                locked_upload_parts.push(
                    CompletedPart::builder()
                        .e_tag(upload_part_output.e_tag().unwrap())
                        .set_checksum_sha256(
                            upload_part_output
                                .checksum_sha256()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_sha1(
                            upload_part_output
                                .checksum_sha1()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc32(
                            upload_part_output
                                .checksum_crc32()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc32_c(
                            upload_part_output
                                .checksum_crc32_c()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc64_nvme(
                            upload_part_output
                                .checksum_crc64_nvme()
                                .map(|digest| digest.to_string()),
                        )
                        .part_number(part_number)
                        .build(),
                );

                trace!(
                    key = &target_key,
                    upload_id = &target_upload_id,
                    "{locked_upload_parts:?}"
                );

                Ok(())
            });

            upload_parts_join_handles.push(task);

            part_number += 1;
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result??;
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }
        }

        let total_upload_size: i64 = shared_total_upload_size.lock().unwrap().iter().sum();
        let source_total_size = self
            .source_total_size
            .expect("source_total_size is Some in non-streaming upload path");
        if total_upload_size == source_total_size as i64 {
            debug!(
                key,
                total_upload_size, "multipart upload completed successfully."
            );
        } else {
            return Err(anyhow!(format!(
                "multipart upload size mismatch: key={key}, expected = {0}, actual {total_upload_size}",
                source_total_size
            )));
        }

        // Etags are concatenated in the order of part number. Otherwise, ETag verification will fail.
        let mut locked_multipart_etags = shared_multipart_etags.lock().unwrap();
        locked_multipart_etags.sort_by_key(|e| e.part_number);
        for etag in locked_multipart_etags.iter() {
            self.concatnated_md5_hash.append(&mut etag.digest.clone());
        }

        // CompletedParts must be sorted by part number. Otherwise, CompleteMultipartUpload will fail.
        let mut parts = shared_upload_parts.lock().unwrap().clone();
        parts.sort_by_key(|part| part.part_number.unwrap());
        Ok(parts)
    }

    pub async fn upload_stream(
        &mut self,
        bucket: &str,
        key: &str,
        reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> Result<PutObjectOutput> {
        let checksum_type = if self.config.full_object_checksum {
            Some(ChecksumType::FullObject)
        } else {
            None
        };

        // Build minimal metadata from config. For stdin-sourced streams, there is
        // no source GetObjectOutput to pull metadata from; we rely purely on what
        // the user configured on the CLI.
        let create_output = self
            .client
            .create_multipart_upload()
            .set_request_payer(self.request_payer.clone())
            .set_storage_class(self.config.storage_class.clone())
            .bucket(bucket)
            .key(key)
            .set_metadata(self.config.metadata.clone())
            .set_tagging(self.tagging.clone())
            .set_content_type(self.config.content_type.clone())
            .set_server_side_encryption(self.config.sse.clone())
            .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_acl(self.config.canned_acl.clone())
            .set_checksum_algorithm(self.config.additional_checksum_algorithm.clone())
            .set_checksum_type(checksum_type.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client create_multipart_upload() failed.")?;

        let upload_id = create_output.upload_id().unwrap().to_string();

        let result = self
            .upload_parts_stream(bucket, key, &upload_id, reader)
            .await;

        let (parts, total_size, source_additional_checksum) = match result {
            Ok(v) => v,
            Err(e) => {
                let _ = self.abort_multipart_upload(bucket, key, &upload_id).await;
                return Err(e);
            }
        };

        if parts.is_empty() {
            let _ = self.abort_multipart_upload(bucket, key, &upload_id).await;
            return Err(anyhow!(
                "upload_stream: no parts uploaded (empty stream reached streaming path)"
            ));
        }

        let completed_multipart_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();

        let complete_output = match self
            .client
            .complete_multipart_upload()
            .set_request_payer(self.request_payer.clone())
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_multipart_upload)
            .set_sse_customer_algorithm(self.config.target_sse_c.clone())
            .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
            .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
            .set_checksum_type(checksum_type)
            .set_if_none_match(self.if_none_match.clone())
            .send()
            .await
            .context("aws_sdk_s3::client::Client complete_multipart_upload() failed.")
        {
            Ok(output) => output,
            Err(e) => {
                let _ = self.abort_multipart_upload(bucket, key, &upload_id).await;
                return Err(e);
            }
        };

        trace!(key = key, upload_id = upload_id, "{complete_output:?}");

        // ETag verification — for stdin there's no source ETag to compare against,
        // but we compute the synthetic source ETag from the per-part MD5s (same
        // pattern as the local-source branch in upload_parts_and_complete).
        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && !self.config.disable_content_md5_header
        {
            let parts_count = self.calculate_parts_count(total_size as i64);
            let source_e_tag = Some(self.generate_e_tag_hash(parts_count));
            let target_sse = complete_output.server_side_encryption().cloned();
            let target_e_tag = complete_output.e_tag().map(|e| e.to_string());

            self.verify_e_tag(key, &None, false, &source_e_tag, &target_sse, &target_e_tag)
                .await;
        }

        if !self.config.disable_additional_checksum_verify {
            let target_checksum = get_additional_checksum_from_multipart_upload_result(
                &complete_output,
                self.config.additional_checksum_algorithm.clone(),
            );

            self.validate_checksum(
                key,
                source_additional_checksum,
                target_checksum,
                &None,
                false,
            )
            .await;
        }

        Ok(PutObjectOutput::builder()
            .e_tag(complete_output.e_tag().unwrap())
            .build())
    }

    // skipcq: RS-R1000
    async fn upload_parts_stream(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        mut reader: Box<dyn tokio::io::AsyncRead + Send + Unpin>,
    ) -> Result<(Vec<CompletedPart>, u64, Option<String>)> {
        use crate::storage::checksum::AdditionalChecksum;

        let shared_multipart_etags = Arc::new(Mutex::new(Vec::<MutipartEtags>::new()));
        let shared_upload_parts = Arc::new(Mutex::new(Vec::<CompletedPart>::new()));

        let config_chunksize = self.config.transfer_config.multipart_chunksize as usize;
        let mut total_uploaded_size: u64 = 0;
        let mut part_number: i32 = 1;

        let mut additional_checksum = self
            .config
            .additional_checksum_algorithm
            .clone()
            .map(|algo| AdditionalChecksum::new(algo, self.config.full_object_checksum));

        let mut upload_parts_join_handles = FuturesUnordered::new();

        loop {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }

            // Read up to config_chunksize bytes for this part.
            let mut buffer = vec![0u8; config_chunksize];
            let n = read_exact_or_eof(&mut reader, &mut buffer).await?;
            if n == 0 {
                // Clean EOF on a chunk boundary.
                break;
            }
            buffer.truncate(n);
            let is_last_chunk = n < config_chunksize;

            // Incremental additional checksum — synchronous, in part order.
            if let Some(c) = additional_checksum.as_mut() {
                c.update(&buffer);
                let _ = c.finalize();
            }

            total_uploaded_size += n as u64;

            // Capture per-task locals.
            let upload_parts = Arc::clone(&shared_upload_parts);
            let multipart_etags = Arc::clone(&shared_multipart_etags);
            let client = Arc::clone(&self.client);
            let target_bucket = bucket.to_string();
            let target_key = key.to_string();
            let target_upload_id = upload_id.to_string();
            let target_sse_c = self.config.target_sse_c.clone();
            let target_sse_c_key = self.config.target_sse_c_key.clone().key.clone();
            let target_sse_c_key_md5 = self.config.target_sse_c_key_md5.clone();
            let additional_checksum_algorithm = self.config.additional_checksum_algorithm.clone();
            let disable_payload_signing = self.config.disable_payload_signing;
            let disable_content_md5_header = self.config.disable_content_md5_header;
            let express_onezone_storage = self.express_onezone_storage;
            let request_payer = self.request_payer.clone();
            let stats_sender = self.stats_sender.clone();
            let current_part_number = part_number;
            let part_size = n as i64;

            debug!(
                key = key,
                part_number = current_part_number,
                size = part_size,
                is_last = is_last_chunk,
                "upload_part() start (streaming)"
            );

            let permit = self
                .config
                .clone()
                .target_client_config
                .unwrap()
                .parallel_upload_semaphore
                .acquire_owned()
                .await?;

            let task: JoinHandle<Result<()>> = task::spawn(async move {
                let _permit = permit;

                let md5_digest;
                let md5_digest_base64 = if !express_onezone_storage && !disable_content_md5_header {
                    let md5_digest_raw = md5::compute(&buffer);
                    md5_digest = Some(md5_digest_raw);
                    Some(general_purpose::STANDARD.encode(md5_digest_raw.as_slice()))
                } else {
                    md5_digest = None;
                    None
                };

                // Compute per-part additional checksum for the upload_part call itself.
                let part_additional_checksum = additional_checksum_algorithm.as_ref().map(|algo| {
                    let mut c = AdditionalChecksum::new(algo.clone(), false);
                    c.update(&buffer);
                    c.finalize()
                });

                let body = ByteStream::from(buffer);

                let mut upload_part_builder = client
                    .upload_part()
                    .set_request_payer(request_payer.clone())
                    .bucket(&target_bucket)
                    .key(&target_key)
                    .upload_id(&target_upload_id)
                    .part_number(current_part_number)
                    .content_length(part_size)
                    .body(body)
                    .set_content_md5(md5_digest_base64)
                    .set_sse_customer_algorithm(target_sse_c.clone())
                    .set_sse_customer_key(target_sse_c_key.clone())
                    .set_sse_customer_key_md5(target_sse_c_key_md5.clone());

                if let Some(algo) = additional_checksum_algorithm.as_ref() {
                    upload_part_builder =
                        upload_part_builder.set_checksum_algorithm(Some(algo.clone()));
                    match algo {
                        ChecksumAlgorithm::Sha256 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_sha256(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Sha1 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_sha1(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc32 => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc32(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc32C => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc32_c(part_additional_checksum.clone());
                        }
                        ChecksumAlgorithm::Crc64Nvme => {
                            upload_part_builder = upload_part_builder
                                .set_checksum_crc64_nvme(part_additional_checksum.clone());
                        }
                        _ => {}
                    }
                }

                let upload_part_output = if disable_payload_signing {
                    upload_part_builder
                        .customize()
                        .disable_payload_signing()
                        .send()
                        .await
                        .context("aws_sdk_s3::client::Client upload_part() failed.")?
                } else {
                    upload_part_builder
                        .send()
                        .await
                        .context("aws_sdk_s3::client::Client upload_part() failed.")?
                };

                debug!(
                    key = &target_key,
                    part_number = current_part_number,
                    "upload_part() complete (streaming)"
                );
                trace!(key = &target_key, "{upload_part_output:?}");

                if md5_digest.is_some() {
                    let mut locked_etags = multipart_etags.lock().unwrap();
                    locked_etags.push(MutipartEtags {
                        digest: md5_digest.as_ref().unwrap().as_slice().to_vec(),
                        part_number: current_part_number,
                    });
                }

                let mut locked_upload_parts = upload_parts.lock().unwrap();
                locked_upload_parts.push(
                    CompletedPart::builder()
                        .e_tag(upload_part_output.e_tag().unwrap())
                        .set_checksum_sha256(
                            upload_part_output.checksum_sha256().map(|s| s.to_string()),
                        )
                        .set_checksum_sha1(
                            upload_part_output.checksum_sha1().map(|s| s.to_string()),
                        )
                        .set_checksum_crc32(
                            upload_part_output.checksum_crc32().map(|s| s.to_string()),
                        )
                        .set_checksum_crc32_c(
                            upload_part_output.checksum_crc32_c().map(|s| s.to_string()),
                        )
                        .set_checksum_crc64_nvme(
                            upload_part_output
                                .checksum_crc64_nvme()
                                .map(|s| s.to_string()),
                        )
                        .part_number(current_part_number)
                        .build(),
                );
                drop(locked_upload_parts);

                let _ = stats_sender.send_blocking(SyncStatistics::SyncBytes(part_size as u64));

                Ok(())
            });

            upload_parts_join_handles.push(task);
            part_number += 1;

            if is_last_chunk {
                break;
            }
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result
                .context("upload_parts_stream join failed")?
                .context("upload_parts_stream task failed")?;
        }

        // Sort parts by part_number since tasks finished in arbitrary order.
        let mut parts = shared_upload_parts.lock().unwrap().clone();
        parts.sort_by_key(|p| p.part_number.unwrap_or(0));

        // Collect per-part MD5s (in part order) for composite ETag computation later.
        {
            let mut etags = shared_multipart_etags.lock().unwrap();
            etags.sort_by_key(|e| e.part_number);
            for etag in etags.iter() {
                self.concatnated_md5_hash.extend_from_slice(&etag.digest);
            }
        }

        let source_additional_checksum_string = additional_checksum.map(|mut c| c.finalize_all());

        Ok((
            parts,
            total_uploaded_size,
            source_additional_checksum_string,
        ))
    }

    // skipcq: RS-R1000
    async fn upload_parts_with_auto_chunksize(
        &mut self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        get_object_output_first_chunk: GetObjectOutput,
    ) -> Result<Vec<CompletedPart>> {
        let shared_source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());
        let shared_multipart_etags = Arc::new(Mutex::new(Vec::new()));
        let shared_upload_parts = Arc::new(Mutex::new(Vec::new()));
        let shared_total_upload_size = Arc::new(Mutex::new(Vec::new()));

        let source_version_id = get_object_output_first_chunk
            .version_id()
            .map(|v| v.to_string());

        let first_chunk_size = get_object_output_first_chunk.content_length().unwrap();
        let mut body = get_object_output_first_chunk.body.into_async_read();

        let mut upload_parts_join_handles = FuturesUnordered::new();
        let mut part_number = 1;
        let mut offset = 0;

        while part_number <= self.object_parts.as_ref().unwrap().len() as i32 {
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }

            let source = dyn_clone::clone_box(&*(self.source));
            let source_key = self.source_key.clone();
            let copy_source = if self.config.server_side_copy {
                self.source
                    .generate_copy_source_key(source_key.as_ref(), source_version_id.clone())
            } else {
                "".to_string()
            };
            let source_version_id = shared_source_version_id.clone();
            let source_sse_c = self.config.source_sse_c.clone();
            let source_sse_c_key = self.config.source_sse_c_key.clone();
            let source_sse_c_key_string = self.config.source_sse_c_key.clone().key.clone();
            let source_sse_c_key_md5 = self.config.source_sse_c_key_md5.clone();

            let target = dyn_clone::clone_box(&*(self.client));
            let target_bucket = bucket.to_string();
            let target_key = key.to_string();
            let target_upload_id = upload_id.to_string();
            let target_sse_c = self.config.target_sse_c.clone();
            let target_sse_c_key = self.config.target_sse_c_key.clone().key.clone();
            let target_sse_c_key_md5 = self.config.target_sse_c_key_md5.clone();

            let object_part_chunksize = self
                .object_parts
                .as_ref()
                .unwrap()
                .get(part_number as usize - 1)
                .unwrap()
                .size()
                .unwrap();

            let upload_parts = Arc::clone(&shared_upload_parts);
            let multipart_etags = Arc::clone(&shared_multipart_etags);
            let total_upload_size = Arc::clone(&shared_total_upload_size);

            let additional_checksum_mode = self.config.additional_checksum_mode.clone();
            let additional_checksum_algorithm = self.config.additional_checksum_algorithm.clone();
            let disable_payload_signing = self.config.disable_payload_signing;
            let express_onezone_storage = self.express_onezone_storage;
            let disable_content_md5_header = self.config.disable_content_md5_header;
            let request_payer = self.request_payer.clone();
            let server_side_copy = self.config.server_side_copy;

            let stats_sender = self.stats_sender.clone();

            let mut buffer = if !server_side_copy {
                let mut buffer = Vec::<u8>::with_capacity(object_part_chunksize as usize);
                buffer.resize_with(object_part_chunksize as usize, Default::default);
                buffer
            } else {
                Vec::new() // For server-side copy, we do not need to read the body.
            };

            // For the first part, we read the data from the supplied body.
            if part_number == 1 && !server_side_copy {
                let result = body.read_exact(buffer.as_mut_slice()).await;
                if let Err(e) = result {
                    warn!(
                        key = &source_key,
                        part_number = part_number,
                        "Failed to read data from the body: {e:?}"
                    );
                    return Err(anyhow!(S3syncError::DownloadForceRetryableError));
                }
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
                let range = format!("bytes={}-{}", offset, offset + object_part_chunksize - 1);

                debug!(
                    key = &target_key,
                    part_number = part_number,
                    "upload_part() start. range = {range:?}",
                );

                let upload_size;
                // If the part number is greater than 1, we need to get the object from the source storage.
                if part_number > 1 {
                    if !server_side_copy {
                        let get_object_output = source
                            .get_object(
                                &source_key,
                                source_version_id.clone(),
                                additional_checksum_mode,
                                Some(range.clone()),
                                source_sse_c.clone(),
                                source_sse_c_key.clone(),
                                source_sse_c_key_md5.clone(),
                            )
                            .await
                            .context("source.get_object() failed.")?;
                        upload_size = get_object_output.content_length().unwrap();

                        if get_object_output.content_range().is_none() {
                            error!(
                                "get_object() - auto-chunksize returned no content range. This is unexpected."
                            );
                            return Err(anyhow!(
                                "get_object() returned no content range. This is unexpected. key={}.",
                                &target_key
                            ));
                        }
                        let (request_start, request_end) = parse_range_header_string(&range)
                            .context("failed to parse request range header")?;
                        let (response_start, response_end) =
                            get_range_from_content_range(&get_object_output)
                                .context("get_object() returned no content range")?;
                        if (request_start != response_start) || (request_end != response_end) {
                            return Err(anyhow!(
                                "get_object() - auto-chunksize returned unexpected content range. \
                                expected: {}-{}, actual: {}-{}",
                                request_start,
                                request_end,
                                response_start,
                                response_end,
                            ));
                        }

                        // Read body directly — SyncBytes are sent after upload_part completes,
                        // not when reading from source.
                        let mut body = get_object_output.body.into_async_read();

                        let result = body.read_exact(buffer.as_mut_slice()).await;
                        if let Err(e) = result {
                            warn!(
                                key = &source_key,
                                part_number = part_number,
                                "Failed to read data from the body: {e:?}"
                            );
                            return Err(anyhow!(S3syncError::DownloadForceRetryableError));
                        }
                    } else {
                        upload_size = object_part_chunksize;
                    }
                } else {
                    upload_size = first_chunk_size;
                }

                let md5_digest;
                let md5_digest_base64 =
                    if !express_onezone_storage && !disable_content_md5_header && !server_side_copy
                    {
                        let md5_digest_raw = md5::compute(&buffer);
                        md5_digest = Some(md5_digest_raw);
                        Some(general_purpose::STANDARD.encode(md5_digest_raw.as_slice()))
                    } else {
                        md5_digest = None;
                        None
                    };

                let upload_part_output;
                if !server_side_copy {
                    let builder = target
                        .upload_part()
                        .set_request_payer(request_payer)
                        .bucket(&target_bucket)
                        .key(&target_key)
                        .upload_id(target_upload_id.clone())
                        .part_number(part_number)
                        .set_content_md5(md5_digest_base64)
                        .content_length(object_part_chunksize)
                        .set_checksum_algorithm(additional_checksum_algorithm)
                        .set_sse_customer_algorithm(target_sse_c)
                        .set_sse_customer_key(target_sse_c_key)
                        .set_sse_customer_key_md5(target_sse_c_key_md5)
                        .body(ByteStream::from(buffer));

                    upload_part_output = if disable_payload_signing {
                        builder
                            .customize()
                            .disable_payload_signing()
                            .send()
                            .await
                            .context("aws_sdk_s3::client::Client upload_part() failed.")?
                    } else {
                        builder
                            .send()
                            .await
                            .context("aws_sdk_s3::client::Client upload_part() failed.")?
                    };

                    debug!(
                        key = &target_key,
                        part_number = part_number,
                        "upload_part() complete",
                    );

                    trace!(key = &target_key, "{upload_part_output:?}");

                    let _ = stats_sender
                        .send_blocking(SyncStatistics::SyncBytes(object_part_chunksize as u64));

                    if md5_digest.is_some() {
                        let mut locked_multipart_etags = multipart_etags.lock().unwrap();
                        #[allow(clippy::unnecessary_unwrap)]
                        locked_multipart_etags.push(MutipartEtags {
                            digest: md5_digest.as_ref().unwrap().as_slice().to_vec(),
                            part_number,
                        });
                    }
                } else {
                    let upload_part_copy_output = target
                        .upload_part_copy()
                        .copy_source(copy_source)
                        .set_request_payer(request_payer)
                        .set_copy_source_range(Some(range))
                        .bucket(&target_bucket)
                        .key(&target_key)
                        .upload_id(target_upload_id.clone())
                        .part_number(part_number)
                        .set_copy_source_sse_customer_algorithm(source_sse_c)
                        .set_copy_source_sse_customer_key(source_sse_c_key_string)
                        .set_copy_source_sse_customer_key_md5(source_sse_c_key_md5)
                        .set_sse_customer_algorithm(target_sse_c)
                        .set_sse_customer_key(target_sse_c_key)
                        .set_sse_customer_key_md5(target_sse_c_key_md5)
                        .send()
                        .await?;

                    debug!(
                        key = &target_key,
                        part_number = part_number,
                        "upload_part_copy() complete",
                    );

                    trace!(key = &target_key, "{upload_part_copy_output:?}");

                    let _ =
                        stats_sender.send_blocking(SyncStatistics::SyncBytes(upload_size as u64));

                    upload_part_output =
                        convert_copy_to_upload_part_output(upload_part_copy_output);
                }

                let mut locked_upload_parts = upload_parts.lock().unwrap();
                locked_upload_parts.push(
                    CompletedPart::builder()
                        .e_tag(upload_part_output.e_tag().unwrap())
                        .set_checksum_sha256(
                            upload_part_output
                                .checksum_sha256()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_sha1(
                            upload_part_output
                                .checksum_sha1()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc32(
                            upload_part_output
                                .checksum_crc32()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc32_c(
                            upload_part_output
                                .checksum_crc32_c()
                                .map(|digest| digest.to_string()),
                        )
                        .set_checksum_crc64_nvme(
                            upload_part_output
                                .checksum_crc64_nvme()
                                .map(|digest| digest.to_string()),
                        )
                        .part_number(part_number)
                        .build(),
                );

                let mut upload_size_vec = total_upload_size.lock().unwrap();
                upload_size_vec.push(upload_size);

                trace!(
                    key = &target_key,
                    upload_id = &target_upload_id,
                    "{locked_upload_parts:?}"
                );

                Ok(())
            });

            upload_parts_join_handles.push(task);

            offset += object_part_chunksize;
            part_number += 1;
        }

        while let Some(result) = upload_parts_join_handles.next().await {
            result??;
            if self.cancellation_token.is_cancelled() {
                return Err(anyhow!(S3syncError::Cancelled));
            }
        }

        let total_upload_size: i64 = shared_total_upload_size.lock().unwrap().iter().sum();
        let source_total_size = self
            .source_total_size
            .expect("source_total_size is Some in non-streaming upload path");
        if total_upload_size == source_total_size as i64 {
            debug!(
                key,
                total_upload_size, "multipart upload(auto-chunksize) completed successfully."
            );
        } else {
            return Err(anyhow!(format!(
                "multipart upload(auto-chunksize) size mismatch: key={key}, expected = {0}, actual {total_upload_size}",
                source_total_size
            )));
        }

        // Etags are concatenated in the order of part number. Otherwise, ETag verification will fail.
        let mut locked_multipart_etags = shared_multipart_etags.lock().unwrap();
        locked_multipart_etags.sort_by_key(|e| e.part_number);
        for etag in locked_multipart_etags.iter() {
            self.concatnated_md5_hash.append(&mut etag.digest.clone());
        }

        // CompletedParts must be sorted by part number. Otherwise, CompleteMultipartUpload will fail.
        let mut parts = shared_upload_parts.lock().unwrap().clone();
        parts.sort_by_key(|part| part.part_number.unwrap());
        Ok(parts)
    }

    // skipcq: RS-R1000
    async fn singlepart_upload(
        &mut self,
        bucket: &str,
        key: &str,
        mut get_object_output: GetObjectOutput,
    ) -> Result<PutObjectOutput> {
        let source_sse = get_object_output.server_side_encryption().cloned();
        let source_remote_storage = get_object_output.e_tag().is_some();
        let source_e_tag = get_object_output.e_tag().map(|e_tag| e_tag.to_string());
        let source_local_storage = source_e_tag.is_none();
        let source_checksum = self.source_additional_checksum.clone();
        let source_storage_class = get_object_output.storage_class().cloned();
        let source_version_id = get_object_output.version_id().map(|v| v.to_string());

        let buffer = if !self.config.server_side_copy {
            let mut body = get_object_output.body.into_async_read();
            get_object_output.body = ByteStream::from_static(b"");

            let source_total_size = self
                .source_total_size
                .expect("source_total_size is Some in non-streaming upload path")
                as usize;
            let mut buffer = Vec::<u8>::with_capacity(source_total_size);
            buffer.resize_with(source_total_size, Default::default);

            let result = body.read_exact(buffer.as_mut_slice()).await;
            if let Err(e) = result {
                warn!(key = &key, "Failed to read data from the body: {e:?}");
                return Err(anyhow!(S3syncError::DownloadForceRetryableError));
            }

            buffer
        } else {
            Vec::new() // For server-side copy, we do not need to read the body.
        };

        let md5_digest_base64 = if !self.express_onezone_storage
            && !self.config.disable_content_md5_header
            && !self.config.server_side_copy
        {
            let md5_digest = md5::compute(&buffer);

            self.concatnated_md5_hash
                .append(&mut md5_digest.as_slice().to_vec());

            Some(general_purpose::STANDARD.encode(md5_digest.as_slice()))
        } else {
            None
        };

        let buffer_stream = ByteStream::from(buffer);

        let storage_class = if self.config.storage_class.is_none() {
            get_object_output.storage_class().cloned()
        } else {
            self.config.storage_class.clone()
        };

        let upload_metadata = UploadMetadata {
            acl: self.config.canned_acl.clone(),
            cache_control: if self.config.cache_control.is_none() {
                get_object_output
                    .cache_control()
                    .map(|value| value.to_string())
            } else {
                self.config.cache_control.clone()
            },
            content_disposition: if self.config.content_disposition.is_none() {
                get_object_output
                    .content_disposition()
                    .map(|value| value.to_string())
            } else {
                self.config.content_disposition.clone()
            },
            content_encoding: if self.config.content_encoding.is_none() {
                get_object_output
                    .content_encoding()
                    .map(|value| value.to_string())
            } else {
                self.config.content_encoding.clone()
            },
            content_language: if self.config.content_language.is_none() {
                get_object_output
                    .content_language()
                    .map(|value| value.to_string())
            } else {
                self.config.content_language.clone()
            },
            content_type: if self.config.content_type.is_none() {
                get_object_output
                    .content_type()
                    .map(|value| value.to_string())
            } else {
                self.config.content_type.clone()
            },
            expires: if self.config.expires.is_none() {
                get_object_output.expires_string().map(|expires_string| {
                    DateTime::from_str(expires_string, DateTimeFormat::HttpDate).unwrap()
                })
            } else {
                Some(DateTime::from_str(
                    &self.config.expires.unwrap().to_rfc3339(),
                    DateTimeFormat::DateTimeWithOffset,
                )?)
            },
            metadata: if self.config.metadata.is_none() {
                get_object_output.metadata().cloned()
            } else {
                self.config.metadata.clone()
            },
            request_payer: self.request_payer.clone(),
            storage_class: storage_class.clone(),
            website_redirect_location: if self.config.website_redirect.is_none() {
                get_object_output
                    .website_redirect_location()
                    .map(|value| value.to_string())
            } else {
                self.config.website_redirect.clone()
            },
            tagging: self.tagging.clone(),
        };

        let source_total_size = self
            .source_total_size
            .expect("source_total_size is Some in non-streaming upload path");
        let put_object_output = if self.config.server_side_copy {
            let copy_source = self
                .source
                .generate_copy_source_key(self.source_key.as_ref(), source_version_id.clone());
            let copy_object_output = self
                .client
                .copy_object()
                .copy_source(copy_source)
                .set_request_payer(upload_metadata.request_payer)
                .set_storage_class(upload_metadata.storage_class)
                .bucket(bucket)
                .key(key)
                .metadata_directive(MetadataDirective::Replace)
                .tagging_directive(TaggingDirective::Replace)
                .set_metadata(upload_metadata.metadata)
                .set_tagging(upload_metadata.tagging)
                .set_website_redirect_location(upload_metadata.website_redirect_location)
                .set_content_type(upload_metadata.content_type)
                .set_content_encoding(upload_metadata.content_encoding)
                .set_cache_control(upload_metadata.cache_control)
                .set_content_disposition(upload_metadata.content_disposition)
                .set_content_language(upload_metadata.content_language)
                .set_expires(upload_metadata.expires)
                .set_server_side_encryption(self.config.sse.clone())
                .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
                .set_sse_customer_algorithm(self.config.target_sse_c.clone())
                .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
                .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
                .set_copy_source_sse_customer_algorithm(self.config.source_sse_c.clone())
                .set_copy_source_sse_customer_key(self.config.source_sse_c_key.clone().key.clone())
                .set_copy_source_sse_customer_key_md5(self.config.source_sse_c_key_md5.clone())
                .set_acl(upload_metadata.acl)
                .set_checksum_algorithm(self.config.additional_checksum_algorithm.as_ref().cloned())
                .set_if_none_match(self.if_none_match.clone())
                .send()
                .await?;
            let _ = self
                .stats_sender
                .send_blocking(SyncStatistics::SyncBytes(source_total_size));
            convert_copy_to_put_object_output(copy_object_output, source_total_size as i64)
        } else {
            let builder = self
                .client
                .put_object()
                .set_request_payer(upload_metadata.request_payer)
                .set_storage_class(upload_metadata.storage_class)
                .bucket(bucket)
                .key(key)
                .content_length(source_total_size as i64)
                .body(buffer_stream)
                .set_metadata(upload_metadata.metadata)
                .set_tagging(upload_metadata.tagging)
                .set_website_redirect_location(upload_metadata.website_redirect_location)
                .set_content_md5(md5_digest_base64)
                .set_content_type(upload_metadata.content_type)
                .set_content_encoding(upload_metadata.content_encoding)
                .set_cache_control(upload_metadata.cache_control)
                .set_content_disposition(upload_metadata.content_disposition)
                .set_content_language(upload_metadata.content_language)
                .set_expires(upload_metadata.expires)
                .set_server_side_encryption(self.config.sse.clone())
                .set_ssekms_key_id(self.config.sse_kms_key_id.clone().id.clone())
                .set_sse_customer_algorithm(self.config.target_sse_c.clone())
                .set_sse_customer_key(self.config.target_sse_c_key.clone().key.clone())
                .set_sse_customer_key_md5(self.config.target_sse_c_key_md5.clone())
                .set_acl(upload_metadata.acl)
                .set_checksum_algorithm(self.config.additional_checksum_algorithm.as_ref().cloned())
                .set_if_none_match(self.if_none_match.clone());

            if self.config.disable_payload_signing {
                builder
                    .customize()
                    .disable_payload_signing()
                    .send()
                    .await
                    .context("aws_sdk_s3::client::Client put_object() failed.")?
            } else {
                builder
                    .send()
                    .await
                    .context("aws_sdk_s3::client::Client put_object() failed.")?
            }
        };

        debug!(
            key = &key,
            if_none_match = &self.if_none_match.clone(),
            "put_object() complete",
        );

        let _ = self
            .stats_sender
            .send(SyncStatistics::SyncBytes(source_total_size))
            .await;

        let source_e_tag = if source_local_storage {
            Some(self.generate_e_tag_hash(0))
        } else {
            source_e_tag
        };

        if !self.config.disable_etag_verify
            && !self.express_onezone_storage
            && !self.config.disable_content_md5_header
            && source_storage_class != Some(StorageClass::ExpressOnezone)
        {
            let target_sse = put_object_output.server_side_encryption().cloned();
            let target_e_tag = put_object_output.e_tag().map(|e| e.to_string());

            self.verify_e_tag(
                key,
                &source_sse,
                source_remote_storage,
                &source_e_tag,
                &target_sse,
                &target_e_tag,
            )
            .await;
        }

        if !self.config.disable_additional_checksum_verify {
            let target_checksum = get_additional_checksum_from_put_object_result(
                &put_object_output,
                self.config.additional_checksum_algorithm.as_ref().cloned(),
            );

            self.validate_checksum(
                key,
                source_checksum,
                target_checksum,
                &source_e_tag,
                source_remote_storage,
            )
            .await;
        }

        Ok(put_object_output)
    }

    async fn validate_checksum(
        &mut self,
        key: &str,
        source_checksum: Option<String>,
        target_checksum: Option<String>,
        source_e_tag: &Option<String>,
        source_remote_storage: bool,
    ) {
        if self.config.additional_checksum_mode.is_some() && source_checksum.is_none() {
            self.send_stats(SyncWarning {
                key: key.to_string(),
            })
            .await;
            self.has_warning.store(true, Ordering::SeqCst);

            let message = "additional checksum algorithm is different from the target storage. skip additional checksum verification.";
            warn!(key = &key, message);
        }

        #[allow(clippy::unnecessary_unwrap)]
        if target_checksum.is_some() && source_checksum.is_some() {
            let target_checksum = target_checksum.unwrap();
            let source_checksum = source_checksum.unwrap();

            let additional_checksum_algorithm = self
                .config
                .additional_checksum_algorithm
                .as_ref()
                .unwrap()
                .as_str();

            if target_checksum != source_checksum {
                if source_remote_storage
                    && is_multipart_upload_e_tag(source_e_tag)
                    && self.config.disable_multipart_verify
                {
                    debug!(
                        key = &key,
                        additional_checksum_algorithm = additional_checksum_algorithm,
                        target_checksum = target_checksum,
                        source_checksum = source_checksum,
                        "skip additional checksum verification."
                    );
                } else {
                    self.send_stats(SyncWarning {
                        key: key.to_string(),
                    })
                    .await;
                    self.has_warning.store(true, Ordering::SeqCst);

                    let message = if source_remote_storage
                        && is_multipart_upload_e_tag(source_e_tag)
                        && !self.is_auto_chunksize_enabled()
                    {
                        format!("{} {}", "additional checksum", MISMATCH_WARNING_WITH_HELP)
                    } else {
                        "additional checksum mismatch. file in the target storage may be corrupted."
                            .to_string()
                    };

                    warn!(
                        key = &key,
                        additional_checksum_algorithm = additional_checksum_algorithm,
                        target_checksum = target_checksum,
                        source_checksum = source_checksum,
                        message
                    );
                }
            } else {
                self.send_stats(ChecksumVerified {
                    key: key.to_string(),
                })
                .await;

                debug!(
                    key = &key,
                    additional_checksum_algorithm = additional_checksum_algorithm,
                    target_checksum = target_checksum,
                    source_checksum = source_checksum,
                    "additional checksum verified.",
                );
            }
        }
    }

    fn generate_e_tag_hash(&self, parts_count: i64) -> String {
        generate_e_tag_hash(&self.concatnated_md5_hash, parts_count)
    }

    fn calculate_parts_count(&self, content_length: i64) -> i64 {
        calculate_parts_count(
            self.config.transfer_config.multipart_threshold as i64,
            self.config.transfer_config.multipart_chunksize as i64,
            content_length,
        )
    }

    async fn send_stats(&self, stats: SyncStatistics) {
        let _ = self.stats_sender.send(stats).await;
    }

    fn is_auto_chunksize_enabled(&self) -> bool {
        self.config.transfer_config.auto_chunksize && self.object_parts.is_some()
    }
}

fn calculate_parts_count(
    multipart_threshold: i64,
    multipart_chunksize: i64,
    content_length: i64,
) -> i64 {
    if content_length < multipart_threshold {
        return 0;
    }

    if content_length % multipart_chunksize == 0 {
        return content_length / multipart_chunksize;
    }

    (content_length / multipart_chunksize) + 1
}

pub fn get_additional_checksum_from_put_object_result(
    put_object_output: &PutObjectOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => put_object_output
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => put_object_output
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => put_object_output
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => put_object_output
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => put_object_output
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

pub fn get_additional_checksum_from_multipart_upload_result(
    complete_multipart_upload_result: &CompleteMultipartUploadOutput,
    checksum_algorithm: Option<ChecksumAlgorithm>,
) -> Option<String> {
    checksum_algorithm.as_ref()?;

    match checksum_algorithm.unwrap() {
        ChecksumAlgorithm::Sha256 => complete_multipart_upload_result
            .checksum_sha256()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Sha1 => complete_multipart_upload_result
            .checksum_sha1()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32 => complete_multipart_upload_result
            .checksum_crc32()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc32C => complete_multipart_upload_result
            .checksum_crc32_c()
            .map(|checksum| checksum.to_string()),
        ChecksumAlgorithm::Crc64Nvme => complete_multipart_upload_result
            .checksum_crc64_nvme()
            .map(|checksum| checksum.to_string()),
        _ => {
            panic!("unknown algorithm")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::primitives::DateTime;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn modify_last_modified_metadata_with_new() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .build();
        get_object_output = UploadManager::modify_last_modified_metadata(get_object_output);

        assert_eq!(
            get_object_output
                .metadata()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
    }

    #[test]
    fn modify_last_modified_metadata_with_update() {
        init_dummy_tracing_subscriber();

        let mut get_object_output = GetObjectOutput::builder()
            .last_modified(DateTime::from_secs(0))
            .metadata("key1", "value1")
            .build();
        get_object_output = UploadManager::modify_last_modified_metadata(get_object_output);

        assert_eq!(
            get_object_output
                .metadata()
                .unwrap()
                .get(S3SYNC_ORIGIN_LAST_MODIFIED_METADATA_KEY)
                .unwrap(),
            "1970-01-01T00:00:00+00:00"
        );
        assert_eq!(
            get_object_output.metadata().unwrap().get("key1").unwrap(),
            "value1"
        );
    }

    #[test]
    fn calculate_parts_count_test() {
        init_dummy_tracing_subscriber();

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, 8 * 1024 * 1024),
            1
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, (8 * 1024 * 1024) - 1),
            0
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, 16 * 1024 * 1024),
            2
        );

        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, (16 * 1024 * 1024) + 1),
            3
        );
    }

    #[test]
    fn calculate_parts_count_threshold_smaller_than_chunksize() {
        // threshold gates whether we go multipart at all; chunksize controls part size.
        // If a 6 MiB upload qualifies (threshold=5 MiB) but its size < chunksize=8 MiB,
        // we get a single part — not zero.
        let result = calculate_parts_count(5 * 1024 * 1024, 8 * 1024 * 1024, 6 * 1024 * 1024);
        assert_eq!(result, 1);
    }

    #[test]
    fn calculate_parts_count_zero_content_length() {
        // 0-byte object is below any positive threshold → no multipart parts.
        assert_eq!(
            calculate_parts_count(8 * 1024 * 1024, 8 * 1024 * 1024, 0),
            0
        );
    }

    #[test]
    fn calculate_parts_count_exactly_at_threshold() {
        // content_length == threshold qualifies (the < is strict): one part.
        assert_eq!(
            calculate_parts_count(5 * 1024 * 1024, 5 * 1024 * 1024, 5 * 1024 * 1024),
            1
        );
    }

    #[test]
    fn get_additional_checksum_from_put_result_returns_none_when_algorithm_none() {
        let put = PutObjectOutput::builder().build();
        assert!(get_additional_checksum_from_put_object_result(&put, None).is_none());
    }

    #[test]
    fn get_additional_checksum_from_put_result_extracts_each_algorithm() {
        let put = PutObjectOutput::builder()
            .checksum_sha256("sha256-value")
            .checksum_sha1("sha1-value")
            .checksum_crc32("crc32-value")
            .checksum_crc32_c("crc32c-value")
            .checksum_crc64_nvme("crc64-value")
            .build();
        assert_eq!(
            get_additional_checksum_from_put_object_result(&put, Some(ChecksumAlgorithm::Sha256))
                .unwrap(),
            "sha256-value"
        );
        assert_eq!(
            get_additional_checksum_from_put_object_result(&put, Some(ChecksumAlgorithm::Sha1))
                .unwrap(),
            "sha1-value"
        );
        assert_eq!(
            get_additional_checksum_from_put_object_result(&put, Some(ChecksumAlgorithm::Crc32))
                .unwrap(),
            "crc32-value"
        );
        assert_eq!(
            get_additional_checksum_from_put_object_result(&put, Some(ChecksumAlgorithm::Crc32C))
                .unwrap(),
            "crc32c-value"
        );
        assert_eq!(
            get_additional_checksum_from_put_object_result(
                &put,
                Some(ChecksumAlgorithm::Crc64Nvme)
            )
            .unwrap(),
            "crc64-value"
        );
    }

    #[test]
    fn get_additional_checksum_from_put_result_returns_none_when_field_missing() {
        // When the requested algorithm's field is empty, return None even though the
        // algorithm was specified — callers rely on this to skip verification.
        let put = PutObjectOutput::builder().build();
        assert!(
            get_additional_checksum_from_put_object_result(&put, Some(ChecksumAlgorithm::Sha256))
                .is_none()
        );
    }

    #[test]
    fn get_additional_checksum_from_multipart_result_returns_none_when_algorithm_none() {
        let mp = CompleteMultipartUploadOutput::builder().build();
        assert!(get_additional_checksum_from_multipart_upload_result(&mp, None).is_none());
    }

    #[test]
    fn get_additional_checksum_from_multipart_result_extracts_each_algorithm() {
        let mp = CompleteMultipartUploadOutput::builder()
            .checksum_sha256("mp-sha256")
            .checksum_sha1("mp-sha1")
            .checksum_crc32("mp-crc32")
            .checksum_crc32_c("mp-crc32c")
            .checksum_crc64_nvme("mp-crc64")
            .build();
        assert_eq!(
            get_additional_checksum_from_multipart_upload_result(
                &mp,
                Some(ChecksumAlgorithm::Sha256)
            )
            .unwrap(),
            "mp-sha256"
        );
        assert_eq!(
            get_additional_checksum_from_multipart_upload_result(
                &mp,
                Some(ChecksumAlgorithm::Crc64Nvme)
            )
            .unwrap(),
            "mp-crc64"
        );
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                EnvFilter::try_from_default_env()
                    .or_else(|_| EnvFilter::try_new("dummy=trace"))
                    .unwrap(),
            )
            .try_init();
    }
}

#[cfg(test)]
mod read_exact_or_eof_tests {
    use super::read_exact_or_eof;
    use std::io::Cursor;

    #[tokio::test]
    async fn reads_full_buffer_when_reader_has_more() {
        let mut reader = Cursor::new(vec![1u8; 100]);
        let mut buf = vec![0u8; 50];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 50);
        assert_eq!(buf, vec![1u8; 50]);
    }

    #[tokio::test]
    async fn reads_partial_when_reader_has_less() {
        let mut reader = Cursor::new(vec![7u8; 30]);
        let mut buf = vec![0u8; 50];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 30);
        assert_eq!(&buf[..30], &vec![7u8; 30][..]);
        assert_eq!(&buf[30..], &vec![0u8; 20][..]);
    }

    #[tokio::test]
    async fn reads_zero_when_reader_is_empty() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut buf = vec![0u8; 10];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[tokio::test]
    async fn reads_exact_amount_when_reader_matches_buffer() {
        let mut reader = Cursor::new(vec![9u8; 64]);
        let mut buf = vec![0u8; 64];
        let n = read_exact_or_eof(&mut reader, &mut buf).await.unwrap();
        assert_eq!(n, 64);
        assert_eq!(buf, vec![9u8; 64]);
    }
}
