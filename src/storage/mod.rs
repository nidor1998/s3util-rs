use anyhow::{Result, anyhow};
use async_channel::Sender;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::operation::copy_object::CopyObjectOutput;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::operation::put_object::PutObjectOutput;
use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
use aws_sdk_s3::operation::upload_part::UploadPartOutput;
use aws_sdk_s3::operation::upload_part_copy::UploadPartCopyOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ChecksumMode, ObjectPart, RequestPayer, Tagging};
use aws_smithy_types::body::SdkBody;
use dyn_clone::DynClone;
use futures_util::stream::TryStreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use leaky_bucket::RateLimiter;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::io::{AsyncRead, BufReader};
use tokio_util::io::ReaderStream;

use crate::Config;
use crate::config::ClientConfig;
use crate::storage::checksum::AdditionalChecksum;
use crate::types::async_callback::AsyncReadWithCallback;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SseCustomerKey, StoragePath, SyncStatistics};

pub mod additional_checksum_verify;
pub mod checksum;
pub mod e_tag_verify;
pub mod local;
pub mod s3;

pub type Storage = Box<dyn StorageTrait + Send + Sync>;

pub struct StoragePair {
    pub source: Storage,
    pub target: Storage,
}

#[async_trait]
pub trait StorageFactory {
    #[allow(clippy::too_many_arguments)]
    async fn create(
        config: Config,
        path: StoragePath,
        cancellation_token: PipelineCancellationToken,
        stats_sender: Sender<SyncStatistics>,
        client_config: Option<ClientConfig>,
        request_payer: Option<RequestPayer>,
        rate_limit_bandwidth: Option<Arc<RateLimiter>>,
        has_warning: Arc<AtomicBool>,
        object_to_list: Option<String>,
    ) -> Storage;
}

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait StorageTrait: DynClone {
    fn is_local_storage(&self) -> bool;
    fn is_express_onezone_storage(&self) -> bool;
    #[allow(clippy::too_many_arguments)]
    async fn get_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<GetObjectOutput>;
    async fn get_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<GetObjectTaggingOutput>;
    async fn head_object(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        range: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput>;
    async fn head_object_first_part(
        &self,
        key: &str,
        version_id: Option<String>,
        checksum_mode: Option<ChecksumMode>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<HeadObjectOutput>;

    async fn get_object_parts(
        &self,
        key: &str,
        version_id: Option<String>,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>>;
    async fn get_object_parts_attributes(
        &self,
        key: &str,
        version_id: Option<String>,
        max_parts: i32,
        sse_c: Option<String>,
        sse_c_key: SseCustomerKey,
        sse_c_key_md5: Option<String>,
    ) -> Result<Vec<ObjectPart>>;
    #[allow(clippy::too_many_arguments)]
    async fn put_object(
        &self,
        key: &str,
        source: Storage,
        source_key: &str,
        source_size: u64,
        source_additional_checksum: Option<String>,
        get_object_output_first_chunk: GetObjectOutput,
        tagging: Option<String>,
        object_checksum: Option<ObjectChecksum>,
        if_none_match: Option<String>,
    ) -> Result<PutObjectOutput>;
    async fn put_object_tagging(
        &self,
        key: &str,
        version_id: Option<String>,
        tagging: Tagging,
    ) -> Result<PutObjectTaggingOutput>;
    async fn put_object_stream(
        &self,
        _key: &str,
        _reader: Box<dyn AsyncRead + Send + Unpin>,
        _tagging: Option<String>,
        _object_checksum: Option<ObjectChecksum>,
        _if_none_match: Option<String>,
    ) -> Result<PutObjectOutput> {
        Err(anyhow!(
            "put_object_stream is not supported on this storage"
        ))
    }
    async fn delete_object(
        &self,
        key: &str,
        version_id: Option<String>,
    ) -> Result<DeleteObjectOutput>;
    fn get_client(&self) -> Option<Arc<Client>>;
    fn get_stats_sender(&self) -> Sender<SyncStatistics>;
    async fn send_stats(&self, stats: SyncStatistics);
    fn get_local_path(&self) -> PathBuf;
    fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>>;
    fn generate_copy_source_key(&self, key: &str, version_id: Option<String>) -> String;
    fn set_warning(&self);
}

#[rustfmt::skip] // For coverage tool incorrectness
pub fn convert_to_buf_byte_stream_with_callback<R>(
    byte_stream: R,
    stats_sender: Option<Sender<SyncStatistics>>,
    rate_limit_bandwidth: Option<Arc<RateLimiter>>,
    additional_checksum: Option<Arc<AdditionalChecksum>>,
    object_checksum: Option<ObjectChecksum>,
) -> ByteStream
where
    R: AsyncRead + Send + 'static + Sync,
{
    let async_read = AsyncReadWithCallback::new(byte_stream, stats_sender, rate_limit_bandwidth, additional_checksum, object_checksum);

    let buf_reader = BufReader::new(async_read);

    let reader_stream = ReaderStream::new(buf_reader).map_ok(Frame::data);

    let stream_body = StreamBody::new(reader_stream);

    let boxed_body = BodyExt::boxed(stream_body);

    let sdk_body = SdkBody::from_body_1_x(boxed_body);

    ByteStream::new(sdk_body)
}

pub fn get_range_from_content_range(get_object_output: &GetObjectOutput) -> Option<(u64, u64)> {
    let content_range = get_object_output.content_range()?;
    let parts: Vec<&str> = content_range.split_whitespace().collect();
    if parts.len() == 2 {
        let range_parts: Vec<&str> = parts[1].split('/').collect();
        if range_parts.len() == 2 {
            let byte_range: Vec<&str> = range_parts[0].split('-').collect();
            if byte_range.len() == 2 {
                let start = byte_range[0].parse::<u64>().ok()?;
                let end = byte_range[1].parse::<u64>().ok()?;
                return Some((start, end));
            }
        }
    }

    None
}

/// Build a synthetic `GetObjectOutput` from a `HeadObjectOutput`.
///
/// When `range_override` is `Some((start, end, total_size))`, the returned
/// `content_length` and `content_range` fields are synthesized to match what
/// a ranged GET would have returned — so callers can avoid a second ranged
/// HEAD round-trip when the range was computed locally. The total size is
/// passed explicitly rather than read from the head output because a
/// non-ranged HEAD's `content_length` *is* the total size, but we prefer an
/// explicit parameter to make the contract obvious at the call site.
pub fn convert_head_to_get_object_output(
    head_object_output: HeadObjectOutput,
    range_override: Option<(u64, u64, u64)>,
) -> GetObjectOutput {
    let (content_length, content_range) = match range_override {
        Some((start, end, total_size)) => (
            Some((end - start + 1) as i64),
            Some(format!("bytes {start}-{end}/{total_size}")),
        ),
        None => (
            head_object_output.content_length(),
            head_object_output.content_range().map(|s| s.to_string()),
        ),
    };
    GetObjectOutput::builder()
        .set_accept_ranges(head_object_output.accept_ranges().map(|s| s.to_string()))
        .set_body(Some(ByteStream::from(vec![])))
        .set_bucket_key_enabled(head_object_output.bucket_key_enabled())
        .set_cache_control(head_object_output.cache_control().map(|s| s.to_string()))
        .set_checksum_crc32(head_object_output.checksum_crc32().map(|s| s.to_string()))
        .set_checksum_crc32_c(head_object_output.checksum_crc32_c().map(|s| s.to_string()))
        .set_checksum_crc64_nvme(
            head_object_output
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(head_object_output.checksum_sha1().map(|s| s.to_string()))
        .set_checksum_sha256(head_object_output.checksum_sha256().map(|s| s.to_string()))
        .set_checksum_type(head_object_output.checksum_type().cloned())
        .set_content_disposition(
            head_object_output
                .content_disposition()
                .map(|s| s.to_string()),
        )
        .set_content_encoding(head_object_output.content_encoding().map(|s| s.to_string()))
        .set_content_language(head_object_output.content_language().map(|s| s.to_string()))
        .set_content_length(content_length)
        .set_content_range(content_range)
        .set_content_type(head_object_output.content_type().map(|s| s.to_string()))
        .set_delete_marker(head_object_output.delete_marker())
        .set_e_tag(head_object_output.e_tag().map(|s| s.to_string()))
        .set_expiration(head_object_output.expiration().map(|s| s.to_string()))
        .set_expires_string(head_object_output.expires_string().map(|s| s.to_string()))
        .set_last_modified(head_object_output.last_modified().cloned())
        .set_metadata(head_object_output.metadata().cloned())
        .set_missing_meta(head_object_output.missing_meta())
        .set_object_lock_legal_hold_status(
            head_object_output.object_lock_legal_hold_status().cloned(),
        )
        .set_object_lock_mode(head_object_output.object_lock_mode().cloned())
        .set_object_lock_retain_until_date(
            head_object_output.object_lock_retain_until_date().cloned(),
        )
        .set_parts_count(head_object_output.parts_count())
        .set_replication_status(head_object_output.replication_status().cloned())
        .set_request_charged(head_object_output.request_charged().cloned())
        .set_restore(head_object_output.restore().map(|s| s.to_string()))
        .set_server_side_encryption(head_object_output.server_side_encryption().cloned())
        .set_sse_customer_algorithm(
            head_object_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            head_object_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(head_object_output.ssekms_key_id().map(|s| s.to_string()))
        .set_storage_class(head_object_output.storage_class().cloned())
        .set_tag_count(head_object_output.tag_count())
        .set_version_id(head_object_output.version_id().map(|s| s.to_string()))
        .set_website_redirect_location(
            head_object_output
                .website_redirect_location()
                .map(|s| s.to_string()),
        )
        .build()
}

pub fn convert_copy_to_put_object_output(
    copy_object_output: CopyObjectOutput,
    size: i64,
) -> PutObjectOutput {
    PutObjectOutput::builder()
        .set_bucket_key_enabled(copy_object_output.bucket_key_enabled())
        .set_checksum_crc32(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc32()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32_c(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc32_c()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc64_nvme(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_sha1()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha256(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_sha256()
                .map(|s| s.to_string()),
        )
        .set_checksum_type(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .checksum_type()
                .cloned(),
        )
        .set_e_tag(
            copy_object_output
                .copy_object_result()
                .unwrap()
                .e_tag()
                .map(|s| s.to_string()),
        )
        .set_expiration(
            copy_object_output
                .clone()
                .expiration()
                .map(|s| s.to_string()),
        )
        .set_request_charged(copy_object_output.request_charged().cloned())
        .set_server_side_encryption(copy_object_output.server_side_encryption().cloned())
        .set_size(Some(size))
        .set_sse_customer_algorithm(
            copy_object_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            copy_object_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_encryption_context(
            copy_object_output
                .ssekms_encryption_context()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(copy_object_output.ssekms_key_id().map(|s| s.to_string()))
        .set_version_id(copy_object_output.version_id().map(|s| s.to_string()))
        .build()
}

pub fn convert_copy_to_upload_part_output(
    upload_part_copy_output: UploadPartCopyOutput,
) -> UploadPartOutput {
    UploadPartOutput::builder()
        .set_server_side_encryption(upload_part_copy_output.server_side_encryption().cloned())
        .set_e_tag(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .e_tag()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc32()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc32_c(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc32_c()
                .map(|s| s.to_string()),
        )
        .set_checksum_crc64_nvme(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_crc64_nvme()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha1(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_sha1()
                .map(|s| s.to_string()),
        )
        .set_checksum_sha256(
            upload_part_copy_output
                .copy_part_result()
                .unwrap()
                .checksum_sha256()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_algorithm(
            upload_part_copy_output
                .sse_customer_algorithm()
                .map(|s| s.to_string()),
        )
        .set_sse_customer_key_md5(
            upload_part_copy_output
                .sse_customer_key_md5()
                .map(|s| s.to_string()),
        )
        .set_ssekms_key_id(
            upload_part_copy_output
                .ssekms_key_id()
                .map(|s| s.to_string()),
        )
        .set_bucket_key_enabled(upload_part_copy_output.bucket_key_enabled())
        .set_request_charged(upload_part_copy_output.request_charged().cloned())
        .build()
}

#[derive(Clone)]
pub struct FileRange {
    pub offset: u64,
    pub size: u64,
}

pub fn parse_range_header(range_header: &str) -> Result<FileRange> {
    if !range_header.starts_with("bytes=") {
        return Err(anyhow!(
            "Range header must start with 'bytes=': {}",
            range_header
        ));
    }

    let range = range_header.trim_start_matches("bytes=");
    let parts: Vec<_> = range.split('-').collect();
    if parts.len() != 2 {
        return Err(anyhow!("Invalid range format: {}", range));
    }

    let offset = parts[0].parse::<u64>()?;
    let size = if parts[1].is_empty() {
        return Err(anyhow!("Invalid range format: {}", range));
    } else {
        let end = parts[1].parse::<u64>()?;
        if end < offset {
            return Err(anyhow!("End of range cannot be less than start: {}", range));
        }
        end - offset + 1
    };

    Ok(FileRange { offset, size })
}

pub fn parse_range_header_string(range: &str) -> Option<(u64, u64)> {
    let parts: Vec<&str> = range.trim_start_matches("bytes=").split('-').collect();
    if parts.len() == 2 {
        let start = parts[0].parse::<u64>().ok()?;
        let end = parts[1].parse::<u64>().ok()?;
        return Some((start, end));
    }
    None
}

/// Shared test fixtures used by `#[cfg(test)]` code across the
/// `storage` submodules. Kept in one place so the fixture body — in
/// particular the tempfile + atomic-rename pattern that protects
/// parallel tests from a half-written file — can't drift between
/// copies.
#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;

    /// Materialize a `size`-byte zero-filled file at `path`, creating
    /// `dir` first. Idempotent: returns immediately if `path` already
    /// exists. Writes to a unique temp path inside `dir` and atomically
    /// renames into place so concurrent test runs never observe a
    /// partially-written fixture.
    #[cfg_attr(coverage_nightly, coverage(off))]
    pub async fn create_large_file(path: &str, dir: &str, size: usize) {
        if PathBuf::from(path).try_exists().unwrap() {
            return;
        }

        tokio::fs::create_dir_all(dir).await.unwrap();

        let tmp_path = tempfile::Builder::new()
            .prefix("large_file_")
            .tempfile_in(dir)
            .unwrap()
            .into_temp_path();
        let data = vec![0_u8; size];
        tokio::fs::write(&tmp_path, data.as_slice()).await.unwrap();
        let _ = tmp_path.persist(path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::EnvFilter;

    #[test]
    fn get_range_from_content_range_test() {
        init_dummy_tracing_subscriber();

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-1000/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 1000);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-0/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 0);
        assert_eq!(end, 0);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 500-999/67589")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 500);
        assert_eq!(end, 999);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 500-999/*")
            .build();
        let (start, end) = get_range_from_content_range(&get_object_output).unwrap();
        assert_eq!(start, 500);
        assert_eq!(end, 999);
    }

    #[test]
    fn get_range_from_content_range_error_test() {
        init_dummy_tracing_subscriber();

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("0-1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-1000")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes -1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes a-1000/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-a/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);
    }

    #[test]
    fn test_parse_range_header() {
        let range = parse_range_header("bytes=55-120").unwrap();
        assert_eq!(range.offset, 55);
        assert_eq!(range.size, 66);

        assert!(parse_range_header("bytes=65-65").is_ok());
    }

    #[test]
    fn test_parse_range_header_error() {
        assert!(parse_range_header("0-55").is_err());
        assert!(parse_range_header("bytes=0-").is_err());
        assert!(parse_range_header("bytes=-55").is_err());
        assert!(parse_range_header("bytes=60-55").is_err());
        assert!(parse_range_header("bytes=65-64").is_err());
    }

    #[test]
    fn test_parse_range_header_string() {
        let (start, end) = parse_range_header_string("bytes=55-120").unwrap();
        assert_eq!(start, 55);
        assert_eq!(end, 120);

        assert!(parse_range_header("bytes=65-65").is_ok());
    }

    #[test]
    fn test_parse_range_header_string_error() {
        assert!(parse_range_header_string("bytes=0-").is_none());
        assert!(parse_range_header_string("bytes=-55").is_none());
    }

    #[test]
    fn test_parse_range_header_string_returns_none_when_split_yields_wrong_count() {
        // Not exactly two parts after splitting on '-' → falls through to the final None.
        assert!(parse_range_header_string("bytes=0-50-100").is_none());
        assert!(parse_range_header_string("bytes=0").is_none());
        assert!(parse_range_header_string("").is_none());
    }

    #[test]
    fn test_parse_range_header_invalid_when_dash_count_wrong() {
        // Triple-dash range and zero-dash range both fail the parts.len() == 2 check.
        assert!(parse_range_header("bytes=0-50-100").is_err());
        assert!(parse_range_header("bytes=0").is_err());
    }

    #[test]
    fn get_range_from_content_range_byte_range_split_wrong_returns_none() {
        // byte_range with no dash or extra dashes should not satisfy len == 2.
        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 12345/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);

        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(67589))
            .content_range("bytes 0-50-100/67589")
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);
    }

    #[test]
    fn get_range_from_content_range_no_content_range_returns_none() {
        // content_range absent altogether — early ? returns None.
        let get_object_output = GetObjectOutput::builder()
            .set_content_length(Some(100))
            .build();
        assert_eq!(get_range_from_content_range(&get_object_output), None);
    }

    #[test]
    fn convert_head_to_get_object_output_with_range_override_synthesizes_fields() {
        use aws_sdk_s3::operation::head_object::HeadObjectOutput;

        let head = HeadObjectOutput::builder()
            .e_tag("\"abc\"")
            .content_length(1000)
            .build();

        // Range override overrides content_length and synthesizes content_range.
        let got = convert_head_to_get_object_output(head, Some((100, 199, 1000)));
        assert_eq!(got.content_length(), Some(100));
        assert_eq!(got.content_range(), Some("bytes 100-199/1000"));
        assert_eq!(got.e_tag(), Some("\"abc\""));
    }

    #[test]
    fn convert_head_to_get_object_output_without_range_override_preserves_head_fields() {
        use aws_sdk_s3::operation::head_object::HeadObjectOutput;

        let head = HeadObjectOutput::builder()
            .e_tag("\"abc\"")
            .content_length(1000)
            .checksum_sha256("sha")
            .checksum_crc32("crc32")
            .build();

        let got = convert_head_to_get_object_output(head, None);
        assert_eq!(got.content_length(), Some(1000));
        assert_eq!(got.content_range(), None);
        assert_eq!(got.checksum_sha256(), Some("sha"));
        assert_eq!(got.checksum_crc32(), Some("crc32"));
    }

    #[test]
    fn parse_range_header_size_calculation_is_inclusive() {
        // bytes=0-9 → 10 bytes, bytes=100-199 → 100 bytes.
        let r = parse_range_header("bytes=0-9").unwrap();
        assert_eq!(r.offset, 0);
        assert_eq!(r.size, 10);
        let r = parse_range_header("bytes=100-199").unwrap();
        assert_eq!(r.offset, 100);
        assert_eq!(r.size, 100);
    }

    #[tokio::test]
    async fn put_object_stream_default_impl_returns_unsupported_error() {
        // Verifies the default trait method body — any storage that doesn't
        // override put_object_stream should propagate this error rather than
        // silently succeed. Use a minimal stub so we exercise the default impl.
        let storage: Box<dyn StorageTrait + Send + Sync> = Box::new(StubStorage);
        let reader: Box<dyn tokio::io::AsyncRead + Send + Unpin> = Box::new(tokio::io::empty());
        let result = storage
            .put_object_stream("key", reader, None, None, None)
            .await;
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not supported"));
    }

    #[derive(Clone)]
    struct StubStorage;

    #[async_trait::async_trait]
    impl StorageTrait for StubStorage {
        fn is_local_storage(&self) -> bool {
            false
        }
        fn is_express_onezone_storage(&self) -> bool {
            false
        }
        async fn get_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<GetObjectOutput> {
            unreachable!("not used in default impl test")
        }
        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            unreachable!()
        }
        async fn head_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _range: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            unreachable!()
        }
        async fn head_object_first_part(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _checksum_mode: Option<ChecksumMode>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<HeadObjectOutput> {
            unreachable!()
        }
        async fn get_object_parts(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unreachable!()
        }
        async fn get_object_parts_attributes(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _max_parts: i32,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unreachable!()
        }
        async fn put_object(
            &self,
            _key: &str,
            _source: Storage,
            _source_key: &str,
            _source_size: u64,
            _source_additional_checksum: Option<String>,
            _get_object_output_first_chunk: GetObjectOutput,
            _tagging: Option<String>,
            _object_checksum: Option<ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            unreachable!()
        }
        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
            unreachable!()
        }
        async fn delete_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<DeleteObjectOutput> {
            unreachable!()
        }
        fn get_client(&self) -> Option<Arc<Client>> {
            None
        }
        fn get_stats_sender(&self) -> Sender<SyncStatistics> {
            unreachable!()
        }
        async fn send_stats(&self, _stats: SyncStatistics) {
            unreachable!()
        }
        fn get_local_path(&self) -> PathBuf {
            unreachable!()
        }
        fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
            None
        }
        fn generate_copy_source_key(&self, _key: &str, _version_id: Option<String>) -> String {
            unreachable!()
        }
        fn set_warning(&self) {
            unreachable!()
        }
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

    // ------------------------------------------------------------------
    // Direct StubStorage trait coverage. The default-impl test above only
    // calls `put_object_stream` on the trait; the assertions below pin
    // the few real-return methods and verify each `unreachable!()` stub
    // still panics (so the regression guard remains intact).
    // ------------------------------------------------------------------

    async fn assert_future_panics<F, T>(future: F)
    where
        F: std::future::Future<Output = T>,
    {
        use futures::FutureExt;
        use std::panic::AssertUnwindSafe;
        let result = AssertUnwindSafe(future).catch_unwind().await;
        assert!(result.is_err(), "expected the future to panic");
    }

    fn assert_call_panics<F, R>(f: F)
    where
        F: FnOnce() -> R,
    {
        use std::panic::AssertUnwindSafe;
        let result = std::panic::catch_unwind(AssertUnwindSafe(f));
        assert!(result.is_err(), "expected the call to panic");
    }

    fn no_sse_c_key() -> SseCustomerKey {
        SseCustomerKey { key: None }
    }

    fn dummy_tagging() -> Tagging {
        Tagging::builder()
            .set_tag_set(Some(vec![]))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn stub_storage_real_return_methods_behave_as_expected() {
        let stub = StubStorage;

        assert!(!stub.is_local_storage());
        assert!(!stub.is_express_onezone_storage());
        assert!(stub.get_client().is_none());
        assert!(stub.get_rate_limit_bandwidth().is_none());
    }

    #[tokio::test]
    async fn stub_storage_unreachable_methods_panic() {
        let stub = StubStorage;

        assert_future_panics(stub.get_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(stub.get_object_tagging("k", None)).await;
        assert_future_panics(stub.head_object("k", None, None, None, None, no_sse_c_key(), None))
            .await;
        assert_future_panics(stub.head_object_first_part(
            "k",
            None,
            None,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(stub.get_object_parts("k", None, None, no_sse_c_key(), None)).await;
        assert_future_panics(stub.get_object_parts_attributes(
            "k",
            None,
            0,
            None,
            no_sse_c_key(),
            None,
        ))
        .await;
        assert_future_panics(stub.put_object(
            "k",
            Box::new(StubStorage),
            "src",
            0,
            None,
            GetObjectOutput::builder().build(),
            None,
            None,
            None,
        ))
        .await;
        assert_future_panics(stub.put_object_tagging("k", None, dummy_tagging())).await;
        assert_future_panics(stub.delete_object("k", None)).await;
        assert_future_panics(stub.send_stats(SyncStatistics::SyncComplete { key: "k".into() }))
            .await;

        assert_call_panics(|| stub.get_stats_sender());
        assert_call_panics(|| stub.get_local_path());
        assert_call_panics(|| stub.generate_copy_source_key("k", None));
        assert_call_panics(|| stub.set_warning());
    }
}
