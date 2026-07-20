use anyhow::{Context, Result};
use async_channel::Sender;
use tracing::{debug, warn};

use crate::Config;
use crate::storage::Storage;
use crate::transfer::TransferOutcome;
use crate::transfer::first_chunk;
use crate::types::token::PipelineCancellationToken;
use crate::types::{SyncStatistics, get_additional_checksum};

/// Transfer a local file to S3.
///
/// Uses first-chunk optimization: for files above the multipart threshold,
/// only the first chunk is fetched initially via a range request. The upload
/// manager then fetches remaining chunks lazily during multipart upload.
/// Ported from s3sync/src/pipeline/syncer.rs.
pub async fn transfer(
    config: &Config,
    source: Storage,
    target: Storage,
    source_key: &str,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<TransferOutcome> {
    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    let source_clone = dyn_clone::clone_box(&*source);

    // Get source size before get_object to calculate first chunk range
    let head_object_output = source
        .head_object(
            source_key,
            None,
            None,
            None,
            None,
            crate::types::SseCustomerKey { key: None },
            None,
        )
        .await
        .context(format!(
            "source file not found or not accessible: {source_key}"
        ))?;

    let source_size = head_object_output.content_length().unwrap_or(0);

    // Get the first chunk range if multipart upload is required.
    // If not, the whole object will be downloaded.
    let range =
        first_chunk::get_first_chunk_range(&*source, config, source_size, source_key, None).await?;

    debug!(
        key = source_key,
        size = source_size,
        range = range.as_deref(),
        "first chunk range for the object",
    );

    // Get object from local storage (with range if multipart)
    let get_object_output = source
        .get_object(
            source_key,
            None,
            config.additional_checksum_mode.clone(),
            range.clone(),
            None,
            crate::types::SseCustomerKey { key: None },
            None,
        )
        .await
        .context(format!("failed to read source file: {source_key}"))?;

    if cancellation_token.is_cancelled() {
        return Ok(TransferOutcome::default());
    }

    // Validate content range if range was used
    if range.is_some() {
        first_chunk::validate_content_range(&get_object_output, range.as_ref().unwrap())?;
    }

    let source_additional_checksum = get_additional_checksum(
        &get_object_output,
        config.additional_checksum_algorithm.clone(),
    );

    // Get tagging from config
    let tagging = if config.disable_tagging {
        None
    } else {
        config.tagging.clone()
    };

    // Build checksum algorithm slice matching s3sync's object.checksum_algorithm() format
    let checksum_algorithms: Option<Vec<_>> = config
        .additional_checksum_algorithm
        .as_ref()
        .map(|a| vec![a.clone()]);
    let checksum_algorithm_slice = checksum_algorithms.as_deref();

    // Build object checksum using the s3sync-ported helper
    let final_checksum = first_chunk::get_final_checksum(
        &*source,
        config,
        &get_object_output,
        range.as_deref(),
        source_key,
        None,
        checksum_algorithm_slice,
    )
    .await;

    let object_checksum = first_chunk::build_object_checksum(
        &*source,
        &*target,
        config,
        target_key,
        &get_object_output,
        checksum_algorithm_slice,
        final_checksum,
    )
    .await?;

    let if_none_match = if config.if_none_match {
        Some("*".to_string())
    } else {
        None
    };

    let put_object_output = target
        .put_object(
            target_key,
            source_clone,
            source_key,
            source_size as u64,
            source_additional_checksum,
            get_object_output,
            tagging,
            object_checksum,
            if_none_match,
        )
        .await
        .context(format!("failed to upload to target: {target_key}"))?;

    if put_object_output.e_tag.is_some() {
        debug!(
            source_key = source_key,
            target_key = target_key,
            size = source_size,
            "transfer completed."
        );
    } else {
        warn!(
            source_key = source_key,
            target_key = target_key,
            "transfer completed but no ETag returned."
        );
    }

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(TransferOutcome::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::args::{Commands, parse_from_args};
    use crate::storage::StorageTrait;
    use crate::types::SseCustomerKey;
    use crate::types::token::{PipelineCancellationToken, create_pipeline_cancellation_token};
    use async_trait::async_trait;
    use aws_sdk_s3::Client;
    use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;
    use aws_sdk_s3::operation::get_object::GetObjectOutput;
    use aws_sdk_s3::operation::get_object_tagging::GetObjectTaggingOutput;
    use aws_sdk_s3::operation::head_object::HeadObjectOutput;
    use aws_sdk_s3::operation::put_object::PutObjectOutput;
    use aws_sdk_s3::operation::put_object_tagging::PutObjectTaggingOutput;
    use aws_sdk_s3::primitives::{ByteStream, DateTime};
    use aws_sdk_s3::types::{ChecksumMode, ObjectPart, Tagging};
    use leaky_bucket::RateLimiter;
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Local-file-shaped source: HEAD and GET answer a 4-byte object, and the
    /// optional token is cancelled from inside `get_object` to drive the
    /// post-read cancellation guard.
    #[derive(Clone)]
    struct StubSource {
        cancel_on_get: Option<PipelineCancellationToken>,
    }

    #[async_trait]
    impl StorageTrait for StubSource {
        fn is_local_storage(&self) -> bool {
            true
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
            if let Some(token) = &self.cancel_on_get {
                token.cancel();
            }
            Ok(GetObjectOutput::builder()
                .body(ByteStream::from(b"data".to_vec()))
                .content_length(4)
                .e_tag("\"abc\"")
                .last_modified(DateTime::from_secs(0))
                .build())
        }
        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            unimplemented!()
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
            Ok(HeadObjectOutput::builder()
                .content_length(4)
                .last_modified(DateTime::from_secs(0))
                .build())
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
            unimplemented!()
        }
        async fn get_object_parts(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unimplemented!()
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
            unimplemented!()
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            unimplemented!()
        }
        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
            unimplemented!()
        }
        async fn delete_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<DeleteObjectOutput> {
            unimplemented!()
        }
        fn get_client(&self) -> Option<Arc<Client>> {
            None
        }
        fn get_stats_sender(&self) -> Sender<SyncStatistics> {
            async_channel::unbounded().0
        }
        async fn send_stats(&self, _stats: SyncStatistics) {}
        fn get_local_path(&self) -> PathBuf {
            PathBuf::new()
        }
        fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
            None
        }
        fn generate_copy_source_key(&self, _key: &str, _version_id: Option<String>) -> String {
            unimplemented!()
        }
        fn set_warning(&self) {}
    }

    /// Target whose put succeeds but reports no ETag — drives the
    /// "transfer completed but no ETag returned" warning branch.
    #[derive(Clone)]
    struct NoEtagTarget;

    #[async_trait]
    impl StorageTrait for NoEtagTarget {
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
            unimplemented!()
        }
        async fn get_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<GetObjectTaggingOutput> {
            unimplemented!()
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
            unimplemented!()
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
            unimplemented!()
        }
        async fn get_object_parts(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _sse_c: Option<String>,
            _sse_c_key: SseCustomerKey,
            _sse_c_key_md5: Option<String>,
        ) -> Result<Vec<ObjectPart>> {
            unimplemented!()
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
            unimplemented!()
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
            _object_checksum: Option<crate::types::ObjectChecksum>,
            _if_none_match: Option<String>,
        ) -> Result<PutObjectOutput> {
            Ok(PutObjectOutput::builder().build())
        }
        async fn put_object_tagging(
            &self,
            _key: &str,
            _version_id: Option<String>,
            _tagging: Tagging,
        ) -> Result<PutObjectTaggingOutput> {
            unimplemented!()
        }
        async fn delete_object(
            &self,
            _key: &str,
            _version_id: Option<String>,
        ) -> Result<DeleteObjectOutput> {
            unimplemented!()
        }
        fn get_client(&self) -> Option<Arc<Client>> {
            None
        }
        fn get_stats_sender(&self) -> Sender<SyncStatistics> {
            async_channel::unbounded().0
        }
        async fn send_stats(&self, _stats: SyncStatistics) {}
        fn get_local_path(&self) -> PathBuf {
            PathBuf::new()
        }
        fn get_rate_limit_bandwidth(&self) -> Option<Arc<RateLimiter>> {
            None
        }
        fn generate_copy_source_key(&self, _key: &str, _version_id: Option<String>) -> String {
            unimplemented!()
        }
        fn set_warning(&self) {}
    }

    fn config() -> Config {
        let cli = parse_from_args(vec![
            "s3util",
            "cp",
            "test_data/5byte.dat",
            "s3://target-bucket/key",
        ])
        .unwrap();
        let Commands::Cp(cp_args) = cli.command else {
            panic!("expected Cp variant");
        };
        Config::try_from(cp_args).unwrap()
    }

    #[tokio::test]
    async fn transfer_returns_default_when_cancelled_at_entry() {
        let token = create_pipeline_cancellation_token();
        token.cancel();
        let source: Storage = Box::new(StubSource {
            cancel_on_get: None,
        });
        let target: Storage = Box::new(NoEtagTarget);
        let (stats_tx, stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config(),
            source,
            target,
            "src/file",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();
        assert_eq!(outcome.source_version_id, None);
        assert!(stats_rx.try_recv().is_err(), "nothing may be reported");
    }

    #[tokio::test]
    async fn transfer_returns_default_when_cancelled_during_read() {
        let token = create_pipeline_cancellation_token();
        let source: Storage = Box::new(StubSource {
            cancel_on_get: Some(token.clone()),
        });
        let target: Storage = Box::new(NoEtagTarget);
        let (stats_tx, stats_rx) = async_channel::unbounded::<SyncStatistics>();

        let outcome = transfer(
            &config(),
            source,
            target,
            "src/file",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();
        assert_eq!(outcome.source_version_id, None);
        assert!(
            stats_rx.try_recv().is_err(),
            "a cancelled transfer must not report completion"
        );
    }

    #[tokio::test]
    async fn transfer_completes_with_warning_when_put_returns_no_e_tag() {
        let token = create_pipeline_cancellation_token();
        let source: Storage = Box::new(StubSource {
            cancel_on_get: None,
        });
        let target: Storage = Box::new(NoEtagTarget);
        let (stats_tx, stats_rx) = async_channel::unbounded::<SyncStatistics>();

        transfer(
            &config(),
            source,
            target,
            "src/file",
            "dst/key",
            token,
            stats_tx,
        )
        .await
        .unwrap();

        let mut saw_complete = false;
        while let Ok(stat) = stats_rx.try_recv() {
            if matches!(stat, SyncStatistics::SyncComplete { .. }) {
                saw_complete = true;
            }
        }
        assert!(
            saw_complete,
            "an ETag-less put still completes the transfer"
        );
    }
}
