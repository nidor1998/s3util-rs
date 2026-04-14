use anyhow::{Context, Result};
use async_channel::Sender;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_types::DateTime;
use aws_smithy_types::body::SdkBody;
use futures_util::stream::TryStreamExt;
use http_body_util::{BodyExt, StreamBody};
use hyper::body::Frame;
use tokio::io::BufReader;
use tokio_util::io::ReaderStream;
use tracing::info;

use crate::Config;
use crate::storage::Storage;
use crate::types::token::PipelineCancellationToken;
use crate::types::{ObjectChecksum, SyncStatistics};

/// Transfer data from stdin to an S3 object.
///
/// Reads from stdin, wraps it in a synthetic GetObjectOutput, and uploads via
/// target.put_object(). Since the size is unknown, the upload manager will
/// use multipart upload.
pub async fn transfer(
    config: &Config,
    target: Storage,
    target_key: &str,
    cancellation_token: PipelineCancellationToken,
    stats_sender: Sender<SyncStatistics>,
) -> Result<()> {
    if cancellation_token.is_cancelled() {
        return Ok(());
    }

    let target_clone = dyn_clone::clone_box(&*target);

    // Read from stdin
    let stdin = tokio::io::stdin();
    let buf_reader = BufReader::new(stdin);
    let reader_stream = ReaderStream::new(buf_reader).map_ok(Frame::data);
    let stream_body = StreamBody::new(reader_stream);
    let boxed_body = BodyExt::boxed(stream_body);
    let sdk_body = SdkBody::from_body_1_x(boxed_body);
    let byte_stream = ByteStream::new(sdk_body);

    // Build a synthetic GetObjectOutput with the stdin stream
    let get_object_output = GetObjectOutput::builder()
        .set_body(Some(byte_stream))
        .set_content_length(None)
        .set_content_type(config.content_type.clone())
        .set_last_modified(Some(DateTime::from_secs(chrono::Utc::now().timestamp())))
        .build();

    // For stdin, size is unknown so we use 0 and let the upload manager handle it
    // The upload manager will buffer and use multipart upload
    let source_size = 0u64;

    let tagging = if config.disable_tagging {
        None
    } else {
        config.tagging.clone()
    };

    let object_checksum = ObjectChecksum {
        key: target_key.to_string(),
        version_id: None,
        checksum_algorithm: config.additional_checksum_algorithm.clone(),
        checksum_type: None,
        object_parts: None,
        final_checksum: None,
    };

    let _put_object_output = target
        .put_object(
            target_key,
            target_clone,
            source_size,
            None,
            get_object_output,
            tagging,
            Some(object_checksum),
            None,
            None,
            None,
        )
        .await
        .context("stdio_to_s3: target.put_object() failed.")?;

    info!(target_key = target_key, "stdin transfer completed.");

    let _ = stats_sender
        .send(SyncStatistics::SyncComplete {
            key: target_key.to_string(),
        })
        .await;

    Ok(())
}
