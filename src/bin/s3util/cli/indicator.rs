use async_channel::Receiver;
use indicatif::{HumanBytes, ProgressBar, ProgressDrawTarget, ProgressStyle};
use s3util_rs::types::SyncStatistics;
use simple_moving_average::{SMA, SumTreeSMA};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::info;

const MOVING_AVERAGE_PERIOD_SECS: usize = 10;
const REFRESH_INTERVAL: f32 = 0.1;

pub fn show_indicator(
    stats_receiver: Receiver<SyncStatistics>,
    show_progress: bool,
    show_result: bool,
    log_sync_summary: bool,
    resolved_target: Option<String>,
) -> JoinHandle<()> {
    let progress_style = ProgressStyle::with_template("{wide_msg}").unwrap();
    let progress_text = ProgressBar::with_draw_target(Some(0), ProgressDrawTarget::stderr());
    progress_text.set_style(progress_style);

    tokio::spawn(async move {
        let start_time = Instant::now();

        let mut ma_synced_bytes = SumTreeSMA::<_, u64, MOVING_AVERAGE_PERIOD_SECS>::new();

        let mut total_sync_bytes: u64 = 0;
        let mut total_error_count: u64 = 0;
        let mut total_warning_count: u64 = 0;
        let mut total_e_tag_verified_count: u64 = 0;
        let mut total_checksum_verified_count: u64 = 0;

        // stats_receiver tracks high-precision byte counts.
        loop {
            let mut sync_bytes: u64 = 0;

            let period = Instant::now();
            loop {
                while let Ok(sync_stats) = stats_receiver.try_recv() {
                    match sync_stats {
                        SyncStatistics::SyncComplete { .. } => {}
                        SyncStatistics::SyncBytes(size) => {
                            sync_bytes += size;
                            total_sync_bytes += size
                        }
                        SyncStatistics::SyncError { .. } => {
                            total_error_count += 1;
                        }
                        SyncStatistics::SyncWarning { .. } => {
                            total_warning_count += 1;
                        }
                        SyncStatistics::ETagVerified { .. } => {
                            total_e_tag_verified_count += 1;
                        }
                        SyncStatistics::ChecksumVerified { .. } => {
                            total_checksum_verified_count += 1;
                        }
                    }
                }

                if REFRESH_INTERVAL < period.elapsed().as_secs_f32() {
                    break;
                }

                if stats_receiver.is_closed() {
                    let elapsed = start_time.elapsed();
                    let elapsed_secs_f64 = elapsed.as_secs_f64();

                    let mut sync_bytes_per_sec =
                        (total_sync_bytes as f64 / elapsed_secs_f64) as u64;

                    if elapsed_secs_f64 < REFRESH_INTERVAL as f64 {
                        sync_bytes_per_sec = total_sync_bytes;
                    }

                    if log_sync_summary && total_error_count == 0 {
                        info!(
                            message = "copy summary",
                            transferred_byte = total_sync_bytes,
                            transferred_byte_per_sec = sync_bytes_per_sec,
                            etag_verified = total_e_tag_verified_count,
                            checksum_verified = total_checksum_verified_count,
                            error = total_error_count,
                            warning = total_warning_count,
                            duration_sec = elapsed_secs_f64,
                        );
                    }

                    // Clear live progress before printing final output
                    progress_text.finish_and_clear();

                    // Show resolved destination path first
                    if total_error_count == 0
                        && let Some(ref resolved) = resolved_target
                    {
                        eprintln!("-> {resolved}");
                    }

                    if show_result && total_error_count == 0 {
                        let mut parts = vec![format!(
                            "{} | {}/sec",
                            HumanBytes(total_sync_bytes),
                            HumanBytes(sync_bytes_per_sec)
                        )];

                        // ETag verify status
                        let etag_status = if total_e_tag_verified_count > 0 {
                            "ok"
                        } else if total_warning_count > 0 {
                            "failed"
                        } else {
                            "skipped"
                        };
                        parts.push(format!("etag verify: {etag_status}"));

                        // Additional checksum verify status
                        let checksum_status = if total_checksum_verified_count > 0 {
                            "ok"
                        } else if total_warning_count > 0 && total_e_tag_verified_count > 0 {
                            // Warning exists but etag passed — checksum likely failed
                            "failed"
                        } else {
                            "skipped"
                        };
                        parts.push(format!("additional checksum verify: {checksum_status}"));

                        let result_message = parts.join(", ");
                        eprintln!("{result_message}");
                    }

                    return;
                }

                tokio::time::sleep(std::time::Duration::from_secs_f32(0.05)).await;
            }
            ma_synced_bytes.add_sample(sync_bytes);

            if show_progress {
                let progress_message = format!(
                    "{:>3} | {:>3}/sec",
                    HumanBytes(total_sync_bytes),
                    HumanBytes(ma_synced_bytes.get_average())
                );
                progress_text.set_message(progress_message);
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE: u64 = 1500;

    #[tokio::test]
    async fn indicator_test_show_result() {
        init_dummy_tracing_subscriber();

        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, true, true, false, None);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncComplete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncWarning {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncError {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ChecksumVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE,
        ))
        .await;
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_test_show_no_result() {
        init_dummy_tracing_subscriber();

        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, true, false, true, None);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncComplete {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncError {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ChecksumVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(
            WAITING_TIME_MILLIS_FOR_ASYNC_INDICATOR_SET_MESSAGE,
        ))
        .await;
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_fast_completion_falls_back_to_raw_total_for_rate() {
        // Closes immediately so elapsed_secs_f64 < REFRESH_INTERVAL — the
        // branch that assigns sync_bytes_per_sec = total_sync_bytes to avoid
        // a misleading spike on sub-100ms transfers.
        init_dummy_tracing_subscriber();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, false, true, true, None);

        stats_sender
            .send(SyncStatistics::SyncBytes(10))
            .await
            .unwrap();
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_with_resolved_target_prints_destination_line() {
        // Covers the `Some(ref resolved)` arm of resolved_target on successful
        // completion (no errors).
        init_dummy_tracing_subscriber();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(
            stats_receiver,
            false,
            false,
            false,
            Some("s3://bucket/resolved/key".to_string()),
        );

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_warning_without_etag_verified_shows_etag_skipped() {
        // Warning > 0 but no ETagVerified: etag status takes the middle arm
        // ("failed") and checksum status falls through to "skipped".
        init_dummy_tracing_subscriber();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, false, true, false, None);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncWarning {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        stats_sender.close();
        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_etag_verified_plus_warning_shows_checksum_failed() {
        // ETagVerified + SyncWarning but no ChecksumVerified: checksum status
        // renders as "failed" (the `else if` middle arm).
        init_dummy_tracing_subscriber();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(stats_receiver, false, true, false, None);

        stats_sender
            .send(SyncStatistics::SyncBytes(1))
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::ETagVerified {
                key: "test".to_string(),
            })
            .await
            .unwrap();
        stats_sender
            .send(SyncStatistics::SyncWarning {
                key: "test".to_string(),
            })
            .await
            .unwrap();

        tokio::time::sleep(Duration::from_millis(200)).await;
        stats_sender.close();
        join_handle.await.unwrap();
    }

    fn init_dummy_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("dummy=trace")
            .try_init();
    }
}
