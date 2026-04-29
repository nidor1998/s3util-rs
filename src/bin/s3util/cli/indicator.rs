use async_channel::Receiver;
use indicatif::{HumanBytes, ProgressBar, ProgressDrawTarget, ProgressStyle};
use s3util_rs::types::SyncStatistics;
use simple_moving_average::{SMA, SumTreeSMA};
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tracing::info;

const MOVING_AVERAGE_PERIOD_SECS: usize = 10;
const REFRESH_INTERVAL: f32 = 1.0;

fn verification_status(
    etag_verified: u64,
    etag_mismatch: u64,
    checksum_verified: u64,
    checksum_mismatch: u64,
) -> (&'static str, &'static str) {
    let etag = if etag_verified > 0 {
        "ok"
    } else if etag_mismatch > 0 {
        "failed"
    } else {
        "skipped"
    };
    let checksum = if checksum_verified > 0 {
        "ok"
    } else if checksum_mismatch > 0 {
        "failed"
    } else {
        "skipped"
    };
    (etag, checksum)
}

pub fn show_indicator(
    stats_receiver: Receiver<SyncStatistics>,
    show_progress: bool,
    show_result: bool,
    log_sync_summary: bool,
    resolved_target: String,
    source_key: String,
    target_key: String,
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
        let mut total_e_tag_mismatch_count: u64 = 0;
        let mut total_checksum_verified_count: u64 = 0;
        let mut total_checksum_mismatch_count: u64 = 0;

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
                        SyncStatistics::ETagMismatch { .. } => {
                            total_e_tag_mismatch_count += 1;
                            total_warning_count += 1;
                        }
                        SyncStatistics::ChecksumVerified { .. } => {
                            total_checksum_verified_count += 1;
                        }
                        SyncStatistics::ChecksumMismatch { .. } => {
                            total_checksum_mismatch_count += 1;
                            total_warning_count += 1;
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
                            message = "Transfer completed.",
                            source_key = source_key,
                            target_key = target_key,
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

                    if show_result && total_error_count == 0 {
                        eprintln!("-> {resolved_target}");

                        let mut parts = vec![format!(
                            "Transferred: {} | {}/sec",
                            HumanBytes(total_sync_bytes),
                            HumanBytes(sync_bytes_per_sec)
                        )];

                        let (etag_status, checksum_status) = verification_status(
                            total_e_tag_verified_count,
                            total_e_tag_mismatch_count,
                            total_checksum_verified_count,
                            total_checksum_mismatch_count,
                        );
                        parts.push(format!("etag verify: {etag_status}"));
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
                    "Transferred: {:>3} | {:>3}/sec",
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
        let join_handle = show_indicator(
            stats_receiver,
            true,
            true,
            false,
            String::new(),
            String::new(),
            String::new(),
        );

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
        let join_handle = show_indicator(
            stats_receiver,
            true,
            false,
            true,
            String::new(),
            "src".to_string(),
            "dst".to_string(),
        );

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
        let join_handle = show_indicator(
            stats_receiver,
            false,
            true,
            true,
            String::new(),
            "src".to_string(),
            "dst".to_string(),
        );

        stats_sender
            .send(SyncStatistics::SyncBytes(10))
            .await
            .unwrap();
        stats_sender.close();

        join_handle.await.unwrap();
    }

    #[tokio::test]
    async fn indicator_with_resolved_target_prints_destination_line() {
        // Destination line is printed on successful completion (no errors),
        // gated on `show_result` so `--show-progress` controls whether the
        // line is printed.
        init_dummy_tracing_subscriber();
        let (stats_sender, stats_receiver) = async_channel::unbounded();
        let join_handle = show_indicator(
            stats_receiver,
            false,
            true,
            false,
            "s3://bucket/resolved/key".to_string(),
            String::new(),
            String::new(),
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
        let join_handle = show_indicator(
            stats_receiver,
            false,
            true,
            false,
            String::new(),
            String::new(),
            String::new(),
        );

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
        let join_handle = show_indicator(
            stats_receiver,
            false,
            true,
            false,
            String::new(),
            String::new(),
            String::new(),
        );

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

    #[test]
    fn verification_status_etag_skipped_and_checksum_failed() {
        // Regression test: ETag verification was never attempted (e.g. SSE-C
        // source), and the additional checksum verification failed. Before the
        // per-verification counters were introduced, the aggregate warning
        // counter made ETag render as "failed" and checksum as "skipped"
        // — both incorrect. Expected: etag skipped, checksum failed.
        let (etag, checksum) = verification_status(
            0, // etag_verified
            0, // etag_mismatch
            0, // checksum_verified
            1, // checksum_mismatch
        );
        assert_eq!(etag, "skipped");
        assert_eq!(checksum, "failed");
    }

    #[test]
    fn verification_status_etag_failed_and_checksum_skipped() {
        // Symmetric case: ETag mismatched, checksum verification not performed.
        let (etag, checksum) = verification_status(0, 1, 0, 0);
        assert_eq!(etag, "failed");
        assert_eq!(checksum, "skipped");
    }

    #[test]
    fn verification_status_both_verified() {
        let (etag, checksum) = verification_status(1, 0, 1, 0);
        assert_eq!(etag, "ok");
        assert_eq!(checksum, "ok");
    }

    #[test]
    fn verification_status_etag_ok_and_checksum_failed() {
        let (etag, checksum) = verification_status(1, 0, 0, 1);
        assert_eq!(etag, "ok");
        assert_eq!(checksum, "failed");
    }

    #[test]
    fn verification_status_etag_failed_and_checksum_ok() {
        let (etag, checksum) = verification_status(0, 1, 1, 0);
        assert_eq!(etag, "failed");
        assert_eq!(checksum, "ok");
    }

    #[test]
    fn verification_status_both_skipped() {
        let (etag, checksum) = verification_status(0, 0, 0, 0);
        assert_eq!(etag, "skipped");
        assert_eq!(checksum, "skipped");
    }
}
