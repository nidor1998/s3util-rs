use indicatif::{ProgressBar, ProgressStyle};

pub struct TransferProgress {
    bar: ProgressBar,
    enabled: bool,
}

impl TransferProgress {
    /// Create a progress bar for transfers where size is known
    pub fn with_size(total_bytes: u64, enabled: bool) -> Self {
        if !enabled {
            return Self {
                bar: ProgressBar::hidden(),
                enabled: false,
            };
        }
        let bar = ProgressBar::new(total_bytes);
        bar.set_style(
            ProgressStyle::with_template(
                "[{bar:40.cyan/blue}] {percent}% {bytes}/{total_bytes} {bytes_per_sec} ETA {eta}",
            )
            .unwrap()
            .progress_chars("=>-"),
        );
        Self { bar, enabled: true }
    }

    /// Create a progress bar for transfers where size is unknown (stdin)
    pub fn without_size(enabled: bool) -> Self {
        if !enabled {
            return Self {
                bar: ProgressBar::hidden(),
                enabled: false,
            };
        }
        let bar = ProgressBar::new_spinner();
        bar.set_style(
            ProgressStyle::with_template("{spinner:.green} {bytes} {bytes_per_sec}").unwrap(),
        );
        Self { bar, enabled: true }
    }

    pub fn advance(&self, bytes: u64) {
        self.bar.inc(bytes);
    }

    pub fn finish(&self) {
        if self.enabled {
            self.bar.finish_and_clear();
        }
    }
}
