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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_size_disabled_creates_hidden_bar() {
        let p = TransferProgress::with_size(1024, false);
        assert!(!p.enabled);
        assert!(p.bar.is_hidden());
    }

    #[test]
    fn with_size_enabled_creates_bar_with_length() {
        // We don't assert visibility (depends on TTY), only that the bar carries
        // the expected length and is marked enabled.
        let p = TransferProgress::with_size(1024, true);
        assert!(p.enabled);
        assert_eq!(p.bar.length(), Some(1024));
    }

    #[test]
    fn without_size_disabled_creates_hidden_bar() {
        let p = TransferProgress::without_size(false);
        assert!(!p.enabled);
        assert!(p.bar.is_hidden());
    }

    #[test]
    fn without_size_enabled_creates_spinner_without_length() {
        let p = TransferProgress::without_size(true);
        assert!(p.enabled);
        // new_spinner() has no fixed length.
        assert_eq!(p.bar.length(), None);
    }

    #[test]
    fn advance_increments_position() {
        let p = TransferProgress::with_size(1000, true);
        p.advance(250);
        p.advance(100);
        assert_eq!(p.bar.position(), 350);
    }

    #[test]
    fn finish_is_safe_on_disabled_bar() {
        // Smoke check: finish() must not panic on a disabled progress bar.
        let p = TransferProgress::with_size(1024, false);
        p.finish();
    }

    #[test]
    fn finish_is_safe_on_enabled_bar() {
        let p = TransferProgress::with_size(1024, true);
        p.finish();
    }
}
