//! Byte-level transfer progress (backlog phase 2.1).
//!
//! A 20 GB file used to be a single opaque "in progress" for hours. The writer
//! already counted bytes as it streamed; this turns that count into something
//! the UI can show, at a rate a UI can actually consume.
//!
//! The rate limit is the whole design problem. A download emitting an event per
//! 8 KiB chunk would push ~2.5 million events for a 20 GB file, and the cost of
//! serializing them across the IPC boundary would measurably slow the transfer
//! it is reporting on. So updates are throttled to roughly 4 Hz — fast enough
//! that a progress bar looks continuous, slow enough to be free.
//!
//! Two updates are never dropped: the first, so a bar appears immediately
//! rather than a quarter-second late, and the last, so it finishes at 100%
//! instead of freezing at 97% forever.
//!
//! Tauri-free so the throttling and rate maths can be tested directly.

use std::time::{Duration, Instant};

/// Minimum gap between progress events for one transfer (~4 Hz).
const MIN_INTERVAL: Duration = Duration::from_millis(250);

/// One progress update for one file.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct TransferProgress {
    pub pair_id: String,
    pub path: String,
    pub bytes_done: u64,
    /// `None` when the provider did not say how big the object is; the UI
    /// shows an indeterminate bar rather than inventing a denominator.
    pub bytes_total: Option<u64>,
    /// Bytes per second over the life of this transfer, once measurable.
    pub bytes_per_second: Option<u64>,
}

impl TransferProgress {
    /// Completion in the range 0.0..=1.0, when the total is known.
    pub fn fraction(&self) -> Option<f64> {
        let total = self.bytes_total?;
        if total == 0 {
            // A zero-byte object is complete the moment it exists; reporting
            // 0/0 as "0%" would leave the bar stuck at empty.
            return Some(1.0);
        }
        Some((self.bytes_done as f64 / total as f64).clamp(0.0, 1.0))
    }
}

/// Decides when a transfer should report progress.
pub(crate) struct ProgressThrottle {
    pair_id: String,
    path: String,
    bytes_total: Option<u64>,
    started_at: Instant,
    last_emit_at: Option<Instant>,
}

impl ProgressThrottle {
    pub fn new(pair_id: &str, path: &str, bytes_total: Option<u64>, now: Instant) -> Self {
        Self {
            pair_id: pair_id.to_string(),
            path: path.to_string(),
            bytes_total,
            started_at: now,
            last_emit_at: None,
        }
    }

    /// The update to emit now, or `None` to stay quiet.
    ///
    /// `final_update` forces one through regardless of timing — a transfer that
    /// ends between ticks must still report its completion.
    pub fn update(
        &mut self,
        now: Instant,
        bytes_done: u64,
        final_update: bool,
    ) -> Option<TransferProgress> {
        let due = match self.last_emit_at {
            // The first update always goes out, so a bar appears at once.
            None => true,
            Some(last) => now.duration_since(last) >= MIN_INTERVAL,
        };

        if !due && !final_update {
            return None;
        }

        self.last_emit_at = Some(now);
        Some(TransferProgress {
            pair_id: self.pair_id.clone(),
            path: self.path.clone(),
            bytes_done,
            bytes_total: self.bytes_total,
            bytes_per_second: self.rate(now, bytes_done),
        })
    }

    /// Average throughput so far.
    ///
    /// `None` for the first instants: dividing by a near-zero elapsed time
    /// produces a wild number that makes any "time remaining" estimate built on
    /// it jump around, which reads as a broken UI.
    fn rate(&self, now: Instant, bytes_done: u64) -> Option<u64> {
        let elapsed = now.duration_since(self.started_at);
        if elapsed < Duration::from_millis(100) {
            return None;
        }
        let seconds = elapsed.as_secs_f64();
        (seconds > 0.0).then(|| (bytes_done as f64 / seconds) as u64)
    }
}

/// A throttle plus somewhere to send the updates it allows.
///
/// Bundled so the transfer signatures carry one optional argument rather than
/// two that must be kept in step.
pub(crate) struct ProgressReporter {
    throttle: ProgressThrottle,
    sink: Box<dyn FnMut(TransferProgress) + Send>,
}

impl ProgressReporter {
    pub fn new(
        throttle: ProgressThrottle,
        sink: impl FnMut(TransferProgress) + Send + 'static,
    ) -> Self {
        Self {
            throttle,
            sink: Box::new(sink),
        }
    }

    /// Report `bytes_done`, if the throttle allows it.
    pub fn report(&mut self, bytes_done: u64, final_update: bool) {
        if let Some(update) = self
            .throttle
            .update(Instant::now(), bytes_done, final_update)
        {
            (self.sink)(update);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ProgressThrottle, TransferProgress, MIN_INTERVAL};
    use std::time::{Duration, Instant};

    fn throttle(total: Option<u64>) -> (ProgressThrottle, Instant) {
        let start = Instant::now();
        (
            ProgressThrottle::new("pair-a", "big.iso", total, start),
            start,
        )
    }

    #[test]
    fn the_first_update_is_always_emitted() {
        let (mut throttle, start) = throttle(Some(1000));
        // Otherwise the UI shows nothing for the first quarter second, which
        // on a small file is the entire transfer.
        assert!(throttle.update(start, 0, false).is_some());
    }

    #[test]
    fn updates_between_ticks_are_dropped() {
        let (mut throttle, start) = throttle(Some(1000));
        throttle.update(start, 0, false).expect("first");

        // A chunked download would otherwise emit millions of these.
        assert!(throttle
            .update(start + Duration::from_millis(10), 100, false)
            .is_none());
        assert!(throttle
            .update(start + Duration::from_millis(200), 200, false)
            .is_none());
        assert!(throttle.update(start + MIN_INTERVAL, 300, false).is_some());
    }

    #[test]
    fn the_final_update_is_never_dropped() {
        let (mut throttle, start) = throttle(Some(1000));
        throttle.update(start, 0, false).expect("first");

        // A transfer finishing between ticks must not leave the bar stuck.
        let last = throttle
            .update(start + Duration::from_millis(5), 1000, true)
            .expect("the final update must always be emitted");
        assert_eq!(last.bytes_done, 1000);
        assert_eq!(last.fraction(), Some(1.0));
    }

    #[test]
    fn the_rate_is_withheld_until_it_is_meaningful() {
        let (mut throttle, start) = throttle(Some(1_000_000));

        // 1 byte in 1 microsecond is a megabyte a second — a number that would
        // make any "time remaining" estimate built on it useless.
        let first = throttle.update(start, 1, false).expect("first");
        assert_eq!(first.bytes_per_second, None);

        let later = throttle
            .update(start + Duration::from_secs(2), 2_000_000, true)
            .expect("later");
        assert_eq!(later.bytes_per_second, Some(1_000_000));
    }

    #[test]
    fn an_unknown_total_reports_no_fraction_rather_than_a_wrong_one() {
        let (mut throttle, start) = throttle(None);
        let update = throttle.update(start, 500, false).expect("first");

        assert_eq!(update.bytes_total, None);
        // Inventing a denominator would show a bar that is confidently wrong.
        assert_eq!(update.fraction(), None);
    }

    #[test]
    fn a_zero_byte_object_reports_complete_rather_than_empty() {
        let progress = TransferProgress {
            pair_id: "p".into(),
            path: "empty.txt".into(),
            bytes_done: 0,
            bytes_total: Some(0),
            bytes_per_second: None,
        };
        assert_eq!(progress.fraction(), Some(1.0));
    }

    #[test]
    fn a_fraction_never_exceeds_one() {
        // Providers do occasionally under-report Content-Length; the bar must
        // not overflow its track because of it.
        let progress = TransferProgress {
            pair_id: "p".into(),
            path: "x".into(),
            bytes_done: 150,
            bytes_total: Some(100),
            bytes_per_second: None,
        };
        assert_eq!(progress.fraction(), Some(1.0));
    }
}
