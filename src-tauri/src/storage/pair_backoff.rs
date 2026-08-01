//! Backoff for pairs whose sync cycle keeps failing (backlog phase 2.2/2.4).
//!
//! The polling worker schedules a pair from its `last_sync_at`. A failed cycle
//! never sets that, so a pair that cannot sync is *immediately* due again —
//! with no successful sync ever recorded, the worker spins as fast as the loop
//! allows. Wrong credentials or a pulled network cable therefore meant a tight
//! loop: continuous provider calls, continuous error activity, and a log the
//! user could not read past.
//!
//! So a failed cycle now gates the pair, growing the wait as failures repeat:
//! 30s, 1m, 2m, 4m … capped at 15 minutes. Short at first because the common
//! failure is a blip and the user is waiting; capped so that even a permanently
//! broken pair still re-probes, and a fix made outside the app is noticed
//! without a restart.
//!
//! Nothing here is a dead end. A successful cycle clears the gate, and a manual
//! sync ignores it entirely — an explicit "sync now" is precisely the signal
//! that the user believes they have fixed the problem.
//!
//! **Not yet kind-aware.** An expired credential cannot be fixed by waiting, so
//! it ideally jumps straight to the ceiling instead of walking up to it. That
//! needs `run_sync_cycle_for_pair` to report a typed [`SyncError`] — today it
//! signals failure by returning `Ok(status)` with `phase: "error"`, which says
//! *that* it failed but not *why*. The curve below reaches the ceiling after
//! six failures either way, so the cost of not distinguishing them is a handful
//! of extra log lines in the first half hour, not a hot loop.
//!
//! [`SyncError`]: super::error::SyncError

use std::time::Duration;

/// Longest a pair is ever gated. Even a permanently broken pair re-probes this
/// often, so a fix made outside the app is eventually noticed on its own.
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(15 * 60);

/// First delay after a failure. Short, because the common case is a blip and
/// the user is waiting for their file to appear.
pub(crate) const BASE_BACKOFF: Duration = Duration::from_secs(30);

/// How long to gate a pair after `consecutive_failures` failed cycles (1 for
/// the first failure).
pub(crate) fn backoff_after_failure(consecutive_failures: u32) -> Duration {
    // Saturating shift: a pair that has failed 40 times must not wrap to zero
    // and resurrect the hot loop this module exists to prevent.
    // Cap the shift before applying it: `1 << 32` is undefined for u32 and a
    // wrap to zero here would silently restore the hot loop.
    let steps = consecutive_failures.saturating_sub(1).min(20);
    BASE_BACKOFF.saturating_mul(1_u32 << steps).min(MAX_BACKOFF)
}

#[cfg(test)]
mod tests {
    use super::{backoff_after_failure, BASE_BACKOFF, MAX_BACKOFF};
    use std::time::Duration;

    #[test]
    fn the_first_failures_back_off_gently() {
        assert_eq!(backoff_after_failure(1), Duration::from_secs(30));
        assert_eq!(backoff_after_failure(2), Duration::from_secs(60));
        assert_eq!(backoff_after_failure(3), Duration::from_secs(120));
        assert_eq!(backoff_after_failure(4), Duration::from_secs(240));
    }

    #[test]
    fn a_long_outage_stops_growing_at_the_ceiling() {
        assert_eq!(backoff_after_failure(6), MAX_BACKOFF);
        assert_eq!(backoff_after_failure(50), MAX_BACKOFF);
    }

    #[test]
    fn an_absurd_failure_count_never_wraps_back_to_zero() {
        // A shift overflow here would silently restore the hot loop, and it
        // would only show up after a pair had been failing for a long time.
        for failures in [u32::MAX, u32::MAX - 1, 1_000_000, 33, 32] {
            assert_eq!(
                backoff_after_failure(failures),
                MAX_BACKOFF,
                "failure count {failures} must stay capped"
            );
        }
    }

    #[test]
    fn no_failure_count_ever_yields_a_zero_wait() {
        // Zero is the whole bug: it makes the pair immediately due again.
        for failures in [0, 1, 2, 7, 21, 64, u32::MAX] {
            assert!(
                backoff_after_failure(failures) >= BASE_BACKOFF,
                "failure count {failures} produced a wait short enough to spin"
            );
        }
    }
}
