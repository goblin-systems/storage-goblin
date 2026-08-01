//! Retry policy for transient transfer failures (backlog phase 2.2).
//!
//! A dropped connection or a 503 is not a reason to fail a file — it is a
//! reason to wait a moment and try again. Only [`SyncError::is_retryable`]
//! errors (the `Transient` class from the phase-0.4 taxonomy) are retried;
//! auth failures, missing objects, and precondition mismatches fail
//! immediately, because repeating them cannot change the outcome and would
//! just delay the user's feedback.
//!
//! Backoff is exponential with jitter. The jitter matters when a whole queue
//! resumes after an outage: without it every pending item would retry in
//! lockstep and re-create the thundering herd that caused the failure.

use std::time::Duration;

use super::error::SyncError;

/// How many times an operation is retried after its first failure.
pub(crate) const MAX_RETRY_ATTEMPTS: u32 = 3;

const BASE_DELAY_MS: u64 = 500;
const MAX_DELAY_MS: u64 = 30_000;

/// Delay before the given retry attempt (1-based).
///
/// Exponential from [`BASE_DELAY_MS`], capped at [`MAX_DELAY_MS`], plus up to
/// 25% jitter. A server-supplied `Retry-After` always wins — it is the one
/// party that knows when it will be ready.
pub(crate) fn retry_delay(attempt: u32, retry_after_seconds: Option<u64>) -> Duration {
    if let Some(seconds) = retry_after_seconds {
        return Duration::from_secs(seconds.min(MAX_DELAY_MS / 1000));
    }

    let exponential = BASE_DELAY_MS.saturating_mul(1_u64 << attempt.min(6).saturating_sub(1));
    let capped = exponential.min(MAX_DELAY_MS);
    Duration::from_millis(capped + jitter_ms(capped))
}

/// Deterministic pseudo-jitter derived from the clock, so retries spread out
/// without pulling in a random-number dependency.
fn jitter_ms(capped: u64) -> u64 {
    let spread = capped / 4;
    if spread == 0 {
        return 0;
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % spread
}

/// Run `operation`, retrying transient failures with backoff.
///
/// `describe` names the operation for the log line emitted before each wait.
pub(crate) async fn with_retry<T, F, Fut>(
    describe: impl Fn() -> String,
    mut operation: F,
) -> Result<T, SyncError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, SyncError>>,
{
    let mut attempt = 0;
    loop {
        match operation().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                attempt += 1;
                if attempt > MAX_RETRY_ATTEMPTS || !error.is_retryable() {
                    return Err(error);
                }

                let delay = retry_delay(attempt, error.retry_after_seconds);
                tracing_note(&format!(
                    "{}: transient failure ({}), retry {}/{} in {}ms",
                    describe(),
                    error.message,
                    attempt,
                    MAX_RETRY_ATTEMPTS,
                    delay.as_millis()
                ));
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Retry progress is worth recording, but the activity channel needs an
/// `AppHandle` this layer does not have. Until the phase-0.4 `tracing`
/// migration lands, keep it on stderr rather than dropping it silently.
fn tracing_note(message: &str) {
    eprintln!("[storage-goblin] {message}");
}

#[cfg(test)]
mod tests {
    use super::{retry_delay, with_retry, MAX_RETRY_ATTEMPTS};
    use crate::storage::error::SyncError;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
    }

    #[test]
    fn delay_grows_exponentially_and_is_capped() {
        let first = retry_delay(1, None).as_millis();
        let second = retry_delay(2, None).as_millis();
        let third = retry_delay(3, None).as_millis();

        // Each step at least doubles the base, before jitter.
        assert!((500..=625).contains(&first), "first delay was {first}ms");
        assert!(
            (1000..=1250).contains(&second),
            "second delay was {second}ms"
        );
        assert!((2000..=2500).contains(&third), "third delay was {third}ms");

        // A very late attempt stays under the ceiling (+ jitter).
        assert!(retry_delay(20, None).as_millis() <= 37_500);
    }

    #[test]
    fn a_server_supplied_retry_after_wins_over_backoff() {
        assert_eq!(retry_delay(1, Some(7)).as_secs(), 7);
        // …but cannot park the queue indefinitely.
        assert_eq!(retry_delay(1, Some(9_999)).as_secs(), 30);
    }

    #[test]
    fn a_transient_failure_is_retried_until_it_succeeds() {
        let calls = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&calls);

        let result = runtime().block_on(with_retry(
            || "test upload".to_string(),
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    if seen.fetch_add(1, Ordering::SeqCst) < 2 {
                        Err(SyncError::transient("connection reset"))
                    } else {
                        Ok(42)
                    }
                }
            },
        ));

        assert_eq!(result.expect("should eventually succeed"), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two failures then success");
    }

    #[test]
    fn a_permanent_failure_is_not_retried() {
        let calls = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&calls);

        let result: Result<(), SyncError> = runtime().block_on(with_retry(
            || "test upload".to_string(),
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    // Wrong credentials will still be wrong in 500ms.
                    Err(SyncError::auth("access denied"))
                }
            },
        ));

        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "auth errors must fail fast"
        );
    }

    #[test]
    fn retries_are_bounded() {
        let calls = Arc::new(AtomicU32::new(0));
        let seen = Arc::clone(&calls);

        let result: Result<(), SyncError> = runtime().block_on(with_retry(
            || "test upload".to_string(),
            move || {
                let seen = Arc::clone(&seen);
                async move {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Err(SyncError::transient("still down"))
                }
            },
        ));

        assert!(result.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            MAX_RETRY_ATTEMPTS + 1,
            "one initial attempt plus the retry budget, then give up"
        );
    }
}
