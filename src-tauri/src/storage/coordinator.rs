//! Who is allowed to do what, right now (backlog phase 3.3).
//!
//! `SyncState` was a bag of independently-locked fields. Every field was
//! individually safe and the *combination* enforced nothing, so the invariant
//! that actually matters — **at most one sync cycle per pair at a time** — was
//! not represented anywhere. It held only by luck of scheduling.
//!
//! It did not always hold. `start_sync` calls `stop_polling_worker`, which
//! *signals* the worker but does not wait for it; the worker could be halfway
//! through a cycle for the same pair. Two cycles would then scan the same tree,
//! rebuild the same plan, and drain the same durable queue concurrently —
//! double-uploading, or racing each other's anchor writes.
//!
//! So the invariant gets a type. A [`PairLease`] *is* the right to run a cycle
//! for one pair: you cannot run one without holding it, and it is released on
//! drop, including on panic or early return. The lock lives across `.await`
//! points, which is why it is a `tokio::sync::Mutex` and not a `std` one.
//!
//! The coordinator also owns two things the lease model makes possible:
//!
//! - **Cancellation** per pair, so pause and shutdown can interrupt a running
//!   cycle instead of waiting for it to finish on its own.
//! - **A global transfer budget**, so phase 2.2 can run transfers concurrently
//!   without every pair independently deciding to open four connections.
//!
//! Deliberately Tauri-free: this is the piece with the subtle concurrency
//! semantics, so it must be testable without the app framework — which matters
//! doubly here, because the Tauri mock runtime does not load on the development
//! machine and anything behind it cannot be run at all.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard, OwnedSemaphorePermit, Semaphore};

/// How many transfers may be in flight at once, across all pairs.
///
/// Four is the usual sweet spot for consumer uplinks: enough to hide
/// per-request latency, few enough that no single sync saturates the link or
/// trips provider rate limits.
pub(crate) const DEFAULT_MAX_CONCURRENT_TRANSFERS: usize = 4;

/// Per-pair runtime coordination: the cycle lock and the cancel flag.
struct PairRuntime {
    /// Held for the duration of a cycle. Async because a cycle awaits.
    cycle_lock: Arc<AsyncMutex<()>>,
    /// Set to ask the *current* cycle to stop early.
    cancel: Arc<AtomicBool>,
}

impl PairRuntime {
    fn new() -> Self {
        Self {
            cycle_lock: Arc::new(AsyncMutex::new(())),
            cancel: Arc::new(AtomicBool::new(false)),
        }
    }
}

/// The exclusive right to run a sync cycle for one pair.
///
/// Cannot be constructed except by the coordinator, and releases the pair on
/// drop — so an early `return`, a `?`, or a panic all free it. That is the
/// point: a lock you can forget to release is a lock that will be forgotten.
pub(crate) struct PairLease {
    pair_id: String,
    cancel: Arc<AtomicBool>,
    /// Ownership of the pair's cycle lock; dropping it admits the next cycle.
    _guard: OwnedMutexGuard<()>,
}

impl PairLease {
    pub(crate) fn pair_id(&self) -> &str {
        &self.pair_id
    }

    /// The flag a running cycle polls to notice it should stop.
    ///
    /// Handed out as an `Arc` so it can be passed to the existing
    /// `stop_signal: Option<&AtomicBool>` plumbing without reworking every
    /// cycle stage at once.
    pub(crate) fn cancel_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.cancel)
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancel.load(Ordering::SeqCst)
    }
}

/// A slot in the global transfer budget. Returns the permit on drop.
pub(crate) struct TransferSlot {
    _permit: OwnedSemaphorePermit,
}

/// Runtime coordination for all pairs.
pub(crate) struct SyncCoordinator {
    pairs: Mutex<HashMap<String, Arc<PairRuntime>>>,
    transfers: Arc<Semaphore>,
    max_concurrent_transfers: usize,
}

impl Default for SyncCoordinator {
    fn default() -> Self {
        Self::with_transfer_limit(DEFAULT_MAX_CONCURRENT_TRANSFERS)
    }
}

impl SyncCoordinator {
    pub(crate) fn with_transfer_limit(max_concurrent_transfers: usize) -> Self {
        // Zero permits would deadlock every transfer forever, which is a far
        // worse failure than ignoring a nonsensical setting.
        let max_concurrent_transfers = max_concurrent_transfers.max(1);
        Self {
            pairs: Mutex::new(HashMap::new()),
            transfers: Arc::new(Semaphore::new(max_concurrent_transfers)),
            max_concurrent_transfers,
        }
    }

    fn runtime_for(&self, pair_id: &str) -> Arc<PairRuntime> {
        let mut pairs = self
            .pairs
            .lock()
            // The map holds only Arcs and a bool; a poisoned lock cannot have
            // left it torn, so recovering is safe and beats killing sync.
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Arc::clone(
            pairs
                .entry(pair_id.to_string())
                .or_insert_with(|| Arc::new(PairRuntime::new())),
        )
    }

    /// Take the pair's cycle lease, waiting for any cycle already running.
    ///
    /// Use this when the work must happen — a manual "sync now". The caller
    /// queues behind the in-flight cycle rather than racing it.
    pub(crate) async fn lease_pair(&self, pair_id: &str) -> PairLease {
        let runtime = self.runtime_for(pair_id);
        let guard = Arc::clone(&runtime.cycle_lock).lock_owned().await;
        Self::fresh_lease(pair_id, &runtime, guard)
    }

    /// Take the pair's cycle lease if it is free, otherwise `None`.
    ///
    /// Use this for scheduled work. A pair that is already syncing does not
    /// need a second cycle queued behind the first — by the time it ran, its
    /// plan would be stale anyway, and queueing them is how a slow pair turns
    /// a poll interval into an unbounded backlog.
    pub(crate) fn try_lease_pair(&self, pair_id: &str) -> Option<PairLease> {
        let runtime = self.runtime_for(pair_id);
        let guard = Arc::clone(&runtime.cycle_lock).try_lock_owned().ok()?;
        Some(Self::fresh_lease(pair_id, &runtime, guard))
    }

    fn fresh_lease(pair_id: &str, runtime: &PairRuntime, guard: OwnedMutexGuard<()>) -> PairLease {
        // Clear any cancellation aimed at the cycle that just finished. A new
        // cycle must not inherit a stop request meant for its predecessor.
        runtime.cancel.store(false, Ordering::SeqCst);
        PairLease {
            pair_id: pair_id.to_string(),
            cancel: Arc::clone(&runtime.cancel),
            _guard: guard,
        }
    }

    /// Is a cycle currently running for this pair?
    #[cfg(test)]
    pub(crate) fn is_pair_busy(&self, pair_id: &str) -> bool {
        self.runtime_for(pair_id).cycle_lock.try_lock().is_err()
    }

    /// Ask the pair's running cycle to stop at its next checkpoint.
    ///
    /// Does not wait: cancellation is cooperative, and the cycle checks between
    /// stages. Returns whether a cycle was actually running to receive it.
    pub(crate) fn cancel_pair(&self, pair_id: &str) -> bool {
        let runtime = self.runtime_for(pair_id);
        let was_running = runtime.cycle_lock.try_lock().is_err();
        runtime.cancel.store(true, Ordering::SeqCst);
        was_running
    }

    /// Ask every running cycle to stop. Used on shutdown and on pause.
    pub(crate) fn cancel_all(&self) -> usize {
        let pairs = self
            .pairs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pairs
            .values()
            .filter(|runtime| {
                let was_running = runtime.cycle_lock.try_lock().is_err();
                runtime.cancel.store(true, Ordering::SeqCst);
                was_running
            })
            .count()
    }

    /// Drop runtime entries for pairs that no longer exist.
    ///
    /// Without this the map grows for the life of the process as locations are
    /// added and removed. A pair mid-cycle is kept regardless — forgetting it
    /// would hand a second lease to a pair that already has one.
    pub(crate) fn retain_pairs(&self, live_pair_ids: &[String]) {
        let mut pairs = self
            .pairs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        pairs.retain(|pair_id, runtime| {
            live_pair_ids.iter().any(|live| live == pair_id)
                || runtime.cycle_lock.try_lock().is_err()
        });
    }

    /// Wait for a slot in the global transfer budget.
    ///
    /// Held for the duration of one transfer; dropping it admits the next.
    pub(crate) async fn acquire_transfer_slot(&self) -> TransferSlot {
        let permit = Arc::clone(&self.transfers)
            .acquire_owned()
            .await
            // The semaphore is owned by this struct and never closed; if that
            // ever changes, failing loudly beats transferring unbounded.
            .expect("transfer semaphore is never closed");
        TransferSlot { _permit: permit }
    }

    /// How many transfer slots are free right now.
    #[cfg(test)]
    pub(crate) fn available_transfer_slots(&self) -> usize {
        self.transfers.available_permits()
    }

    /// The configured transfer ceiling, for callers sizing their own buffers.
    pub(crate) fn max_concurrent_transfers(&self) -> usize {
        self.max_concurrent_transfers
    }
}

#[cfg(test)]
mod tests {
    use super::{SyncCoordinator, DEFAULT_MAX_CONCURRENT_TRANSFERS};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime should build")
    }

    #[test]
    fn a_pair_can_only_be_leased_once_at_a_time() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let first = coordinator.lease_pair("pair-a").await;
            assert_eq!(first.pair_id(), "pair-a");

            // This is the invariant the old shared-mutex state never had: a
            // second cycle for the same pair cannot start.
            assert!(coordinator.try_lease_pair("pair-a").is_none());
            assert!(coordinator.is_pair_busy("pair-a"));

            drop(first);
            assert!(coordinator.try_lease_pair("pair-a").is_some());
        });
    }

    #[test]
    fn different_pairs_do_not_block_each_other() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let _a = coordinator.lease_pair("pair-a").await;
            let b = coordinator.try_lease_pair("pair-b");
            assert!(b.is_some(), "pairs must sync independently");
            assert!(!coordinator.is_pair_busy("pair-c"));
        });
    }

    #[test]
    fn the_lease_is_released_even_when_the_cycle_panics() {
        let coordinator = Arc::new(SyncCoordinator::default());

        let result = {
            let coordinator = Arc::clone(&coordinator);
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                runtime().block_on(async {
                    let _lease = coordinator.lease_pair("pair-a").await;
                    panic!("cycle blew up");
                })
            }))
        };

        assert!(result.is_err(), "the panic should propagate");
        // A leaked lease would wedge this pair until restart — the exact class
        // of bug that made the old ad-hoc locking untrustworthy.
        assert!(
            coordinator.try_lease_pair("pair-a").is_some(),
            "a panicking cycle must not wedge its pair forever"
        );
    }

    #[test]
    fn a_manual_sync_waits_for_the_running_cycle_instead_of_racing_it() {
        let coordinator = Arc::new(SyncCoordinator::default());
        let order = Arc::new(std::sync::Mutex::new(Vec::<&'static str>::new()));

        runtime().block_on(async {
            let poll_coordinator = Arc::clone(&coordinator);
            let poll_order = Arc::clone(&order);
            let polling_cycle = tokio::spawn(async move {
                let _lease = poll_coordinator.lease_pair("pair-a").await;
                poll_order.lock().expect("lock").push("poll-start");
                tokio::time::sleep(Duration::from_millis(50)).await;
                poll_order.lock().expect("lock").push("poll-end");
            });

            // Let the polling cycle take the lease first.
            tokio::time::sleep(Duration::from_millis(10)).await;

            let manual_coordinator = Arc::clone(&coordinator);
            let manual_order = Arc::clone(&order);
            let manual_cycle = tokio::spawn(async move {
                let _lease = manual_coordinator.lease_pair("pair-a").await;
                manual_order.lock().expect("lock").push("manual-start");
            });

            polling_cycle.await.expect("polling cycle should finish");
            manual_cycle.await.expect("manual cycle should finish");
        });

        assert_eq!(
            *order.lock().expect("lock"),
            vec!["poll-start", "poll-end", "manual-start"],
            "the manual cycle must not begin until the polling cycle has finished"
        );
    }

    #[test]
    fn cancelling_a_pair_is_visible_to_the_cycle_holding_its_lease() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let lease = coordinator.lease_pair("pair-a").await;
            assert!(!lease.is_cancelled());

            assert!(
                coordinator.cancel_pair("pair-a"),
                "cancelling a running cycle should report that one was running"
            );
            assert!(lease.is_cancelled());
            // The shared flag is what the existing stop_signal plumbing reads.
            assert!(lease.cancel_flag().load(Ordering::SeqCst));
        });
    }

    #[test]
    fn a_new_cycle_does_not_inherit_the_previous_cycles_cancellation() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let first = coordinator.lease_pair("pair-a").await;
            coordinator.cancel_pair("pair-a");
            assert!(first.is_cancelled());
            drop(first);

            // Otherwise a pause would silently poison every future cycle for
            // this pair, and sync would appear to work while doing nothing.
            let second = coordinator.lease_pair("pair-a").await;
            assert!(!second.is_cancelled());
        });
    }

    #[test]
    fn cancel_all_reports_only_the_pairs_actually_running() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let _a = coordinator.lease_pair("pair-a").await;
            let _b = coordinator.lease_pair("pair-b").await;
            // Known to the coordinator but idle.
            drop(coordinator.lease_pair("pair-c").await);

            assert_eq!(coordinator.cancel_all(), 2);
        });
    }

    #[test]
    fn transfers_are_capped_by_the_global_budget() {
        let coordinator = SyncCoordinator::with_transfer_limit(2);
        runtime().block_on(async {
            let first = coordinator.acquire_transfer_slot().await;
            let second = coordinator.acquire_transfer_slot().await;
            assert_eq!(coordinator.available_transfer_slots(), 0);

            // A third transfer must wait rather than pile onto the link.
            let waited = tokio::time::timeout(
                Duration::from_millis(50),
                coordinator.acquire_transfer_slot(),
            )
            .await;
            assert!(waited.is_err(), "the budget must actually block");

            drop(first);
            assert_eq!(coordinator.available_transfer_slots(), 1);
            drop(second);
            assert_eq!(coordinator.available_transfer_slots(), 2);
        });
    }

    #[test]
    fn concurrent_transfers_never_exceed_the_budget() {
        const LIMIT: usize = 3;
        let coordinator = Arc::new(SyncCoordinator::with_transfer_limit(LIMIT));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));

        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .expect("runtime should build")
            .block_on(async {
                let mut handles = Vec::new();
                for _ in 0..24 {
                    let coordinator = Arc::clone(&coordinator);
                    let in_flight = Arc::clone(&in_flight);
                    let peak = Arc::clone(&peak);
                    handles.push(tokio::spawn(async move {
                        let _slot = coordinator.acquire_transfer_slot().await;
                        let now = in_flight.fetch_add(1, Ordering::SeqCst) + 1;
                        peak.fetch_max(now, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        in_flight.fetch_sub(1, Ordering::SeqCst);
                    }));
                }
                for handle in handles {
                    handle.await.expect("transfer should finish");
                }
            });

        assert!(
            peak.load(Ordering::SeqCst) <= LIMIT,
            "peak concurrency was {}, over the budget of {LIMIT}",
            peak.load(Ordering::SeqCst)
        );
        assert!(
            peak.load(Ordering::SeqCst) > 1,
            "the test did not actually exercise concurrency"
        );
    }

    #[test]
    fn a_zero_transfer_budget_is_clamped_rather_than_deadlocking() {
        let coordinator = SyncCoordinator::with_transfer_limit(0);
        runtime().block_on(async {
            // Deadlocking every transfer forever is a far worse response to a
            // nonsensical setting than quietly running one at a time.
            let _slot = coordinator.acquire_transfer_slot().await;
        });
        assert_eq!(
            SyncCoordinator::default().available_transfer_slots(),
            DEFAULT_MAX_CONCURRENT_TRANSFERS
        );
    }

    #[test]
    fn retaining_pairs_forgets_removed_ones_but_never_a_running_cycle() {
        let coordinator = SyncCoordinator::default();
        runtime().block_on(async {
            let running = coordinator.lease_pair("pair-running").await;
            drop(coordinator.lease_pair("pair-idle").await);

            coordinator.retain_pairs(&[]);

            // Forgetting a busy pair would hand out a second lease for it.
            assert!(coordinator.is_pair_busy("pair-running"));
            assert!(coordinator.try_lease_pair("pair-running").is_none());
            drop(running);
        });
    }
}
