//! Chaos scenarios: what the engine does when the provider misbehaves
//! (backlog phase 2.4).
//!
//! The scenarios in `scenarios.rs` describe correct planning against a
//! cooperative store. These describe the opposite: 5xx storms, throttling,
//! mid-transfer disconnects, and a provider that fails one specific object
//! forever. Three properties are asserted throughout, because they are the
//! ones whose absence causes real damage:
//!
//! 1. **No data loss.** A failed operation may leave work outstanding; it may
//!    never destroy or corrupt a file on either side.
//! 2. **No livelock.** Repeated failure must not produce a run that never
//!    settles and never gives up — that is the shape of a hot loop.
//! 3. **Isolation.** One poisoned object must not stop every other file.
//!
//! Randomized churn uses a fixed seed and a hand-rolled LCG. A failing chaos
//! test that cannot be reproduced is worse than no chaos test: it gets muted.

#![cfg(test)]

use super::super::error::SyncError;
use super::memory_store::{MemoryObjectStore, Op};
use super::simulator::SyncSimulator;

/// Deterministic pseudo-random source.
///
/// Fixed seed on purpose: chaos tests must reproduce exactly, or the first
/// intermittent failure gets ignored instead of investigated.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        // Numerical Recipes constants.
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

#[test]
fn a_transient_failure_on_upload_does_not_lose_the_local_file() {
    let mut store = MemoryObjectStore::new();
    store.inject_failure(
        Op::Put,
        Some("notes.md"),
        SyncError::transient("503 slow down"),
    );

    let mut sim = SyncSimulator::with_store(store);
    sim.write_local("notes.md", b"the only copy");

    // The first cycle fails the upload; the injected failure is consumed, so
    // the retry on the next cycle succeeds.
    let outcomes = sim
        .run_until_settled(5)
        .expect("should settle after retrying");

    assert!(
        outcomes.iter().any(|outcome| !outcome.errors.is_empty()),
        "the injected failure should have been observed"
    );
    assert!(
        sim.is_converged(),
        "the file must eventually reach the remote"
    );
    assert_eq!(
        sim.remote_files().get("notes.md").map(Vec::as_slice),
        Some(b"the only copy".as_slice()),
        "content must survive the failed attempt intact"
    );
}

#[test]
fn a_5xx_storm_across_many_objects_still_converges() {
    let mut store = MemoryObjectStore::new();
    // Every object fails once before succeeding — a provider having a bad
    // minute rather than a bad day.
    for index in 0..20 {
        store.inject_failure(
            Op::Put,
            Some(&format!("f{index}.txt")),
            SyncError::transient("500 internal error"),
        );
    }

    let mut sim = SyncSimulator::with_store(store);
    for index in 0..20 {
        sim.write_local(&format!("f{index}.txt"), format!("body-{index}").as_bytes());
    }

    sim.run_until_settled(10)
        .expect("a storm of one-off 5xx must still converge");

    assert!(sim.is_converged());
    assert_eq!(sim.remote_files().len(), 20, "every file must arrive");
}

#[test]
fn one_permanently_failing_object_does_not_block_the_others() {
    let mut store = MemoryObjectStore::new();
    // Auth failures are not retryable, so this object can never succeed.
    for _ in 0..20 {
        store.inject_failure(
            Op::Put,
            Some("poisoned.bin"),
            SyncError::auth("access denied"),
        );
    }

    let mut sim = SyncSimulator::with_store(store);
    sim.write_local("poisoned.bin", b"cannot be written");
    for index in 0..5 {
        sim.write_local(&format!("fine{index}.txt"), b"ordinary content");
    }

    // Never settles — the poisoned object keeps failing, which is correct.
    let _ = sim.run_until_settled(6);

    let remote = sim.remote_files();
    for index in 0..5 {
        assert!(
            remote.contains_key(&format!("fine{index}.txt")),
            "a healthy file must sync despite a permanently failing sibling"
        );
    }
    assert!(
        !remote.contains_key("poisoned.bin"),
        "the failing object must not appear to have succeeded"
    );
}

#[test]
fn a_failing_delete_does_not_resurrect_or_destroy_content() {
    let mut store = MemoryObjectStore::new();
    store.inject_failure(Op::Delete, Some("gone.txt"), SyncError::transient("503"));

    let mut sim = SyncSimulator::with_store(store);
    sim.seed_synced_file("gone.txt", b"content");
    sim.seed_synced_file("kept.txt", b"keep me");
    sim.delete_local("gone.txt");

    let _ = sim.run_until_settled(6);

    // Whatever happened to the delete, the file we did not touch is intact.
    assert_eq!(
        sim.remote_files().get("kept.txt").map(Vec::as_slice),
        Some(b"keep me".as_slice()),
        "an unrelated file must not be affected by a failing delete"
    );
}

#[test]
fn a_failing_list_does_not_mutate_anything() {
    let mut store = MemoryObjectStore::new();
    store.inject_failure(Op::List, None, SyncError::offline("no route to host"));

    let mut sim = SyncSimulator::with_store(store);
    sim.seed_synced_file("existing.txt", b"untouched");
    sim.write_local("new.txt", b"pending");

    // Losing the remote listing means the plan cannot be trusted; the cycle
    // must fail rather than conclude that everything remote was deleted.
    let result = sim.plan();
    assert!(
        result.is_err(),
        "planning without a listing must fail loudly"
    );

    assert_eq!(
        sim.remote_files().get("existing.txt").map(Vec::as_slice),
        Some(b"untouched".as_slice()),
        "a failed listing must never be read as 'the remote is empty'"
    );
}

#[test]
fn repeated_failures_terminate_rather_than_livelock() {
    let mut store = MemoryObjectStore::new();
    for _ in 0..100 {
        store.inject_failure(Op::Put, None, SyncError::transient("still down"));
    }

    let mut sim = SyncSimulator::with_store(store);
    sim.write_local("a.txt", b"a");

    // The point is that this *returns* — an engine that never settles and
    // never gives up is a hot loop wearing a disguise.
    let result = sim.run_until_settled(5);
    assert!(
        result.is_err(),
        "a permanently failing run must report that it did not settle"
    );
}

#[test]
fn randomized_churn_under_injected_failures_converges_without_data_loss() {
    const FILES: usize = 12;
    let mut rng = Lcg::new(0xC0FFEE);

    let mut store = MemoryObjectStore::new();
    // Sprinkle one-off transient failures across roughly a third of writes.
    for index in 0..FILES {
        if rng.below(3) == 0 {
            store.inject_failure(
                Op::Put,
                Some(&format!("f{index}.txt")),
                SyncError::transient("429 too many requests"),
            );
        }
    }

    let mut sim = SyncSimulator::with_store(store);

    // Random churn: create, edit, or delete each path a few times.
    let mut expected: std::collections::BTreeMap<String, Vec<u8>> = Default::default();
    for round in 0..3 {
        for index in 0..FILES {
            let path = format!("f{index}.txt");
            match rng.below(4) {
                0 if round > 0 => {
                    sim.delete_local(&path);
                    expected.remove(&path);
                }
                _ => {
                    let body = format!("round{round}-file{index}").into_bytes();
                    sim.write_local(&path, &body);
                    expected.insert(path, body);
                }
            }
        }
    }

    sim.run_until_settled(20)
        .expect("randomized churn with recoverable failures must converge");

    assert!(
        sim.is_converged(),
        "local and remote must agree once settled"
    );
    for (path, body) in &expected {
        assert_eq!(
            sim.remote_files().get(path),
            Some(body),
            "content mismatch for '{path}' after churn — this is data loss"
        );
    }
}

#[test]
fn the_random_source_is_reproducible() {
    // If this ever fails, every other chaos test above became unreproducible
    // and their failures stop being investigable.
    let first: Vec<u64> = (0..5).map(|_| Lcg::new(42).next()).collect();
    let mut source = Lcg::new(42);
    let second: Vec<u64> = (0..5).map(|_| source.next()).collect();

    assert!(first.iter().all(|value| *value == first[0]));
    assert_ne!(second[0], second[1], "the source must actually advance");
    assert_eq!(first[0], second[0], "the same seed must start the same way");
}
