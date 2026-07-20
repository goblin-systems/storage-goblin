//! Table of sync scenarios run through the simulator.
//!
//! Two kinds of tests live here:
//!
//! 1. **Current-behavior scenarios** — document what the engine does today,
//!    including behaviors that phase 1 will change (those say so explicitly).
//! 2. **Truth cases** — `#[ignore]`d tests asserting the *correct* behavior
//!    the product brief promises. They are the executable definition of done
//!    for phase 1 (backlog/phase-1-sync-correctness.md); un-ignore each as the
//!    matching workstream lands.

#![cfg(test)]

use super::super::error::SyncError;
use super::memory_store::{EtagMode, MemoryObjectStore, Op};
use super::simulator::SyncSimulator;

// ---------------------------------------------------------------------------
// Current-behavior scenarios
// ---------------------------------------------------------------------------

#[test]
fn empty_states_produce_empty_plan() {
    let mut sim = SyncSimulator::new();
    let plan = sim.plan().expect("plan");
    assert_eq!(plan.summary.pending_operation_count, 0);
    assert_eq!(plan.summary.observed_path_count, 0);
}

#[test]
fn new_local_file_uploads_and_settles() {
    let mut sim = SyncSimulator::new();
    sim.write_local("docs/readme.md", b"hello");

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert_eq!(outcomes[0].uploaded, vec!["docs/readme.md"]);
    assert!(sim.is_converged());
}

#[test]
fn new_remote_file_downloads_and_settles() {
    let mut sim = SyncSimulator::new();
    sim.write_remote("photos/cat.jpg", b"meow");

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert_eq!(outcomes[0].downloaded, vec!["photos/cat.jpg"]);
    assert!(sim.is_converged());
}

#[test]
fn anchored_unchanged_file_is_noop() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"stable");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.transfer_count(), 0);
    assert!(outcome.review.is_empty());
}

#[test]
fn local_edit_uploads() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_local("a.txt", b"v2");

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert_eq!(outcomes[0].uploaded, vec!["a.txt"]);
    assert_eq!(sim.remote_files()["a.txt"], b"v2");
}

#[test]
fn remote_edit_downloads() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_remote("a.txt", b"v2");

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert_eq!(outcomes[0].downloaded, vec!["a.txt"]);
    assert_eq!(sim.local_files["a.txt"], b"v2");
}

#[test]
fn both_edited_preserve_both_keeps_both_versions_everywhere() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_local("a.txt", b"local v2");
    sim.write_remote("a.txt", b"remote v2");

    sim.run_until_settled(4).expect("settle");
    assert!(sim.is_converged());
    assert_eq!(sim.local_files.len(), 2, "original + conflict copy");
    assert_eq!(sim.local_files["a.txt"], b"remote v2");
    let conflict_path = sim
        .local_files
        .keys()
        .find(|path| path.contains("(conflict"))
        .expect("conflict copy exists")
        .clone();
    assert_eq!(sim.local_files[&conflict_path], b"local v2");
}

#[test]
fn both_edited_prefer_local_uploads() {
    let mut sim = SyncSimulator::new().with_conflict_strategy("prefer-local");
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_local("a.txt", b"local v2");
    sim.write_remote("a.txt", b"remote v2");

    sim.run_until_settled(3).expect("settle");
    assert_eq!(sim.remote_files()["a.txt"], b"local v2");
}

#[test]
fn both_edited_prefer_remote_downloads() {
    let mut sim = SyncSimulator::new().with_conflict_strategy("prefer-remote");
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_local("a.txt", b"local v2");
    sim.write_remote("a.txt", b"remote v2");

    sim.run_until_settled(3).expect("settle");
    assert_eq!(sim.local_files["a.txt"], b"remote v2");
}

#[test]
fn local_directory_creates_remote_placeholder() {
    let mut sim = SyncSimulator::new();
    sim.local_directories.insert("projects/empty".to_string());

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert_eq!(outcomes[0].directories_created, vec!["projects/empty"]);
    assert!(sim.store.keys().contains(&"projects/empty/".to_string()));
}

#[test]
fn kind_mismatch_parks_for_review() {
    let mut sim = SyncSimulator::new();
    sim.write_local("thing", b"i am a file");
    sim.store.seed("thing/", b"");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.review, vec!["thing"]);
}

#[test]
fn cold_storage_object_is_skipped() {
    // Phase 5 surfaces this to the user; today it is a silent noop.
    let mut sim = SyncSimulator::new();
    sim.store
        .seed_with_storage_class("archive/old.zip", b"frozen", "GLACIER");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.transfer_count(), 0);
    assert!(outcome.review.is_empty());
    assert!(!sim.local_files.contains_key("archive/old.zip"));
}

#[test]
fn unanchored_same_content_both_sides_anchors_silently() {
    let mut sim = SyncSimulator::new();
    sim.write_local("shared.txt", b"same bytes");
    sim.write_remote("shared.txt", b"same bytes");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.anchored, vec!["shared.txt"]);
    assert_eq!(outcome.transfer_count(), 0);
    assert!(outcome.review.is_empty());
}

#[test]
fn unanchored_different_content_both_sides_parks_for_review() {
    let mut sim = SyncSimulator::new();
    sim.write_local("shared.txt", b"local");
    sim.write_remote("shared.txt", b"remote");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.review, vec!["shared.txt"]);
}

#[test]
fn mixed_adds_converge() {
    let mut sim = SyncSimulator::new();
    sim.write_local("local-only/a.txt", b"a");
    sim.write_local("local-only/b.txt", b"b");
    sim.write_remote("remote-only/c.txt", b"c");

    sim.run_until_settled(4).expect("settle");
    assert!(sim.is_converged());
    assert_eq!(sim.local_files.len(), 3);
}

#[test]
fn local_delete_with_concurrent_remote_edit_parks_for_review() {
    // The genuinely ambiguous case: the file was deleted here but edited
    // there. Never auto-resolve; a human decides.
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.delete_local("a.txt");
    sim.write_remote("a.txt", b"v2 remote edit");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.review, vec!["a.txt"]);
    assert!(sim.remote_files().contains_key("a.txt"));
}

#[test]
fn remote_delete_with_concurrent_local_edit_parks_for_review() {
    // Mirror ambiguity: deleted there, edited here.
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.delete_remote("a.txt");
    sim.write_local("a.txt", b"v2 local edit");

    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.review, vec!["a.txt"]);
    assert!(sim.local_files.contains_key("a.txt"));
}

#[test]
fn upload_failure_is_isolated_and_retried_next_cycle() {
    let mut sim = SyncSimulator::new();
    sim.write_local("ok.txt", b"fine");
    sim.write_local("flaky.txt", b"eventually");
    sim.store.inject_failure(
        Op::Put,
        Some("flaky"),
        SyncError::transient("simulated 503"),
    );

    let first = sim.run_cycle().expect("cycle");
    assert_eq!(first.uploaded, vec!["ok.txt"]);
    assert_eq!(first.errors.len(), 1);
    assert!(first.errors[0].1.is_retryable());
    assert!(
        !sim.anchors.contains_key("flaky.txt"),
        "failed upload must not create an anchor"
    );

    let second = sim.run_cycle().expect("cycle");
    assert_eq!(second.uploaded, vec!["flaky.txt"]);
    assert!(sim.is_converged());
}

#[test]
fn multipart_style_etags_still_settle() {
    // Etags must be treated as opaque change tokens, never content hashes.
    let mut sim =
        SyncSimulator::with_store(MemoryObjectStore::with_etag_mode(EtagMode::MultipartLike));
    sim.write_local("big.bin", b"pretend this is 6 GB");

    sim.run_until_settled(3).expect("settle");
    assert!(sim.is_converged());

    // A second pass with nothing changed must stay quiet.
    let outcome = sim.run_cycle().expect("cycle");
    assert_eq!(outcome.transfer_count(), 0);
}

#[test]
fn plan_summary_counts_match_queue() {
    let mut sim = SyncSimulator::new();
    sim.write_local("up.txt", b"u");
    sim.write_remote("down.txt", b"d");
    sim.write_local("conflict.txt", b"l");
    sim.write_remote("conflict.txt", b"r");
    sim.local_directories.insert("newdir".to_string());

    let plan = sim.plan().expect("plan");
    assert_eq!(plan.summary.upload_count, 1);
    assert_eq!(plan.summary.download_count, 1);
    assert_eq!(plan.summary.conflict_count, 1);
    assert_eq!(plan.summary.create_directory_count, 1);
    assert_eq!(
        plan.summary.pending_operation_count,
        plan.queue_items.len() as u64
    );
}

// ---------------------------------------------------------------------------
// Truth cases — the phase-1 definition of done (ignored until implemented)
// ---------------------------------------------------------------------------

#[test]
fn truth_local_delete_propagates_to_remote() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.delete_local("a.txt");

    sim.run_until_settled(3).expect("settle");
    assert!(!sim.remote_files().contains_key("a.txt"));
    assert!(sim.is_converged());
}

#[test]
fn truth_remote_delete_propagates_to_local() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.delete_remote("a.txt");

    sim.run_until_settled(3).expect("settle");
    assert!(!sim.local_files.contains_key("a.txt"));
    assert!(sim.is_converged());
}

#[test]
fn truth_rename_propagates_without_review() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("old-name.txt", b"contents");
    sim.delete_local("old-name.txt");
    sim.write_local("new-name.txt", b"contents");

    let outcomes = sim.run_until_settled(3).expect("settle");
    assert!(outcomes.iter().all(|outcome| outcome.review.is_empty()));
    assert!(sim.is_converged());
    assert!(!sim.remote_files().contains_key("old-name.txt"));
    assert_eq!(sim.remote_files()["new-name.txt"], b"contents");
}

#[test]
fn truth_preserve_both_duplicates_on_conflict() {
    let mut sim = SyncSimulator::new();
    sim.seed_synced_file("a.txt", b"v1");
    sim.write_local("a.txt", b"local v2");
    sim.write_remote("a.txt", b"remote v2");

    let outcomes = sim.run_until_settled(4).expect("settle");
    assert!(outcomes.iter().all(|outcome| outcome.review.is_empty()));
    assert!(sim.is_converged());
    // Both versions survive: original name plus one conflict-suffixed copy.
    assert_eq!(sim.local_files.len(), 2);
}

#[test]
fn truth_identical_first_sync_requires_no_transfers() {
    let mut sim = SyncSimulator::new();
    sim.write_local("shared.txt", b"same bytes");
    sim.write_remote("shared.txt", b"same bytes");

    let outcomes = sim.run_until_settled(2).expect("settle");
    assert!(outcomes.iter().all(|outcome| outcome.review.is_empty()));
    assert_eq!(
        outcomes
            .iter()
            .map(CycleOutcomeExt::transfers)
            .sum::<usize>(),
        0
    );
    assert!(sim.anchors.contains_key("shared.txt"));
}

trait CycleOutcomeExt {
    fn transfers(&self) -> usize;
}

impl CycleOutcomeExt for super::simulator::CycleOutcome {
    fn transfers(&self) -> usize {
        self.transfer_count()
    }
}

// ---------------------------------------------------------------------------
// Property-style convergence test (promote to proptest per phase 1.5).
//
// Full convergence is NOT the invariant: random sequences produce genuinely
// ambiguous states (same path created differently on both sides, or
// deleted-here-edited-there), which must park for review rather than
// auto-resolve. The invariant is:
//   1. the engine settles in bounded cycles,
//   2. every path where local and remote still differ is parked for review —
//      silent divergence is the bug class this guards against.
// ---------------------------------------------------------------------------

#[test]
fn truth_random_mutation_sequences_settle_with_no_silent_divergence() {
    for seed in 0_u64..25 {
        let mut rng = Lcg::new(seed);
        let mut sim = SyncSimulator::new();

        for step in 0..30 {
            let path = format!("f{}.txt", rng.next() % 6);
            let contents = format!("seed{seed}-step{step}");
            match rng.next() % 4 {
                0 => sim.write_local(&path, contents.as_bytes()),
                1 => sim.write_remote(&path, contents.as_bytes()),
                2 => sim.delete_local(&path),
                _ => sim.delete_remote(&path),
            }
        }

        let outcomes = sim
            .run_until_settled(10)
            .unwrap_or_else(|error| panic!("seed {seed} failed to settle: {error}"));

        let review: std::collections::BTreeSet<String> = outcomes
            .last()
            .expect("at least one cycle")
            .review
            .iter()
            .cloned()
            .collect();

        let remote = sim.remote_files();
        let mut divergent: Vec<String> = Vec::new();
        for path in sim.local_files.keys().chain(remote.keys()) {
            if sim.local_files.get(path) != remote.get(path) && !divergent.contains(path) {
                divergent.push(path.clone());
            }
        }

        for path in &divergent {
            assert!(
                review.contains(path),
                "seed {seed}: path '{path}' diverged silently (not parked for review)"
            );
        }
    }
}

/// Minimal deterministic RNG so the skeleton has zero dependencies.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(
            seed.wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407),
        )
    }

    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}
