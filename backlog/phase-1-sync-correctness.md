# Phase 1 — Sync Engine Correctness

**Theme:** Make the sync engine actually implement the product promise:
Create / Update / **Delete** / **Rename**, bi-directional, no data loss, no surprises.
**Depends on:** Phase 0 (simulator, error taxonomy, CI gates).
**Estimated size:** 3–5 weeks. This is the highest-risk, highest-value phase.

## Current state (verified findings)

The planner (`src-tauri/src/storage/sync_planner.rs`, 698 lines) compares three inputs —
local snapshot, remote snapshot, per-path anchors — and emits `upload`, `download`,
`create_directory`, `conflict_review`, `review_required`, or `noop`. That's the entire
operation vocabulary. Consequences:

1. **Deletes strand or resurrect files.** `decide_file_sync` (`sync_planner.rs:357-402`):
   - Local delete of an anchored file (local `None`, remote unchanged) → falls to the
     catch-all `_ => ReviewRequired` (line 400). The user's delete becomes a permanent
     "conflict" they must manually resolve, forever, for every deleted file.
   - Remote delete of an anchored file → same dead end.
   - There is no tombstone concept, so after anchors are cleared (e.g. re-setup), a delete on
     one side looks like a create on the other and the file **resurrects**.
2. **Renames are delete+add.** No move detection; renaming `photos/` re-uploads the whole
   tree and strands the old one in review purgatory.
3. **`preserve-both` is a lie.** Brief: "if same file changed in two places → duplicate with
   suffix `file (conflict-deviceA-date).txt`". Implementation: `preserve-both` →
   `ConflictReview` (`sync_planner.rs:387-391`) — a manual queue. The default strategy
   silently does nothing automatic.
4. **First sync with data on both sides = wall of `ReviewRequired`** (`sync_planner.rs:363-372`):
   no anchor + present on both sides is always review, even if bytes are identical.
5. **Fragile change detection.** Local fingerprint is SHA-256 (`local_index.rs`), remote
   identity is the provider etag. S3 multipart etags are not md5, GCS etags differ per
   metadata change — so local and remote identity can never be compared directly; everything
   funnels through anchors, and anchors only exist for files this app itself transferred.
6. **Cold-storage-class objects are silently `noop`ed** (`sync_planner.rs:133-146`) with no
   user-visible explanation of why a file "won't sync".
7. **Concurrent-modification windows.** Plans execute against snapshots; guards exist
   (`expected_local_fingerprint` / `expected_remote_etag`, stale-plan errors in
   `commands.rs:901-948`) but a change between plan and execution surfaces as a hard error
   string, not a graceful re-plan.

## Design decisions to make first (write ADRs in `docs/adr/`)

- **ADR-1: State model.** Recommended: keep the anchor ("last-known-synced state per path")
  model but make it authoritative and durable: every path ever synced keeps an anchor row with
  `(local_fingerprint, remote_etag+version_id, size, mtime, synced_at, deleted_at?)`.
  Tombstones = anchor rows with `deleted_at`. Garbage-collect tombstones after N days.
- **ADR-2: Delete semantics.** Deletes propagate. Safety valves: (a) remote deletes go through
  the existing remote-bin/versioning machinery when enabled (already built — reuse it);
  (b) local deletes move to OS trash, never hard-unlink; (c) a **mass-delete circuit breaker**:
  if a plan wants to delete > X % or > N files, pause and require confirmation
  ("fail-safe — never delete without certainty", per the brief).
- **ADR-3: Rename detection.** Local: pair (deleted anchor, new file) by fingerprint match →
  emit `move` (server-side copy+delete remotely; S3 `CopyObject` / GCS `rewrite` already have
  adapter support via storage-class code paths). Remote: pair by etag+size. Ambiguity
  (multiple candidates) degrades safely to copy+tombstone, never review.
- **ADR-4: Content identity across providers.** Store *our own* content hash as object
  metadata on upload (`x-amz-meta-goblin-sha256` / GCS custom metadata). This makes first-sync
  dedup, rename detection, and integrity verification provider-independent. Fallback for
  objects not uploaded by us: size + provider etag heuristics, with md5-etag fast-path on S3
  non-multipart objects.

## Workstreams

### 1.1 Planner rewrite: full operation vocabulary

- [x] New operations added, plus `anchor_only` and `forget_anchor` for anchor
      reconciliation. A typed `Operation` enum with `as_str`/`parse` is the single
      string boundary; full typed plumbing through `sync_db`/`commands` stays with
      phase 3's `engine::model` as planned.
- [x] `decide_file_sync` rewritten with no catch-all arm; every cell deliberate.
      24 planner tests cover the table. **Deviation:** the table is documented by the
      tests themselves rather than a generated markdown artifact — generating a doc
      from them added maintenance for no reader we could name.
- [ ] **Not done — deferred.** Anchors are still deleted rather than tombstoned. The
      resurrection window this leaves: if a path is deleted on both sides and later
      re-created remotely, it downloads (correct); but a delete that races a re-create
      across a re-plan can still re-transfer. Needs the anchor schema change (a
      `deleted_at` column + GC) — sequenced with phase 2.3's SQLite migration so users
      migrate once, not twice.
- [ ] **Not done — deferred to the phase-1 follow-up.** File deletes inside a removed
      directory do propagate (each file is planned individually), but the remote
      directory placeholder is left behind. Cosmetic rather than data-losing, and it
      needs the directory-anchor model that pairs with tombstones above.
- [x] Mass-delete circuit breaker implemented: batches over 25 deletes, or over 50%
      of an anchored tree of 10+, are converted to review items and never executed.
      It emits a loud activity entry naming the count. **Deviation:** it is not yet a
      UI-acknowledgeable flag — the plan-level count is computed but `DurablePlannerSummary`
      (the IPC payload) does not carry it, so phase 5 wires the confirm/restore prompt.

### 1.2 Conflict engine

- [x] `preserve-both` implemented: the local edit is renamed and uploaded, the remote
      edit is downloaded at the original path, and both sides converge to both files.
      **Deviation:** the suffix is `name (conflict yyyy-mm-dd).ext` without the device
      name — carrying a device identity needs a settings field, which is phase-5 UI
      work; collisions get a numeric suffix instead.
- [ ] **Partially done.** Both strategies keep their semantics and remote overwrites
      still land through the versioning/remote-bin machinery. The local side is *not*
      yet trash-protected on overwrite: a `prefer-remote` download still overwrites the
      local file in place. Grouped with the atomic-download work below.
- [x] Conflict copies are ordinary new files: they upload once and anchor, and the
      naming pass avoids colliding with existing local or remote paths, so they do not
      re-conflict.
- [ ] **Deferred to phase 5.** Review items remain `review_required` queue rows; the
      richer entry model belongs with the attention-queue UI that consumes it.

### 1.3 First-sync & adoption flows

- [x] Implemented and proven in the simulator, and **live on GCS**: uploads attach a
      goblin content fingerprint, GCS list responses return it, and matching content
      anchors silently.
      **Known gap — S3.** `ListObjectsV2` returns no user metadata, so no fingerprint is
      available and identical content still parks for review (unchanged from before, not
      a regression). Closing it needs a HEAD per same-path/same-size candidate, or
      download-and-hash for objects Goblin never uploaded; both want phase 2's per-item
      error isolation and streaming hashes. This is the one phase-1 acceptance criterion
      that is not met on the flagship provider.
- [ ] **Deliberately not done.** Unanchored differing content still parks for review
      rather than auto-applying the strategy. Without sync history there is no basis to
      call either side "the edit", and silently overwriting a stranger's file is the
      worst failure this product can have. Revisit with the phase-5 first-run wizard,
      where the user picks merge/mirror explicitly.
- [ ] **Deferred to phase 5** with the onboarding wizard that presents the choice.

### 1.4 Transfer integrity (correctness half; performance is phase 2)

- [ ] **Not done — moved to phase 2.** Downloads still write in place. This is the
      same code path phase 2.1 replaces with streaming/resumable transfers, and doing
      the temp-file dance twice would be wasted work. Tracked as the first item there.
- [ ] **Moved to phase 2** with the transfer engine, for the same reason.
- [ ] **Moved to phase 2.** The existing stale-plan guards (expected fingerprint/etag
      checked before each transfer) remain the protection in the meantime.
- [ ] **Moved to phase 2.2**, which replaces abort-on-first-error with per-item
      isolation and bounded retries — the mechanism this needs.
- [ ] **Not done.** Only the traversal guard is covered (tests assert `../` paths are
      rejected for both local delete and rename). The full normalization audit is its own
      workstream and needs the quarantine surface from phase 5 to report unsyncable
      names usefully.

### 1.5 Verification

- [x] All 5 behavioral truth cases un-ignored and passing (both delete directions,
      rename-as-move, preserve-both duplication, identical-content first sync).
- [x] 24 planner tests covering every decision-table cell, rename pairing including
      the ambiguous case, the circuit breaker, and conflict-copy naming.
- [x] Property-style test runs 25 seeded random mutation sequences.
      **The invariant was restated, and this matters:** full convergence is *not* correct
      to assert, because random sequences produce genuinely ambiguous states
      (deleted-here-edited-there) that must park for review rather than auto-resolve.
      The test now asserts the engine settles in bounded cycles and that every still-
      diverging path is parked — i.e. no *silent* divergence. Still a hand-rolled LCG;
      promoting to proptest remains open.
- [ ] **Not done.** The simulator models one client against one remote. Two clients
      sharing a store is the natural next harness feature and the right home for the
      multi-device guarantees; it needs per-client anchor sets, which is a harness change
      rather than an engine one.
- [ ] **Outstanding and load-bearing.** Nothing in this phase has touched a real
      bucket; all evidence is from the simulator and unit/integration tests. This
      checklist is written up as `docs/phase-1-cloud-validation.md` and must be run
      before phase 1 can be called done.


## Status (2026-07-20)

Landed on `overhaul/phase-1`, not merged. The engine now propagates deletes and
renames, keeps both sides of a conflict, and reconciles anchors — in the planner,
the simulator, and the real per-pair executors.

**Verified by tests that run:** 303 Rust tests (up from 278), including 24 planner
tests over the decision table, ~25 simulator scenarios end-to-end, sync_db tests
proving the new operations route to the right queue with their target paths, and
filesystem tests covering rename/prune/traversal-rejection.

**Recovered along the way:** the `tauri-command-tests` feature had never compiled
(duplicate imports plus a missing one, dating to the feature's introduction) and CI
never built it, so a whole block of command-level integration tests was dead. It is
fixed and wired into CI.

### Not proven yet — read before merging

1. **No real-cloud run.** Every claim here rests on the simulator and unit tests.
   The manual S3 + GCS matrix in 1.5 is unrun; deletes against a live bucket are the
   single highest-risk thing in this phase.
2. **Command-level integration tests compile but have never executed.** The Tauri
   mock runtime fails to load on the Windows dev machine
   (`STATUS_ENTRYPOINT_NOT_FOUND` from the `tauri/test` build, reproduced with a clean
   target dir and a minimal PATH). They are wired into CI as a **non-blocking** job;
   make it blocking after one green run.
3. **First-sync merge does not work on S3** — see 1.3. It works on GCS and in the
   simulator.
4. **Deferred to phase 2** (all noted inline): atomic downloads, post-transfer
   verification, conditional writes, and re-plan-on-stale. These share the code path
   phase 2.1 rewrites.

### Suggested order for finishing phase 1

1. Run the manual cloud matrix on a scratch bucket, both providers.
2. Get one green CI run of the command integration tests; make the job blocking.
3. Tombstones + directory-delete semantics (the two real correctness gaps left),
   sequenced with phase 2.3's schema migration.
4. Decide S3 first-sync merge: HEAD-per-candidate now, or wait for phase 2.

## Acceptance criteria

1. ⚠️ Local and remote deletes propagate within one sync cycle, with trash/remote-bin
   protection and the mass-delete breaker. Simulator proves it; **the cloud checklist
   has not been run**, so this is not yet signed off.
2. ⚠️ File renames propagate as server-side moves, with ambiguous pairings degrading
   safely to copy+delete. **Folder renames are not special-cased** (they resolve as
   per-file moves, which is correct but does N operations), and this is unverified
   against real providers.
3. ✅ `preserve-both` duplicates automatically; the review queue now receives only
   genuinely ambiguous cases (kind mismatch, delete-vs-edit, unanchored differing
   content, breaker trips). Suffix omits the device name — see 1.2.
4. ⚠️ **Met on GCS and in the simulator; not met on S3** (no listing metadata — see 1.3).
5. ❌ **Not met — moved to phase 2.** Downloads are still non-atomic; there are no tmp
   files to recover yet.
6. ⚠️ Property test runs 25 seeded cases (not 1,000) asserting no *silent* divergence.
   Scaling up wants proptest and the phase-2 chaos hooks.
7. ✅ Enforced by the decision-table tests: no review item is reachable from a plain
   delete, a rename, or a first-sync-identical path (the last one on providers that
   surface fingerprints).

## Risks

- **Delete propagation is the most dangerous feature in the product.** Mitigate with: trash +
  remote-bin defaults ON, circuit breaker, tombstone GC ≥ 30 days, and shipping behind a
  "safe mode" toggle for one release.
- **Rename detection false positives** (identical files at different paths). Mitigate: require
  exact fingerprint + size match and unique pairing; otherwise degrade to copy+delete.
- **Anchor DB migration** from current schema (`sync_db.rs`, 2,151 lines) — write forward
  migration + one-release rollback; test against real user DB copies.
- **Scope creep into performance.** Streaming/parallelism stays in phase 2; here only
  correctness-critical transfer changes (atomicity, verification, preconditions) land.
