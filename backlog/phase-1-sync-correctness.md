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

- [ ] New operations: `delete_remote`, `delete_local`, `move_remote`, `move_local`,
      `duplicate_conflict` (implements preserve-both). Typed enum end-to-end (no more
      `operation: String`) — coordinate with phase 3's typed-domain work.
- [ ] Rewrite `decide_file_sync` as an explicit, exhaustively-tested decision table over
      `(anchor_state, local_state, remote_state)` — every cell deliberate, no catch-all arm.
      The table itself becomes documentation (generate a markdown table from the test).
- [ ] Tombstone handling in plan build: anchor-with-tombstone + reappearing side = re-create
      vs resurrect decision based on which timestamp is newer.
- [ ] Directory semantics: deleting a directory locally must plan deletes for its remote
      children (currently directories are mostly `noop`); empty-dir placeholders reconciled.
- [ ] Mass-delete circuit breaker (ADR-2c) as a plan-level flag the UI must acknowledge.

### 1.2 Conflict engine

- [ ] Implement `preserve-both` for real: loser is renamed
      `name (conflict <device> <yyyy-mm-dd>).ext` locally and uploaded; winner keeps the name;
      both sides converge to both files. Device name from OS hostname, persisted in settings.
- [ ] `prefer-local` / `prefer-remote` keep semantics but the overwritten side is protected:
      remote overwrite relies on versioning/remote-bin when available; local overwrite moves
      the old file to OS trash.
- [ ] Conflicted-copy suppression: don't re-conflict the conflict files themselves.
- [ ] Review queue becomes an explicit, bounded state (needed by phase 5 UX): each entry
      carries both sides' metadata + why it conflicted + one-click resolutions.

### 1.3 First-sync & adoption flows

- [ ] "Merge" first sync: matching path + matching content hash (ADR-4, or size+etag fast
      path) → create anchor silently, no transfer, no review.
- [ ] Path present both sides, different content, no anchor → apply the pair's conflict
      strategy (with preserve-both actually working, the default first sync is lossless and
      automatic).
- [ ] Explicit first-sync modes surfaced at pair creation (phase 5 wires the UI):
      *merge* (default), *mirror local → remote*, *mirror remote → local* — the mirror modes
      use the delete machinery with the circuit breaker forced on.

### 1.4 Transfer integrity (correctness half; performance is phase 2)

- [ ] Atomic downloads: write to `.goblin-tmp` sibling, fsync, atomic rename, then set mtime;
      clean up orphaned tmp files on startup. Fixes `s3_adapter.rs:333` /
      `commands.rs` download paths.
- [ ] Verify after transfer: compare stored content hash (ADR-4) after download; verify
      returned etag/generation after upload; mismatch → `SyncError::Precondition` → re-plan,
      not silent success.
- [ ] Conditional writes everywhere: `If-Match`/`x-goog-if-generation-match` on overwrites and
      deletes so a remote change between plan and execute can never be clobbered blind.
      (S3 now supports conditional writes; GCS has generation preconditions.)
- [ ] Stale-plan handling becomes re-plan-and-retry (bounded), not a terminal error string
      (`commands.rs:901-948`).
- [ ] Case-sensitivity and path normalization audit: NFC/NFD, Windows reserved names
      (`CON`, `NUL`, trailing dots/spaces), `/` vs `\`, long paths. Quarantine unsyncable
      names with a visible reason rather than failing the queue. Extend `sanitizer.rs`.

### 1.5 Verification

- [ ] Un-ignore the phase-0 simulator truth cases; all pass.
- [ ] Decision-table exhaustive test: every `(anchor, local, remote)` combination asserted.
- [ ] Property test (proptest): arbitrary interleaved mutation sequences on two replicas
      converge with zero lost writes and zero resurrections; run with injected transient
      failures from `MemoryObjectStore`.
- [ ] Cross-device simulation: two simulated clients sharing one fake remote (this is the
      multi-device story the brief promises — it has likely never been tested).
- [ ] Manual cloud matrix (checklist doc): real S3 + real GCS runs of: delete file/folder both
      directions, rename file/folder, conflict both-sides-edit, kill app mid-download,
      kill mid-upload, offline edit + reconnect.

## Acceptance criteria

1. Local and remote deletes propagate within one sync cycle, with trash/remote-bin protection
   and the mass-delete breaker; simulator + cloud checklist prove it.
2. File and folder renames propagate as moves (no re-upload of content ≥ the ambiguity
   threshold) on both providers.
3. `preserve-both` produces the documented duplicate-with-suffix outcome automatically;
   review queue only receives genuinely ambiguous cases (kind mismatch, breaker trips).
4. First sync over two pre-populated identical trees performs zero transfers and zero reviews.
5. Kill -9 during any transfer leaves no corrupt or partial visible files and recovers to a
   consistent state on restart (existing durable-queue recovery extended to cover tmp files).
6. Property tests run in CI ≥ 1,000 cases without loss/resurrection findings.
7. No `ReviewRequired` reachable from a plain delete, rename, or first-sync-identical path —
   enforced by the decision-table test.

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
