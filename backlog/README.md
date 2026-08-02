# Storage Goblin — Flagship Overhaul Backlog

This backlog turns Storage Goblin (v0.2.0, AI-generated, "sort of works") into a flagship-quality
personal cloud drive. It is organized as seven phases. Each phase has its own detailed document in
this directory with findings, workstreams, task breakdowns, acceptance criteria, and risks.

| Phase | Document | Theme | Depends on | Status |
|-------|----------|-------|------------|--------|
| 0 | [phase-0-foundations.md](phase-0-foundations.md) | CI, quality gates, repo hygiene, safety nets | — | landed |
| 1 | [phase-1-sync-correctness.md](phase-1-sync-correctness.md) | Sync engine correctness: deletes, renames, conflicts, first sync | 0 | landed, awaiting cloud validation |
| 2 | [phase-2-transfer-reliability.md](phase-2-transfer-reliability.md) | Transfer reliability & performance: streaming, retries, parallelism | 0, 1 | landed; resume + soak test open |
| 3 | [phase-3-backend-architecture.md](phase-3-backend-architecture.md) | Backend decomposition: kill the 12.4k-line god module & legacy path | 0 (interleaves with 1–2) | landed; 3.5 partial, Azure spike deferred to 6 |
| 4 | [phase-4-frontend-architecture.md](phase-4-frontend-architecture.md) | Frontend decomposition: kill the 4k-line bootstrap closure | 0, 3 | in progress — 4.1 done, 2 of 7 views extracted |
| 5 | [phase-5-ux-overhaul.md](phase-5-ux-overhaul.md) | UX overhaul: onboarding, status model, conflict UX, language | 4 | not started |
| 6 | [phase-6-flagship-distribution.md](phase-6-flagship-distribution.md) | Distribution: signing, auto-update, crash reporting, docs, Azure | 1–5 | not started |

Every "landed" above means *landed on the `overhaul/phase-1` branch* (pushed 2026-08-02, not
merged). Functionality has been verified locally; nothing has run in CI and nothing has been
validated against a real bucket. Read each phase doc's Status section for the honest per-item
picture — the checkboxes alone overstate completeness.

**[next-steps.md](next-steps.md) is the current plan.** The phase docs below are the detailed
specs; that document is the sequencing argument for what to do next, grounded in the state of
the code after phases 0–3 rather than the assumptions the phase docs were written under.

## Current-state assessment (evidence summary)

Full details live in each phase doc; headline findings, verified against the code on 2026-07-19:

### Correctness (worst problems — these gate everything else)

1. **Deletes are never propagated.** `sync_planner.rs` contains no delete operation at all
   (grep for `delete` in the planner: zero hits). A file deleted locally that still exists
   remotely falls through `decide_file_sync` (`sync_planner.rs:357-402`) into `ReviewRequired`,
   i.e. it becomes a manual "conflict" forever — or worse, gets re-downloaded. The project brief
   explicitly requires Create/Update/Delete/Rename support.
2. **No rename/move detection.** A rename is seen as an unrelated delete + add; the add uploads,
   the delete side dead-ends into review. Renaming a large folder re-uploads everything and
   strands the old tree.
3. **`preserve-both` doesn't preserve both.** The default conflict strategy
   (`profile_store.rs:18`, brief: "duplicate with suffix") actually maps to `ConflictReview`
   in `sync_planner.rs:387-391` — a manual review queue, not the promised auto-duplication.
4. **First sync of pre-existing data is a wall of conflicts.** With no anchor, any path present
   on both sides is `ReviewRequired` (`sync_planner.rs:363-373`), even when contents are identical
   in practice (etag/fingerprint comparison across providers is not attempted).

### Reliability

5. **Whole-file buffering.** GCS uploads read the entire file into memory
   (`gcs_adapter.rs:504` `std::fs::read`); S3 downloads collect the whole body into memory
   (`s3_adapter.rs:317-322`). Large files (multi-GB) will exhaust memory or fail. No multipart
   upload (S3 `put_object` caps at 5 GB), no resumable GCS upload, no ranged/resumable download.
6. **Non-atomic downloads.** Downloads write directly to the destination
   (`s3_adapter.rs:333`); a crash mid-write leaves a corrupt file that the next scan
   fingerprints as a "local change" and uploads back. No temp-file + rename, no fsync.
7. **First failure aborts the whole queue.** Queue execution is strictly sequential and
   `break`s on the first error (`commands.rs:1758+`, e.g. 1852-1854). One bad file stops the
   other 10,000. No retry, no backoff, no per-item error isolation, no parallelism.

### Architecture / code health

8. **`commands.rs` is 12,404 lines** — commands, sync orchestration, queue execution, watcher
   reconciliation, file comparison, OS shell integration all in one module. Nearly everything
   exists twice: a legacy single-profile path and a per-pair path
   (`execute_planned_upload_queue` vs `..._for_pair`, `run_sync_cycle` vs `run_sync_cycle_for_pair`,
   plus duplicate command aliases `*_sync_pair` / `*_sync_location`, `validate_s3_connection` /
   `validate_storage_connection`). 118 `legacy/TODO` markers across the Rust tree.
9. **`bootstrap.ts` is 4,076 lines**, with a single ~2,800-line `bootstrapStorageGoblin()`
   closure holding all app state, all event wiring, and all rendering. `index.html` is a 579-line
   static template addressed through a 120+-entry element registry (`dom.ts`).
10. **Stringly-typed domain.** `kind: "file" | "directory"`, operations, resolutions, phases,
    conflict strategies are all raw strings compared ad hoc on both sides of the IPC boundary.

### Process / delivery

11. **CI never runs the Rust tests** (`ci.yml` runs `bun run test` + builds only; `test:rust`
    exists but is not wired in). No clippy, no rustfmt check, no ESLint/Prettier, no coverage,
    no E2E. Verified 2026-07-19: 221 vitest tests pass, 254 Rust tests pass locally, and
    `tsc --noEmit` is clean — a decent base to build on, but unprotected.
12. **Releases are unsigned** (`--no-sign`), there is no auto-updater, no crash reporting,
    no telemetry, no install docs.

### UX (assessment; full audit in phase 5)

13. Setup is fragmented across disconnected screens (Credentials, Sync Locations, Polling,
    Conflict Defaults) behind menu-bar dropdowns; there is no onboarding path from install to
    first successful sync. Status is expressed in engine jargon ("Unconfigured", phases,
    review-required) rather than user outcomes. The activity log is the primary feedback
    mechanism. No tray icon, no progress for individual transfers, no clear "your files are safe"
    signal — the single most important message a sync product must communicate.

## Sequencing rationale

- **Phase 0 first**: every later phase rewrites core logic; without Rust tests in CI, clippy,
  lint, and a sync-engine test harness, the overhaul would fly blind.
- **Correctness (1) before performance (2)**: parallelizing a planner that silently strands
  deletes just corrupts data faster.
- **Backend refactor (3) interleaves with 1–2**: the legacy/pair duplication must die early so
  correctness fixes land once, not twice; but full decomposition can proceed alongside.
- **Frontend (4) before UX (5)**: the monolith closure cannot support the redesigned flows;
  restructure state/rendering first, then reshape the product on top.
- **Distribution (6) last**: signing, auto-update and docs are only worth polishing once the
  product underneath is trustworthy.

## Definition of "flagship"

The overhaul is done when all of these hold:

- **Trustworthy**: a user can point Storage Goblin at 100 GB / 200k files, work for a month
  across two machines, and never lose a byte or be surprised. Deletes, renames, conflicts,
  crashes mid-transfer, and offline periods are all handled and visible.
- **Comprehensible**: a non-expert can go from install to first sync in under 5 minutes without
  reading docs, and can always answer "is my stuff synced?" in one glance.
- **Maintainable**: no module over ~800 lines, no duplicated orchestration paths, typed domain
  model end to end, >80 % coverage on the sync engine, all gates in CI.
- **Distributable**: signed installers, auto-update, crash reporting, a real README/website,
  and a provider abstraction proven by a third provider (Azure) landing without core changes.

## Working agreements for the overhaul

- Every phase doc's acceptance criteria are the merge gate for that phase's epic branch.
- No new features on the legacy single-profile path; it is frozen until deleted in phase 3.
- Sync-engine changes require simulator coverage (phase 0 deliverable) before merge.
- Keep `AGENTS.md` updated as decisions land; keep this backlog updated as scope shifts.
