# Phase 3 — Backend Architecture Decomposition

**Theme:** Kill the god module and the duplicated legacy path; establish a layered backend that
phases 1–2 land into once, not twice.
**Depends on:** Phase 0. **Interleaves with:** phases 1–2 (see sequencing below).
**Estimated size:** 3–4 weeks, partially parallel with 1–2.

## Current state (verified findings)

1. **`src-tauri/src/storage/commands.rs` is 12,404 lines** and contains: 40+ Tauri commands,
   the entire sync orchestration (`run_sync_cycle` ~460 lines at `commands.rs:2440`,
   `run_sync_cycle_for_pair` ~440 lines at `commands.rs:5365`), four copies of queue execution
   (`execute_planned_upload_queue` / `..._download_queue` / both `_for_pair` variants, each
   300–370 lines of near-identical inline logic), polling workers, watcher reconciliation,
   credential resolution, remote-bin lifecycle reconciliation, OS shell integration
   (`reveal_in_file_manager`, `open_path_with_default_app`), inline text/image comparison
   preparation, and ~200 free helper functions.
2. **Legacy vs. pair duplication.** A deprecated single-profile model coexists with the
   multi-pair model: `snapshot_for_profile`/`snapshot_for_pair`,
   `resolve_credentials_for_pair` vs profile variants, and duplicate registered command aliases
   (`list_sync_pairs`/`list_sync_locations`, `add_sync_pair`/`add_sync_location`, etc. —
   `lib.rs:30-38`) plus `validate_s3_connection` alongside `validate_storage_connection`
   (`lib.rs:12-13`). Every bug fix currently needs to land in 2–4 places.
3. **Stringly-typed domain everywhere.** `kind: String` ("file"/"directory"),
   `operation: String`, `resolution: String`, phase strings ("Unconfigured", ...), provider
   strings with alias normalization ("gcp"→"gcs"), conflict-strategy strings — compared ad hoc
   on both sides of IPC. 118 `legacy|TODO` markers across the Rust tree.
4. **`Result<_, String>` as the universal error type** (being fixed from phase 0.4 onward).
5. **Blocking bridges**: `run_async_blocking` (`commands.rs:289`) spawns runtimes ad hoc;
   sync/async boundaries are improvised per call site.
6. Adapters are in decent shape relative to the rest (`object_store.rs` is a real seam;
   `provider.rs` centralizes capabilities) — the rot is orchestration, not the provider layer.

## Target architecture

```
src-tauri/src/
  engine/            # no Tauri types allowed — unit-testable pure core
    model.rs         # typed domain: EntryKind, Operation, Resolution, Phase,
                     # ConflictStrategy, ProviderId (serde to stable wire strings)
    planner.rs       # phase-1 planner (moved, typed)
    scheduler.rs     # phase-2 queue scheduler
    transfer.rs      # phase-2 transfer engine
    anchors.rs       # anchor/tombstone store API over sync_db
    error.rs         # SyncError (from phase 0.4)
  providers/
    traits.rs        # ObjectStorage trait (from phase 0.2)
    s3.rs, gcs.rs, memory.rs
  services/          # app-level orchestration, owns state, no UI knowledge
    sync_service.rs  # run_sync_cycle, per-pair lifecycle, pause/resume
    poll_service.rs  # polling + watcher reconciliation
    credential_service.rs
    profile_service.rs
    bin_service.rs   # remote-bin + versioning flows
  ipc/               # thin Tauri layer
    commands/*.rs    # one file per domain; commands validate, call services, map errors
    events.rs        # typed event payloads (status, progress, activity)
  platform/          # shell reveal/open, trash, DPAPI specifics
```

Rules: `engine` depends on nothing app-specific; `services` depend on `engine` + `providers`;
`ipc` depends on `services`. Enforce with a CI check (e.g. cargo-modules or a simple grep gate
for `tauri::` outside `ipc`/`platform`).

## Workstreams

### 3.1 Delete the legacy single-profile path (do this FIRST — it halves phases 1–2)

- [ ] Confirm the frontend only calls the `*_sync_location` + `validate_storage_connection`
      forms (grep `src/app/client.ts`); delete the alias commands from `lib.rs`.
- [ ] Migrate any remaining profile-level state to pairs on startup (migration code exists —
      `migrate_flat_fields_to_sync_pairs` in `profile_store.rs`; make it terminal: after one
      release, drop the legacy read path).
- [ ] Delete `run_sync_cycle`, `execute_planned_*_queue` (non-pair), `snapshot_for_profile`,
      `start_polling_worker` (non-pair) and friends; keep only `_for_pair` implementations,
      then drop the suffix.
- [ ] Expected outcome: `commands.rs` shrinks by roughly a third before any real refactor.

### 3.2 Typed domain model

- [ ] Introduce the `model.rs` enums; serde-rename to today's wire strings so the frontend and
      persisted JSON/SQLite stay compatible; exhaustive `match` replaces string comparison.
- [ ] Generate TypeScript types from Rust (ts-rs or specta/tauri-specta) so `src/app/types.ts`
      (781 hand-written lines) becomes generated output — single source of truth for the IPC
      contract, drift becomes a compile error. (Coordinates with phase 4.)
- [ ] Provider alias normalization ("gcp"→"gcs") happens exactly once, at deserialization.

### 3.3 Extract services & engine

- [ ] Mechanical moves first (functions relocated verbatim + imports), one domain per PR:
      credentials → `credential_service`, remote bin/versioning → `bin_service`,
      shell/OS calls → `platform`, comparison prep → its own service.
- [ ] `SyncState` (global mutable state) redesigned: per-pair actor or `tokio::sync` model
      owned by `sync_service`; this is the enabler for phase 2.2's parallel scheduler —
      **sequence 3.3 before 2.2**.
- [ ] Planner moves into `engine` with phase 1's rewrite (do the move as part of the rewrite
      to avoid double churn).
- [ ] `run_async_blocking` eliminated; commands are `async fn`, services expose async APIs,
      blocking file IO goes through `spawn_blocking`.
- [ ] Module size gate in CI: no file > 800 lines in `src-tauri/src` (tests exempt);
      clippy `too_many_lines` on.

### 3.4 State & persistence consolidation

- [ ] One SQLite database as the single durable store (queues + anchors already there;
      phase 2.3 adds indexes; migrate profile JSON if practical — evaluate) with
      versioned migrations (refinery or hand-rolled migration table — `sync_db.rs` already
      has an ad-hoc version mechanism to formalize).
- [ ] Snapshot/queue/anchor access behind repository APIs in `engine/anchors.rs` +
      `services`; no raw SQL outside the repository layer.
- [ ] Startup consistency pass: orphaned tmp files, stale in-progress queue items, watcher
      re-registration — one documented recovery routine instead of scattered recovery calls.

### 3.5 Test relocation & coverage

- [ ] Existing 254 Rust tests move with their code; commands-level tests convert to
      service-level tests against `MemoryObjectStore` (faster, no `tauri/test` feature wall).
- [ ] Coverage target: engine ≥ 85 %, services ≥ 70 % lines (measured by cargo llvm-cov from
      phase 0).

## Sequencing with phases 1–2

1. 3.1 (legacy deletion) — immediately after phase 0, before phase 1 lands its planner.
2. 3.2 (typed domain) — alongside phase 1's planner rewrite (shared enums).
3. 3.3 services/state — before phase 2.2 (scheduler needs the new state model).
4. 3.4–3.5 — trailing, before phase 4 starts consuming generated types.

## Acceptance criteria

1. Zero duplicate command aliases; legacy profile path deleted; one orchestration code path.
2. `commands.rs` no longer exists; largest backend file ≤ 800 lines; layering rule enforced
   in CI; `tauri::` types absent from `engine/`.
3. Domain enums replace operation/resolution/kind/phase/strategy strings in Rust;
   `src/app/types.ts` is generated, not hand-written.
4. All Rust tests green on 3 OSes in CI; engine coverage ≥ 85 %.
5. A dry-run Azure adapter spike (stub implementing `ObjectStorage` against the simulator
   contract tests) compiles without touching `engine` or `services` — proves the seam for
   phase 6.

## Risks

- **Refactor + rewrite collision.** Phases 1–3 touch the same files; mitigation is the strict
  sequencing above and mechanical-move-only PRs (no behavior change + tests move together).
- **Wire-format breakage** with serde renames — contract tests snapshotting JSON for every IPC
  payload before starting, replayed after.
- **Long-lived epic branch divergence.** Keep PRs into master behind the phase gate small;
  avoid a mega-branch; the module moves are safe to land continuously.
