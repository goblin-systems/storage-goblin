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

- [x] Confirmed and deleted. The frontend used the `*_sync_location` forms throughout;
      the `*_sync_pair` aliases and `validate_s3_connection` are gone.
- [x] The migration already ran on every `read_profile_from_disk` (and relocates the index
      snapshots), so a configured flat profile becomes a location on first read. The legacy
      *read* path is retained deliberately — it is what performs the migration — but every
      legacy *execution* path is deleted, so there is nothing left for it to feed.
- [x] All deleted, plus 23 further helpers the compiler then reported unreachable, the 12
      flat-profile variants in `sync_db`, and the flat index writers. **Deviation:** the
      `_for_pair` suffixes were *kept*. Renaming ~40 functions would have churned every call
      site and test for cosmetics; better done when phase 3.3's module boundaries make the
      suffix genuinely redundant.
- [x] Outcome: 12,890 -> 10,926 lines (~2,800 production lines removed; the file also
      carries ~4,000 lines of tests).

### 3.2 Typed domain model

- [ ] Introduce the `model.rs` enums; serde-rename to today's wire strings so the frontend and
      persisted JSON/SQLite stay compatible; exhaustive `match` replaces string comparison.
- [ ] Generate TypeScript types from Rust (ts-rs or specta/tauri-specta) so `src/app/types.ts`
      (781 hand-written lines) becomes generated output — single source of truth for the IPC
      contract, drift becomes a compile error. (Coordinates with phase 4.)
- [ ] Provider alias normalization ("gcp"→"gcs") happens exactly once, at deserialization.

### 3.3 Extract services & engine

- [x] Done, and further than planned. Eleven modules extracted, all mechanical (bodies
      unchanged): `platform`, `compare_service`, `bin_service`, `credential_service`,
      `lifecycle_service`, `transfer_service`, `polling_service`, `sync_service`,
      `queue_service`, `conflict_service`.
- [ ] **Not done.** `SyncState` is still the shared-mutex model; the actor/`tokio::sync`
      redesign has not started. This remains the blocker for phase 2.2's parallel scheduler,
      and the sequencing note still holds. Related: phase 3.1 removed the legacy
      cycle-overlap lock along with its only caller, and the per-pair path has never had
      one — see the note in phase 2.
- [ ] **Not done.** The planner was rewritten in place during phase 1; it still lives at
      `storage/sync_planner.rs` rather than under an `engine/` directory. The rewrite avoided
      double churn as intended, but the directory move did not happen.
- [ ] **Not done.** `run_async_blocking` survives, and blocking file IO still runs on the
      async executor rather than `spawn_blocking`.
- [ ] **Not added.** Adding the gate now would fail: `commands.rs` (2,666 production lines)
      and `bin_service.rs` (1,007) are still over. Worth adding together with the remaining
      splits so it lands green.

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


## Status (2026-07-20)

Landed on `overhaul/phase-1` (phase 3 work continued on the same branch rather
than a fresh epic branch — see the caveat below). **3.1 is complete; 3.3 is
substantially done; 3.2, 3.4, and 3.5 are not started.**

### What the backend looks like now

`commands.rs` went from 12,404 lines to **2,666 lines of production code**
(plus ~4,000 lines of tests still co-located). Eleven modules now own one
concern each:

| Module | Prod lines | Owns |
|--------|-----------:|------|
| `commands.rs` | 2,666 | Tauri command surface, shared response types, profile/location CRUD |
| `bin_service.rs` | 1,007 | remote bin: listing, restore, purge, destination validation |
| `sync_service.rs` | 746 | per-location cycle, snapshots, plan rebuild, polling worker |
| `queue_service.rs` | 737 | draining the durable upload/download queues |
| `transfer_service.rs` | ~540 | executing one planned operation (transfer or structural) |
| `compare_service.rs` | 356 | inline vs external comparison payloads |
| `conflict_service.rs` | ~300 | manual conflict resolution |
| `platform.rs` | ~270 | OS integration and local filesystem effects |
| `lifecycle_service.rs` | 251 | per-bucket lifecycle rules, versioning application |
| `polling_service.rs` | ~150 | when each location is due |
| `credential_service.rs` | 138 | credential resolution, test contexts, capability messages |

296 Rust tests and 219 frontend tests green; clippy clean at `-D warnings` in
both feature configurations; rustfmt clean.

### Not done — do not mistake this for phase 3 complete

1. **3.2 typed domain model: not started.** The only typed enum is
   `sync_planner::Operation` (from phase 1). `EntryKind`, `Resolution`, `Phase`,
   `ConflictStrategy`, and `ProviderId` are still compared as raw strings, and
   `src/app/types.ts` is still 781 hand-written lines rather than generated from
   Rust. This is the highest-value remaining item: it is what stops IPC drift
   from being a runtime surprise.
2. **3.4 persistence consolidation: not started.** No repository layer, no
   formal migration table — raw SQL still lives outside a repository boundary.
   Note this is now entangled with two pending schema changes (phase 1's
   tombstones, phase 2.3's indexes); doing all three as one migration is the
   right sequencing.
3. **3.5 test relocation: only what the compiler forced.** Tests moved when
   their subject moved and the import broke; the two large test modules
   (~4,000 lines) still sit in `commands.rs` testing code that now lives
   elsewhere. Coverage targets (engine ≥ 85 %) are unmeasured.
4. **The target directory layout was not adopted.** Modules are flat siblings
   under `storage/` rather than `engine/` + `providers/` + `services/` + `ipc/`.
   Consequently the layering rule is **not** enforced: services still take
   `AppHandle`, so `tauri::` types reach into what should be a Tauri-free core.
   The extraction did the hard part (separating concerns); the directory move
   and the dependency rule are still open.
5. **`SyncState` redesign not started**, which still blocks phase 2.2.
6. **The Azure adapter spike (acceptance criterion 5) was not attempted.**

### Process caveat

CONTRIBUTING says overhaul work happens on `overhaul/phase-N` branches. Phases
1 and 3 both landed on `overhaul/phase-1`, so that branch now carries two
phases' worth of change and has never been pushed or CI-verified. Splitting it,
or at minimum getting one green CI run before merging, is worth doing before
this grows further.

## Acceptance criteria

1. ✅ Zero duplicate command aliases; legacy execution paths deleted; one orchestration
   code path. (The legacy profile *read* path remains, because it is the migration.)
2. ⚠️ Partially met. `commands.rs` still exists at 2,666 production lines (down from
   12,404) and `bin_service.rs` is 1,007; the ≤ 800 gate is therefore not added. The
   layering rule is **not** enforced and `tauri::` types are present throughout the
   services, because the `engine/` boundary was not created.
3. ❌ Not met. Only `Operation` is an enum; `types.ts` is still hand-written.
4. ⚠️ Tests are green locally on Windows (296 Rust, 219 frontend) and CI is configured
   for 3 OSes, but this branch has never been pushed, so "green in CI" is unverified.
   Engine coverage is unmeasured.
5. ❌ Not attempted.

## Risks

- **Refactor + rewrite collision.** Phases 1–3 touch the same files; mitigation is the strict
  sequencing above and mechanical-move-only PRs (no behavior change + tests move together).
- **Wire-format breakage** with serde renames — contract tests snapshotting JSON for every IPC
  payload before starting, replayed after.
- **Long-lived epic branch divergence.** Keep PRs into master behind the phase gate small;
  avoid a mega-branch; the module moves are safe to land continuously.
