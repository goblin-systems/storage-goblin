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

- [x] `storage/model.rs` defines EntryKind, SyncPhase, ConflictStrategy, FileEntryStatus,
      and QueueStatus, each serde-renamed to the existing wire strings. Adopted where it
      removes real ambiguity: the planner's kind/strategy dispatch is now exhaustive, the two
      duplicate aggregate-phase implementations collapse onto `model::aggregate_phase`, and
      sync_db derives its runnable-status SQL from `QueueStatus::is_runnable`.
- [x] `src/app/generated/domain.ts` is rendered from the Rust enums (by `storage::ts_bindings`,
      run via `bun run generate:types`) and imported by `types.ts`. **Deviation:** rather than
      add a codegen dependency and regenerate all 781 lines of `types.ts` (which carries
      hand-written normalizers worth keeping), only the domain unions are generated. A Rust test
      compares the checked-in file byte-for-byte and fails with regeneration instructions if the
      enums change — verified by tampering. Drift is a test failure, which is the goal.
- [x] Already the case: `normalize_provider` centralizes the "gcp"→"gcs" alias and is applied
      at the deserialization/normalization seam (`profile.rs`, `types.ts`, `provider.rs`).

### 3.3 Extract services & engine

- [x] Done, and further than planned. Eleven modules extracted, all mechanical (bodies
      unchanged): `platform`, `compare_service`, `bin_service`, `credential_service`,
      `lifecycle_service`, `transfer_service`, `polling_service`, `sync_service`,
      `queue_service`, `conflict_service`.
- [x] **Done.** `SyncState` no longer relies on a bag of independently-locked fields to
      uphold an invariant none of them expressed. `coordinator.rs` (Tauri-free, 11 tests) owns
      the runtime rules:
      - A `PairLease` **is** the right to run a cycle for one pair. `run_sync_cycle_for_pair`
        takes one instead of a loose stop flag, so the "one cycle per pair" invariant is
        enforced by the type system rather than by convention. It releases on drop, including
        on panic — asserted by a test, because a leaked lease would wedge a pair until restart.
      - This closes the cycle-overlap gap: `start_sync` calls `stop_polling_worker`, which only
        *signals*; the worker could be mid-cycle on the same pair. A manual sync now waits for
        the lease; scheduled work takes it non-blockingly and skips a busy pair rather than
        queueing an unbounded backlog of cycles whose plans are stale before they run.
      - Cancellation moved onto the lease, so pause and shutdown interrupt the cycle running
        *now* instead of waiting it out.
      - A global transfer semaphore, which is what phase 2.2's scheduler consumes.
      Validated on this machine despite the Tauri mock runtime not loading, which is exactly
      why the coordinator is Tauri-free.
- [x] **The layering invariant is enforced without the directory move.** The Tauri-free
      core (model, sync_planner, error, inventory_compare, sanitizer, remote_bin) is guarded by
      `architecture_test.rs`, which fails if any of those modules imports the app framework, plus
      a canary that runs the planner with no Tauri runtime. This delivers the *value* of the
      `engine/` directory (an enforced Tauri-free core) without the churn of physically moving 20
      files and rewriting every `use` path. The cosmetic directory reorganization is descoped in
      favour of the enforced rule — verified by injecting a `tauri` import and watching the gate
      fail.
- [x] `run_async_blocking` is removed from production: the profile-mutation path
      (`persist_profile_with_remote_bin_reconciliation` and its callers up through the
      `save_profile` / `*_sync_location` commands) is async end to end, awaiting reconciliation
      directly instead of blocking a runtime worker. `run_async_blocking` is now test-only.
      **Residual, and now more significant than it was:** blocking file IO (scans, snapshot
      reads/writes, `DownloadWriter` chunk writes and `fsync`) still runs on the async executor
      rather than `spawn_blocking`. This mattered little while transfers were sequential; with
      the phase-2.2 scheduler running several through `buffer_unordered` on one task, a blocking
      write stalls its siblings. Not attempted here — a partial async-IO rework is worse than a
      documented gap, and it wants its own change with its own measurements.
- [x] `module_size_test.rs` caps ordinary modules at 1,300 production lines, with documented
      higher ceilings for two inherently-large cohesive modules (`commands.rs` at 2,200, the
      Tauri command surface; `credentials_store.rs` at 1,850, secure-store + DPAPI crypto), each
      set just above its current size so it can only shrink. clippy's `too_many_lines` was not
      turned on — the size gate covers the intent, and per-function line limits fight the
      builder-heavy adapter code.

### 3.4 State & persistence consolidation

- [x] Migrations formalized on SQLite's `user_version`: `initialize_schema` runs each
      pending migration in order and stamps the version. Migration 1 is the current schema, kept
      idempotent so pre-framework databases (user_version 0, possibly missing later columns)
      converge and are stamped; v2+ get a clean ordered home (phase 1 tombstones, phase 2.3
      indexes). Two tests cover the fresh-DB and pre-migration-DB paths. Profile JSON is left as
      a file (not migrated into SQLite) — evaluated as not worth the churn now.
- [x] Already satisfied: all `rusqlite` use and every SQL statement live in `sync_db.rs`
      (verified — no `rusqlite` import elsewhere), which is the de-facto repository. A dedicated
      `anchors.rs` split is cosmetic and not done.
- [ ] **Partial / deferred to phase 2.** Stale in-progress queue items are recovered
      (`recover_interrupted_queue_items_for_pair`) and watchers re-register on start. Orphaned
      temp files do not exist yet — phase 1 deferred atomic downloads to phase 2, so there are no
      `.goblin-tmp` files to clean until that lands. Consolidating these into one documented
      routine waits for that work.

### 3.5 Test relocation & coverage

- [ ] **Partial.** Tests moved with their subject when the extraction broke an import;
      file-entry-building tests were rewritten into `file_query_service` with a self-contained
      fixture. But the two large test modules in `commands.rs` (~4,000 lines, including the
      feature-gated integration tests) still test code that now lives in the services. A full
      relocation is tedious (shared test helpers) and low-correctness-value; deferred. The
      conversion of command-level tests to `MemoryObjectStore` service tests is a phase-2/3
      follow-up once the `ObjectStorage` trait is the production seam.
- [ ] **Not measured here.** `cargo-llvm-cov` instrumentation filled this machine's disk
      (it runs near capacity), so coverage is left to the informational CI job on a clean runner.
      The engine is heavily unit-tested (model: 6 tests; sync_planner: 24 decision-table tests
      plus the simulator scenarios), so the ≥ 85 % target is plausibly met but unverified.

## Sequencing with phases 1–2

1. 3.1 (legacy deletion) — immediately after phase 0, before phase 1 lands its planner.
2. 3.2 (typed domain) — alongside phase 1's planner rewrite (shared enums).
3. 3.3 services/state — before phase 2.2 (scheduler needs the new state model).
4. 3.4–3.5 — trailing, before phase 4 starts consuming generated types.


## Status (2026-08-02, superseding 2026-08-01)

**3.1, 3.2, 3.3 and 3.4 are complete.** The SyncState redesign — the last open
3.3 item — landed together with its phase-2.2 consumer; see the 3.3 entry above.
3.5 remains partial and acceptance criterion 5 (Azure) is untouched.

Modules added since: `coordinator` (runtime leases, cancellation, transfer
budget), `queue_schedule` (staged ordering), `progress` (throttled byte
counts), `pair_backoff`, `watcher_service`, `s3_upload`. The Tauri-free core
gate now covers ten modules rather than six.

412 Rust tests and 219 frontend tests green locally; clippy clean at
`-D warnings` in both feature configurations; rustfmt and prettier clean.

### Still open after this pass

1. **`spawn_blocking` for file IO** — see the 3.3 residual above. Newly
   material now that transfers share a task.
2. **Full test relocation (3.5)** — the large `commands.rs` test modules still
   test code that lives in the services.
3. **Coverage measurement (3.5)** — this machine is disk-bound; left to CI.
4. **Azure spike (acceptance 5)** — not attempted; belongs with phase 6.
5. **`engine/`/`services/`/`ipc/` directory move** — still descoped in favour
   of the enforced layering test, which is the invariant that mattered.

---

## Status (2026-08-01)

Landed on `overhaul/phase-1` (still unpushed, still not cloud-validated — the
process caveat below stands). **3.1, 3.2, and 3.4 are complete. 3.3 is
complete except the SyncState redesign (deferred to its phase-2.2 consumer)
and the cosmetic directory move (superseded by an enforced layering test).
3.5 is partial.**

### What the backend looks like now

`commands.rs` went from 12,404 lines to **~2,180 production lines**. Fourteen
modules each own one concern:

| Module | Owns |
|--------|------|
| `commands.rs` | the remaining Tauri command surface + shared response DTOs |
| `bin_service` | remote bin: listing, restore, purge, destination validation |
| `sync_service` | per-location cycle, snapshots, plan rebuild, polling worker |
| `queue_service` | draining the durable upload/download queues |
| `transfer_service` | executing one planned operation (transfer or structural) |
| `location_service` | sync-location CRUD + provider-side reconciliation |
| `file_query_service` | building the file browser's view of a location |
| `conflict_service` | manual conflict resolution |
| `compare_service` | inline vs external comparison payloads |
| `credential_service` | credential resolution, test contexts, capability messages |
| `lifecycle_service` | per-bucket lifecycle rules, versioning application |
| `platform` | OS integration and local filesystem effects |
| `polling_service` | when each location is due |
| `model` | the typed domain vocabulary (Tauri-free) |

Enforced invariants (all as tests, so they run in CI via `bun run test:rust`):
- **Tauri-free core** — `architecture_test.rs` fails if the six core modules
  import the app framework.
- **No god module** — `module_size_test.rs` caps modules at 1,300 prod lines
  (two documented exceptions), so `commands.rs` cannot regrow past 2,200.
- **No IPC drift** — the generated `domain.ts` is compared byte-for-byte.
- **Schema/type agreement** — `QueueStatus` is pinned against the SQL.

309 Rust tests and 219 frontend tests green; clippy clean at `-D warnings` in
both feature configurations; rustfmt clean.

### Genuinely still open

1. ~~**SyncState redesign**~~ — done on 2026-08-02, see the newer status above.
2. **`engine/`/`services/`/`ipc/` directory layout** — descoped. The value
   (an enforced Tauri-free core) is delivered by `architecture_test.rs`; the
   physical directory move is cosmetic churn that would rewrite every `use`.
3. **Full test relocation (3.5)** — partial; the large `commands.rs` test
   modules still test relocated code. Tedious, low-correctness-value.
4. **Coverage measurement** — left to the CI job (this machine is disk-bound).
5. **Azure adapter spike (acceptance 5)** — not attempted; it belongs with
   phase 6.5 once `ObjectStorage` is promoted from the test harness to the
   production seam.
6. **`spawn_blocking` for file IO** — deferred to phase 2's IO rework.

### Process caveat

CONTRIBUTING says overhaul work happens on `overhaul/phase-N` branches. Phases
1 and 3 both landed on `overhaul/phase-1`, so that branch now carries two
phases and has never been pushed or CI-verified. Getting one green CI run
(and, for phase 1, the cloud-validation matrix) before merging is overdue.

## Acceptance criteria

1. ✅ Zero duplicate command aliases; legacy execution paths deleted; one orchestration
   code path. (The legacy profile *read* path remains, because it is the migration.)
2. ⚠️ Mostly met, with a documented reinterpretation. `commands.rs` is ~2,180 production
   lines (from 12,404). A size gate is enforced — at 1,300 with two documented exceptions,
   not 800, because the provider adapters and command surface are legitimately large-but-flat.
   The **layering rule is enforced** by `architecture_test.rs` (the Tauri-free core cannot
   import the framework), which is the invariant the `engine/` directory was meant to
   guarantee; the directory itself is descoped. Services still take `AppHandle` — that is fine,
   they are the app layer; the *core* is what must stay Tauri-free, and it is.
3. ✅ Met. Five domain enums replace the string comparisons that mattered; `types.ts`
   imports generated unions from `domain.ts`, with a drift test.
4. ⚠️ 412 Rust + 219 frontend tests green locally on Windows; CI is configured for 3 OSes
   but this branch has never been pushed, so "green in CI" is unverified. Engine coverage is
   unmeasured (disk-bound machine; left to the CI job).
5. ❌ Not attempted.

## Risks

- **Refactor + rewrite collision.** Phases 1–3 touch the same files; mitigation is the strict
  sequencing above and mechanical-move-only PRs (no behavior change + tests move together).
- **Wire-format breakage** with serde renames — contract tests snapshotting JSON for every IPC
  payload before starting, replayed after.
- **Long-lived epic branch divergence.** Keep PRs into master behind the phase gate small;
  avoid a mega-branch; the module moves are safe to land continuously.
