# Phase 0 — Foundations & Guardrails

**Theme:** Make the codebase safe to overhaul before overhauling it.
**Depends on:** nothing. **Blocks:** all other phases.
**Estimated size:** 1–2 weeks of focused work.

## Why this phase exists

Phases 1–5 rewrite the sync engine, the backend module layout, and the entire frontend shell.
Today the only automated gates are 221 vitest tests and an implicit `tsc` via `bun run build`.
The Rust side — where all the dangerous logic lives — has tests (`cargo test` target exists) but
**CI never runs them** (`.github/workflows/ci.yml` runs `bun run test`, builds, and stops).
There is no clippy, no rustfmt check, no ESLint, no coverage measurement, and no way to test the
sync engine against a fake remote without real cloud credentials.

## Workstreams

### 0.1 CI hardening

- [x] Add `cargo test --manifest-path src-tauri/Cargo.toml` to `ci.yml` on all three OS runners.
      Done — wired into the build matrix. 278 tests pass locally on Windows; watch the first
      macOS/Linux CI runs for path/keyring assumptions.
- [x] Add `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` jobs.
      Done — the lint backlog turned out small (~35): all fixed except three
      `too_many_arguments` allows annotated as phase-3 restructuring debt.
- [x] Add ESLint (typescript-eslint, strict + stylistic) and Prettier. Enforce in CI.
      Done — 204 initial violations fixed (none suppressed in production code; targeted
      rule relaxations for test-file ergonomics only).
      Configure rules that specifically target the current failure modes:
      `no-floating-promises`, `switch-exhaustiveness-check`, `no-unnecessary-condition`.
- [x] Run `tsc --noEmit` as an explicit CI step (done: `bun run typecheck`).
- [x] Add coverage reporting: `vitest --coverage` (gating job runs it; baseline recorded
      below) and an informational `cargo llvm-cov` CI job (Rust baseline lands with the
      first CI run). Ratchet, don't gate, initially.
- [x] Cache Rust builds in CI (`Swatinem/rust-cache` on all Rust jobs).

### 0.2 Sync-engine test harness (the key deliverable)

The single most valuable safety net for phases 1–3: an in-process fake object store plus a
scenario runner, so sync behavior can be asserted without cloud credentials.

- [x] Extract an `ObjectStorage` trait from the current provider seam.
      Done as `storage/sim/object_storage.rs` (list/head/get/put/delete/copy; versions and
      lifecycle join in phase 1 when the engine needs them). The trait is sync and test-gated
      for now; phase 3 promotes it, makes it async, and implements it for the real adapters.
- [x] Implement `MemoryObjectStore` — done: one-shot injectable failures per op/key,
      op accounting, md5-like and multipart-like etag modes. (Latency injection deferred
      until phase 2's timeout logic can observe it.)
- [x] Build a **sync simulator** — done (`storage/sim/simulator.rs`): in-memory local
      tree + MemoryObjectStore + anchors driven through plan → execute cycles with per-item
      error isolation, plus convergence assertions (`run_until_settled`, `is_converged`).
- [x] Scenario suite — done: 20 scenario tests documenting current behavior (including
      the delete/first-sync gaps, marked as such) + 6 `#[ignore]`d truth cases encoding the
      phase-1 definition of done (`storage/sim/scenarios.rs`).
- [x] Property-style convergence skeleton — done with a dependency-free seeded-LCG loop
      (`truth_random_mutation_sequences_converge_without_loss`, ignored). **Deviation:**
      proptest is not added yet; promote the skeleton to proptest when phase 1 enables it,
      so CI doesn't pay the dependency for an ignored test.

### 0.3 Repo hygiene

- [x] Commit the uncommitted working tree — done (GCS-parity work landed as its own commit).
- [x] Branching model documented in CONTRIBUTING (`overhaul/phase-N` epic branches, PRs into
      master, CI green required). Note: phase 0 itself landed as direct commits on master —
      the gates it creates are what make the branch discipline enforceable from phase 1 on.
- [x] Briefs moved to `docs/`; README roadmap section now points at `backlog/`;
      `AGENTS.md` carries the current-phase pointer. (Full user-facing README rewrite is
      phase 6.4.)
- [x] `CONTRIBUTING.md` added; bun is the single package manager; `package-lock.json`
      deleted.
- [x] Stray `NUL` deleted; `.gitignore` fixed (node_modiules typo) and extended
      (target/, coverage/, .eslintcache).
- [x] Issue templates added (`bug_report`, `overhaul_task`).

### 0.4 Error taxonomy & activity/logging groundwork

Every Rust error today is a `Result<_, String>` built with `format!`. Phases 1–2 need to
distinguish retryable vs fatal vs conflict vs configuration errors, and phase 5 needs to render
them for humans. Laying the type down now avoids re-touching every call site twice.

- [x] `SyncError` introduced (`storage/error.rs`). **Deviation:** a struct with a
      `SyncErrorKind` field instead of a thiserror enum — the flat shape serializes more
      stably (`kind`, `message`, `retryAfterSeconds`) and avoids a new dependency; Display
      preserves existing message text exactly.
- [x] Both adapters and the `object_store` seam converted end-to-end. Classification:
      S3 via SDK error codes + dispatch/timeout variants; GCS via HTTP status (+ Retry-After
      capture); local fs failures are `Storage`; network sends are `Transient`. `commands.rs`
      still speaks `Result<_, String>` through the `From` escape hatches, as planned.
- [ ] **Deferred to phase 3.** The `tracing` migration touches every orchestration call
      site in `commands.rs`, which phase 3 is about to dismantle — doing it now would be
      done twice. Not required by any phase-0 acceptance criterion.

### 0.5 Baseline measurements

Record before/after numbers for the overhaul's marketing and for regression detection:

- [x] Scan + plan baselines recorded below (scaled to 10k real files / 100k synthetic
      paths to keep the probe rerunnable in seconds). Cloud transfer baselines (1 GB
      up/down per provider, 10k-small-file sync) **pending a manual run with credentials**
      — tracked for the start of phase 2.
- [ ] Cold-start / time-to-interactive: pending a manual desktop-app run (needs the app
      launched with a populated profile; measure at phase 2 start alongside transfer
      baselines).
- [x] Frontend bundle size recorded below; installer size lands with the first signed
      build (phase 6).
- [x] `scripts/bench/run-benches.ps1` + ignored `bench_*` Rust probes
      (`storage/bench_baseline.rs`), rerunnable via
      `cargo test --release -- --ignored bench_ --nocapture`.


### Baseline results (2026-07-19, Windows 11, release build)

| Probe | Result |
|-------|--------|
| `build_sync_plan`, 100k observed paths (50k synced / 25k up / 25k down) | **203 ms** (~493k paths/s) |
| `scan_local_folder`, 10k small files (walk + SHA-256) | **1.28 s** (~7.8k files/s, ~13 s per 100k extrapolated) |
| `bun run build` (tsc + vite) | **20.6 s** |
| `dist/` bundle size | **988 KB** |
| vitest suite (221 tests) | **~33 s** |
| cargo test suite (278 tests) | **< 1 s** execution (post-compile) |
| Frontend line coverage (vitest v8) | **80.9 %** (5101/6305 lines) |
| Rust line coverage (cargo llvm-cov) | pending first CI run of the informational job |
| 1 GB transfer per provider / 10k-file cloud sync / app cold start | pending manual run with credentials (phase 2 start) |

## Acceptance criteria

1. ✅ CI runs and gates on: vitest, `tsc --noEmit`, ESLint, Prettier, `cargo test` (3 OSes),
   clippy `-D warnings`, rustfmt. *(Workflows merged; verify green on the first push to a
   remote with Actions enabled — the macOS/Linux runners are the untested surface.)*
2. ✅ `MemoryObjectStore` + simulator exist; 20 scenario tests run in CI; 6 ignored truth
   cases reference phase-1 tasks.
3. ✅ Working tree clean; bun only; docs moved; `CONTRIBUTING.md` exists.
4. ✅ `SyncError` used by both provider adapters (and the `object_store` seam) end-to-end.
5. ✅ Baselines recorded above; cloud-transfer and cold-start rows intentionally pending
   (tracked for phase 2 start).

## Risks

- **Clippy/lint backlog explodes.** Mitigation: allow-list with tracked debt items; do not let
  phase 0 become a rewrite — mechanical fixes only.
- **Rust tests fail on CI runners** (keyring/DPAPI/OS assumptions). Mitigation: feature-gate
  OS-secure-store tests (`tauri-command-tests` feature already exists as a pattern), fake the
  keyring in tests.
- **Trait extraction destabilizes adapters before phase 1.** Mitigation: extraction is
  signature-only; adapters keep their bodies; simulator is additive.
