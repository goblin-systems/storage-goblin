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

- [ ] Add `cargo test --manifest-path src-tauri/Cargo.toml` to `ci.yml` on all three OS runners.
      All 254 Rust tests pass locally on Windows (verified 2026-07-19); expect some breakage on
      the macOS/Linux runners from Windows-path and keyring-dependent assumptions.
- [ ] Add `cargo clippy --all-targets -- -D warnings` and `cargo fmt --check` jobs.
      First run will produce a large backlog of lints; triage into (a) auto-fix now,
      (b) `#[allow]` with a linked backlog item, (c) real bugs → move to phase 1 list.
- [ ] Add ESLint (typescript-eslint, strict + stylistic) and Prettier. Enforce in CI.
      Configure rules that specifically target the current failure modes:
      `no-floating-promises`, `switch-exhaustiveness-check`, `no-unnecessary-condition`.
- [ ] Run `tsc --noEmit` as an explicit CI step (don't rely on the build step to catch it).
- [ ] Add coverage reporting: `vitest --coverage` and `cargo llvm-cov`. Record the baseline
      numbers in this document; ratchet, don't gate, initially.
- [ ] Cache Rust builds in CI (sccache or `Swatinem/rust-cache`) — the Tauri build already
      compiles everything, so wall-time should not meaningfully regress.

### 0.2 Sync-engine test harness (the key deliverable)

The single most valuable safety net for phases 1–3: an in-process fake object store plus a
scenario runner, so sync behavior can be asserted without cloud credentials.

- [ ] Extract an `ObjectStorage` trait from the current provider seam
      (`object_store.rs` already routes S3/GCS; formalize the interface it implies:
      list, head, get, put, delete, copy, versions, lifecycle).
- [ ] Implement `MemoryObjectStore` (HashMap-backed, with injectable failures, latency, and
      etag semantics for both "md5-like" and "multipart-like" etags).
- [ ] Build a **sync simulator**: given (local fixture tree, remote fixture state, anchor DB
      state) run plan → execute → assert (final local tree, final remote state, final anchors,
      emitted operations). Express scenarios as table-driven tests.
- [ ] Port the highest-value existing scenarios into the simulator, then add the currently
      *failing* truth cases as `#[ignore]`d tests documenting phase-1 targets:
      local delete propagates, rename propagates, preserve-both duplicates, identical-content
      first sync is a no-op.
- [ ] Property-based test skeleton (proptest): random sequences of local/remote mutations must
      converge to equal trees with no data loss (enable in phase 1 once deletes exist).

### 0.3 Repo hygiene

- [ ] Commit the currently uncommitted working tree (28 modified + 5 new files sitting on
      `master`) as one or more reviewed commits so the overhaul starts from a clean, known state.
- [ ] Adopt a branching model for the overhaul: `overhaul/phase-N` epic branches, PRs into
      master, CI green required.
- [ ] Move `brief.md` / `gcs-parity-brief.md` into `docs/` (or this `backlog/`); make `README.md`
      user-facing only. `AGENTS.md` stays at root and gets a "current overhaul phase" pointer.
- [ ] Add `CONTRIBUTING.md` covering: bun vs npm (both lockfiles exist today — pick **bun**,
      delete `package-lock.json`), how to run each test suite, how to run the app.
- [ ] Delete or gitignore stray artifacts (`NUL` in repo root).
- [ ] Add issue templates mapping to this backlog's phase/task IDs.

### 0.4 Error taxonomy & activity/logging groundwork

Every Rust error today is a `Result<_, String>` built with `format!`. Phases 1–2 need to
distinguish retryable vs fatal vs conflict vs configuration errors, and phase 5 needs to render
them for humans. Laying the type down now avoids re-touching every call site twice.

- [ ] Introduce `enum SyncError` (thiserror): `Auth`, `NotFound`, `Precondition` (etag/If-Match
      failures), `Transient` (network/5xx/throttle, carries retry-after), `Storage` (disk),
      `Config`, `Internal`. Serialize a stable shape over IPC (`code`, `message`, `detail`).
- [ ] Convert the provider adapters (`s3_adapter.rs`, `gcs_adapter.rs`) first; leave a
      `From<String>` escape hatch so conversion of `commands.rs` can proceed incrementally.
- [ ] Replace ad-hoc `emit_*_activity` string plumbing with `tracing` spans + a subscriber that
      feeds the existing activity event channel, so log detail stops being hand-formatted
      key=value strings (`commands.rs:1793-1798` style).

### 0.5 Baseline measurements

Record before/after numbers for the overhaul's marketing and for regression detection:

- [ ] Time + peak RSS for: initial scan of 100k-file tree; plan build; 1 GB single-file
      upload/download per provider; 10k-small-file sync.
- [ ] Cold-start time of the app; time-to-interactive of the UI with 50k-entry tree.
- [ ] Binary/installer size.
- [ ] Script these (`scripts/bench/`) so they're rerunnable in phase 2 and phase 6.

## Acceptance criteria

1. CI runs and gates on: vitest, `tsc --noEmit`, ESLint, Prettier, `cargo test` (3 OSes),
   clippy `-D warnings`, rustfmt.
2. `MemoryObjectStore` + simulator exist; ≥ 15 scenario tests run in CI; the known-broken truth
   cases are encoded as ignored tests referencing phase-1 tasks.
3. Working tree is clean; bun is the single package manager; docs moved; `CONTRIBUTING.md` exists.
4. `SyncError` taxonomy is used by both provider adapters end-to-end.
5. Baseline benchmark numbers are recorded in this file.

## Risks

- **Clippy/lint backlog explodes.** Mitigation: allow-list with tracked debt items; do not let
  phase 0 become a rewrite — mechanical fixes only.
- **Rust tests fail on CI runners** (keyring/DPAPI/OS assumptions). Mitigation: feature-gate
  OS-secure-store tests (`tauri-command-tests` feature already exists as a pattern), fake the
  keyring in tests.
- **Trait extraction destabilizes adapters before phase 1.** Mitigation: extraction is
  signature-only; adapters keep their bodies; simulator is additive.
