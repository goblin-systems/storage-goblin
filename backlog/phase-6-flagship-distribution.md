# Phase 6 — Flagship Polish & Distribution

**Theme:** Everything between "great app on my machine" and "product strangers install,
trust, and recommend": signing, updates, crash visibility, security posture, docs, and the
third provider that proves the architecture.
**Depends on:** Phases 1–5. Some items (signing, crash reporting) can start earlier in
parallel; sequencing notes inline.
**Estimated size:** 3–4 weeks + ongoing release cadence.

## Current state (verified findings)

1. **Unsigned builds**: CI builds with `--no-sign` (`ci.yml:65`); the release workflow
   (`release.yml`) produces artifacts with no code signing on any platform → SmartScreen and
   Gatekeeper warnings on every install. No notarization for macOS.
2. **No update mechanism** — users must manually discover, download, and reinstall releases.
3. **No crash reporting or diagnostics channel**; the only field data is a user manually
   sending the debug log file.
4. **Security posture unaudited**: CSP carries dev-server entries in production config
   (`tauri.conf.json:29`), `withGlobalTauri: true` (phase 4.4 removes), capability file
   unreviewed, credential redaction claims untested against adversarial log content,
   dependency audit not in CI.
5. **Docs are internal-facing**: README describes architecture, not the product; no website,
   no screenshots, no setup guides for creating least-privilege S3/GCS credentials — the
   hardest real-world step for the target user.
6. **Provider abstraction unproven** beyond the two founding providers; Azure is named in the
   roadmap.

## Workstreams

### 6.1 Release engineering (start early — can run parallel to phases 3–5)

- [ ] Windows: Authenticode signing (EV or standard cert decision; consider Azure Trusted
      Signing for cost); sign both installer (NSIS/MSI) and binary.
- [ ] macOS: Developer ID signing + notarization + stapling in `release.yml`; universal or
      per-arch builds decided by download telemetry-free heuristic (ship both).
- [ ] Linux: AppImage + .deb + .rpm from Tauri bundler; publish a signed apt/repo story or at
      minimum checksums + GPG signatures. Evaluate Flathub as the primary Linux channel.
- [ ] **Auto-update**: `tauri-plugin-updater` with signed update manifests, staged rollout
      (percentage-based), release channels (stable/beta), and an in-app "what's new" note.
      Update check is opt-out, background, and never interrupts a running sync.
- [ ] Reproducible-ish builds: pin toolchains (`rust-toolchain.toml`, bun version), lockfile
      discipline (single lockfile — from phase 0.3), SBOM generation in release artifacts.
- [ ] Release checklist doc + smoke-test matrix (fresh install, upgrade-with-existing-data,
      migration from oldest supported version) executed per release.

### 6.2 Observability & support

- [ ] Opt-in crash reporting (sentry-rust + JS SDK, or a self-hosted alternative given the
      privacy-sensitive audience — decide via ADR; default OFF, one-click enable at first
      run with plain-language scope description).
- [ ] Structured diagnostics bundle: one button producing a zip (redacted logs, config
      shape without secrets, versions, OS info, anonymized path stats) for issue reports;
      redaction covers keys, bucket names, and user paths — with tests.
- [ ] In-app health surface (About → Diagnostics): DB integrity check, last N sync outcomes,
      watcher status, pending queue depth — turns "it's being weird" reports into data.
- [ ] Public issue templates that reference the diagnostics bundle.

### 6.3 Security hardening & audit

- [ ] Threat-model doc (STRIDE-lite): credentials at rest (Credential Manager + DPAPI files —
      document and verify the design in `credentials_store.rs`), credentials in memory/logs,
      IPC surface, update channel, malicious bucket contents (path traversal on download —
      `resolve_local_download_path` exists at `commands.rs:578`; add adversarial tests:
      `..`, absolute paths, symlinks, Windows device names).
- [ ] Production CSP tightened (drop dev-server entries); Tauri capability file minimized to
      used APIs.
- [ ] `cargo audit` + `bun audit`-equivalent (osv-scanner) in CI, scheduled weekly.
- [ ] Redaction test-suite for the activity log (secrets never render even if a provider
      error echoes them back).
- [ ] External or community security review of the credential and update paths before 1.0.

### 6.4 Documentation & product presence

- [ ] User docs site (static, in-repo `docs/` published via Pages): install guides per OS,
      **least-privilege credential setup walkthroughs for S3 and GCS** (IAM policy / service
      account JSON snippets — the single highest-leverage doc for this audience), sync
      concepts (what merge/mirror/conflict-keep-both actually do), cost notes (storage class,
      egress, request pricing implications of polling intervals), troubleshooting.
- [ ] README rewritten as the product front door: what/why, screenshots/GIF (goblin-canvas
      animations for the homepage card), install buttons, feature matrix per provider,
      link to backlog/roadmap.
- [ ] Versioned changelog discipline (conventional commits already loosely in use → generate).
- [ ] Beta program: recruit 10–20 target users after phase 5, feedback loop into a 1.0
      punch-list milestone.

### 6.5 Azure Blob Storage (the architecture proof)

Ship the third provider entirely through the seams built in phases 0–3:

- [ ] `providers/azure.rs` implementing `ObjectStorage` (azure_storage_blobs SDK or REST):
      list/get/put/delete, blob versioning, soft-delete (maps to remote-bin), access tiers
      Hot/Cool/Archive (maps to storage-class actions), block-blob chunked upload through the
      phase-2 transfer engine.
- [ ] Credential model: account+key and SAS first; note Entra ID as follow-up. Reuse the
      DPAPI/keyring storage patterns.
- [ ] Capability declaration in `provider.rs`; conformance suite = the simulator contract
      tests from phase 0.2 run against Azurite in CI + manual cloud checklist.
- [ ] **Success metric: zero changes required in `engine/` or `services/`.** Any change
      needed there is filed as an architecture defect and fixed at the seam.

### 6.6 Performance & quality bar for 1.0

- [ ] Re-run phase-0 benchmarks; publish numbers in docs (flagship products state their
      limits: max tested file size, tree size, memory envelope).
- [ ] Startup < 2 s to interactive with 100k-entry tree (virtualized tree + SQLite indexes
      from phase 2.3 should make this achievable — measure, don't assume).
- [ ] Installer size budget; strip debug symbols to a separate upload for crash symbolication.
- [ ] Two-week zero-P1 soak on the beta channel before tagging 1.0.

## Acceptance criteria

1. Signed, notarized installers for Windows/macOS + Linux packages, produced by CI with no
   manual steps; auto-update delivers a canary release end-to-end on all three OSes.
2. Opt-in crash reporting live; diagnostics bundle redaction verified by tests; issue
   templates use it.
3. Threat-model doc merged; adversarial download-path tests pass; dependency audit in CI;
   production CSP/capabilities minimized.
4. Docs site live with per-provider least-privilege guides; README is user-facing with visuals.
5. Azure ships to beta with the conformance suite green and zero engine/services diffs.
6. 1.0 tagged after the beta soak; benchmarks published.

## Risks

- **Signing certificates and Apple accounts take calendar time** (weeks) — kick off
  procurement at the start of phase 3, not phase 6.
- **Privacy backlash on telemetry** for this audience — everything opt-in, transparent, and
  self-hostable; state it loudly in docs.
- **Azure semantics differences** (no true directories, flat listing quirks, tier rehydration
  latency for Archive) — timebox a spike against Azurite in phase 3's dry-run before
  committing scope here.
