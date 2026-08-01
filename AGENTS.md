# AGENTS.md

## Overhaul status

- **Phases 0, 1, 3 landed on `overhaul/phase-1`; phase 2 is partially landed** (all unpushed,
  no CI run, phase 1 not cloud-validated). Read the Status sections of
  `backlog/phase-1-sync-correctness.md`, `backlog/phase-2-transfer-reliability.md`, and
  `backlog/phase-3-backend-architecture.md` before building on this.
- **Backend layout:** the 12,404-line `commands.rs` is now ~2,180 lines (Tauri command surface +
  shared DTOs). Concerns live in sibling modules: `sync_service`, `queue_service`,
  `transfer_service`, `bin_service`, `location_service`, `file_query_service`, `conflict_service`,
  `compare_service`, `credential_service`, `lifecycle_service`, `polling_service`, `platform`,
  and the Tauri-free `model`. These are flat siblings, NOT the `engine/services/ipc` directory
  the doc once targeted — that layout is descoped in favour of an enforced invariant (below).
- **Enforced invariants (tests, so they run in CI):** `architecture_test.rs` — the Tauri-free
  core (model, sync_planner, error, inventory_compare, sanitizer, remote_bin) must not import the
  app framework; `module_size_test.rs` — no module over 1,300 prod lines (commands 2,200,
  credentials_store 1,850); `ts_bindings` drift test — `src/app/generated/domain.ts` matches the
  Rust enums byte-for-byte. Don't break these; regenerate types with `bun run generate:types`.
- **Typed domain:** `storage/model.rs` (EntryKind, SyncPhase, ConflictStrategy, FileEntryStatus,
  QueueStatus) serde-renamed to the existing wire strings. `decide_file_sync` and the
  aggregate-phase logic are exhaustive matches — keep them so.
- **Async:** profile mutations are async end to end; `run_async_blocking` is test-only. Provider
  adapters + object_store return `storage::error::SyncError`; `commands.rs` still uses
  `Result<_, String>` via `From` escape hatches.
- **DB migrations:** `sync_db` uses SQLite `user_version`; add a migration + bump `SCHEMA_VERSION`
  for any schema/data change. All SQL lives in `sync_db` — keep it there.
- **Sync engine (phase 1):** propagates deletes/renames, keeps both sides of a conflict,
  reconciles anchors. Local deletes go to the OS trash (`trash` crate), never `remove_file`.
  Mass-delete breaker: >25 deletes or >50% of a 10+ anchored tree become review items. Content
  fingerprints: GCS listings return them, S3 listings do not — first-sync merge is GCS-only.
- **Transfers (phase 2):** downloads stream through `transfer::DownloadWriter` to a
  `.goblin-tmp` sibling, then fsync + atomic rename — never write the destination path
  directly, and never index a temp file (`local_index` skips them; doing otherwise uploads
  partial content as a "local edit"). GCS objects ≥16 MiB use resumable sessions
  (`gcs_upload.rs`); S3 is still single-shot `put_object` (5 GB ceiling, no resume). Transfer
  timeouts scale with size (`transfer_timeout`) — don't reintroduce a flat cap. Wrap remote
  calls in `retry::with_retry`; it only retries `SyncError::Transient`.
- **Queue loops:** a failure on one *item* records and continues; a failure to write the
  *database* still aborts the batch. Keep that distinction when touching `queue_service`.
- **Scanning:** `scan_local_folder_with_cache(root, previous)` reuses fingerprints when
  `(size, mtime)` match — ~3.1x faster on unchanged trees. Passing `None` forces a full
  rescan; that is the escape hatch for the mtime-granularity blind spot.
- **Known gaps:** the per-pair path has no cycle-overlap guard (phase 2.2 owns it, blocked on
  the `SyncState` redesign); SQLite in_progress marking bounds the damage. Transfers are still
  sequential, downloads do not resume, and nothing verifies content after transfer.
- `tauri-command-tests` compiles (`bun run test:rust:commands`) but its tests have never run —
  the Tauri mock runtime won't load on this Windows machine. CI job is non-blocking until one
  green run.
- Sync-engine changes require simulator coverage (`src-tauri/src/storage/sim/`).
- Package manager is bun only (`bun.lock`); never generate `package-lock.json`.

## Auto

- Native GCS credentials use service account JSON; do not use AWS-style key ID / secret fields for GCS UX or storage.
- Normalize provider aliases `gcp` -> `gcs`; keep legacy compatibility when touching provider state.
- Provider metadata/capabilities live in `src-tauri/src/storage/provider.rs` and are hydrated in frontend state from `src/app/bootstrap.ts` / `src/app/types.ts`.
- Least-privilege GCS credential validation should succeed bucket-scoped when bucket listing is unavailable.
- GCS object versioning and storage class parity is complete. GCS uses `generation` (int64 string) as version IDs; cold classes are NEARLINE/COLDLINE/ARCHIVE; rewrite API handles storage class changes with multi-step token loop.
- `is_cold_storage_class()` in `remote_index.rs` covers both AWS Glacier and GCS cold tiers.
- Frontend `handleStorageClassChange` in `bootstrap.ts` is provider-aware (Glacier for AWS, Coldline for GCS).
- `object_store.rs` is the provider-aware seam; `gcs_adapter.rs` has all raw GCS JSON API methods; `s3_adapter.rs` has AWS SDK calls.
- README should describe AWS S3 + GCS as current first-class providers; roadmap should treat Azure as next provider work, not imply unfinished core GCS parity.
- Secure-store credential payloads must stay read-compatible with legacy entries lacking tagged `kind`; breaking that causes polling-time credential resolution loops.
- AWS secrets stay in secure store/keyring.
- Windows GCS secrets use DPAPI-encrypted files referenced by credential metadata; Credential Manager size limits are too small for reliable GCS secret storage.
- Compact/canonical GCS JSON still matters for normalization/minimal persisted payloads, but it is not the Windows storage strategy; avoid storing full pasted service-account JSON in metadata/keyring.
- GCS remote bin is shipped in-repo; README/brief should describe remaining work as manual cloud validation or UX follow-up, not missing provider parity.
