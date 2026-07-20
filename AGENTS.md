# AGENTS.md

## Overhaul status

- **Phase 0 complete. Phases 1 and 3 landed on `overhaul/phase-1`** (not merged, never pushed,
  no CI run, no real-cloud validation). Read the "Status" sections of
  `backlog/phase-1-sync-correctness.md` and `backlog/phase-3-backend-architecture.md` before
  building on this.
- **Backend layout:** `commands.rs` is now the Tauri command surface + shared response types +
  profile/location CRUD (2,666 prod lines, down from 12,404). Concerns live in sibling modules:
  `sync_service`, `queue_service`, `transfer_service`, `bin_service`, `conflict_service`,
  `compare_service`, `credential_service`, `lifecycle_service`, `polling_service`, `platform`.
  These are flat siblings, NOT the `engine/`+`services/`+`ipc/` layout the phase-3 doc targets —
  services still take `AppHandle`, so there is no Tauri-free core yet.
- **The legacy single-profile execution path is gone.** Only per-pair paths remain (the
  `_for_pair` suffixes are still there). The flat-profile *read* path stays because it performs
  the migration to sync locations.
- Sync engine propagates deletes and renames, keeps both sides of a conflict, reconciles anchors.
  Operations live in `sync_planner::Operation`; `decide_file_sync` has no catch-all arm — keep it
  that way. Local deletes go to the OS trash (`trash` crate), never `remove_file`.
- Mass-delete breaker: >25 deletes, or >50% of a 10+ anchored tree, become review items
  (`MAX_AUTO_DELETE_COUNT` / `MAX_AUTO_DELETE_RATIO`).
- Content fingerprints: uploads attach `LOCAL_FINGERPRINT_METADATA_KEY`; **GCS listings return
  it, S3 listings do not** — first-sync merge works on GCS only. Never assume
  `RemoteObjectEntry.fingerprint` is populated on S3.
- **No cycle-overlap guard exists on the per-pair path.** Queue items move to `in_progress` in
  SQLite before execution, which bounds the damage, but phase 2.2 owns the real fix.
- `tauri-command-tests` compiles (`bun run test:rust:commands`) but its tests have never
  executed — the Tauri mock runtime fails to load on this Windows machine. CI job is
  non-blocking until one green run.
- Sync-engine changes require simulator coverage (`src-tauri/src/storage/sim/`).
- Errors: provider adapters + object_store return `storage::error::SyncError` (classified);
  `commands.rs` still uses `Result<_, String>` via `From` escape hatches.
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
