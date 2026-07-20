# AGENTS.md

## Overhaul status

- **Phase 0 complete. Phase 1 landed on `overhaul/phase-1` (not merged, not cloud-verified).**
  See `backlog/phase-1-sync-correctness.md` — read its "Not proven yet" section before merging.
- Sync engine now propagates deletes and renames, keeps both sides of a conflict
  (`preserve-both` duplicates with a dated suffix), and reconciles anchors. Operations live in
  `sync_planner::Operation`; the decision table in `decide_file_sync` has no catch-all arm —
  keep it that way.
- Local deletes go to the OS trash (`trash` crate), never `remove_file`. Remote deletes honor
  versioning/remote-bin exactly like the manual `delete_file` command.
- Mass-delete breaker: >25 deletes, or >50% of a 10+ anchored tree, become review items.
  Constants are `MAX_AUTO_DELETE_COUNT` / `MAX_AUTO_DELETE_RATIO`.
- Content fingerprints: uploads attach `LOCAL_FINGERPRINT_METADATA_KEY`; **GCS listings return
  it, S3 listings do not** — so first-sync merge works on GCS only. Do not assume
  `RemoteObjectEntry.fingerprint` is populated on S3.
- The `tauri-command-tests` feature had never compiled and CI never ran it. It compiles now
  (`bun run test:rust:commands`) but its tests have never executed — the Tauri mock runtime
  fails to load on this Windows machine. The CI job is non-blocking until one green run.
- Sync-engine changes require simulator coverage (`src-tauri/src/storage/sim/`).
- Errors: provider adapters + object_store return `storage::error::SyncError` (classified);
  `commands.rs` still uses `Result<_, String>` via `From` escape hatches until phase 3.
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
