# AGENTS.md

## Overhaul status

- **Current overhaul phase: 0 (Foundations & Guardrails)** — see `backlog/phase-0-foundations.md`.
- Roadmap/backlog lives in `backlog/` (one MD per phase); update the phase doc during session handoff.
- Package manager is bun only (`bun.lock`); never generate `package-lock.json`.
- Sync-engine changes require simulator coverage (`src-tauri/src/storage/sim/`).

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
