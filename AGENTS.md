# AGENTS.md

## Overhaul status

- **Read `backlog/overhaul-status.md` first.** It is the single source of truth for what is
  done and what is left. Phases 0–3 have landed on `overhaul/phase-1` (pushed, not merged);
  phase 4 is in progress. Nothing has run in CI and nothing has been validated against a real
  bucket — the checkboxes in the phase docs overstate completeness on their own.
- **Backend layout:** the 12,404-line `commands.rs` is now ~2,180 lines (Tauri command surface +
  shared DTOs). Concerns live in sibling modules: `sync_service`, `queue_service`,
  `transfer_service`, `bin_service`, `location_service`, `file_query_service`, `conflict_service`,
  `compare_service`, `credential_service`, `lifecycle_service`, `polling_service`, `platform`,
  `watcher_service`, and the Tauri-free `model`, `coordinator`, `queue_schedule` and `progress`. These are flat siblings, NOT the `engine/services/ipc` directory
  the doc once targeted — that layout is descoped in favour of an enforced invariant (below).
- **Enforced invariants (tests, so they run in CI):** `architecture_test.rs` — the Tauri-free
  core (model, sync_planner, error, inventory_compare, sanitizer, remote_bin, pair_backoff,
  coordinator, queue_schedule, progress, retry, s3_upload) must not import the app framework; `module_size_test.rs` — no module over 1,300 prod lines (commands 2,200,
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
- **Concurrency (phase 3.3):** `coordinator.rs` owns the runtime rules and is Tauri-free so it
  can actually be tested here. A `PairLease` **is** the right to run a cycle for one pair —
  `run_sync_cycle_for_pair` takes one, so the invariant is enforced by the type system. Never
  add a path that runs a cycle without leasing. Scheduled work uses `try_lease_pair` and skips
  a busy pair; manual work uses `lease_pair` and waits. Cancellation rides the lease.
- **Transfers (phase 2):** downloads stream through `transfer::DownloadWriter` to a
  `.goblin-tmp` sibling, are **verified before the rename**, then fsync + atomic rename —
  never write the destination path directly, and never index a temp file (`local_index` skips
  them; doing otherwise uploads partial content as a "local edit"). GCS objects ≥16 MiB use
  resumable sessions (`gcs_upload.rs`); S3 objects ≥16 MiB use multipart (`s3_upload.rs`) and
  **must** abort on failure — orphaned parts are billed but invisible to `ListObjects`.
  Transfer timeouts scale with size (`transfer_timeout`) — don't reintroduce a flat cap. Wrap
  remote calls in `retry::with_retry`; it only retries `SyncError::Transient` and `Offline`.
- **Queue scheduling (phase 2.2):** `queue_schedule.rs` stages the queue and **only** the
  content-transfer stage runs concurrently. A new operation defaults to sequential; if you add
  one, decide deliberately rather than letting it inherit concurrency. Concurrency is bounded
  by the coordinator's global semaphore, not per queue.
- **Errors:** `SyncErrorKind::Offline` means we never reached the provider (SDK dispatch
  failure, reqwest connect/timeout). It is reported to the user as a wait, not an error. Never
  classify an answered HTTP status as offline — a test enforces this.
- **Poll scheduling:** a failed cycle returns `Ok(status)` with `phase: "error"` and no
  `last_sync_at`, which makes the pair due immediately — so `sync_state::record_pair_failure`
  gates it (`pair_backoff.rs`, 30s → 15min). Any new wake path must honour those gates or the
  loop spins; `next_dirty_pair_deadline` is the non-obvious one. Manual sync clears the gate.
- **Queue loops:** a failure on one *item* records and continues; a failure to write the
  *database* still aborts the batch. Keep that distinction when touching `queue_service`.
- **Scanning:** `scan_local_folder_with_cache(root, previous)` reuses fingerprints when
  `(size, mtime)` match (~3.1x on unchanged trees). `rescan_changed_paths` goes further,
  re-walking only the subtrees the watcher reported (~26x for a 1-file edit in a 10k tree). It
  returns `None` whenever it cannot be certain the incremental view is complete — keep it that
  way; falling back to a full scan is always safe, guessing strands a file forever.
- **SQLite:** connections use WAL + a 15s busy timeout (`sync_db::configure_connection`)
  because several run concurrently. Don't open a connection that bypasses it.
- **Known gaps:** neither direction resumes after an app restart (no persisted offset/upload
  id), and a crash leaves S3 multipart parts that nothing aborts. Transfer timeouts are
  wall-clock rather than idle-based. Blocking file IO still runs on the async executor, which
  now matters because several transfers share one task. Local/remote index snapshots are still
  whole-file JSON rewrites.
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
