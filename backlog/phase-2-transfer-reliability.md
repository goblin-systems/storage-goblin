# Phase 2 — Transfer Reliability & Performance

**Theme:** Big files, big trees, bad networks. Make transfers streaming, resumable, parallel,
and self-healing.
**Depends on:** Phase 0 (harness, benchmarks), Phase 1 (integrity semantics: hashes,
preconditions, atomic writes).
**Estimated size:** 3–4 weeks.

## Current state (verified findings)

1. **Whole-file memory buffering.**
   - GCS upload: `std::fs::read(path)` loads the entire file (`gcs_adapter.rs:504`), then a
     single non-resumable JSON-API upload. A 4 GB video = 4 GB RSS spike, and any network blip
     restarts from byte 0.
   - S3 download: `response.body.collect()` buffers the whole object
     (`s3_adapter.rs:317-322`) before writing. Same for versioned downloads
     (`s3_adapter.rs:997-1001`).
   - S3 upload streams from path (`ByteStream::from_path`, `s3_adapter.rs:266`) but is plain
     `put_object`: hard 5 GB cap, no resume, no progress granularity.
2. **Strictly sequential execution, abort-on-first-error.** `execute_planned_upload_queue`
   (`commands.rs:1758+`) and its three near-clones iterate one item at a time and `break` on
   any failure (e.g. `commands.rs:1852-1854`) — one locked file or one 403 stops the entire
   remaining queue. No retry, no backoff, no error isolation, no reordering.
3. **Fixed global timeouts** (`PLANNED_UPLOAD_TIMEOUT`) regardless of file size — a large file
   on a slow uplink *cannot* succeed; a hung small transfer wastes the full window.
4. **No transfer progress.** Progress exists only at queue-item granularity; a 20 GB file is
   a single opaque "in progress" for hours. Nothing feeds a UI progress bar (phase 5 needs
   byte-level events).
5. **Scan performance unknowns.** Local scan SHA-256s files (cost on 100k files?), remote
   inventory relists everything per cycle; virtual file tree exists in the UI but backend
   snapshots are monolithic JSON files per pair (`local_index.rs:107-109`), rewritten whole.
6. **Watcher → full rescan.** `notify` events only mark a pair dirty
   (`watchers.rs:49-51`); every dirty tick rescans the entire tree instead of the touched
   paths, so "near real-time" scales inversely with tree size.

## Workstreams

### 2.1 Transfer engine (new module, provider-agnostic)

Build a `transfer` module used by both adapters through the `ObjectStorage` trait:

- [ ] **Chunked/multipart upload**: S3 multipart (init/part/complete, ≥ 8 MB parts, parallel
      parts, abort-on-drop cleanup of stale multipart uploads); GCS resumable sessions
      (session URI persisted → survives restart). Threshold-based: simple PUT below ~16 MB.
- [ ] **Streaming, resumable download**: ranged GETs to the `.goblin-tmp` file with persisted
      offset + expected etag/generation; resume after crash/offline; final hash verification
      (phase 1's ADR-4 hash) before atomic rename.
- [ ] **Progress events**: `(pair, path, bytes_done, bytes_total, rate)` throttled to ~4 Hz,
      emitted over the existing Tauri event channel; aggregate per-pair totals for the UI.
- [ ] **Dynamic timeouts**: idle-based (no bytes for N s) instead of absolute per-file caps.
- [ ] Memory budget: hard cap on in-flight buffered bytes across all transfers (e.g. 256 MB),
      enforced by the scheduler below.

### 2.2 Queue scheduler

Replace the four sequential `execute_planned_*_queue*` loops with one scheduler:

- [ ] Bounded parallelism (default ~4 transfers; small-file batches get higher concurrency,
      huge files get part-level parallelism instead), per-pair fairness.
- [ ] **Per-item error isolation**: an item failure marks that item failed and continues; the
      queue outcome is a summary (n ok / n failed / n skipped), not a single error string.
- [ ] **Retry with exponential backoff + jitter** for `SyncError::Transient` (respect
      Retry-After); permanent errors (auth, 4xx) fail fast and pause the pair with a clear
      status instead of hot-looping on the poll interval.
- [ ] Rate limiting hooks: optional user-configurable up/down bandwidth caps (flagship
      feature; cheap once transfers are chunked).
- [ ] Ordering: parents-before-children for creates, children-before-parents for deletes,
      small files first within a class (perceived speed), moves before copies.
- [ ] Pause/resume/cancel at scheduler level (feeds phase 5's UI controls); durable queue
      states extended (`pending → in_progress → done | failed(retryable, attempts) | cancelled`).

### 2.3 Scan & index scalability

- [ ] Incremental local rescan: watcher events carry paths (`notify` already provides them —
      currently discarded, `watchers.rs:49`); rescan only affected subtrees; debounce per
      subtree. Full rescan remains as a periodic consistency pass and manual action.
- [ ] Fingerprint cache: skip re-hashing when `(size, mtime)` unchanged from the previous
      snapshot (classic rsync heuristic); measure and record hashing throughput.
- [ ] Move local/remote indexes from monolithic JSON files into the existing SQLite DB
      (`sync_db.rs`) with per-path rows — enables incremental updates, removes
      whole-file rewrites, and unifies state into one durable store. Migration required.
- [ ] Remote listing: use paginated list with `startAfter`/prefix scoping where the plan only
      needs a subtree; keep full listing for the consistency pass.
- [ ] Benchmarks from phase 0.5 rerun; targets: 100k-file scan < 30 s warm (hash-cache hit),
      steady-state sync cycle for a 1-file change < 5 s end-to-end.

### 2.4 Failure-mode hardening

- [ ] Offline detection: distinguish "no network" from "provider error"; pause pairs with a
      visible reason rather than logging failures every poll; auto-resume with backoff probes.
- [ ] Disk-full and permission-denied handling on the local side (write probe before large
      downloads; per-item quarantine on EPERM instead of queue abort).
- [ ] Clock-skew tolerance audit (anchors store both-side identities, so wall-clock must never
      decide sync direction — assert this in review).
- [ ] Chaos tests in the simulator: injected 5xx storms, mid-part disconnects, throttling
      (429 + Retry-After), slow-loris responses; assertions: no data loss, no livelock,
      bounded retry volume.
- [ ] Long-run soak test (nightly CI job, simulator-based): 24 h of random churn on a
      10k-file pair; memory ceiling asserted.

## Acceptance criteria

1. 20 GB single-file upload and download succeed on S3 and GCS with < 300 MB peak app RSS,
   survive a mid-transfer network drop and an app restart, and resume rather than restart.
2. A queue of 10,000 items with 1 % injected failures completes the other 99 %, retries
   transients successfully, and reports a per-item failure summary.
3. Byte-level progress events drive a visible per-file progress bar (wired fully in phase 5;
   an interim debug surface is fine here).
4. Editing one file in a 100k-file tree results in a sync cycle < 5 s (watcher → incremental
   scan → plan → transfer), measured by the phase-0 bench scripts.
5. Pulling the network cable results in a "paused — offline" pair state, zero error-log spam,
   and automatic recovery on reconnect.
6. Nightly soak job green for two consecutive weeks before phase exit.

## Risks

- **GCS resumable-session and S3 multipart edge cases** (expiry, part limits, abort billing
  leaks). Mitigation: stale-upload janitor on startup + lifecycle rule doc for
  `AbortIncompleteMultipartUpload`.
- **SQLite index migration** is the second schema migration in two phases — coordinate with
  phase 1's anchor migration so users migrate once, not twice.
- **Parallelism vs. the borrow of shared state in `commands.rs`.** The current module's shared
  mutable state won't survive a concurrent scheduler; do 2.2 after (or as part of) phase 3's
  extraction of an engine crate/module — see sequencing note in phase 3.
