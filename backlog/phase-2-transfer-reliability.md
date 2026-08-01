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

- [x] **GCS resumable sessions** done: objects ≥ 16 MiB stream in 8 MiB chunks with
      Content-Range framing, so memory is bounded regardless of object size. Verified against
      a local HTTP server driving the real protocol.
      **S3 multipart** also done: objects ≥ 16 MiB upload as multipart, raising the 5 GB
      single-object ceiling to 5 TB. Part size grows with the object so the plan never
      exceeds the 10,000-part limit (a 5 TB file at a fixed 16 MiB part size would be
      rejected *after* every byte had been sent). Any failure aborts the upload, because
      orphaned parts are billed as storage while being invisible to `ListObjects`.
      **Not done: session URI persistence** — a resumable session survives a network blip
      within one run, but not an app restart.
      **Not done: a startup janitor.** An upload orphaned by a *crash* is never aborted;
      that needs a sweep over `ListMultipartUploads` at startup.
- [x] **Streaming + atomic writes** done for both providers (current and versioned
      objects): chunks stream to a `.goblin-tmp` sibling, hashed on the way, then fsync +
      atomic rename. This also closes phase 1.4's deferred atomic-download item — an
      interrupted download is now a no-op instead of leaving a truncated file at the real
      path that the next scan would upload as a "local edit".
      **Not done: resume.** There is no persisted offset, so an interrupted download restarts
      from byte 0. The writer already computes the content hash, but it is not yet compared
      against an expected value (post-transfer verification remains open).
- [ ] **Progress events** — `(pair, path, bytes_done, bytes_total, rate)` at ~4 Hz.
      **Not done.** `DownloadWriter` tracks bytes as it streams, which is the hard half,
      but nothing is emitted yet. Phase 5 is the consumer, so this is best done with the UI
      that displays it.
- [x] **Dynamic timeouts** — **partially done.** The flat 300s cap made large files
      *impossible* (20 GB cannot move in 300s at any speed), so budgets now scale with object
      size. Still wall-clock rather than idle-based; detecting a stalled-but-not-dead
      transfer promptly needs the progress plumbing above.
- [ ] **Memory budget** — hard cap on in-flight buffered bytes across all transfers.
      **Not done, and less pressing than it was.** Transfers are sequential and now stream,
      so peak memory is one chunk rather than one whole object. A global budget only becomes
      necessary alongside the parallel scheduler.

### 2.2 Queue scheduler

Replace the four sequential `execute_planned_*_queue*` loops with one scheduler:

- [ ] **Bounded parallelism** (default ~4 transfers, per-pair fairness).
      **Not done.** Still strictly sequential. This is the item that needs phase 3.3's
      deferred `SyncState` redesign first — the current shared-mutex state cannot support
      concurrent transfers safely.
- [x] **Per-item error isolation** done: the 22 item-level failure sites now record the
      failure and continue, while the 5 sites that indicate the *store itself* is broken
      (DB write failures) still abort — losing durable state is not something to soldier on
      through. The outcome is a summary string, verbatim for a single failure.
- [x] **Retry with exponential backoff + jitter** done at two levels. Per operation:
      `SyncError::Transient` only, honouring `Retry-After`, capped at 3 attempts. Per pair:
      a failed *cycle* now gates the pair for 30s → 15min (see 2.4 below).
      **Not done:** kind-aware pair backoff. An expired credential should jump straight to
      the ceiling rather than walking up to it, but `run_sync_cycle_for_pair` signals
      failure by returning `Ok(status)` with `phase: "error"` — that says *that* it failed,
      not *why*. Needs the cycle to return a typed `SyncError`.
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
- [x] **Fingerprint cache** (skip re-hashing when `(size, mtime)` is unchanged) done and
      measured: 10,000 unchanged files scan in 439ms instead of 1.376s, a
      **3.1x improvement** (7,267 -> 22,804 files/s). The mtime-granularity tradeoff is
      documented at the function, with `None` as the full-rescan escape hatch.
- [ ] Move local/remote indexes from monolithic JSON files into the existing SQLite DB
      (`sync_db.rs`) with per-path rows — enables incremental updates, removes
      whole-file rewrites, and unifies state into one durable store. Migration required.
- [ ] Remote listing: use paginated list with `startAfter`/prefix scoping where the plan only
      needs a subtree; keep full listing for the consistency pass.
- [ ] Benchmarks from phase 0.5 rerun; targets: 100k-file scan < 30 s warm (hash-cache hit),
      steady-state sync cycle for a 1-file change < 5 s end-to-end.

### 2.4 Failure-mode hardening

- [x] **The log-spam and hot-loop half is done.** This was worse than "logging failures
      every poll": a failing pair never records a `last_sync_at`, so it was due on *every*
      pass and the worker spun with zero delay — continuous provider calls, not one per
      interval. Failed cycles now gate the pair (30s → 15min, capped), gates are per pair so
      one bad bucket cannot slow a healthy one, dirty pairs are gated too (otherwise the
      loop still never sleeps), and a manual sync clears the gate outright.
      **Not done:** distinguishing "no network" from "provider error", and showing the
      user *why* a pair is waiting. The status carries `last_error`; presenting it as a
      first-class paused-with-reason state is phase 5's job.
- [ ] Disk-full and permission-denied handling on the local side (write probe before large
      downloads; per-item quarantine on EPERM instead of queue abort).
- [ ] Clock-skew tolerance audit (anchors store both-side identities, so wall-clock must never
      decide sync direction — assert this in review).
- [ ] Chaos tests in the simulator: injected 5xx storms, mid-part disconnects, throttling
      (429 + Retry-After), slow-loris responses; assertions: no data loss, no livelock,
      bounded retry volume.
- [ ] Long-run soak test (nightly CI job, simulator-based): 24 h of random churn on a
      10k-file pair; memory ceiling asserted.


## Status (2026-08-01)

Landed on `overhaul/phase-1` (the branch now carries phases 0-3 plus this;
still unpushed, still no CI run, phase 1 still not cloud-validated).
**Partially complete** — the correctness-critical half of 2.1 and the
error-handling half of 2.2 are done; parallelism and progress reporting are
not.

### What changed

| Area | Before | After |
|------|--------|-------|
| Download memory | whole object buffered in RAM | streamed in chunks |
| Interrupted download | truncated file at the real path, later uploaded as a "local edit" | no-op; destination untouched until an atomic rename |
| GCS upload memory | whole file read into RAM | ≥16 MiB streams via resumable session |
| One bad file in a queue | aborted the entire remaining queue | fails that item, queue drains, summary reported |
| Transient network error | failed the item | bounded retry with jittered backoff |
| Large-file timeout | flat 300s — 20 GB could never finish | budget scales with size |
| S3 objects over 5 GB | could not be synced at all | multipart, up to 5 TB |
| A pair that cannot sync | poll loop spun with zero delay | gated 30s → 15min, per pair |
| Rescan of unchanged tree | re-hashed every file | 3.1x faster via fingerprint reuse |

### Genuinely still open

1. **Bounded parallelism (2.2)** — blocked on the `SyncState` redesign
   deferred from phase 3.3. Transfers remain sequential.
2. **Upload resume** — GCS resumable sessions and S3 multipart both survive a
   blip *within* a run, but neither persists its session/upload id, so an app
   restart begins again at byte 0. A crash also leaves S3 parts that nothing
   aborts (no startup janitor over `ListMultipartUploads`).
3. **Download resume** — an interrupted download restarts from byte 0. The
   temp file exists but no offset is persisted.
4. **Post-transfer verification** — the writer computes the content hash but
   nothing compares it to an expected value yet. (On S3 there is no expected
   hash available from listings anyway — see phase 1's known gap.)
5. **Progress events (2.1)** — byte counts are tracked, nothing is emitted.
   Best done with phase 5, which consumes them.
6. **Conditional writes / re-plan on stale** — still deferred from phase 1.4.
7. **Most of 2.4** — the pair-level hot loop is fixed, but offline is still not
   *named* as such (the user sees "error", not "waiting for network"),
   disk-full and permission-denied get no special handling, and there are no
   chaos or soak tests.
8. **The rest of 2.3** — incremental rescan from watcher paths, the SQLite
   index migration, and remote listing pagination.

### Verification note

Everything above is covered by tests that run on this machine (the transfer
writer, the GCS resumable protocol against a local HTTP server, the retry
policy, the failure-reporting contract, the fingerprint cache with a sentinel
proving reuse, and the timeout budget). Two things are **not** verified here:
the queue-loop isolation itself, which lives behind the command integration
tests that compile but cannot run (Tauri mock runtime won't load on this
box), and anything against a real bucket.

## Acceptance criteria

1. ⚠️ Partially met, unverified against real clouds. Memory is now bounded (streaming both
   directions; GCS resumable upload), and the size-aware budget means a 20 GB transfer is no
   longer killed by the clock, and S3 multipart lifts the 5 GB cap to 5 TB.
   **Not met:** no resume after an app restart, in either direction.
2. ⚠️ Implemented (isolation + bounded retry + summary), but only unit-verified. The
   end-to-end 10,000-item scenario needs the command integration tests to run.
3. ❌ Not met — no events emitted yet.
4. ⚠️ Improved but not met. The fingerprint cache cuts the rescan cost ~3.1x, but the
   watcher still triggers a *full* tree rescan rather than an incremental one, so this scales
   with tree size rather than change count.
5. ⚠️ Half met. "Zero error-log spam" and "automatic recovery on reconnect" now hold: a
   failing pair backs off to at most one attempt per 15 minutes and recovers on its own
   when the network returns. **Not met:** the pair does not say *"paused — offline"*; it
   says "error" with the underlying message, and no code distinguishes offline from a
   provider fault.
6. ❌ Not started.

## Risks

- **GCS resumable-session and S3 multipart edge cases** (expiry, part limits, abort billing
  leaks). Mitigation: stale-upload janitor on startup + lifecycle rule doc for
  `AbortIncompleteMultipartUpload`.
- **SQLite index migration** is the second schema migration in two phases — coordinate with
  phase 1's anchor migration so users migrate once, not twice.
- **Parallelism vs. the borrow of shared state in `commands.rs`.** The current module's shared
  mutable state won't survive a concurrent scheduler; do 2.2 after (or as part of) phase 3's
  extraction of an engine crate/module — see sequencing note in phase 3.
