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
      **Post-transfer verification** now also done: the received size (and the content hash
      where the provider records one) is checked *before* the rename, so a truncated body
      never reaches the destination path where the next scan would read it as a local edit.
      **Not done: resume.** There is no persisted offset, so an interrupted download restarts
      from byte 0.
- [x] **Progress events** done for the backend half: downloads emit
      `(locationId, path, bytesDone, bytesTotal, bytesPerSecond, fraction)` on
      `storage://transfer-progress`, throttled to ~4 Hz by `progress.rs`. Separate channel
      from the status event because it fires orders of magnitude more often. The frontend
      consumer is phase 5's.
- [x] **Dynamic timeouts** — **partially done.** The flat 300s cap made large files
      *impossible* (20 GB cannot move in 300s at any speed), so budgets now scale with object
      size. Still wall-clock rather than idle-based; detecting a stalled-but-not-dead
      transfer promptly needs the progress plumbing above.
- [x] **Memory budget** effectively delivered by construction rather than by a byte cap:
      transfers stream (one chunk in flight each) and the scheduler caps concurrency at 4, so
      peak buffered bytes are bounded by `4 x chunk size` rather than by object size. An
      explicit byte-denominated cap would add a knob without changing the bound.

### 2.2 Queue scheduler

Replace the four sequential `execute_planned_*_queue*` loops with one scheduler:

- [x] **Bounded parallelism** done, once phase 3.3's coordinator landed. Content transfers
      run concurrently up to a **global** semaphore (default 4), so two locations draining at
      once cannot open eight connections between them. Verified by a multi-threaded test
      asserting observed peak concurrency, not just configuration.
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
- [x] **Ordering** done, and it is what makes parallelism safe. `queue_schedule.rs` cuts the queue into
      ordered stages: creates (parents first) -> transfers (concurrent, smallest first) ->
      structural (sequential) -> deletes (children first, so nothing is destroyed before its
      replacement has landed). Only the transfer stage is concurrent; anything the module
      cannot prove independent — including any operation added later that it has not been
      taught about — is sequential by default.
- [x] **Pause/resume/cancel** — backend half done. Cancellation is carried by the pair lease, so pause and
      shutdown interrupt the cycle that is running rather than waiting it out, and the cycle
      re-checks between stages. **Not done:** the extended durable queue states
      (`cancelled`, attempt counts) and the phase-5 UI controls that would drive them.

### 2.3 Scan & index scalability

- [x] **Incremental local rescan** done and measured: editing one file in a 10,000-file tree costs **21ms instead of
      553ms (26x)**. Watcher paths are carried through to the cycle, which re-walks only the
      affected subtrees. It declines rather than guesses — a path outside the root, a snapshot
      of another folder, more than 64 distinct subtrees, or a truncated change list all fall
      back to a full scan.
- [x] **Fingerprint cache** (skip re-hashing when `(size, mtime)` is unchanged) done and
      measured: 10,000 unchanged files scan in 439ms instead of 1.376s, a
      **3.1x improvement** (7,267 -> 22,804 files/s). The mtime-granularity tradeoff is
      documented at the function, with `None` as the full-rescan escape hatch.
- [ ] Move local/remote indexes from monolithic JSON files into the existing SQLite DB
      (`sync_db.rs`) with per-path rows — enables incremental updates, removes
      whole-file rewrites, and unifies state into one durable store. Migration required.
- [x] **Already the case** (verified, not newly written): S3 uses `continuation_token` and
      GCS uses `pageToken`, so neither truncates at 1,000 objects. Prefix scoping to a subtree
      is not implemented — every cycle lists the whole bucket.
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
- [x] Done. Downloads preflight free space (64 MiB headroom, advisory — unknown free space
      never blocks). Disk-full, permission-denied and read-only now say what happened and that
      nothing was overwritten, instead of "os error 112"; the Windows raw code is matched
      because it does not map to `ErrorKind::StorageFull`. Per-item isolation means these
      quarantine the item rather than aborting the queue.
- [ ] Clock-skew tolerance audit (anchors store both-side identities, so wall-clock must never
      decide sync direction — assert this in review).
- [x] Done — `sim/chaos.rs`, 8 scenarios: 5xx storms, throttling, an offline listing, a
      permanently failing object, and randomized churn under injected failures. Asserts no
      data loss, no livelock, and isolation. Fixed-seed randomization with a reproducibility
      test, because an irreproducible chaos test gets muted rather than investigated.
- [ ] Long-run soak test (nightly CI job, simulator-based): 24 h of random churn on a
      10k-file pair; memory ceiling asserted.


## Status (2026-08-02, superseding 2026-08-01)

All of phase 2 landed on `overhaul/phase-1` except the items under "Genuinely
still open" below. **Still unpushed, still no CI run, still not validated
against a real bucket** — that caveat has not moved and is the single largest
risk on this branch.

412 Rust tests, 219 frontend tests; clippy clean at `-D warnings` in both
feature configurations; rustfmt and prettier clean.

### What changed across the whole phase

| Area | Before | After |
|------|--------|-------|
| Download memory | whole object buffered in RAM | streamed in chunks |
| Interrupted download | truncated file at the real path, later uploaded as a "local edit" | no-op; destination untouched until a verified atomic rename |
| Truncated download | landed silently and was uploaded back | rejected before the rename |
| GCS upload memory | whole file read into RAM | >=16 MiB streams via resumable session |
| S3 objects over 5 GB | could not be synced at all | multipart, up to 5 TB |
| One bad file in a queue | aborted the entire remaining queue | fails that item, queue drains, summary reported |
| Transient network error | failed the item | bounded retry with jittered backoff |
| Large-file timeout | flat 300s — 20 GB could never finish | budget scales with size |
| A pair that cannot sync | poll loop spun with **zero delay** | gated 30s to 15min, per pair |
| Offline | an error, indistinguishable from bad credentials | named; the pair waits and recovers on its own |
| Disk full / permission denied | "os error 112" | named cause, and says nothing was overwritten |
| Transfers | strictly sequential | staged; content transfers concurrent to a global cap of 4 |
| Queue order | row order | creates, transfers, structural, deletes — dependency-safe |
| Rescan of unchanged tree | re-hashed every file | 3.1x faster via fingerprint reuse |
| Rescan after a 1-file edit | full tree walk | **26x faster** (553ms to 21ms on 10k files) |
| 20 GB file progress | one opaque "in progress" for hours | byte-level events at ~4 Hz |
| Concurrent DB access | instant SQLITE_BUSY | WAL + 15s busy timeout |
| Provider misbehaviour | untested | 8 chaos scenarios |

### Genuinely still open

1. **Resume after an app restart**, in either direction. GCS resumable sessions
   and S3 multipart both survive a blip *within* a run, but neither persists its
   session/upload id. A crash also leaves S3 parts that nothing aborts — that
   needs a startup janitor over `ListMultipartUploads`.
2. **Idle-based timeouts.** Budgets scale with size but are still wall-clock, so
   a stalled-but-not-dead transfer is only noticed at the cap. The progress
   plumbing needed to fix this now exists; wiring it does not.
3. **Rate limiting hooks** (user-configurable bandwidth caps) — not started.
4. **Extended durable queue states** (`cancelled`, attempt counts) and the
   phase-5 UI that would drive pause/resume.
5. **Local/remote indexes are still monolithic JSON**, not SQLite rows. The
   incremental rescan removed most of the pressure for this by making the *scan*
   proportional to the change, but the snapshot file is still rewritten whole on
   every cycle.
6. **Prefix-scoped remote listing.** Listing paginates correctly on both
   providers, but every cycle still lists the whole bucket.
7. **Clock-skew audit** — not performed. The design does not use wall-clock to
   decide direction (anchors carry both-side identities), but that has not been
   audited line by line as the workstream asks.
8. **Nightly soak test** — not started, and not startable here: a 24-hour job
   needs CI, which this branch has never run.
9. **`spawn_blocking` for file IO** (phase 3.3 residual) — newly material, since
   several transfers now share one task and a blocking write stalls its siblings.

### Verification honesty

Everything claimed above is covered by tests that run on this machine, and both
performance numbers (3.1x, 26x) were measured here rather than estimated. Three
things remain unverified:

- **Nothing has run in CI.** The branch has never been pushed.
- **Nothing has touched a real bucket.** S3 multipart in particular has never
  spoken to AWS; it is tested through a seam, which verifies the sequencing but
  not the wire format.
- **The command integration tests still cannot run** (the Tauri mock runtime
  will not load here), so the queue drains are verified through `drain_stage`
  and the item functions rather than end to end.

## Acceptance criteria

1. ⚠️ Partially met, unverified against real clouds. Memory is bounded, the 5 GB cap is gone,
   and the size-aware budget means a 20 GB transfer is no longer killed by the clock.
   **Not met:** no resume after an app restart, in either direction.
2. ⚠️ Implemented (isolation + bounded retry + summary + staged concurrency) and covered by
   chaos scenarios in the simulator, but the literal 10,000-item end-to-end run needs the
   command integration tests, which cannot execute on this machine.
3. ⚠️ Backend met: events are emitted and throttled. The visible progress bar is phase 5.
4. ✅ Met for the scan half, by a wide margin: a 1-file edit in a 10,000-file tree costs 21ms.
   Full cycle time additionally depends on the remote listing, which still lists the whole
   bucket — so "under 5s end to end" is likely but unmeasured at 100k scale.
5. ✅ Met at the backend. A pulled cable produces a paused pair reporting "Waiting for a
   network connection", logged at info rather than error, backing off to at most one probe per
   15 minutes and recovering on its own. How phase 5 presents that is phase 5's.
6. ❌ Not started, and not startable here — a nightly job needs CI, which this branch has
   never run.

## Risks

- **GCS resumable-session and S3 multipart edge cases** (expiry, part limits, abort billing
  leaks). Mitigation: stale-upload janitor on startup + lifecycle rule doc for
  `AbortIncompleteMultipartUpload`.
- **SQLite index migration** is the second schema migration in two phases — coordinate with
  phase 1's anchor migration so users migrate once, not twice.
- **Parallelism vs. the borrow of shared state in `commands.rs`.** The current module's shared
  mutable state won't survive a concurrent scheduler; do 2.2 after (or as part of) phase 3's
  extraction of an engine crate/module — see sequencing note in phase 3.
