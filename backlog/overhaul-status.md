# Overhaul Status

**Snapshot: 2026-08-02.** Single source of truth for where the overhaul stands
and what is left. The phase docs are the detailed specs; this is the summary
and the plan.

Branch `overhaul/phase-1` — 41 commits, pushed to `origin`, **not merged**.
Functionality verified locally by the project owner. 412 Rust tests, 297
frontend tests; clippy clean at `-D warnings` in both feature configurations;
tsc, eslint, rustfmt and prettier clean.

---

## Where each phase stands

| Phase | State | Short version |
|-------|-------|---------------|
| 0 — Foundations | ✅ Complete | Gates, simulator, error taxonomy, baselines. CI is configured but has never run. |
| 1 — Sync correctness | ✅ Landed | Deletes, renames, conflicts, anchors, mass-delete breaker. Never validated against a real bucket. |
| 2 — Transfer reliability | ✅ Landed | Streaming, multipart, verification, parallelism, backoff, offline. 8 items open. |
| 3 — Backend architecture | ✅ Landed | God module dismantled, typed domain, enforced layering, coordinator. 5 items open. |
| 4 — Frontend architecture | 🔵 In progress | 4.1 complete; 3 of 7 views extracted. |
| 5 — UX overhaul | ⚪ Not started | The largest remaining gap to a flagship product. |
| 6 — Distribution | ⚪ Not started | Signing, auto-update, crash reporting, Azure. |

### The three caveats that apply to everything above

1. **Nothing has run in CI.** The branch is pushed but no workflow has executed.
   Deferred deliberately by the project owner.
2. **Nothing has touched a real bucket.** `docs/phase-1-cloud-validation.md` is a
   17-row manual matrix that has never been run. S3 multipart is the least-proven
   code on the branch — verified through a seam, which confirms the sequencing but
   never the wire format.
3. **The `tauri-command-tests` suite has never executed anywhere.** It compiles;
   the Tauri mock runtime will not load on the development machine.

---

## What the overhaul has actually changed

Measured, not estimated:

| | Before | Now |
|---|---|---|
| Deletes and renames | never propagated; dead-ended in review | propagated, with a mass-delete breaker |
| Interrupted download | truncated file at the real path, re-uploaded as a "local edit" | no-op; verified before an atomic rename |
| S3 objects > 5 GB | could not be synced at all | multipart, up to 5 TB |
| One bad file in a queue | aborted the remaining queue | isolated; queue drains |
| A pair that cannot sync | poll loop spun with **zero delay** | gated 30s → 15min, per pair |
| Offline | an error, indistinguishable from bad credentials | named; waits and recovers on its own |
| Transfers | strictly sequential | staged; content transfers concurrent to a global cap of 4 |
| Rescan after a 1-file edit | full tree walk | **26× faster** (553ms → 21ms on 10k files) |
| Rescan of an unchanged tree | re-hashed every file | **3.1× faster** |
| Concurrent DB access | instant `SQLITE_BUSY` | WAL + 15s busy timeout |
| `commands.rs` | 12,404 lines | ~2,180 production lines (+ ~4,000 tests) |
| `bootstrap.ts` | 4,324 at phase-4 start | 3,778 |
| Rust tests | 254 | 412 |
| Frontend tests | 221 | 297 |

---

## What remains, by phase

### Phase 1 — sync correctness

1. **Run the cloud validation matrix** on a scratch bucket per provider. The
   highest-risk unvalidated behaviour is deletes against a live bucket.
2. **S3 first-sync merge does not work.** It works on GCS and in the simulator;
   S3 listings carry no content fingerprint, so identical files on both sides
   still become review items on first sync.
3. **Anchor tombstones and directory-delete semantics** — the two real
   correctness gaps left.

### Phase 2 — transfer reliability

1. **Resume after an app restart**, either direction. Neither GCS resumable
   sessions nor S3 multipart persist their session/upload id.
2. **S3 startup janitor.** A crash leaves multipart parts that nothing aborts —
   billed as storage, invisible to `ListObjects`. *The only open item that
   silently costs the user money.*
3. **Idle-based timeouts.** Budgets scale with size but are wall-clock, so a
   stalled-but-not-dead transfer is only caught at the cap. The progress
   plumbing that makes this easy already exists.
4. **Rate limiting hooks** — not started.
5. **Extended durable queue states** (`cancelled`, attempt counts) — wants the
   phase-5 pause/resume UI to exist first.
6. **SQLite index storage** — snapshots are still whole-file JSON rewrites.
7. **Prefix-scoped remote listing** — pagination works; every cycle still lists
   the whole bucket.
8. **Clock-skew audit** — the design does not use wall-clock to decide
   direction, but that has not been audited line by line.
9. **Nightly soak test** — needs CI.

### Phase 3 — backend architecture

1. **`spawn_blocking` for file IO.** Newly material: several transfers now share
   one task via `buffer_unordered`, so a blocking write stalls its siblings. The
   parallelism added in 2.2 is partly notional until this lands.
2. **Full test relocation (3.5)** — ~4,000 lines of `commands.rs` tests still
   cover code that lives in the services. Best done opportunistically.
3. **Coverage measurement** — this machine is disk-bound; left to CI.
4. **Azure spike** — belongs with phase 6; its purpose is to prove the provider
   seam.
5. **`engine/`/`services/`/`ipc/` directory move** — descoped, deliberately. The
   value (an enforced Tauri-free core) is delivered by `architecture_test.rs`.

### Phase 4 — frontend architecture (in progress)

**Done:** the store and state migration, `latest()` request sequencing (adopted),
`ipc/events.ts` (adopted), debounce/dom helpers, the size and layering gates, and
three views — settings, activity, credentials.

**Remaining, in order:**

1. **Locations view — the hard one.** ~550 lines across 15 interdependent
   functions: a create-vs-edit form state machine with provider-dependent
   fields, plus the versioning toggle, remote-bin state, location dropdown, and
   the merge between listed locations and the stored profile. Larger than
   everything extracted so far combined; wants its own session, because a
   half-extracted screen is worse than an un-extracted one.
2. **Home / status view**, then modals and shell.
3. **Delete `dom.ts`'s global registry** once each view owns its subtree.
4. **Split `client.ts` by domain** — under its cap now, but still one file for
   every command.
5. **Split `bootstrap.test.ts`** (4,520 lines) alongside each view, not in one
   final pass — that is how test migrations get abandoned.

**Completion signal:** the transitional `state` mirror in `bootstrap.ts` should
disappear when the last view lands. If it is still there, something was
extracted badly.

### Phases 5 and 6 — not started

Phase 5 is the largest remaining gap to a flagship product, and the original
complaint that has not been addressed at all. Highest-value items, in the order a
user notices them: an honest glanceable "are my files safe?", onboarding,
per-file progress (the events already exist), offline as a first-class state (the
backend already reports it), conflict UX, and a language pass to remove engine
jargon.

Phase 6 is signing, auto-update, crash reporting, docs, and the Azure adapter.

---

## Recommended next session

**Tier 0 — cheap finishes, ~1–2 days.** Worth doing before switching context
again, because each closes something that costs real money, time, or trust:

1. S3 startup janitor (phase 2) — the money leak.
2. `spawn_blocking` for transfer IO (phase 3) — measure before and after; revert
   if the number does not move.
3. Idle-based timeouts (phase 2) — the hard part is already built.

**Then continue phase 4** with the locations view, which needs a session of its
own, and carry on to phase 5 — that is where the remaining product value is.

---

## Cross-cutting

**Dependency hygiene.** GitHub reports 5 vulnerabilities on the default branch
(1 critical, 3 high). Checked with `bun audit`: the critical and high findings
are all `node-tar`, reached via `goblin-web › firebase-tools` — a sibling
workspace, not this app. Storage Goblin's own vulnerable packages are entirely
dev dependencies. **Nothing vulnerable ships today.** The real gap is that
`cargo audit` is not installed, so the Rust surface — the one that does ship —
has never been checked.

**CI.** Explicitly deferred by the project owner. Recorded here only so the
record is straight.

---

*Supersedes `next-steps.md` (2026-08-02), which was written before phase 4
started and whose still-relevant content is folded in above.*
