# Next Steps — plan as of 2026-08-02

Phases 0–3 have landed on `overhaul/phase-1` (now pushed) and the app has been
verified working locally. This document says what to do next and why, grounded
in the state of the code today rather than the assumptions the phase docs were
written under.

The phase docs remain the detailed specs. This is the sequencing argument.

## Where the remaining value actually is

The original complaint was: *"it sort of works, but has several bugs, not too
reliable, and generally a bit sloppy including a UX."*

Phases 1–3 addressed the first three. **The UX is the part that has not been
touched at all** — and it is now the largest gap between this product and a
flagship one. A user still cannot answer "is my stuff safe?" at a glance, still
meets engine jargon ("Unconfigured", "review-required"), and still has no
onboarding path from install to first sync.

So the destination is phase 5. Phase 4 is the enabler: `bootstrap.ts` is 4,305
lines holding 99 functions in a single closure, and the redesigned flows cannot
be built on top of it.

That sets the shape of the plan: **a short backend finish, then phase 4, then
phase 5.** Everything else is scheduled around not blocking that line.

---

## Tier 0 — Cheap finishes worth doing before switching context (~1–2 days)

These are small, self-contained, and each closes something that costs the user
real money, real time, or real trust. Doing them now avoids paging the transfer
engine back into memory later.

### 0.1 S3 multipart startup janitor

**Why first:** this is the only open item that silently costs the user money.
An upload orphaned by a crash (not by an error — those already abort) leaves
parts that are billed as storage while being invisible to `ListObjects`. A user
whose app crashes during large uploads accrues charges they cannot see or
explain.

- Sweep `ListMultipartUploads` on startup, abort anything older than ~24h
  belonging to our key prefix.
- Document the `AbortIncompleteMultipartUpload` lifecycle rule as the belt to
  this braces, since a client that never starts again cannot clean up after
  itself.
- Test through the existing `MultipartSink` seam.

### 0.2 `spawn_blocking` for transfer file IO

**Why now:** this got worse in the last session rather than being newly found.
Blocking IO on the async executor barely mattered while transfers were
sequential; now several share one task via `buffer_unordered`, so a blocking
`write_all` or `fsync` stalls its siblings. The parallelism that was just added
is partly notional until this lands.

- Move `DownloadWriter`'s writes and `fsync` off the executor.
- **Measure before and after** with concurrent transfers; the point is a number,
  not a code change. If it does not move, say so and revert.
- Scans and snapshot IO can follow, but the transfer path is where concurrency
  now exists.

### 0.3 Idle-based transfer timeouts

**Why now:** the hard part is already built. Budgets scale with size but are
still wall-clock, so a stalled-but-not-dead transfer is only noticed at the cap
— which for a large file is hours. The progress plumbing added in phase 2.1
already tracks bytes; this is wiring, not design.

- Reset the deadline whenever bytes arrive; fail after ~60s of no movement.
- Keep the size-scaled budget as the outer bound.

---

## Tier 1 — Phase 4: frontend architecture (~2–3 weeks)

The full spec is `backlog/phase-4-frontend-architecture.md`. Two adjustments to
it, given what has changed:

1. **`types.ts` is partly generated already.** Phase 3.2 generates the domain
   unions into `src/app/generated/domain.ts` with a drift test. Phase 4.1's
   "swap hand-written `types.ts` for generated types" should be re-scoped to
   *extend* that generation rather than replace a file that now carries
   hand-written normalizers worth keeping.
2. **The typed event module (4.1) has a second consumer now.** Phase 2.1 emits
   `storage://transfer-progress` with a stable payload; the subscription module
   should cover it from the start, since phase 5's progress UI depends on it.

Suggested order within phase 4, chosen so each step is independently shippable:

1. Store + typed IPC + event subscription (4.1) — nothing else can move first.
2. Settings screens (4.2) — simplest, proves the pattern before the hard ones.
3. Credentials, then locations (the form state machine is the genuinely hard
   one: create vs edit, provider-dependent fields).
4. Activity, home, modals, shell.
5. Add the `src/` module size gate **early**, not at the end — it is what stops
   the closure reforming somewhere else.

The `bootstrap.test.ts` split (4.3) should happen alongside each view rather
than as a final pass. That file is 4,520 lines; migrating it in one go at the
end is how test migrations get abandoned.

---

## Tier 2 — Phase 5: UX overhaul

This is the payoff, and it is where "flagship" is won or lost. Nothing here
should start before phase 4's store exists, or it will be rebuilt.

Highest-value items from `backlog/phase-5-ux-overhaul.md`, in the order a user
would notice them:

1. **"Are my files safe?"** — one honest, glanceable answer. The backend now has
   everything needed to answer it truthfully: per-pair phase, typed failure
   kind, pending counts, last successful sync.
2. **Onboarding** — install to first sync without reading docs.
3. **Per-file progress** — the events already exist and are already throttled.
4. **Offline as a first-class state.** The backend already reports "Waiting for
   a network connection" as a paused pair rather than an error; the UI currently
   has no way to show that distinction.
5. **Conflict UX** — phase 1 keeps both sides; the user still has to understand
   what happened.
6. **Language pass** — remove engine jargon from every user-facing string.

---

## Tier 3 — Deferred deliberately, with reasons

These are real, but none of them blocks the line above.

| Item | Why it waits |
|------|--------------|
| **Resume across app restarts** (persisted offset / upload id) | The largest remaining transfer gap, but it needs durable per-transfer state and a resume protocol per provider. It is a phase of its own, not a follow-up, and interrupted transfers currently restart safely rather than corrupting. |
| **SQLite index storage** (replacing whole-file JSON snapshots) | The incremental rescan removed most of the pressure by making the *scan* proportional to the change. The remaining cost is rewriting the snapshot file per cycle — real, but no longer the bottleneck it was when this was planned. |
| **Rate limiting hooks** | A genuine flagship feature, cheap now that transfers are chunked and bounded. Better placed with phase 5, where the setting has a home in the UI. |
| **Prefix-scoped remote listing** | Listing paginates correctly; every cycle just lists the whole bucket. Matters at 100k+ objects, which no one has run yet. |
| **Extended durable queue states** (`cancelled`, attempt counts) | Wants the phase-5 pause/resume UI to exist first, or it is state nobody reads. |
| **Clock-skew audit** | The design does not use wall-clock to decide direction (anchors carry both-side identities). A line-by-line audit is worth doing but is confirmation, not repair. |
| **Full test relocation (3.5)** | `commands.rs` is ~6,250 lines, of which ~4,000 are tests for code that now lives in the services. Tedious, low correctness value, and best done opportunistically when touching each service. |
| **Azure adapter spike** | Belongs with phase 6, and its actual purpose is to prove the provider abstraction — worth doing once, late, as a test of the seam. |

---

## Cross-cutting, do whenever convenient

### Dependency hygiene — less alarming than the alert suggests

GitHub reports 5 vulnerabilities on the default branch (1 critical, 3 high, 1
moderate). Checked with `bun audit`: **the critical and high findings are all
`node-tar`, reached via `goblin-web › firebase-tools` — a sibling workspace, not
this app.** Storage Goblin's own vulnerable packages are entirely dev
dependencies: `eslint`, `typescript-eslint`, `@vitest/coverage-v8`, `vite`,
`jsdom`, `vitest`.

So nothing vulnerable ships in Storage Goblin today. Still worth doing, in
descending order of actual risk:

- Bump this workspace's dev dependencies (low risk, low effort).
- The `firebase-tools` chain is `goblin-web`'s to fix; raise it there rather
  than treating it as a Storage Goblin blocker.
- Add `cargo audit` to the toolchain — it is not currently installed, so the
  **Rust** dependency surface has never been checked at all. That is the real
  gap here, and it is the surface that actually ships.

### Cloud validation

`docs/phase-1-cloud-validation.md` is a 17-row manual S3/GCS matrix that has
never been run. Local functional testing (done) is not the same thing: it does
not cover provider-specific behaviour, S3 multipart against real AWS, versioning,
storage classes, or the remote bin. Worth running once against a scratch bucket
per provider before phase 5 work starts changing what the UI reports.

The S3 multipart path is the least-proven code on the branch — verified through
a seam, which confirms the sequencing but never the wire format.

### CI

Explicitly deferred by the project owner. Noted here only so the record is
straight: the branch is pushed but no CI run has ever validated it, and the
`tauri-command-tests` suite has never executed anywhere.
