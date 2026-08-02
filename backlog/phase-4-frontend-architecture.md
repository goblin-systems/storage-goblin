# Phase 4 — Frontend Architecture Decomposition

**Theme:** Break the 4,076-line bootstrap closure into a maintainable vanilla-TS app with real
state management, so phase 5 can reshape the product without archaeology.
**Depends on:** Phase 0 (lint), Phase 3 (generated IPC types).
**Estimated size:** 2–3 weeks.

## Current state (verified findings)

1. **`src/app/bootstrap.ts` (4,076 lines)**: `bootstrapStorageGoblin()` at line 1237 runs to
   the end of the file — a ~2,800-line closure declaring the entire app state object
   (`bootstrap.ts:1245-1273`), ~10 pieces of orphan mutable state alongside it
   (`fileTreeHandle`, `selectedBinPaths`, request sequence counters, debounce timers,
   `activeVersionCounts`…), every event handler, every render function, and all IPC calls.
   Nothing inside is directly unit-testable; `bootstrap.test.ts` (4,039 lines — as large as
   the implementation) tests it end-to-end through the DOM.
2. **`index.html` (579 lines)** is one static template containing every screen, drawer, and
   modal; visibility is toggled by class. `dom.ts` registers **120+ elements by id** into a
   flat `AppDom` object — every UI change edits three files (`index.html`, `dom.ts`,
   `bootstrap.ts`).
3. **State/render coupling:** render functions read and write `state` directly and re-render
   imperative slices; there's no subscription model, so ordering of update-then-render calls
   is by convention. Debouncing is bespoke per call site (`bootstrap.ts:1283-1312`).
4. **`types.ts` (781 lines)** hand-mirrors the Rust wire format (replaced by generated types
   in phase 3.2).
5. Good news worth preserving: `client.ts` is already a decent typed IPC wrapper; the virtual
   file tree (`file-tree-virtual.ts`) is a solid piece; the design system
   (`@goblin-systems/goblin-design-system`) gives a consistent visual base; 221 tests pass.

## Decision: stay vanilla TS (no framework)

Recommended. Rationale: design-system package is vanilla-first, the app is one window with a
handful of screens, and a framework migration would add risk to phase 5 without enabling
anything the redesign needs. What we adopt instead is *structure*: a store, view modules with
a mount/update/destroy contract, and generated IPC types. Revisit only if phase 5's designs
demand heavy dynamic composition (record the decision as an ADR either way).

## Target architecture

```
src/
  main.ts                 # entry: create store, mount shell, register views
  ipc/
    client.ts             # evolved from app/client.ts; only place invoke() is called
    events.ts             # typed Tauri event subscriptions (status, progress, activity)
    generated/            # ts-rs/specta output from phase 3.2 (checked in, CI-verified fresh)
  state/
    store.ts              # single app store: typed state + actions + subscribe(selector)
    selectors.ts
    actions/*.ts          # IPC-calling actions grouped by domain (locations, credentials,
                          # sync, bin, versions) — the only writers of state
  views/                  # one directory per screen/surface, each owning its DOM subtree:
    shell/                #   window chrome, nav, status badge
    home/                 #   dashboard + file tree host
    locations/            #   list + editor form
    credentials/
    activity/
    settings/             #   polling, conflict defaults, debug
    modals/               #   confirm, conflict-resolution, version drawer
  components/             # reusable widgets: file-tree (moved), toast, badge, form-field
  lib/                    # debounce, formatters (bytes, counts, dates) — pure & unit-tested
```

View contract: `createView(host: HTMLElement, store: Store): { destroy(): void }` — views build
their own DOM (template strings or `<template>` clones), subscribe to store selectors, and
clean up. `index.html` shrinks to the shell skeleton + view mount points.

## Workstreams

### 4.1 Store & IPC layer

- [x] **Done.** `src/state/store.ts`, ~150 lines, no library. Tests cover the failures that
      actually bite: a no-op setState notifies nobody, unsubscribing mid-notification does not
      shift the iteration and skip a listener, one throwing listener does not freeze the rest,
      and a setState issued from inside a listener settles in order rather than recursing.
      *(Original text: implement the store: immutable-ish state object, action*
      functions, `subscribe(selector, cb)` with equality-gated notifications.
- [x] **Done.** All eleven `state` fields moved; the closure keeps a transitional read mirror
      so its ~180 read sites keep working while views are extracted one at a time. Freezing the
      state caught two writes the migration missed.
      *(Original text: port `state` fields out of the bootstrap closure, including the orphan*
      variables (they are state: selected bin paths, view mode, request sequencing).
- [x] **Done and adopted.** `src/lib/latest.ts`, wired into the file-tree load where the race
      was already known. It rejects rather than hanging, because a superseded call that never
      settles leaks every spinner it started.
      *(Original text: a `latest()` helper wrapping IPC calls*
      replaces the hand-rolled `fileTreeRequestSequence` counter.
- [ ] Swap hand-written `types.ts` for generated types; delete the drift (its 116-line test
      file too, where redundant).
- [x] **Module done, partially adopted.** `src/ipc/events.ts` declares every channel and
      payload in one table, including phase 2.1's `storage://transfer-progress`. `client.ts`
      still has its own `listen*` methods; migrating them is pending.
      *(Original text: typed event subscription module: sync status, per-file progress,*
      activity — single subscription point dispatching into the store.

### 4.2 View extraction (incremental, screen by screen)

Order chosen so the riskiest, most-entangled surfaces go last:

- [x] **Done** — `src/views/settings/settings-view.ts`, 14 tests. Established the contract:
      `createView(deps) => { destroy() }`, narrow structural dependencies, store subscriptions
      released on teardown. Surfaced and fixed a real bug: a failed save was an unhandled
      rejection, so a save that did not happen looked exactly like one that did.
- [ ] credentials screen
- [ ] locations screen (form state machine: create vs edit, provider-dependent fields —
      currently scattered through dozens of helpers like `renderCredentialFormState`,
      `setLocationOptions`)
- [x] **Done** — `src/views/activity/activity-view.ts`, 11 tests. Also fixed two things the
      inline version got wrong: it appended 36 items into the live list (now one detached swap)
      and it left a debounce timer to fire into a torn-down view.
- [ ] home screen: dashboard, location selector, file-tree host, bin toolbar
- [ ] modals & drawers: async-confirm controller, conflict-resolution modal
      (`bootstrap.ts:355-705` and 706-879 are already quasi-components — move, don't rewrite),
      version drawer
- [ ] shell: nav, window controls, status badge
- [ ] Delete `dom.ts`'s global registry once each view owns its subtree; delete the emptied
      `bootstrap.ts`.

### 4.3 Test migration

- [ ] Split `bootstrap.test.ts` along the same seams; store/actions get fast pure tests;
      views get thin jsdom mount tests; keep a small number of whole-app smoke tests
      (mount everything against a mocked IPC client).
- [ ] Mock IPC at the `ipc/client` boundary with a typed fake (replaces per-test
      `mockIPC`-style plumbing).
- [ ] Coverage target: `state/` + `lib/` ≥ 90 %, views ≥ 60 %.
- [x] **Done early, deliberately** — `src/architecture.test.ts`. 500 lines, with every current
      offender listed at its size so each can only shrink; removing an entry is the progress
      marker. A second gate keeps `state/`, `lib/` and `ipc/` free of DOM references. Both
      verified by tampering.

### 4.4 Frontend platform hygiene

- [ ] Single toast/notification service (currently `toast` is threaded through constructors).
- [ ] Centralized error presentation: `SyncError` codes (phase 0.4) map to user-facing
      messages in one module — the copy will be rewritten in phase 5, the plumbing lands here.
- [ ] Icon handling: `applyIcons()`/lucide usage consolidated so views can declare icons.
- [ ] Audit and remove the `withGlobalTauri: true` config (`tauri.conf.json:13`) — the app
      imports `@tauri-apps/api` properly; the global object is an unnecessary surface.
      Also revisit CSP dev-only entries (`ws://localhost:1433`) leaking into production
      config (`tauri.conf.json:29`).


## Status (2026-08-02) — in progress

**4.1 is essentially complete; 4.2 has started.** Landed on
`overhaul/phase-1` (pushed). 281 frontend tests, tsc and eslint clean.

### What exists now

```
src/
  state/store.ts        the app store (+ app-state.ts: AppState, selectors)
  lib/latest.ts         request sequencing
  lib/debounce.ts       trailing-edge debounce
  lib/dom.ts            setButtonBusy
  ipc/events.ts         typed channels, incl. transfer-progress
  views/settings/       first extracted view
  views/activity/       second extracted view
  architecture.test.ts  size + layering gates
```

`bootstrap.ts`: **4,324 → 4,140 lines.** The number is unimpressive on
purpose — the first two views are the *small* ones, chosen to establish
the contract cheaply. The state migration underneath them is what makes
the rest possible, and it is done.

### Two bugs found by extracting

Neither was known before the views got tests:

1. **A failed settings save was silently swallowed.** The
   `() => void handleSaveSettings(...)` wiring turned a rejection into an
   unhandled promise rejection: the button un-stuck itself and nothing
   was reported, so a save that did not happen was indistinguishable
   from one that did.
2. **The activity render leaked a timer past teardown**, and rebuilt the
   list by appending 36 items into the live DOM.

### Next, in order

1. **Credentials view** — bigger than the first two (~290 lines across
   render + create/test/delete handlers) and the first with per-row
   actions that call back into refresh logic.
2. **Locations view** — the genuinely hard one. Its form is a state
   machine (create vs edit, provider-dependent fields) and it owns the
   versioning and remote-bin toggles.
3. **Home, modals, shell**, then `dom.ts`'s global registry can go.
4. **Migrate `client.ts`'s `listen*` methods onto `ipc/events.ts`** and
   split the client by domain.
5. **Split `bootstrap.test.ts`** (4,520 lines) alongside each view
   rather than in one final pass — that is how test migrations get
   abandoned.

### Honest notes

- The views bind existing markup rather than generating DOM. The design
  system is markup-driven, and regenerating markup phase 5 is about to
  redesign would be work done twice. The `views/` contract does not
  depend on which of the two it is.
- `ipc/events.ts` is written and tested but only half adopted: nothing
  consumes `onTransferProgress` yet, because its consumer is phase 5's
  progress UI.
- The transitional `state` mirror in `bootstrap.ts` is exactly that. It
  should be gone by the time the last view is extracted; if it is not,
  something was extracted badly.

## Acceptance criteria

1. `bootstrap.ts` and `dom.ts` deleted; no file in `src/` over 500 lines; `index.html` under
   ~120 lines (shell only).
2. All state lives in the store; views communicate only via store actions/selectors; grep
   gate: `invoke(` appears only in `src/ipc/`.
3. IPC types are generated from Rust and CI fails if regeneration produces a diff.
4. Test count ≥ current 221 with the smoke suite retained; suite runtime ≤ current (~33 s).
5. Zero user-visible behavior change (this phase is deliberately invisible — pixel-identical
   before/after screenshots on each screen).

## Risks

- **Behavior drift during extraction.** Mitigation: screen-by-screen PRs, before/after
  screenshot comparison, keep the existing e2e-ish tests running against the old shell until
  its screen is ported.
- **`bootstrap.test.ts` entanglement** — the 4k-line test file may resist splitting; budget
  real time for it; do not delete coverage to make progress.
- **Temptation to redesign while extracting.** Phase 5 owns redesign; this phase forbids UX
  changes to keep diffs reviewable.
