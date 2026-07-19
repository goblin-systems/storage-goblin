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

- [ ] Implement the store (~100 lines, no library needed): immutable-ish state object, action
      functions, `subscribe(selector, cb)` with equality-gated notifications.
- [ ] Port `state` fields out of the bootstrap closure into the store, including the orphan
      variables (they are state: selected bin paths, view mode, request sequencing).
- [ ] Request-sequencing/cancellation made systematic: a `latest()` helper wrapping IPC calls
      replaces the hand-rolled `fileTreeRequestSequence` counter.
- [ ] Swap hand-written `types.ts` for generated types; delete the drift (its 116-line test
      file too, where redundant).
- [ ] Typed event subscription module: sync status, per-file progress (from phase 2.1),
      activity — single subscription point dispatching into the store.

### 4.2 View extraction (incremental, screen by screen)

Order chosen so the riskiest, most-entangled surfaces go last:

- [ ] settings screens (polling / conflict / debug) — simplest, proves the pattern
- [ ] credentials screen
- [ ] locations screen (form state machine: create vs edit, provider-dependent fields —
      currently scattered through dozens of helpers like `renderCredentialFormState`,
      `setLocationOptions`)
- [ ] activity screen
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
- [ ] Add the module size gate: no file in `src/` > 500 lines (generated code exempt).

### 4.4 Frontend platform hygiene

- [ ] Single toast/notification service (currently `toast` is threaded through constructors).
- [ ] Centralized error presentation: `SyncError` codes (phase 0.4) map to user-facing
      messages in one module — the copy will be rewritten in phase 5, the plumbing lands here.
- [ ] Icon handling: `applyIcons()`/lucide usage consolidated so views can declare icons.
- [ ] Audit and remove the `withGlobalTauri: true` config (`tauri.conf.json:13`) — the app
      imports `@tauri-apps/api` properly; the global object is an unnecessary surface.
      Also revisit CSP dev-only entries (`ws://localhost:1433`) leaking into production
      config (`tauri.conf.json:29`).

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
