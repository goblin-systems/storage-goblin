# Phase 5 — UX Overhaul

**Theme:** From "engine debug console" to "trustworthy personal cloud drive". The product's
job is to make a user feel — correctly — that their files are safe, and to need attention
only when something genuinely needs a decision.
**Depends on:** Phase 4 (view architecture), Phase 1 (conflict/first-sync semantics),
Phase 2 (progress events, pause/resume).
**Estimated size:** 4–6 weeks including design iteration.

## Current-state UX audit

Observed from `index.html`, `bootstrap.ts` flows, and the interaction model:

1. **No path from install to first sync.** A new user lands on a Home screen with an empty
   file tree and a nav of dropdown menus. To sync anything they must discover, in order:
   Settings → Credentials (paste keys into a form with provider-specific fields), then
   Settings → Sync Locations (a separate form: label, provider, folder, region, bucket,
   credential dropdown, capability badges, versioning toggle, polling interval, conflict
   strategy, remote-bin retention — a dozen decisions), then find the sync trigger. The brief
   promised "install → paste credentials → sync".
2. **Menu-bar navigation for a 6-screen app.** View / Settings / Help dropdowns
   (`index.html:30-109`) hide primary surfaces (Sync Locations, Credentials, Activity) behind
   two clicks with no visible selected state. Desktop-app menus signal document apps, not
   dashboards.
3. **Status speaks engine, not outcome.** The headline badge shows phase strings like
   "Unconfigured"; counts are "in sync / not in sync" numbers; the primary feedback channel is
   a raw activity log. Nothing answers the only three user questions: *Is everything synced?
   Is it working right now? Does anything need me?*
4. **Jargon throughout**: "Sync Locations", "Remote bin", "Conflict Defaults", "review
   required", "Polling", "Rescan", storage-class names, "queue items". Confirmation dialogs
   are paragraph-length technical text (`getDeleteConfirmationMessage`,
   `bootstrap.ts:175-206`).
5. **No transfer feedback.** No per-file progress, no speed, no ETA, no "what's uploading
   right now" surface (phase 2 provides the events; nothing consumes them yet).
6. **Conflicts and review items** — the most decision-heavy moment in the product — live in a
   modal reached from tree rows, with metadata formatted as etag/size/timestamp dumps
   (`formatConflictEtag`, `bootstrap.ts:309-331`).
7. **Desktop integration absent**: no system tray/menu-bar icon, no minimize-to-tray, no OS
   notifications, no launch-at-startup, no Explorer/Finder integration, window min-size
   1024×720 forces a large footprint for what could be a glanceable widget.
8. **Accessibility & polish debt**: focus management in modals/drawers is untested,
   keyboard-only flows unverified, dropdown nav is hover-dependent, three themes exist but
   contrast is unaudited.

## Design principles (agree before pixels)

1. **Status-first**: one glanceable state for the whole app — Synced ✓ / Syncing (n files,
   progress) / Attention needed (n items) / Paused / Offline. Everything else hangs off it.
2. **Progressive disclosure**: defaults for everything at setup (versioning on where
   supported, remote bin on, merge-mode first sync); advanced knobs live behind "Advanced".
3. **Speak files, not sync internals**: "3 files need a decision" not "review_required: 3";
   "Keeping both copies" not "preserve-both strategy".
4. **Attention is expensive**: OS notification only for decisions and failures, never for
   routine success.
5. **Reversibility everywhere**: every destructive surface offers the undo path (remote bin,
   version history, trash) inline — the safety features already built become visible trust.

## Workstreams

### 5.1 Design foundation

- [ ] Write the UX spec for the five core journeys: first-run, add-a-location, steady-state
      monitoring, resolve-a-conflict, recover-a-file. Wireframe → review → hi-fi in the
      design system's language (use `goblin-design-system` skill/package as the constraint).
- [ ] Define the **status model** as a typed contract with the backend (one enum + counts +
      current transfer summary) — this is the API phase 5 asks of phases 2–3's event system.
- [ ] Microcopy pass: a glossary mapping every internal term to user language; error-message
      rewrite driven by `SyncError` codes (plumbing from phase 4.4). Every message states:
      what happened, what Goblin already did about it, what (if anything) you can do.

### 5.2 First-run & setup flow

- [ ] Onboarding wizard replacing the credentials/locations scatter: pick provider → paste
      credentials (with live validation and provider-specific guidance, reusing the
      permission-probe machinery) → pick bucket (list existing where allowed) → pick local
      folder → choose first-sync mode (merge default; mirror options behind Advanced, wired
      to phase 1.3) → done, syncing.
- [ ] Target: **under 5 minutes, zero documentation**, measured with 3–5 usability tests.
- [ ] Credential management remains as a screen for the multi-credential case but is no
      longer on the critical path.
- [ ] Empty states everywhere teach the next step (current empty states exist but are
      passive).

### 5.3 Main window redesign

- [ ] **Dashboard**: per-location cards (status, last sync, live transfer progress with
      speed/ETA from phase 2 events, pause/resume button); global status header; "needs
      attention" section pinned to top when non-empty.
- [ ] **Sidebar navigation** (Locations / Activity / Settings) replacing menu-bar dropdowns;
      dropdown menu retained only for app-level actions (theme, about, quit).
- [ ] **File browser**: keep virtual tree; add per-file sync-state icons (synced / syncing /
      pending / attention / cold-storage with plain-language tooltip — fixes the silent
      cold-class noop from phase 1), search/filter box, breadcrumbs, and a details pane
      consolidating per-file actions (open, reveal, history, restore, storage class) that are
      currently spread across row buttons and modals.
- [ ] **Activity** becomes human-readable history grouped by sync session ("Uploaded 14 files
      (230 MB) · 2 min"), with the raw log demoted to a debug view toggle.

### 5.4 Conflict & recovery UX

- [ ] "Needs attention" queue: full-screen list (not per-row modal), each item showing both
      versions with human diff cues (newer/older, bigger/smaller, this device/other device,
      preview where inline compare already exists) and three buttons: Keep mine / Keep theirs
      / Keep both. Bulk resolution for homogeneous batches.
- [ ] Mass-delete circuit breaker surface (phase 1.1): "Storage Goblin paused: this would
      delete 1,204 files from your bucket. Review / Proceed / Restore them locally."
- [ ] Recovery center: unified remote-bin + version-history browsing ("Deleted files" and
      "Older versions" in file terms), restore flows with progress.

### 5.5 Desktop citizenship

- [ ] System tray icon with status glyph, quick menu (pause all, open, recent activity),
      minimize-to-tray, launch-at-startup toggle (tauri autostart plugin).
- [ ] OS notifications for: attention items, first completion of initial sync, sustained
      failures (auth expired, disk full) — with per-category opt-out.
- [ ] Window polish: remember size/position, sensible min-size (< 1024px), decorated fallback
      audit on macOS/Linux (custom chrome is currently Windows-styled).

### 5.6 Accessibility & quality pass

- [ ] Keyboard-complete: every journey operable without a mouse; visible focus; modal/drawer
      focus traps and Esc semantics; tree keyboard navigation.
- [ ] Screen-reader pass: landmarks, live-region for status changes, labels (many exist —
      verify), tree ARIA pattern.
- [ ] Contrast audit across the three themes; reduced-motion support for progress animations.
- [ ] Empty/loading/error triads verified for every view (loading indicators exist —
      standardize skeletons vs spinners).

## Acceptance criteria

1. Usability test: ≥ 4 of 5 first-time participants complete install → first successful sync
   unaided in < 5 min, and can afterwards answer "are your files synced right now?" correctly.
2. The three-question status (synced? working? needs me?) is answerable from (a) the tray
   icon, (b) the dashboard header, in one glance each.
3. Per-file progress with speed/ETA visible during transfers; pause/resume works from
   dashboard and tray.
4. All conflict resolution flows pass through the attention queue; no raw etag/phase/queue
   jargon reachable in non-debug UI (copy-audit checklist).
5. Keyboard-only and screen-reader walkthroughs of the five core journeys pass; contrast
   AA across themes.
6. Tray, notifications, autostart, and window-state persistence shipped on Windows + macOS +
   Linux.

## Risks

- **Design capacity** — this phase needs actual design iteration, not just implementation;
  timebox wireframe cycles, test with real users early (recruit from the target prosumer
  audience).
- **Scope explosion in the file browser** — it can absorb unlimited effort; the details-pane
  consolidation and status icons are the flagship-critical parts; defer thumbnails/previews
  beyond the existing inline compare.
- **Tray/notification platform quirks** across three OSes; prototype early in the phase.
