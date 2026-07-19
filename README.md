# Storage Goblin

A desktop application that turns supported object storage into a personal cloud drive with bi-directional sync.

Built with **Tauri v2** (Rust backend, vanilla TypeScript frontend).

---

## What It Does

Storage Goblin keeps a local folder and a remote object storage location in sync. Changes on either side are detected, compared, and resolved so files stay consistent across local and remote storage.

Supported providers today:

- **AWS S3**
- **Google Cloud Storage (GCS)**

### Core Features

- **Bi-directional sync** -- Local folder scan, remote inventory, diff comparison, sync plan generation, and execution with progress tracking. Sync plans are persisted to a durable SQLite queue so work survives restarts.

- **Multi-location support** -- Multiple sync pairs (local folder <-> remote bucket), each with independent credentials, polling configuration, and conflict strategy. Includes migration from a legacy single-profile model.

- **Secure credential management** -- Named credentials stored in the OS secure store (Windows Credential Manager) with a write-then-verify pattern and rollback on failure.

- **Provider support** -- First-class AWS S3 and Google Cloud Storage support with provider-aware credentials, capability gating, object operations, version-aware flows, lifecycle-backed remote bin flows, and storage class transitions (AWS Standard <-> Glacier Instant Retrieval, GCS Standard <-> Coldline).

- **File tree browser** -- Two rendering modes: standard DOM for small trees (< 2,000 entries) and virtual scrolling for large ones. Checkbox tree with parent/child propagation, per-file actions (delete, change storage class).

- **Background polling** -- Configurable polling worker per sync pair with independent intervals and stop signals. Aggregated status across all active pairs.

- **Activity log** -- Real-time event stream via Tauri events, optional debug log file (capped at 2,000 lines), and automatic credential redaction in the UI.

- **Custom title bar** -- Native-feeling window chrome with a drag region and custom decorations.

## Architecture

```
src/
  main.ts              -- App entry point
  styles.css           -- Global styles
  app/
    bootstrap.ts       -- App initialization and location management
    client.ts          -- Provider-aware client wrapper (list, put, get, delete, transitions)
    profile.ts         -- Credential and sync-pair persistence
    activity.ts        -- Event logging and debug file output
    status.ts          -- Sync status aggregation
    file-tree.ts       -- DOM-based file tree renderer
    file-tree-virtual.ts -- Virtualised file tree for large buckets
    dom.ts             -- DOM helpers
    persistence.ts     -- SQLite-backed sync queue
    types.ts           -- Shared type definitions
    *.test.ts          -- Unit tests (Vitest)
src-tauri/             -- Rust backend (Tauri commands, provider adapters, SQLite)
```

## Status

Early stage (v0.2.0). Functional sync pipeline, multi-location sync orchestration, durable sync-plan recovery, local filesystem watcher support, lifecycle-backed remote bin flows, and file browser are in place. Native multi-provider support now covers AWS S3 and Google Cloud Storage as first-class providers, with secure provider-specific credentials, capability-aware UI, provider-aware object operations, object versioning flows, remote bin support, and storage class actions.

## Roadmap

The project is undergoing a staged overhaul toward a 1.0-quality release. The full plan —
current-state assessment, phases, and acceptance criteria — lives in
[`backlog/`](backlog/README.md). Contributor workflow is documented in
[`CONTRIBUTING.md`](CONTRIBUTING.md); product briefs live in [`docs/`](docs/).

## License

MIT
