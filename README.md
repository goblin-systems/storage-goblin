# Storage Goblin

A desktop application that turns an Amazon S3 bucket into a personal cloud drive with bi-directional sync.

Built with **Tauri v2** (Rust backend, vanilla TypeScript frontend).

---

## What It Does

Storage Goblin keeps a local folder and an S3 bucket in sync. Changes on either side are detected, compared, and resolved so files stay consistent across local and remote storage.

### Core Features

- **Bi-directional sync** -- Local folder scan, remote inventory, diff comparison, sync plan generation, and execution with progress tracking. Sync plans are persisted to a durable SQLite queue so work survives restarts.

- **Multi-location support** -- Multiple sync pairs (local folder <-> S3 bucket), each with independent credentials, polling configuration, and conflict strategy. Includes migration from a legacy single-profile model.

- **Secure credential management** -- Named credentials stored in the OS secure store (Windows Credential Manager) with a write-then-verify pattern and rollback on failure.

- **S3 operations** -- List, upload, download, and delete objects. Supports pagination, automatic retries, and storage class transitions (Standard <-> Glacier Instant Retrieval).

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
    client.ts          -- S3 client wrapper (list, put, get, delete, transitions)
    profile.ts         -- Credential and sync-pair persistence
    activity.ts        -- Event logging and debug file output
    status.ts          -- Sync status aggregation
    file-tree.ts       -- DOM-based file tree renderer
    file-tree-virtual.ts -- Virtualised file tree for large buckets
    dom.ts             -- DOM helpers
    persistence.ts     -- SQLite-backed sync queue
    types.ts           -- Shared type definitions
    *.test.ts          -- Unit tests (Vitest)
src-tauri/             -- Rust backend (Tauri commands, S3 SDK calls, SQLite)
```

## Status

Early stage (v0.1.0). Functional sync pipeline, multi-location sync orchestration, durable sync-plan recovery, local filesystem watcher support, remote bin restore flows, and file browser are in place. Not yet implemented: object versioning.

## Roadmap

### Now

- Object versioning-aware workflows for safer recovery and history.
- Richer conflict handling beyond size-based file/file decisions.
- Clearer conflict diagnostics and recovery tooling in the UI.

### Next

- More advanced sync safety and auditability features.
- Bulk remote bin operations and richer restore flows.
- Deeper watcher observability and watcher-health surfacing.

### Later

- Content-aware reconciliation and history workflows built on versioning.
- Broader operational tooling for support, diagnostics, and reporting.

## License

MIT
