Storage Goblin

🧾 Project Brief — “BYO Cloud Drive (S3-first)” - 
1) Vision

A simple, open-source desktop app that turns an S3 bucket into a personal cloud drive with seamless sync — no servers, no subscriptions, no lock-in.

Users bring their own storage → software provides the experience.

2) Goals (MVP)
Zero-friction setup (install → paste credentials → sync)
Reliable bi-directional sync
Works like a normal folder locally
Uses S3 as primary backend
No data loss, predictable behaviour
3) Non-Goals (MVP constraints)
No real-time collaboration
No browser UI
No document editing
No enterprise features (SSO, audit logs)
No multi-user sharing (initially)
4) Target User
Technical users / prosumers
People with:
large storage needs
cost sensitivity
distrust of SaaS lock-in
5) Core Use Cases
Use case	Description
Backup	Local folder continuously synced to S3
Multi-device sync	Same folder across 2–3 devices
Archive	Cheap storage via S3 lifecycle
Offline work	Always works locally
6) Key Principles
Local-first → disk is source of truth
Eventual consistency → avoid blocking UX
Transparent behaviour → no surprises
Fail-safe → never delete without certainty
🏗️ System Overview
Local FS ↔ Sync Engine ↔ Metadata Index ↔ S3 Backend
Components
Component	Responsibility
File watcher	detect local changes
Sync engine	reconcile local ↔ remote
Metadata index	track state
S3 adapter	storage operations
Client UI	setup + status
⚙️ Functional Requirements (MVP)
1) Setup
Input:
access key
secret
bucket name
region (optional)
Select local folder
Validate connection
2) Sync (core)
Must support:
Create
Update
Delete
Rename (handled as move)
Behaviour:
bi-directional sync
near real-time (event-driven)
retry on failure
3) Conflict Handling

Rule:

If same file changed in two places → duplicate with suffix

Example:

file.txt
file (conflict-deviceA-2026-04-03).txt
4) Metadata Tracking

Track per file:

path
size
last modified
hash
version id (if available)
5) Error Handling
network failure → retry with backoff
auth failure → surface immediately
partial upload → resume or retry
6) Delete Safety
never immediately hard delete
use:
soft delete marker OR
delayed delete window
7) Status & Observability

User can see:

syncing / idle
errors
last sync time
📦 MVP Backlog (AI-Agent Ready)
🔹 EPIC 1 — Project Foundation
 Define config schema (credentials, folder, settings)
 Implement logging system
 Implement basic CLI entrypoint
🔹 EPIC 2 — S3 Integration
 Connect to S3 with credentials
 List objects (prefix-based)
 Upload object
 Download object
 Delete object
 Handle pagination
 Handle retries + exponential backoff
🔹 EPIC 3 — Local File Scanner
 Scan directory recursively
 Build initial file index
 Detect file metadata (size, mtime)
 Compute file hash (configurable)
🔹 EPIC 4 — Metadata Index
 Define schema (SQLite or equivalent)
 Store file state (local + remote snapshot)
 Track sync status per file
 Track pending operations queue
🔹 EPIC 5 — Sync Engine (Core)
Phase 1 (one-way)
 Upload new files to S3
 Update modified files
 Skip unchanged files
Phase 2 (two-way)
 Detect remote changes
 Download missing/updated files
 Reconcile differences
🔹 EPIC 6 — File Watcher
 Listen to local FS events
 Debounce rapid changes
 Queue sync operations
🔹 EPIC 7 — Conflict Resolution
 Detect concurrent modification
 Duplicate with conflict suffix
 Ensure no overwrite
🔹 EPIC 8 — Delete Handling
 Detect local delete → propagate to S3
 Detect remote delete → apply locally
 Implement safety delay or soft delete
🔹 EPIC 9 — Sync Queue & Scheduler
 Queue operations (upload/download/delete)
 Prioritise small files
 Limit concurrency
 Retry failed jobs
🔹 EPIC 10 — Initial UX (Minimal)
 Setup wizard (CLI or minimal UI)
 Show sync status
 Show errors
🔹 EPIC 11 — Resilience
 Restart-safe sync (resume state)
 Crash recovery
 Idempotent operations
📊 Acceptance Criteria (MVP)
Scenario	Expected
Add file locally	appears in S3
Modify file locally	updated in S3
Delete locally	removed from S3
Add file remotely	appears locally
Network drop	sync resumes
Conflict	both versions preserved
⚠️ Known Risks
Risk	Mitigation
Data loss	never overwrite without version check
Infinite sync loops	track state via metadata
Large file inefficiency	accept for MVP
S3 latency	async sync
🧠 Future (post-MVP)
Chunked uploads (large files)
Encryption (client-side)
Multi-bucket support
Sharing (signed URLs)
Web UI
Mobile client
🔥 Final framing

This is not:

“cheap cloud storage tool”

This is:

“an open, user-owned cloud filesystem layer on top of S3”

## Tech

Use vision goblin as base for tech, design, architecture etc.