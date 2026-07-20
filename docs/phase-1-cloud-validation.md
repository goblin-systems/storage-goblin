# Phase 1 — manual cloud validation checklist

Everything in phase 1 is proven only by the simulator and unit/integration tests.
**Nothing has touched a real bucket.** Deletes against live storage are the highest-risk
change in the overhaul, so this checklist gates merging `overhaul/phase-1`.

Run the whole matrix on **AWS S3** and on **GCS**. Record the date, the app version, and
one line per row. Anything that fails goes back to the phase doc as a task, not a note.

## Setup

- Use a **scratch bucket per provider**, created for this run. Never a bucket holding
  anything you would miss.
- Seed ~50 files across at least 3 nested directories, including one file ≥ 100 MB and one
  with a space and a non-ASCII character in the name.
- Run one clean sync first and confirm a settled state (no pending operations, no review
  items) before starting the matrix.
- Keep the activity log open (Help → Activity Logs) and the debug log file enabled.

## Matrix

| # | Scenario | Expected | S3 | GCS |
|---|----------|----------|----|-----|
| 1 | Delete one file locally | Remote object removed (delete marker if versioning on, moved to remote bin if enabled); anchor dropped; no review item | | |
| 2 | Delete one file remotely (console) | Local file moved to **OS trash**, not hard-deleted; anchor dropped | | |
| 3 | Delete a folder of ~10 files locally | All child objects removed remotely; no review items. *Known gap: the remote directory placeholder is expected to remain* | | |
| 4 | Delete a folder remotely | All local children go to trash; emptied local dirs pruned | | |
| 5 | Rename a file locally | Server-side copy+delete (**not** a re-upload — confirm via egress/activity log, and that a ≥100 MB file renames near-instantly) | | |
| 6 | Rename a file remotely | Local file renamed in place, no re-download | | |
| 7 | Rename a folder locally | Resolves as per-file moves; content is not re-uploaded | | |
| 8 | Edit the same file on both sides, `preserve-both` | Both versions survive on both sides; loser is `name (conflict YYYY-MM-DD).ext`; no review item | | |
| 9 | Edit the same file on both sides, `prefer-local` / `prefer-remote` | Chosen side wins. *Known gap: the overwritten local file is not trash-protected* | | |
| 10 | Delete locally **and** edit remotely | Parks for review; neither side is destroyed | | |
| 11 | Delete remotely **and** edit locally | Parks for review; neither side is destroyed | | |
| 12 | Empty the local folder entirely (simulate an unmounted drive) | **Mass-delete breaker trips**: zero deletions execute, an error-level activity entry names the count, all paths become review items, anchors intact | | |
| 13 | Fresh install against a populated bucket + identical local folder | GCS: silent anchoring, zero transfers. **S3: expected to park for review** (known gap, 1.3) | | |
| 14 | Kill the app mid-upload (large file), restart | Recovers to a consistent state; no phantom "synced" status. *Known gap: downloads are not yet atomic (phase 2)* | | |
| 15 | Kill the app mid-download, restart | As above; note whether a partial file is left behind (expected until phase 2.1) | | |
| 16 | Disconnect the network, edit files, reconnect | Sync resumes; no duplicate or lost files | | |
| 17 | Cold-storage object (Glacier IR / Coldline) present | Skipped, not deleted, not endlessly retried | | |

## After the run

- Verify the bucket object count and the local file count match expectations exactly.
- Verify no object was hard-deleted that should have been protected (check versions /
  remote bin / OS trash for rows 1–4).
- Check the debug log for credentials: **no key, secret, or service-account content may
  appear**, even inside provider error text.
- Update `backlog/phase-1-sync-correctness.md`: tick acceptance criterion 1 and 2, and
  move anything that failed into a task.
