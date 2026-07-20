// GENERATED FILE — do not edit by hand.
//
// Rendered from the Rust domain enums in src-tauri/src/storage/model.rs by
// storage::ts_bindings. Run `cargo test -- --ignored regenerate_` to update.

export type EntryKind =
  | "file"
  | "directory";

export type SyncPhase =
  | "unconfigured"
  | "idle"
  | "polling"
  | "syncing"
  | "paused"
  | "error";

export type ConflictStrategy =
  | "preserve-both"
  | "prefer-local"
  | "prefer-remote";

export const CONFLICT_STRATEGIES: readonly ConflictStrategy[] = ["preserve-both", "prefer-local", "prefer-remote"] as const;

export type FileEntryStatus =
  | "synced"
  | "local-only"
  | "remote-only"
  | "review-required"
  | "conflict"
  | "glacier"
  | "deleted";

export type QueueStatus =
  | "planned"
  | "in_progress"
  | "completed"
  | "failed"
  | "interrupted";
