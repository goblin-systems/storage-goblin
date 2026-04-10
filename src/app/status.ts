import type { SyncPhase, SyncStatus, SyncOverviewStats } from "./types";

export interface StatusPresentation {
  badgeLabel: string;
  badgeTone: "default" | "success" | "error" | "beta";
  indicatorClass: "connected" | "disconnected" | "error" | "untested";
  indicatorLabel: string;
  summary: string;
}

const ACTIONABLE_SYNC_ERROR_SUMMARY = "Sync failed. Open Activity for details.";

function normalizeLastError(lastError: string | null | undefined): string | null {
  const trimmed = lastError?.trim();
  return trimmed ? trimmed : null;
}

function isGenericSyncError(lastError: string | null | undefined): boolean {
  const normalized = normalizeLastError(lastError);
  if (!normalized) return true;

  return [
    /^error[.!:]?$/i,
    /^sync error[.!:]?$/i,
    /^sync failed[.!:]?$/i,
    /^failed[.!:]?$/i,
    /^unknown error[.!:]?$/i,
    /^an error occurred[.!:]?$/i,
    /^the native stub reported an error[.!]?$/i,
  ].some((pattern) => pattern.test(normalized));
}

export function getSyncOverviewStats(status: SyncStatus): SyncOverviewStats {
  return {
    localFiles: status.overview?.localFiles ?? status.comparison.localFileCount ?? status.indexedFileCount,
    remoteFiles: status.overview?.remoteFiles ?? status.comparison.remoteObjectCount ?? status.remoteObjectCount,
    inSync: status.overview?.inSync ?? status.comparison.exactMatchCount,
    notInSync: status.overview?.notInSync
      ?? status.comparison.localOnlyCount + status.comparison.remoteOnlyCount + status.comparison.sizeMismatchCount,
  };
}

function titleCasePhase(phase: SyncPhase): string {
  switch (phase) {
    case "unconfigured": return "Unconfigured";
    case "idle": return "Idle";
    case "polling": return "Polling";
    case "syncing": return "Syncing";
    case "paused": return "Paused";
    case "error": return "Error";
  }
}

export function describeSyncStatus(status: SyncStatus): StatusPresentation {
  switch (status.phase) {
    case "polling":
      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "success",
        indicatorClass: "connected",
        indicatorLabel: "Watching for changes",
        summary: `Connected. Storage Goblin is monitoring this folder and checking the bucket every ${status.pollIntervalSeconds}s.`,
      };
    case "syncing":
      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "success",
        indicatorClass: "connected",
        indicatorLabel: "Sync in progress",
        summary: "Connected. Setup is saved and the desktop app is preparing or running sync work now.",
      };
    case "paused":
      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "beta",
        indicatorClass: "disconnected",
        indicatorLabel: "Sync paused",
        summary: "Setup is saved, but automatic sync work is paused right now.",
      };
    case "error":
      {
        const meaningfulError = isGenericSyncError(status.lastError) ? null : normalizeLastError(status.lastError);

      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "error",
        indicatorClass: "error",
        indicatorLabel: meaningfulError ?? "Sync failed",
        summary: meaningfulError ?? ACTIONABLE_SYNC_ERROR_SUMMARY,
      };
      }
    case "idle":
      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "default",
        indicatorClass: "connected",
        indicatorLabel: "Ready",
        summary: "Connected. Setup is saved and the latest sync status is ready to review.",
      };
    case "unconfigured":
    default:
      return {
        badgeLabel: titleCasePhase(status.phase),
        badgeTone: "default",
        indicatorClass: "untested",
        indicatorLabel: "Setup needed",
        summary: "Choose a folder, bucket target, and saved credential, then run Connect and sync.",
      };
  }
}

export function formatByteCount(value: number): string {
  if (value < 1024) return `${value} B`;

  const units = ["KB", "MB", "GB", "TB"];
  let size = value / 1024;
  let unitIndex = 0;

  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }

  return `${size.toFixed(size >= 10 ? 0 : 1)} ${units[unitIndex]}`;
}

export function formatTimestamp(value: string | null): string {
  if (!value) return "Never";
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return value;
  return parsed.toLocaleString();
}
