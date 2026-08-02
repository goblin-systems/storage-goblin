/**
 * The browser-preview status model (backlog phase 4.1).
 *
 * Without a Tauri backend there is no sync engine, so the preview synthesizes a
 * plausible `SyncStatus` from whatever profile is in local storage. Split out of
 * `client.ts` because it is a *simulation*, not an IPC concern — and keeping it
 * beside the real calls made it easy to mistake one for the other.
 */

import { DEFAULT_STORED_PROFILE } from "./profile";
import type {
  InventoryComparisonSummary,
  StoredStorageProfile,
  SyncPhase,
  SyncStatus,
} from "./types";

function createEmptyComparison(): InventoryComparisonSummary {
  return {
    comparedAt: "",
    localFileCount: 0,
    remoteObjectCount: 0,
    exactMatchCount: 0,
    localOnlyCount: 0,
    remoteOnlyCount: 0,
    sizeMismatchCount: 0,
  };
}

function createOverview(comparison: InventoryComparisonSummary, pendingOperationCount: number) {
  return {
    localFiles: comparison.localFileCount,
    remoteFiles: comparison.remoteObjectCount,
    inSync: comparison.exactMatchCount,
    notInSync: pendingOperationCount,
  };
}

export function createBrowserStatus(
  profile: StoredStorageProfile = DEFAULT_STORED_PROFILE,
): SyncStatus {
  const comparison = createEmptyComparison();
  const configured = Boolean(profile.localFolder && profile.bucket);
  const pendingOperationCount = 0;
  return {
    phase: configured ? "idle" : "unconfigured",
    lastSyncAt: null,
    lastRescanAt: null,
    lastRemoteRefreshAt: null,
    lastError: null,
    currentFolder: profile.localFolder || null,
    currentBucket: profile.bucket || null,
    currentPrefix: null,
    remotePollingEnabled: profile.remotePollingEnabled,
    pollIntervalSeconds: profile.pollIntervalSeconds,
    pendingOperations: 0,
    indexedFileCount: 0,
    indexedDirectoryCount: 0,
    indexedTotalBytes: 0,
    remoteObjectCount: 0,
    remoteTotalBytes: 0,
    comparison,
    overview: createOverview(comparison, pendingOperationCount),
    plan: {
      lastPlannedAt: null,
      observedPathCount: 0,
      uploadCount: 0,
      downloadCount: 0,
      conflictCount: 0,
      noopCount: 0,
      pendingOperationCount,
      credentialsAvailable: profile.selectedCredentialAvailable,
    },
  };
}

export function phaseAfterBrowserSave(
  profile: StoredStorageProfile,
  previousPhase: SyncPhase,
): SyncPhase {
  if (!profile.localFolder || !profile.bucket) {
    return "unconfigured";
  }

  switch (previousPhase) {
    case "paused":
      return "paused";
    case "polling":
      return profile.remotePollingEnabled ? "polling" : "idle";
    case "syncing":
      return profile.remotePollingEnabled ? "idle" : "syncing";
    case "error":
    case "unconfigured":
    case "idle":
      return "idle";
  }
}
