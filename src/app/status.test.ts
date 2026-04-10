import { describe, expect, it } from "vitest";
import { describeSyncStatus, formatByteCount, formatTimestamp, getSyncOverviewStats } from "./status";

describe("status presentation", () => {
  it("maps polling state to connected presentation", () => {
    const view = describeSyncStatus({
      phase: "polling",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: null,
      currentFolder: "C:/sync",
      currentBucket: "demo",
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 0,
      indexedFileCount: 12,
      indexedDirectoryCount: 3,
      indexedTotalBytes: 1024,
        remoteObjectCount: 8,
        remoteTotalBytes: 2048,
        comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 12,
        remoteObjectCount: 8,
        exactMatchCount: 7,
        localOnlyCount: 5,
          remoteOnlyCount: 1,
          sizeMismatchCount: 0,
        },
        plan: {
          lastPlannedAt: "2026-04-03T00:00:00.000Z",
          observedPathCount: 13,
          uploadCount: 5,
          downloadCount: 1,
          conflictCount: 0,
          noopCount: 7,
          pendingOperationCount: 6,
          credentialsAvailable: true,
        },
      });

    expect(view.badgeTone).toBe("success");
    expect(view.indicatorClass).toBe("connected");
    expect(view.summary).toContain("60s");
    expect(view.summary).toContain("Connected");
    expect(view.summary).toContain("checking the bucket");
  });

  it("describes idle state as ready to review", () => {
    const view = describeSyncStatus({
      phase: "idle",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: null,
      currentFolder: "C:/sync",
      currentBucket: "demo",
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 3,
      indexedFileCount: 12,
      indexedDirectoryCount: 3,
      indexedTotalBytes: 1024,
      remoteObjectCount: 8,
      remoteTotalBytes: 2048,
      comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 12,
        remoteObjectCount: 8,
        exactMatchCount: 7,
        localOnlyCount: 5,
        remoteOnlyCount: 1,
        sizeMismatchCount: 0,
      },
      plan: {
        lastPlannedAt: "2026-04-03T00:00:00.000Z",
        observedPathCount: 13,
        uploadCount: 5,
        downloadCount: 1,
        conflictCount: 0,
        noopCount: 7,
        pendingOperationCount: 6,
        credentialsAvailable: true,
      },
    });

    expect(view.summary).toContain("Connected");
    expect(view.summary).toContain("latest sync status");
  });

  it("derives overview stats from comparison counts", () => {
    const stats = getSyncOverviewStats({
      phase: "idle",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: null,
      currentFolder: "C:/sync",
      currentBucket: "demo",
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 3,
      indexedFileCount: 12,
      indexedDirectoryCount: 3,
      indexedTotalBytes: 1024,
      remoteObjectCount: 8,
      remoteTotalBytes: 2048,
      comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 12,
        remoteObjectCount: 8,
        exactMatchCount: 7,
        localOnlyCount: 3,
        remoteOnlyCount: 1,
        sizeMismatchCount: 2,
      },
      plan: {
        lastPlannedAt: "2026-04-03T00:00:00.000Z",
        observedPathCount: 13,
        uploadCount: 5,
        downloadCount: 1,
        conflictCount: 0,
        noopCount: 7,
        pendingOperationCount: 6,
        credentialsAvailable: true,
      },
    });

    expect(stats).toEqual({
      localFiles: 12,
      remoteFiles: 8,
      inSync: 7,
      notInSync: 6,
    });
  });

  it("surfaces native error messages", () => {
    const view = describeSyncStatus({
      phase: "error",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: "Authentication failed",
      currentFolder: null,
      currentBucket: null,
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 0,
      indexedFileCount: 0,
      indexedDirectoryCount: 0,
      indexedTotalBytes: 0,
        remoteObjectCount: 0,
        remoteTotalBytes: 0,
        comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 0,
        remoteObjectCount: 0,
        exactMatchCount: 0,
        localOnlyCount: 0,
          remoteOnlyCount: 0,
          sizeMismatchCount: 0,
        },
        plan: {
          lastPlannedAt: null,
          observedPathCount: 0,
          uploadCount: 0,
          downloadCount: 0,
          conflictCount: 0,
          noopCount: 0,
          pendingOperationCount: 0,
          credentialsAvailable: false,
        },
      });

    expect(view.badgeTone).toBe("error");
    expect(view.summary).toBe("Authentication failed");
  });

  it("falls back to actionable copy for empty error messages", () => {
    const view = describeSyncStatus({
      phase: "error",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: "   ",
      currentFolder: null,
      currentBucket: null,
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 0,
      indexedFileCount: 0,
      indexedDirectoryCount: 0,
      indexedTotalBytes: 0,
      remoteObjectCount: 0,
      remoteTotalBytes: 0,
      comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 0,
        remoteObjectCount: 0,
        exactMatchCount: 0,
        localOnlyCount: 0,
        remoteOnlyCount: 0,
        sizeMismatchCount: 0,
      },
      plan: {
        lastPlannedAt: null,
        observedPathCount: 0,
        uploadCount: 0,
        downloadCount: 0,
        conflictCount: 0,
        noopCount: 0,
        pendingOperationCount: 0,
        credentialsAvailable: false,
      },
    });

    expect(view.indicatorLabel).toBe("Sync failed");
    expect(view.summary).toBe("Sync failed. Open Activity for details.");
  });

  it("falls back to actionable copy for generic error messages", () => {
    const view = describeSyncStatus({
      phase: "error",
      lastSyncAt: null,
      lastRescanAt: null,
      lastRemoteRefreshAt: null,
      lastError: "Error",
      currentFolder: null,
      currentBucket: null,
      currentPrefix: null,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      pendingOperations: 0,
      indexedFileCount: 0,
      indexedDirectoryCount: 0,
      indexedTotalBytes: 0,
      remoteObjectCount: 0,
      remoteTotalBytes: 0,
      comparison: {
        comparedAt: "2026-04-03T00:00:00.000Z",
        localFileCount: 0,
        remoteObjectCount: 0,
        exactMatchCount: 0,
        localOnlyCount: 0,
        remoteOnlyCount: 0,
        sizeMismatchCount: 0,
      },
      plan: {
        lastPlannedAt: null,
        observedPathCount: 0,
        uploadCount: 0,
        downloadCount: 0,
        conflictCount: 0,
        noopCount: 0,
        pendingOperationCount: 0,
        credentialsAvailable: false,
      },
    });

    expect(view.indicatorLabel).toBe("Sync failed");
    expect(view.summary).toBe("Sync failed. Open Activity for details.");
  });

  it("formats null timestamps as Never", () => {
    expect(formatTimestamp(null)).toBe("Never");
  });

  it("formats bytes for index summary display", () => {
    expect(formatByteCount(512)).toBe("512 B");
    expect(formatByteCount(2048)).toBe("2.0 KB");
  });
});
