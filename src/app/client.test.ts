import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_STORED_PROFILE } from "./profile";
import { LOCAL_PROFILE_STORAGE_KEY } from "./persistence";
import { createStorageGoblinClient } from "./client";
import type { FileEntry } from "./file-tree";
import type { StoredStorageProfile, SyncLocationDraft } from "./types";

const invokeMock = vi.fn();

vi.mock("@tauri-apps/api/core", () => ({
  invoke: invokeMock,
}));

function fileEntry(overrides: Partial<FileEntry> & Pick<FileEntry, "path">): FileEntry {
  return {
    kind: "file",
    status: "synced",
    hasLocalCopy: true,
    ...overrides,
  };
}

function legacyNativeProfile(): Partial<StoredStorageProfile> & { syncPairs: NonNullable<StoredStorageProfile["syncLocations"]> } {
  return {
    localFolder: "C:/sync",
    region: "us-east-1",
    bucket: "demo-bucket",
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    conflictStrategy: "preserve-both",
    activityDebugModeEnabled: false,
    credentialProfileId: null,
    selectedCredential: null,
    selectedCredentialAvailable: false,
    credentialsStoredSecurely: false,
    syncPairs: [
      {
        id: "loc-1",
        label: "Docs",
        localFolder: "C:/sync/docs",
        region: "us-east-1",
        bucket: "demo-bucket",
        credentialProfileId: null,
        objectVersioningEnabled: false,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        remoteBin: {
          enabled: true,
          retentionDays: 7,
        },
      },
    ],
  };
}

describe("storage goblin client", () => {
  const tauriWindow = window as Window & { __TAURI_INTERNALS__?: unknown };

  beforeEach(() => {
    window.localStorage.clear();
    delete tauriWindow.__TAURI_INTERNALS__;
    invokeMock.mockReset();
  });

  afterEach(() => {
    window.localStorage.clear();
    delete tauriWindow.__TAURI_INTERNALS__;
    invokeMock.mockReset();
  });

  it("saves browser connect requests locally without pretending sync ran", async () => {
    const client = createStorageGoblinClient();
    const status = await client.connectAndSync({
      ...DEFAULT_STORED_PROFILE,
      localFolder: "C:/sync",
      bucket: "demo-bucket",
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: true,
      credentialsStoredSecurely: true,
    });

    expect(JSON.parse(window.localStorage.getItem(LOCAL_PROFILE_STORAGE_KEY) ?? "{}")).toMatchObject({
      localFolder: "C:/sync",
      bucket: "demo-bucket",
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
    expect(status.phase).toBe("idle");
    expect(status.lastSyncAt).toBeNull();
    expect(status.lastError).toBe("Browser preview saved your setup locally. Connect and sync runs only in the desktop app.");
  });

  it("keeps browser upload execution as a safe no-op with a clear error", async () => {
    window.localStorage.setItem(LOCAL_PROFILE_STORAGE_KEY, JSON.stringify({
      ...DEFAULT_STORED_PROFILE,
      localFolder: "C:/sync",
      bucket: "demo-bucket",
    }));

    const client = createStorageGoblinClient();
    const status = await client.executePlannedUploads();

    expect(status.phase).toBe("idle");
    expect(status.lastSyncAt).toBeNull();
    expect(status.lastError).toBe(
      "Manual upload execution is only available in the native desktop runtime. Browser fallback did not run uploads.",
    );
  });

  it("saves browser settings without needing credentials", async () => {
    const client = createStorageGoblinClient();
    const stored = await client.saveProfileSettings({
      ...DEFAULT_STORED_PROFILE,
      localFolder: "C:/sync",
      bucket: "demo-bucket",
      remotePollingEnabled: false,
      pollIntervalSeconds: 90,
      activityDebugModeEnabled: true,
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: true,
      credentialsStoredSecurely: true,
    });

    expect(stored).toMatchObject({
      localFolder: "C:/sync",
      bucket: "demo-bucket",
      remotePollingEnabled: false,
      pollIntervalSeconds: 90,
      activityDebugModeEnabled: true,
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
    expect(JSON.parse(window.localStorage.getItem(LOCAL_PROFILE_STORAGE_KEY) ?? "{}")).toMatchObject({
      activityDebugModeEnabled: true,
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
  });

  it("reports browser debug log state as unavailable", async () => {
    const client = createStorageGoblinClient();

    await expect(client.getActivityDebugLogState()).resolves.toEqual({
      enabled: false,
      logFilePath: null,
      logDirectoryPath: null,
    });
  });

  it("reports browser credential testing as desktop-only", async () => {
    const client = createStorageGoblinClient();

    await expect(client.testCredential({
      credentialId: "cred-1",
      context: {
        region: "",
        bucket: "demo-bucket",
      },
    })).resolves.toMatchObject({
      ok: false,
      message: "Credential testing is only available in the desktop app.",
      credential: {
        id: "cred-1",
        validationStatus: "untested",
      },
      permissions: null,
    });
  });

  it("allows browser native activity listeners to unsubscribe safely", async () => {
    const client = createStorageGoblinClient();
    const listener = () => undefined;

    const unlisten = await client.listenNativeActivity(listener);

    expect(typeof unlisten).toBe("function");
    unlisten();
  });

  it("returns file entries with explicit kind typing", async () => {
    const entry: FileEntry = fileEntry({ path: "docs/readme.txt" });

    expect(entry.kind).toBe("file");
  });

  it("uses sync location command names in the native runtime", async () => {
    tauriWindow.__TAURI_INTERNALS__ = {};
    invokeMock.mockResolvedValue(null);
    const client = createStorageGoblinClient();
    const draft: SyncLocationDraft = {
      id: "loc-1",
      label: "Docs",
      localFolder: "C:/sync/docs",
      region: "us-east-1",
      bucket: "demo",
      credentialProfileId: "cred-1",
      objectVersioningEnabled: false,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
    };

    await client.listSyncLocations();
    await client.addSyncLocation(draft);
    await client.updateSyncLocation(draft);
    await client.removeSyncLocation("loc-1");
    await client.listBinEntries("loc-1");
    await client.revealTreeEntry("loc-1", "docs/readme.txt");
    await client.deleteFolder("loc-1", "docs");
    await client.restoreBinEntry("loc-1", "opaque-bin-key");
    await client.restoreBinEntries("loc-1", [{ path: "docs", kind: "directory", binKey: null }]);
    await client.purgeBinEntries("loc-1", [{ path: "docs", kind: "directory", binKey: null }]);
    await client.prepareConflictComparison("loc-1", "docs/readme.txt");
    await client.openPath("C:/sync/docs/readme.txt");
    await client.resolveConflict("loc-1", "docs/readme.txt", "keep-local");

    expect(invokeMock).toHaveBeenNthCalledWith(1, "list_sync_locations", undefined);
    expect(invokeMock).toHaveBeenNthCalledWith(2, "add_sync_location", { draft });
    expect(invokeMock).toHaveBeenNthCalledWith(3, "update_sync_location", { draft });
    expect(invokeMock).toHaveBeenNthCalledWith(4, "remove_sync_location", { locationId: "loc-1" });
    expect(invokeMock).toHaveBeenNthCalledWith(5, "list_bin_entries", { locationId: "loc-1" });
    expect(invokeMock).toHaveBeenNthCalledWith(6, "reveal_tree_entry", { locationId: "loc-1", path: "docs/readme.txt" });
    expect(invokeMock).toHaveBeenNthCalledWith(7, "delete_folder", { locationId: "loc-1", path: "docs" });
    expect(invokeMock).toHaveBeenNthCalledWith(8, "restore_bin_entry", { locationId: "loc-1", binKey: "opaque-bin-key" });
    expect(invokeMock).toHaveBeenNthCalledWith(9, "restore_bin_entries", { locationId: "loc-1", entries: [{ path: "docs", kind: "directory", binKey: null }] });
    expect(invokeMock).toHaveBeenNthCalledWith(10, "purge_bin_entries", { locationId: "loc-1", entries: [{ path: "docs", kind: "directory", binKey: null }] });
    expect(invokeMock).toHaveBeenNthCalledWith(11, "prepare_conflict_comparison", { locationId: "loc-1", path: "docs/readme.txt" });
    expect(invokeMock).toHaveBeenNthCalledWith(12, "open_path", { path: "C:/sync/docs/readme.txt" });
    expect(invokeMock).toHaveBeenNthCalledWith(13, "resolve_conflict", { locationId: "loc-1", path: "docs/readme.txt", resolution: "keep-local" });
  });

  it("types native conflict compare payloads with inline compare fields", async () => {
    tauriWindow.__TAURI_INTERNALS__ = {};
    invokeMock.mockResolvedValueOnce({
      locationId: "loc-1",
      path: "docs/readme.txt",
      mode: "text",
      localPath: "C:/sync/docs/readme.txt",
      remoteTempPath: "C:/temp/docs-readme.txt",
      localText: "local",
      remoteText: "remote",
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: null,
    });

    const client = createStorageGoblinClient();
    await expect(client.prepareConflictComparison("loc-1", "docs/readme.txt")).resolves.toEqual({
      locationId: "loc-1",
      path: "docs/readme.txt",
      mode: "text",
      localPath: "C:/sync/docs/readme.txt",
      remoteTempPath: "C:/temp/docs-readme.txt",
      localText: "local",
      remoteText: "remote",
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: null,
    });
  });

  it("rejects browser reveal requests with a clear desktop-only error", async () => {
    const client = createStorageGoblinClient();

    await expect(client.revealTreeEntry("loc-1", "docs/readme.txt")).rejects.toThrow(
      "Reveal in file manager is only available in the desktop app.",
    );
  });

  it("strips prefix and endpointUrl from native add and update sync location payloads", async () => {
    tauriWindow.__TAURI_INTERNALS__ = {};
    invokeMock.mockResolvedValue(null);
    const client = createStorageGoblinClient();
    const draft = {
      id: "loc-1",
      label: "Docs",
      localFolder: "C:/sync/docs",
      endpointUrl: "https://s3.example.test",
      region: "us-east-1",
      bucket: "demo",
      prefix: "docs",
      credentialProfileId: "cred-1",
      objectVersioningEnabled: true,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
    } as unknown as SyncLocationDraft;

    await client.addSyncLocation(draft);
    await client.updateSyncLocation(draft);

    const addDraft = invokeMock.mock.calls[0]?.[1]?.draft as Record<string, unknown>;
    const updateDraft = invokeMock.mock.calls[1]?.[1]?.draft as Record<string, unknown>;

    expect(addDraft).not.toHaveProperty("endpointUrl");
    expect(addDraft).not.toHaveProperty("prefix");
    expect(addDraft).not.toHaveProperty("deleteSafetyHours");
    expect(addDraft).toMatchObject({ objectVersioningEnabled: true, remoteBin: { enabled: true, retentionDays: 7 } });
    expect(updateDraft).not.toHaveProperty("endpointUrl");
    expect(updateDraft).not.toHaveProperty("prefix");
    expect(updateDraft).not.toHaveProperty("deleteSafetyHours");
    expect(updateDraft).toMatchObject({ objectVersioningEnabled: true, remoteBin: { enabled: true, retentionDays: 7 } });
  });

  it("does not persist unsupported endpoint or prefix fields in browser storage", async () => {
    const client = createStorageGoblinClient();

    await client.saveProfileSettings({
      ...DEFAULT_STORED_PROFILE,
      localFolder: "C:/sync",
      region: "us-east-1",
      bucket: "demo-bucket",
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          objectVersioningEnabled: false,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "preserve-both",
          remoteBin: {
            enabled: true,
            retentionDays: 7,
          },
        },
      ],
    } as unknown as StoredStorageProfile);

    const stored = JSON.parse(window.localStorage.getItem(LOCAL_PROFILE_STORAGE_KEY) ?? "{}");

    expect(stored).not.toHaveProperty("endpointUrl");
    expect(stored).not.toHaveProperty("prefix");
    expect(stored.syncLocations[0]).not.toHaveProperty("endpointUrl");
    expect(stored.syncLocations[0]).not.toHaveProperty("prefix");
    expect(stored.syncLocations[0]).not.toHaveProperty("deleteSafetyHours");
    expect(stored.syncLocations[0]).toMatchObject({ remoteBin: { enabled: true, retentionDays: 7 } });
  });

  it("normalizes legacy native profile responses that use syncPairs", async () => {
    tauriWindow.__TAURI_INTERNALS__ = {};
    const client = createStorageGoblinClient();
    const legacyProfile = legacyNativeProfile();
    const expectedSyncLocations = legacyProfile.syncPairs;
    const draft: SyncLocationDraft = {
      id: "loc-1",
      label: "Docs",
      localFolder: "C:/sync/docs",
      region: "us-east-1",
      bucket: "demo-bucket",
      credentialProfileId: null,
      objectVersioningEnabled: false,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
    };

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.loadProfile()).resolves.toMatchObject({ syncLocations: expectedSyncLocations });

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.saveProfile({ ...DEFAULT_STORED_PROFILE, localFolder: "C:/sync", bucket: "demo-bucket" })).resolves.toMatchObject({ syncLocations: expectedSyncLocations });

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.saveProfileSettings({ ...DEFAULT_STORED_PROFILE, localFolder: "C:/sync", bucket: "demo-bucket" })).resolves.toMatchObject({ syncLocations: expectedSyncLocations });

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.addSyncLocation(draft)).resolves.toMatchObject({ syncLocations: expectedSyncLocations });

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.updateSyncLocation(draft)).resolves.toMatchObject({ syncLocations: expectedSyncLocations });

    invokeMock.mockResolvedValue({ ...legacyProfile });
    await expect(client.removeSyncLocation("loc-1")).resolves.toMatchObject({ syncLocations: expectedSyncLocations });
  });
});
