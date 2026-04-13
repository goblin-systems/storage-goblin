import { DEFAULT_STORED_PROFILE, normalizeStoredProfile } from "./profile";
import { loadStoredProfileFromBrowserStorage, saveStoredProfileToBrowserStorage } from "./persistence";
import type { FileEntry } from "./file-tree";
import type {
  ActivityDebugLogState,
  BinEntryMutationSummary,
  BinEntryRequest,
  CredentialDraft,
  CredentialSummary,
  CredentialTestRequest,
  CredentialTestResult,
  ConnectionValidationResult,
  DeleteCredentialResult,
  ConflictResolutionDetails,
  InventoryComparisonSummary,
  NativeActivityEvent,
  StoredStorageProfile,
  StorageProfileDraft,
  SyncLocation,
  SyncLocationDraft,
  SyncPhase,
  SyncStatus,
} from "./types";

declare global {
  interface Window {
    __TAURI_INTERNALS__?: unknown;
  }
}

type StatusListener = (status: SyncStatus) => void;
type ActivityListener = (event: NativeActivityEvent) => void;

const browserListeners = new Set<StatusListener>();
const browserActivityListeners = new Set<ActivityListener>();

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && typeof window.__TAURI_INTERNALS__ !== "undefined";
}

function nowIsoString(): string {
  return new Date().toISOString();
}

function serializeSyncLocationDraft(draft: SyncLocationDraft): Omit<SyncLocationDraft, "id"> & { id: string | null } {
  return {
    id: draft.id,
    label: draft.label,
    localFolder: draft.localFolder,
    region: draft.region,
    bucket: draft.bucket,
    credentialProfileId: draft.credentialProfileId,
    objectVersioningEnabled: draft.objectVersioningEnabled,
    enabled: draft.enabled,
    remotePollingEnabled: draft.remotePollingEnabled,
    pollIntervalSeconds: draft.pollIntervalSeconds,
    conflictStrategy: draft.conflictStrategy,
    remoteBin: draft.remoteBin,
  };
}

function mockValidateConnection(profile: StorageProfileDraft): ConnectionValidationResult {
  const ok = Boolean(
    profile.localFolder
      && profile.bucket
      && (profile.credentialProfileId || profile.selectedCredentialAvailable),
  );
  return {
    ok,
    checkedAt: nowIsoString(),
    message: ok
      ? `Stub validation succeeded for ${profile.bucket}.`
      : "Stub validation requires folder, bucket, and a selected saved credential.",
  };
}

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

function createBrowserStatus(profile: StoredStorageProfile = DEFAULT_STORED_PROFILE): SyncStatus {
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

function phaseAfterBrowserSave(profile: StoredStorageProfile, previousPhase: SyncPhase): SyncPhase {
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
    default:
      return "idle";
  }
}

function applyBrowserProfileSave(profile: StoredStorageProfile): StoredStorageProfile {
  const stored = saveStoredProfileToBrowserStorage(profile);
  browserStatus = {
    ...createBrowserStatus(stored),
    lastSyncAt: browserStatus.lastSyncAt,
    lastRescanAt: browserStatus.lastRescanAt,
    lastRemoteRefreshAt: browserStatus.lastRemoteRefreshAt,
    lastError: browserStatus.lastError,
    phase: phaseAfterBrowserSave(stored, browserStatus.phase),
  };
  emitBrowserStatus(browserStatus);
  return stored;
}

let browserStatus = createBrowserStatus(loadStoredProfileFromBrowserStorage());

function emitBrowserStatus(status: SyncStatus) {
  browserStatus = { ...status };
  for (const listener of browserListeners) {
    listener({ ...browserStatus });
  }
}

async function invokeCommand<T>(command: string, args?: Record<string, unknown>): Promise<T> {
  const core = await import("@tauri-apps/api/core");
  return core.invoke<T>(command, args);
}

async function invokeProfileCommand(command: string, args?: Record<string, unknown>): Promise<StoredStorageProfile> {
  return normalizeStoredProfile(await invokeCommand<StoredStorageProfile>(command, args));
}

export interface StorageGoblinClient {
  readonly supportsNativeProfilePersistence: boolean;
  chooseLocalFolder(): Promise<string | null>;
  connectAndSync(profile: StorageProfileDraft): Promise<SyncStatus>;
  validateS3Connection(profile: StorageProfileDraft): Promise<ConnectionValidationResult>;
  listCredentials(): Promise<CredentialSummary[]>;
  createCredential(draft: CredentialDraft): Promise<CredentialSummary>;
  testCredential(request: CredentialTestRequest): Promise<CredentialTestResult>;
  deleteCredential(credentialId: string): Promise<DeleteCredentialResult>;
  loadProfile(): Promise<StoredStorageProfile>;
  saveProfile(profile: StorageProfileDraft): Promise<StoredStorageProfile>;
  saveProfileSettings(profile: StoredStorageProfile): Promise<StoredStorageProfile>;
  getSyncStatus(): Promise<SyncStatus>;
  startSync(): Promise<SyncStatus>;
  pauseSync(): Promise<SyncStatus>;
  runFullRescan(): Promise<SyncStatus>;
  refreshRemoteInventory(profile: StorageProfileDraft): Promise<SyncStatus>;
  buildSyncPlan(): Promise<SyncStatus>;
  executePlannedUploads(): Promise<SyncStatus>;
  listenSyncStatus(listener: StatusListener): Promise<() => void>;
  listenNativeActivity(listener: ActivityListener): Promise<() => void>;
  getActivityDebugLogState(): Promise<ActivityDebugLogState>;
  openActivityDebugLogFolder(): Promise<void>;
  listSyncLocations(): Promise<SyncLocation[]>;
  listFileEntries(locationId: string): Promise<FileEntry[]>;
  listBinEntries(locationId: string): Promise<FileEntry[]>;
  revealTreeEntry(locationId: string, path: string): Promise<void>;
  toggleLocalCopy(locationId: string, paths: string[], keep: boolean): Promise<void>;
  deleteFile(locationId: string, path: string): Promise<void>;
  deleteFolder(locationId: string, path: string): Promise<void>;
  restoreBinEntry(locationId: string, binKey: string): Promise<void>;
  restoreBinEntries(locationId: string, entries: BinEntryRequest[]): Promise<BinEntryMutationSummary>;
  purgeBinEntries(locationId: string, entries: BinEntryRequest[]): Promise<BinEntryMutationSummary>;
  addSyncLocation(draft: SyncLocationDraft): Promise<StoredStorageProfile>;
  updateSyncLocation(draft: SyncLocationDraft): Promise<StoredStorageProfile>;
  removeSyncLocation(locationId: string): Promise<StoredStorageProfile>;
  changeStorageClass(locationId: string, path: string, storageClass: string): Promise<void>;
  prepareConflictComparison(locationId: string, path: string): Promise<ConflictResolutionDetails>;
  openPath(path: string): Promise<void>;
  resolveConflict(locationId: string, path: string, resolution: "keep-local" | "keep-remote"): Promise<void>;
}

export function createStorageGoblinClient(): StorageGoblinClient {
  const native = isTauriRuntime();

  return {
    supportsNativeProfilePersistence: native,
    async chooseLocalFolder() {
      if (!native) return null;
      const dialog = await import("@tauri-apps/plugin-dialog");
      const result = await dialog.open({ directory: true, multiple: false });
      return typeof result === "string" ? result : null;
    },
    async connectAndSync(profile) {
      if (!native) {
        const stored = saveStoredProfileToBrowserStorage(profile);
        browserStatus = {
          ...createBrowserStatus(stored),
          lastError: "Browser preview saved your setup locally. Connect and sync runs only in the desktop app.",
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("connect_and_sync", { profile });
    },
    async validateS3Connection(profile) {
      if (!native) return mockValidateConnection(profile);
      return invokeCommand<ConnectionValidationResult>("validate_s3_connection", { input: profile });
    },
    async listCredentials() {
      if (!native) return [];
      return invokeCommand<CredentialSummary[]>("list_credentials_command");
    },
    async createCredential(draft) {
      if (!native) {
        return {
          id: `browser-${Date.now()}`,
          name: draft.name.trim(),
          ready: false,
          validationStatus: "untested",
          lastTestedAt: null,
          lastTestMessage: null,
        };
      }
      return invokeCommand<CredentialSummary>("create_credential_command", { draft });
    },
    async testCredential(request) {
      if (!native) {
        return {
          credential: {
            id: request.credentialId,
            name: "Browser preview credential",
            ready: false,
            validationStatus: "untested",
            lastTestedAt: null,
            lastTestMessage: null,
          },
          ok: false,
          checkedAt: nowIsoString(),
          message: "Credential testing is only available in the desktop app.",
          bucketCount: 0,
          buckets: [],
          permissions: null,
        };
      }
      return invokeCommand<CredentialTestResult>("test_credential_command", { request });
    },
    async deleteCredential(credentialId) {
      if (!native) {
        return {
          deleted: false,
          profile: loadStoredProfileFromBrowserStorage(),
        };
      }
      return invokeCommand<DeleteCredentialResult>("delete_credential_command", { credentialId });
    },
    async loadProfile() {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("load_profile");
    },
    async saveProfile(profile) {
      if (!native) return applyBrowserProfileSave(profile);
      return invokeProfileCommand("save_profile", { profile });
    },
    async saveProfileSettings(profile) {
      if (!native) return applyBrowserProfileSave(profile);
      return invokeProfileCommand("save_profile_settings", { profile });
    },
    async getSyncStatus() {
      if (!native) {
        browserStatus = createBrowserStatus(loadStoredProfileFromBrowserStorage());
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("get_sync_status");
    },
    async startSync() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        const configured = Boolean(profile.localFolder && profile.bucket);
        browserStatus = {
          ...createBrowserStatus(profile),
          phase: configured ? (profile.remotePollingEnabled ? "polling" : "syncing") : "unconfigured",
          lastSyncAt: configured ? nowIsoString() : null,
          lastError: configured ? null : "Save setup details before starting sync.",
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("start_sync");
    },
    async pauseSync() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        browserStatus = {
          ...createBrowserStatus(profile),
          phase: profile.localFolder && profile.bucket ? "paused" : "unconfigured",
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("pause_sync");
    },
    async runFullRescan() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        browserStatus = {
          ...createBrowserStatus(profile),
          phase: profile.localFolder && profile.bucket ? (profile.remotePollingEnabled ? "polling" : "idle") : "unconfigured",
          lastRescanAt: nowIsoString(),
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("run_full_rescan");
    },
    async refreshRemoteInventory(profile) {
      if (!native) {
        const stored = loadStoredProfileFromBrowserStorage();
        browserStatus = {
          ...createBrowserStatus(stored),
          phase: stored.localFolder && stored.bucket ? (stored.remotePollingEnabled ? "polling" : "idle") : "unconfigured",
          lastError: null,
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("refresh_remote_inventory", { input: profile });
    },
    async buildSyncPlan() {
      if (!native) {
        browserStatus = {
          ...browserStatus,
          lastError: "Durable sync planning is only available in the native desktop runtime.",
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("build_sync_plan");
    },
    async executePlannedUploads() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        browserStatus = {
          ...createBrowserStatus(profile),
          lastError: "Manual upload execution is only available in the native desktop runtime. Browser fallback did not run uploads.",
        };
        emitBrowserStatus(browserStatus);
        return { ...browserStatus };
      }
      return invokeCommand<SyncStatus>("execute_planned_uploads");
    },
    async listenSyncStatus(listener) {
      if (!native) {
        browserListeners.add(listener);
        return () => {
          browserListeners.delete(listener);
        };
      }

      const event = await import("@tauri-apps/api/event");
      const unlisten = await event.listen<SyncStatus>("storage://sync-status-changed", (payload) => {
        listener(payload.payload);
      });

      return () => {
        unlisten();
      };
    },
    async listenNativeActivity(listener) {
      if (!native) {
        browserActivityListeners.add(listener);
        return () => {
          browserActivityListeners.delete(listener);
        };
      }

      const event = await import("@tauri-apps/api/event");
      const unlisten = await event.listen<NativeActivityEvent>("storage://activity", (payload) => {
        listener(payload.payload);
      });

      return () => {
        unlisten();
      };
    },
    async getActivityDebugLogState() {
      if (!native) {
        return {
          enabled: false,
          logFilePath: null,
          logDirectoryPath: null,
        };
      }
      return invokeCommand<ActivityDebugLogState>("get_activity_debug_log_state");
    },
    async openActivityDebugLogFolder() {
      if (!native) return;
      await invokeCommand<void>("open_activity_debug_log_folder");
    },
    async listSyncLocations() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        return profile.syncLocations ?? [];
      }
      return invokeCommand<SyncLocation[]>("list_sync_locations");
    },
    async listFileEntries(locationId) {
      if (!native) return [];
      return invokeCommand<FileEntry[]>("list_file_entries", { locationId });
    },
    async listBinEntries(locationId) {
      if (!native) return [];
      return invokeCommand<FileEntry[]>("list_bin_entries", { locationId });
    },
    async revealTreeEntry(locationId, path) {
      if (!native) {
        throw new Error("Reveal in file manager is only available in the desktop app.");
      }
      await invokeCommand<void>("reveal_tree_entry", { locationId, path });
    },
    async toggleLocalCopy(locationId, paths, keep) {
      if (!native) return;
      await invokeCommand<void>("toggle_local_copy", { locationId, paths, keep });
    },
    async deleteFile(locationId, path) {
      if (!native) return;
      await invokeCommand<void>("delete_file", { locationId, path });
    },
    async deleteFolder(locationId, path) {
      if (!native) return;
      await invokeCommand<void>("delete_folder", { locationId, path });
    },
    async restoreBinEntry(locationId, binKey) {
      if (!native) return;
      await invokeCommand<void>("restore_bin_entry", { locationId, binKey });
    },
    async restoreBinEntries(locationId, entries) {
      if (!native) return { results: [] };
      return invokeCommand<BinEntryMutationSummary>("restore_bin_entries", { locationId, entries });
    },
    async purgeBinEntries(locationId, entries) {
      if (!native) return { results: [] };
      return invokeCommand<BinEntryMutationSummary>("purge_bin_entries", { locationId, entries });
    },
    async addSyncLocation(draft) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("add_sync_location", { draft: serializeSyncLocationDraft(draft) });
    },
    async updateSyncLocation(draft) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("update_sync_location", { draft: serializeSyncLocationDraft(draft) });
    },
    async removeSyncLocation(locationId) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("remove_sync_location", { locationId });
    },
    async changeStorageClass(locationId, path, storageClass) {
      if (!native) return;
      await invokeCommand<void>("change_storage_class", { locationId, path, storageClass });
    },
    async prepareConflictComparison(locationId, path) {
      if (!native) {
        throw new Error("Conflict compare is only available in the desktop app.");
      }
      return invokeCommand<ConflictResolutionDetails>("prepare_conflict_comparison", { locationId, path });
    },
    async openPath(path) {
      if (!native) {
        throw new Error("Opening local files is only available in the desktop app.");
      }
      await invokeCommand<void>("open_path", { path });
    },
    async resolveConflict(locationId, path, resolution) {
      if (!native) {
        throw new Error("Conflict resolution is only available in the desktop app.");
      }
      await invokeCommand<void>("resolve_conflict", { locationId, path, resolution });
    },
  };
}
