import { createBrowserStatus, phaseAfterBrowserSave } from "./browser-status";
import {
  EVENT_CHANNELS,
  loadEventBackend,
  onTransferProgress,
  subscribe,
  type TransferProgressEvent,
} from "../ipc/events";
import { normalizeStoredProfile } from "./profile";
import {
  loadStoredProfileFromBrowserStorage,
  saveStoredProfileToBrowserStorage,
} from "./persistence";
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
  FileVersionEntry,
  VersionComparisonDetails,
  NativeActivityEvent,
  ProviderDefinition,
  StoredStorageProfile,
  StorageProfileDraft,
  SyncLocation,
  SyncLocationDraft,
  SyncStatus,
  VersionCountEntry,
} from "./types";
import {
  normalizeCredentialSummaryRecord,
  normalizeProvider,
  normalizeProviderDefinition,
} from "./types";

declare global {
  interface Window {
    __TAURI_INTERNALS__?: unknown;
  }
}

type StatusListener = (status: SyncStatus) => void;
type ActivityListener = (event: NativeActivityEvent) => void;
type TransferProgressListener = (event: TransferProgressEvent) => void;

const browserListeners = new Set<StatusListener>();
const browserActivityListeners = new Set<ActivityListener>();

function isTauriRuntime(): boolean {
  return typeof window !== "undefined" && typeof window.__TAURI_INTERNALS__ !== "undefined";
}

function nowIsoString(): string {
  return new Date().toISOString();
}

function serializeSyncLocationDraft(
  draft: SyncLocationDraft,
): Omit<SyncLocationDraft, "id"> & { id: string | null } {
  return {
    id: draft.id,
    label: draft.label,
    provider: draft.provider,
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
    profile.localFolder &&
    profile.bucket &&
    (Boolean(profile.credentialProfileId) || profile.selectedCredentialAvailable),
  );
  return {
    ok,
    checkedAt: nowIsoString(),
    message: ok
      ? `Stub validation succeeded for ${profile.bucket}.`
      : "Stub validation requires folder, bucket, and a selected saved credential.",
  };
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

async function invokeVoidCommand(command: string, args?: Record<string, unknown>): Promise<void> {
  await invokeCommand<unknown>(command, args);
}

async function invokeProfileCommand(
  command: string,
  args?: Record<string, unknown>,
): Promise<StoredStorageProfile> {
  return normalizeStoredProfile(await invokeCommand<StoredStorageProfile>(command, args));
}

export interface StorageGoblinClient {
  readonly supportsNativeProfilePersistence: boolean;
  chooseLocalFolder(): Promise<string | null>;
  validateConnection(profile: StorageProfileDraft): Promise<ConnectionValidationResult>;
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
  listenSyncStatus(listener: StatusListener): Promise<() => void>;
  listenNativeActivity(listener: ActivityListener): Promise<() => void>;
  /** Byte-level transfer progress (backlog phase 2.1). */
  listenTransferProgress(listener: TransferProgressListener): Promise<() => void>;
  getActivityDebugLogState(): Promise<ActivityDebugLogState>;
  listProviderCapabilities(): Promise<ProviderDefinition[]>;
  openActivityDebugLogFolder(): Promise<void>;
  listSyncLocations(): Promise<SyncLocation[]>;
  listFileEntries(locationId: string): Promise<FileEntry[]>;
  listBinEntries(locationId: string): Promise<FileEntry[]>;
  revealTreeEntry(locationId: string, path: string): Promise<void>;
  toggleLocalCopy(locationId: string, paths: string[], keep: boolean): Promise<void>;
  deleteFile(locationId: string, path: string): Promise<void>;
  deleteFolder(locationId: string, path: string): Promise<void>;
  restoreBinEntry(locationId: string, binKey: string): Promise<void>;
  restoreBinEntries(
    locationId: string,
    entries: BinEntryRequest[],
  ): Promise<BinEntryMutationSummary>;
  purgeBinEntries(locationId: string, entries: BinEntryRequest[]): Promise<BinEntryMutationSummary>;
  addSyncLocation(draft: SyncLocationDraft): Promise<StoredStorageProfile>;
  updateSyncLocation(draft: SyncLocationDraft): Promise<StoredStorageProfile>;
  setSyncLocationVersioning(locationId: string, enabled: boolean): Promise<StoredStorageProfile>;
  removeSyncLocation(locationId: string): Promise<StoredStorageProfile>;
  changeStorageClass(locationId: string, path: string, storageClass: string): Promise<void>;
  listFileVersions(locationId: string, path: string): Promise<FileVersionEntry[]>;
  listVersionCounts(locationId: string): Promise<VersionCountEntry[]>;
  restoreFileVersion(locationId: string, path: string, versionId: string): Promise<void>;
  prepareVersionComparison(
    locationId: string,
    path: string,
    versionIdA: string,
    versionIdB: string,
  ): Promise<VersionComparisonDetails>;
  deleteFileVersion(locationId: string, path: string, versionId: string): Promise<void>;
  prepareConflictComparison(locationId: string, path: string): Promise<ConflictResolutionDetails>;
  openPath(path: string): Promise<void>;
  resolveConflict(
    locationId: string,
    path: string,
    resolution: "keep-local" | "keep-remote",
  ): Promise<void>;
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
    async validateConnection(profile) {
      if (!native) return mockValidateConnection(profile);
      return invokeCommand<ConnectionValidationResult>("validate_storage_connection", {
        input: profile,
      });
    },
    async listCredentials() {
      if (!native) return [];
      const credentials = await invokeCommand<unknown[]>("list_credentials_command");
      return credentials
        .map((credential) => normalizeCredentialSummaryRecord(credential))
        .filter((credential): credential is CredentialSummary => credential !== null);
    },
    async createCredential(draft) {
      if (!native) {
        return {
          id: `browser-${Date.now()}`,
          name: draft.name.trim(),
          provider: normalizeProvider(draft.provider),
          ready: false,
          validationStatus: "untested",
          lastTestedAt: null,
          lastTestMessage: null,
          summary: null,
        };
      }
      return (
        normalizeCredentialSummaryRecord(
          await invokeCommand<CredentialSummary>("create_credential_command", { draft }),
        ) ?? {
          id: "",
          name: draft.name.trim(),
          provider: normalizeProvider(draft.provider),
          ready: false,
          validationStatus: "untested",
          lastTestedAt: null,
          lastTestMessage: null,
          summary: null,
        }
      );
    },
    async testCredential(request) {
      if (!native) {
        return {
          credential: {
            id: request.credentialId,
            name: "Browser preview credential",
            provider: normalizeProvider(request.context.provider),
            ready: false,
            validationStatus: "untested",
            lastTestedAt: null,
            lastTestMessage: null,
            summary: null,
          },
          ok: false,
          checkedAt: nowIsoString(),
          message: "Credential testing is only available in the desktop app.",
          bucketCount: 0,
          buckets: [],
          permissions: null,
        };
      }
      const result = await invokeCommand<CredentialTestResult>("test_credential_command", {
        request,
      });
      return {
        ...result,
        credential: normalizeCredentialSummaryRecord(result.credential) ?? result.credential,
      };
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
          phase: configured
            ? profile.remotePollingEnabled
              ? "polling"
              : "syncing"
            : "unconfigured",
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
    // Both subscriptions go through ipc/events, which owns the channel names
    // and the dynamic import. The browser-preview fallback stays here because
    // it is a property of *this* client, not of the event layer.
    async listenSyncStatus(listener) {
      if (!native) {
        browserListeners.add(listener);
        return () => {
          browserListeners.delete(listener);
        };
      }

      return subscribe<SyncStatus>(await loadEventBackend(), EVENT_CHANNELS.syncStatus, listener);
    },
    async listenNativeActivity(listener) {
      if (!native) {
        browserActivityListeners.add(listener);
        return () => {
          browserActivityListeners.delete(listener);
        };
      }

      return subscribe<NativeActivityEvent>(
        await loadEventBackend(),
        EVENT_CHANNELS.activity,
        listener,
      );
    },
    async listenTransferProgress(listener) {
      // No browser-preview equivalent: nothing transfers there.
      if (!native) return () => undefined;
      return onTransferProgress(await loadEventBackend(), listener);
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
    async listProviderCapabilities() {
      if (!native) return [];
      const definitions = await invokeCommand<unknown[]>("list_provider_capabilities_command");
      return definitions
        .map((definition) => normalizeProviderDefinition(definition))
        .filter((definition): definition is ProviderDefinition => definition !== null);
    },
    async openActivityDebugLogFolder() {
      if (!native) return;
      await invokeVoidCommand("open_activity_debug_log_folder");
    },
    async listSyncLocations() {
      if (!native) {
        const profile = loadStoredProfileFromBrowserStorage();
        return profile.syncLocations;
      }
      const locations = await invokeCommand<SyncLocation[]>("list_sync_locations");
      if (!Array.isArray(locations)) {
        return [];
      }
      return locations.map((location) => ({
        ...location,
        provider: normalizeProvider(location.provider),
        providerDefinition:
          normalizeProviderDefinition(
            (location as unknown as Record<string, unknown>).providerDefinition,
          ) ??
          normalizeProviderDefinition(
            (location as unknown as Record<string, unknown>).provider_definition,
          ) ??
          null,
      }));
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
      await invokeVoidCommand("reveal_tree_entry", { locationId, path });
    },
    async toggleLocalCopy(locationId, paths, keep) {
      if (!native) return;
      await invokeVoidCommand("toggle_local_copy", { locationId, paths, keep });
    },
    async deleteFile(locationId, path) {
      if (!native) return;
      await invokeVoidCommand("delete_file", { locationId, path });
    },
    async deleteFolder(locationId, path) {
      if (!native) return;
      await invokeVoidCommand("delete_folder", { locationId, path });
    },
    async restoreBinEntry(locationId, binKey) {
      if (!native) return;
      await invokeVoidCommand("restore_bin_entry", { locationId, binKey });
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
      return invokeProfileCommand("add_sync_location", {
        draft: serializeSyncLocationDraft(draft),
      });
    },
    async updateSyncLocation(draft) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("update_sync_location", {
        draft: serializeSyncLocationDraft(draft),
      });
    },
    async setSyncLocationVersioning(locationId, enabled) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("set_sync_location_versioning", { locationId, enabled });
    },
    async removeSyncLocation(locationId) {
      if (!native) return loadStoredProfileFromBrowserStorage();
      return invokeProfileCommand("remove_sync_location", { locationId });
    },
    async changeStorageClass(locationId, path, storageClass) {
      if (!native) return;
      await invokeVoidCommand("change_storage_class", { locationId, path, storageClass });
    },
    async listFileVersions(locationId, path) {
      if (!native) return [];
      return invokeCommand<FileVersionEntry[]>("list_file_versions", { locationId, path });
    },
    async listVersionCounts(locationId) {
      if (!native) return [];
      return invokeCommand<VersionCountEntry[]>("list_version_counts", { locationId });
    },
    async restoreFileVersion(locationId, path, versionId) {
      if (!native) return;
      await invokeVoidCommand("restore_file_version", { locationId, path, versionId });
    },
    async prepareVersionComparison(locationId, path, versionIdA, versionIdB) {
      if (!native) throw new Error("Version comparison is only available in the desktop app.");
      return invokeCommand<VersionComparisonDetails>("prepare_version_comparison", {
        locationId,
        path,
        versionIdA,
        versionIdB,
      });
    },
    async deleteFileVersion(locationId, path, versionId) {
      if (!native) return;
      await invokeVoidCommand("delete_file_version", { locationId, path, versionId });
    },
    async prepareConflictComparison(locationId, path) {
      if (!native) {
        throw new Error("Conflict compare is only available in the desktop app.");
      }
      return invokeCommand<ConflictResolutionDetails>("prepare_conflict_comparison", {
        locationId,
        path,
      });
    },
    async openPath(path) {
      if (!native) {
        throw new Error("Opening local files is only available in the desktop app.");
      }
      await invokeVoidCommand("open_path", { path });
    },
    async resolveConflict(locationId, path, resolution) {
      if (!native) {
        throw new Error("Conflict resolution is only available in the desktop app.");
      }
      await invokeVoidCommand("resolve_conflict", { locationId, path, resolution });
    },
  };
}
