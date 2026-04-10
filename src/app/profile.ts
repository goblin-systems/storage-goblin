import type { CredentialSummary, StoredStorageProfile, StorageProfileDraft, SyncLocation } from "./types";

export const DEFAULT_STORED_PROFILE: StoredStorageProfile = {
  localFolder: "",
  region: "",
  bucket: "",
  remotePollingEnabled: true,
  pollIntervalSeconds: 60,
  conflictStrategy: "preserve-both",
  deleteSafetyHours: 24,
  activityDebugModeEnabled: false,
  credentialProfileId: null,
  selectedCredential: null,
  selectedCredentialAvailable: false,
  credentialsStoredSecurely: false,
  syncLocations: [],
  activeLocationId: null,
};

export const DEFAULT_PROFILE_DRAFT: StorageProfileDraft = {
  ...DEFAULT_STORED_PROFILE,
};

function normalizeText(value: string | undefined): string {
  return (value ?? "").trim();
}

function normalizeCredentialSummary(value: Partial<CredentialSummary> | null | undefined): CredentialSummary | null {
  if (!value?.id) return null;
  return {
    id: normalizeText(value.id),
    name: normalizeText(value.name),
    ready: Boolean(value.ready),
    validationStatus: value.validationStatus === "passed" || value.validationStatus === "failed"
      ? value.validationStatus
      : "untested",
    lastTestedAt: normalizeText(value.lastTestedAt ?? undefined) || null,
    lastTestMessage: normalizeText(value.lastTestMessage ?? undefined) || null,
  };
}

function clampInt(value: number | undefined, min: number, max: number, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.round(value)));
}

function normalizeSyncLocation(input: Partial<SyncLocation> | null | undefined): SyncLocation | null {
  const id = normalizeText(input?.id);
  if (!id) return null;

  return {
    id,
    label: normalizeText(input?.label),
    localFolder: normalizeText(input?.localFolder),
    region: normalizeText(input?.region),
    bucket: normalizeText(input?.bucket),
    credentialProfileId: normalizeText(input?.credentialProfileId ?? undefined) || null,
    enabled: input?.enabled ?? true,
    remotePollingEnabled: input?.remotePollingEnabled ?? true,
    pollIntervalSeconds: clampInt(input?.pollIntervalSeconds, 15, 3600, DEFAULT_STORED_PROFILE.pollIntervalSeconds),
    conflictStrategy: "preserve-both",
    deleteSafetyHours: clampInt(input?.deleteSafetyHours, 1, 168, DEFAULT_STORED_PROFILE.deleteSafetyHours),
  };
}

export function normalizeStoredProfile(input?: Partial<StoredStorageProfile> | null): StoredStorageProfile {
  const selectedCredential = normalizeCredentialSummary(input?.selectedCredential);
  const credentialProfileId = normalizeText(input?.credentialProfileId ?? undefined) || selectedCredential?.id || "";
  const selectedCredentialReady = selectedCredential?.ready ?? false;
  const selectedCredentialAvailable = input?.selectedCredentialAvailable ?? selectedCredentialReady;
  const credentialsStoredSecurely = input?.credentialsStoredSecurely ?? selectedCredentialReady;

  // Backward-compat: old persisted data may use "syncPairs" instead of "syncLocations"
  const legacy = input as Record<string, unknown> | undefined;
  const syncLocations = Array.isArray(input?.syncLocations)
    ? input.syncLocations
    : Array.isArray(legacy?.syncPairs)
      ? (legacy.syncPairs as SyncLocation[])
      : [];

  const normalizedSyncLocations = syncLocations
    .map((location) => normalizeSyncLocation(location))
    .filter((location): location is SyncLocation => location !== null);

  return {
    localFolder: normalizeText(input?.localFolder),
    region: normalizeText(input?.region),
    bucket: normalizeText(input?.bucket),
    remotePollingEnabled: input?.remotePollingEnabled ?? true,
    pollIntervalSeconds: clampInt(input?.pollIntervalSeconds, 15, 3600, DEFAULT_STORED_PROFILE.pollIntervalSeconds),
    conflictStrategy: "preserve-both",
    deleteSafetyHours: clampInt(input?.deleteSafetyHours, 1, 168, DEFAULT_STORED_PROFILE.deleteSafetyHours),
    activityDebugModeEnabled: input?.activityDebugModeEnabled ?? false,
    credentialProfileId: credentialProfileId || null,
    selectedCredential,
    selectedCredentialAvailable,
    credentialsStoredSecurely,
    syncLocations: normalizedSyncLocations,
    activeLocationId: typeof input?.activeLocationId === "string" && input.activeLocationId.trim() ? input.activeLocationId.trim() : null,
  };
}

export function normalizeProfileDraft(input?: Partial<StorageProfileDraft> | null): StorageProfileDraft {
  return normalizeStoredProfile(input);
}

export function toStoredProfile(profile: StorageProfileDraft): StoredStorageProfile {
  return normalizeStoredProfile(profile);
}

export function applyStoredProfile(profile: StoredStorageProfile): StorageProfileDraft {
  return normalizeStoredProfile(profile);
}

export function isStoredProfileConfigured(profile: StoredStorageProfile): boolean {
  return profile.localFolder.length > 0 && profile.bucket.length > 0;
}

export function hasSelectedCredential(profile: Pick<StoredStorageProfile, "credentialProfileId" | "selectedCredentialAvailable">): boolean {
  return Boolean(profile.credentialProfileId && profile.selectedCredentialAvailable);
}

export function describeProfileTarget(profile: StoredStorageProfile): string {
  if (!profile.localFolder && !profile.bucket) {
    return "No folder or bucket selected";
  }

  const remoteTarget = profile.bucket || "bucket not set";
  const localTarget = profile.localFolder || "folder not set";
  return `${remoteTarget} ↔ ${localTarget}`;
}
