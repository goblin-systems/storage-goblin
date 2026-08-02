import {
  defaultProviderDefinition,
  CONFLICT_STRATEGIES,
  normalizeCredentialSummaryRecord,
  normalizeProviderDefinition,
  normalizeProvider,
  normalizeProviderCapabilities,
  type ConflictStrategy,
  type CredentialSummary,
  type Provider,
  type StoredStorageProfile,
  type StorageProfileDraft,
  type SyncLocation,
} from "./types";

export const DEFAULT_REMOTE_BIN_RETENTION_DAYS = 7;

export const DEFAULT_STORED_PROFILE: StoredStorageProfile = {
  provider: "aws",
  localFolder: "",
  region: "",
  bucket: "",
  remotePollingEnabled: true,
  pollIntervalSeconds: 60,
  conflictStrategy: "preserve-both",
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

function normalizeConflictStrategy(value: string | undefined): ConflictStrategy {
  return CONFLICT_STRATEGIES.includes(value as ConflictStrategy)
    ? (value as ConflictStrategy)
    : "preserve-both";
}

function normalizeText(value: string | undefined): string {
  return (value ?? "").trim();
}

function normalizeCredentialSummary(
  value: Partial<CredentialSummary> | null | undefined,
): CredentialSummary | null {
  return normalizeCredentialSummaryRecord(value);
}

function clampInt(value: number | undefined, min: number, max: number, fallback: number): number {
  if (typeof value !== "number" || !Number.isFinite(value)) return fallback;
  return Math.min(max, Math.max(min, Math.round(value)));
}

function normalizeSyncLocation(
  input: Partial<SyncLocation> | null | undefined,
): SyncLocation | null {
  const id = normalizeText(input?.id);
  if (!id) return null;

  // Legacy persisted locations carried "deleteSafetyHours" instead of remoteBin retention.
  const legacyDeleteSafetyHours = ((input ?? {}) as Record<string, unknown>).deleteSafetyHours;

  const retentionDays = clampInt(
    input?.remoteBin?.retentionDays ??
      (typeof legacyDeleteSafetyHours === "number"
        ? Math.ceil(legacyDeleteSafetyHours / 24)
        : undefined),
    1,
    3650,
    DEFAULT_REMOTE_BIN_RETENTION_DAYS,
  );

  return {
    id,
    label: normalizeText(input?.label),
    provider: normalizeProvider(input?.provider),
    localFolder: normalizeText(input?.localFolder),
    region: normalizeText(input?.region),
    bucket: normalizeText(input?.bucket),
    credentialProfileId: normalizeText(input?.credentialProfileId ?? undefined) || null,
    objectVersioningEnabled: Boolean(input?.objectVersioningEnabled),
    enabled: input?.enabled ?? true,
    remotePollingEnabled: input?.remotePollingEnabled ?? true,
    pollIntervalSeconds: clampInt(
      input?.pollIntervalSeconds,
      15,
      3600,
      DEFAULT_STORED_PROFILE.pollIntervalSeconds,
    ),
    conflictStrategy: normalizeConflictStrategy(input?.conflictStrategy),
    remoteBin: {
      enabled: input?.objectVersioningEnabled ? false : (input?.remoteBin?.enabled ?? true),
      retentionDays,
    },
    providerDefinition:
      normalizeProviderDefinition(
        (input as Record<string, unknown> | undefined)?.providerDefinition,
      ) ?? defaultProviderDefinition(normalizeProvider(input?.provider)),
    capabilities: normalizeProviderCapabilities(
      (input as Record<string, unknown> | undefined)?.capabilities,
      normalizeProvider(input?.provider),
    ),
  };
}

export function normalizeStoredProfile(
  input?: Partial<StoredStorageProfile> | null,
): StoredStorageProfile {
  const selectedCredential = normalizeCredentialSummary(input?.selectedCredential);
  const credentialProfileId =
    normalizeText(input?.credentialProfileId ?? undefined) || (selectedCredential?.id ?? "");
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
    provider: normalizeProvider(input?.provider),
    localFolder: normalizeText(input?.localFolder),
    region: normalizeText(input?.region),
    bucket: normalizeText(input?.bucket),
    remotePollingEnabled: input?.remotePollingEnabled ?? true,
    pollIntervalSeconds: clampInt(
      input?.pollIntervalSeconds,
      15,
      3600,
      DEFAULT_STORED_PROFILE.pollIntervalSeconds,
    ),
    conflictStrategy: normalizeConflictStrategy(input?.conflictStrategy),
    activityDebugModeEnabled: input?.activityDebugModeEnabled ?? false,
    credentialProfileId: credentialProfileId || null,
    selectedCredential,
    selectedCredentialAvailable,
    credentialsStoredSecurely,
    providerDefinition:
      normalizeProviderDefinition(
        (input as Record<string, unknown> | undefined)?.providerDefinition,
      ) ?? defaultProviderDefinition(normalizeProvider(input?.provider)),
    capabilities: normalizeProviderCapabilities(
      (input as Record<string, unknown> | undefined)?.capabilities,
      normalizeProvider(input?.provider),
    ),
    syncLocations: normalizedSyncLocations,
    activeLocationId:
      typeof input?.activeLocationId === "string" && input.activeLocationId.trim()
        ? input.activeLocationId.trim()
        : null,
  };
}

export function normalizeProfileDraft(
  input?: Partial<StorageProfileDraft> | null,
): StorageProfileDraft {
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

export function hasSelectedCredential(
  profile: Pick<StoredStorageProfile, "credentialProfileId" | "selectedCredentialAvailable">,
): boolean {
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

function createUnavailableCredential(
  id: string,
  provider: Provider,
  name?: string | null,
): CredentialSummary {
  return {
    id,
    name: (name ?? "").trim() || "Missing credential",
    provider,
    ready: false,
    validationStatus: "untested",
    lastTestedAt: null,
    lastTestMessage: null,
    summary: null,
  };
}

export function syncProfileCredentialState(
  profile: StorageProfileDraft,
  credentials: CredentialSummary[],
): StorageProfileDraft {
  const trimmedCredentialProfileId = profile.credentialProfileId?.trim() ?? "";
  const credentialProfileId = trimmedCredentialProfileId === "" ? null : trimmedCredentialProfileId;

  if (!credentialProfileId) {
    return normalizeProfileDraft({
      ...profile,
      credentialProfileId: null,
      selectedCredential: null,
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
  }

  const availableCredential =
    credentials.find((credential) => credential.id === credentialProfileId) ?? null;
  const fallbackProvider =
    profile.selectedCredential?.id === credentialProfileId
      ? profile.selectedCredential.provider
      : profile.provider;
  const selectedCredential =
    availableCredential ??
    (profile.selectedCredential?.id === credentialProfileId
      ? createUnavailableCredential(
          credentialProfileId,
          fallbackProvider,
          profile.selectedCredential.name,
        )
      : createUnavailableCredential(credentialProfileId, fallbackProvider));

  return normalizeProfileDraft({
    ...profile,
    credentialProfileId,
    selectedCredential,
    selectedCredentialAvailable: Boolean(availableCredential?.ready),
    credentialsStoredSecurely: Boolean(availableCredential?.ready),
  });
}
