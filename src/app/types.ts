export const CONFLICT_STRATEGIES = ["preserve-both", "prefer-local", "prefer-remote"] as const;

export type ConflictStrategy = typeof CONFLICT_STRATEGIES[number];

export const PROVIDERS = ["aws", "gcs"] as const;

export type Provider = typeof PROVIDERS[number];

export type LegacyProvider = Provider | "gcp" | "google-cloud-storage" | "google cloud storage";

const GCS_PROVIDER_ALIASES = new Set(["gcp", "gcs", "google-cloud-storage", "google cloud storage"]);

export type ProviderCapabilityStatusKind =
  | "supported"
  | "unsupported"
  | "permission-unavailable"
  | "config-unavailable"
  | "runtime-unavailable";

export interface ProviderCapabilityStatus {
  status: ProviderCapabilityStatusKind;
  message: string | null;
}

export interface ProviderCapabilities {
  objectVersioning: ProviderCapabilityStatus;
  remoteBin: ProviderCapabilityStatus;
  archiveStorage: ProviderCapabilityStatus;
}

export type ProviderCredentialKind = "aws-access-key" | "gcs-service-account";

export interface ProviderDefinition {
  provider: Provider;
  displayName: string;
  aliases: string[];
  credentialKind: ProviderCredentialKind;
  supportsBucketCreation: boolean;
  supportsObjectVersioning: boolean;
  supportsRemoteBin: boolean;
  supportsStorageClass: boolean;
  supportsFileVersions: boolean;
  supportsBucketLifecycle: boolean;
  supportsManualCredentials: boolean;
  supportsNativeValidation: boolean;
}

export interface AwsCredentialSummaryDetails {
  accessKeyIdPreview: string | null;
}

export interface GcsCredentialSummaryDetails {
  clientEmail: string | null;
  projectId: string | null;
}

interface CredentialSummaryBase {
  id: string;
  name: string;
  provider: Provider;
  ready: boolean;
  validationStatus: "untested" | "passed" | "failed";
  lastTestedAt: string | null;
  lastTestMessage: string | null;
}

export type CredentialSummary =
  | (CredentialSummaryBase & {
    provider: "aws";
    summary?: AwsCredentialSummaryDetails | null;
  })
  | (CredentialSummaryBase & {
    provider: "gcs";
    summary?: GcsCredentialSummaryDetails | null;
  });

export type CredentialDraft =
  | {
    name: string;
    provider: "aws";
    accessKeyId: string;
    secretAccessKey: string;
  }
  | {
    name: string;
    provider: "gcs";
    credential: {
      kind: "gcsServiceAccount";
      serviceAccountJson: string;
    };
  };

const CAPABILITY_STATUS_ALIASES: Record<string, ProviderCapabilityStatusKind> = {
  available: "supported",
  supported: "supported",
  enabled: "supported",
  unsupported: "unsupported",
  unavailable: "unsupported",
  "permission-unavailable": "permission-unavailable",
  permission_unavailable: "permission-unavailable",
  permissiondenied: "permission-unavailable",
  permission_denied: "permission-unavailable",
  denied: "permission-unavailable",
  forbidden: "permission-unavailable",
  "config-unavailable": "config-unavailable",
  config_unavailable: "config-unavailable",
  configurationrequired: "config-unavailable",
  configuration_required: "config-unavailable",
  misconfigured: "config-unavailable",
  "runtime-unavailable": "runtime-unavailable",
  runtime_unavailable: "runtime-unavailable",
  temporary: "runtime-unavailable",
  temporarily_unavailable: "runtime-unavailable",
};

function normalizeCapabilityStatusKind(value: unknown): ProviderCapabilityStatusKind | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase().replace(/\s+/g, "_");
  return CAPABILITY_STATUS_ALIASES[normalized] ?? null;
}

function normalizeCapabilityMessage(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function createSupportedCapability(message: string | null = null): ProviderCapabilityStatus {
  return {
    status: "supported",
    message,
  };
}

function normalizeSingleCapability(value: unknown, fallback: ProviderCapabilityStatus): ProviderCapabilityStatus {
  if (typeof value === "string") {
    return {
      status: normalizeCapabilityStatusKind(value) ?? fallback.status,
      message: fallback.message,
    };
  }

  if (!value || typeof value !== "object") {
    return fallback;
  }

  const record = value as Record<string, unknown>;
  return {
    status: normalizeCapabilityStatusKind(record.status ?? record.state ?? record.availability) ?? fallback.status,
    message: normalizeCapabilityMessage(record.message ?? record.reason ?? record.detail) ?? fallback.message,
  };
}

function getDefaultArchiveCapability(provider: Provider): ProviderCapabilityStatus {
  return provider === "aws" || provider === "gcs"
    ? createSupportedCapability()
    : {
      status: "runtime-unavailable",
      message: "Storage class changes are not yet available for this provider.",
    };
}

function getDefaultObjectVersioningCapability(provider: Provider): ProviderCapabilityStatus {
  return provider === "aws" || provider === "gcs"
    ? createSupportedCapability()
    : {
      status: "runtime-unavailable",
      message: "Object versioning is not yet available for this provider.",
    };
}

function getDefaultRemoteBinCapability(provider: Provider): ProviderCapabilityStatus {
  return provider === "aws" || provider === "gcs"
    ? createSupportedCapability()
    : {
      status: "unsupported",
      message: "Remote bin is not yet available for this provider.",
    };
}

export function normalizeProvider(value: string | null | undefined): Provider {
  return GCS_PROVIDER_ALIASES.has(value?.trim().toLowerCase() ?? "")
    ? "gcs"
    : "aws";
}

export function getProviderLabel(provider: Provider): string {
  return provider === "aws" ? "AWS S3" : "Google Cloud Storage (GCS)";
}

export function defaultProviderDefinition(provider: Provider): ProviderDefinition {
  return provider === "gcs"
    ? {
        provider: "gcs",
        displayName: "Google Cloud Storage",
        aliases: ["gcp", "google-cloud-storage", "google cloud storage"],
        credentialKind: "gcs-service-account",
        supportsBucketCreation: true,
        supportsObjectVersioning: true,
        supportsRemoteBin: true,
        supportsStorageClass: true,
        supportsFileVersions: true,
        supportsBucketLifecycle: true,
        supportsManualCredentials: true,
        supportsNativeValidation: true,
    }
    : {
      provider: "aws",
      displayName: "Amazon S3",
      aliases: ["s3"],
      credentialKind: "aws-access-key",
      supportsBucketCreation: true,
      supportsObjectVersioning: true,
      supportsRemoteBin: true,
      supportsStorageClass: true,
      supportsFileVersions: true,
      supportsBucketLifecycle: true,
      supportsManualCredentials: true,
      supportsNativeValidation: true,
    };
}

export function normalizeProviderDefinition(value: unknown): ProviderDefinition | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const record = value as Record<string, unknown>;
  const provider = normalizeProvider(normalizeText(record.provider) ?? undefined);
  const fallback = defaultProviderDefinition(provider);
  const aliases = Array.isArray(record.aliases)
    ? record.aliases.filter((entry): entry is string => typeof entry === "string" && entry.trim().length > 0)
    : fallback.aliases;
  const credentialKind = record.credentialKind === "gcsServiceAccount" || record.credentialKind === "gcs-service-account"
    ? "gcs-service-account"
    : record.credentialKind === "awsAccessKey" || record.credentialKind === "aws-access-key"
      ? "aws-access-key"
      : fallback.credentialKind;

  return {
    provider,
    displayName: normalizeText(record.displayName ?? record.display_name) ?? fallback.displayName,
    aliases,
    credentialKind,
    supportsBucketCreation: typeof record.supportsBucketCreation === "boolean"
      ? record.supportsBucketCreation
      : typeof record.supports_bucket_creation === "boolean"
        ? record.supports_bucket_creation
        : fallback.supportsBucketCreation,
    supportsObjectVersioning: typeof record.supportsObjectVersioning === "boolean"
      ? record.supportsObjectVersioning
      : typeof record.supports_object_versioning === "boolean"
        ? record.supports_object_versioning
        : fallback.supportsObjectVersioning,
    supportsRemoteBin: typeof record.supportsRemoteBin === "boolean"
      ? record.supportsRemoteBin
      : typeof record.supports_remote_bin === "boolean"
        ? record.supports_remote_bin
        : fallback.supportsRemoteBin,
    supportsStorageClass: typeof record.supportsStorageClass === "boolean"
      ? record.supportsStorageClass
      : typeof record.supports_storage_class === "boolean"
        ? record.supports_storage_class
        : fallback.supportsStorageClass,
    supportsFileVersions: typeof record.supportsFileVersions === "boolean"
      ? record.supportsFileVersions
      : typeof record.supports_file_versions === "boolean"
        ? record.supports_file_versions
        : fallback.supportsFileVersions,
    supportsBucketLifecycle: typeof record.supportsBucketLifecycle === "boolean"
      ? record.supportsBucketLifecycle
      : typeof record.supports_bucket_lifecycle === "boolean"
        ? record.supports_bucket_lifecycle
        : fallback.supportsBucketLifecycle,
    supportsManualCredentials: typeof record.supportsManualCredentials === "boolean"
      ? record.supportsManualCredentials
      : typeof record.supports_manual_credentials === "boolean"
        ? record.supports_manual_credentials
        : fallback.supportsManualCredentials,
    supportsNativeValidation: typeof record.supportsNativeValidation === "boolean"
      ? record.supportsNativeValidation
      : typeof record.supports_native_validation === "boolean"
        ? record.supports_native_validation
        : fallback.supportsNativeValidation,
  };
}

export function getProviderCredentialLabel(provider: Provider): string {
  return provider === "aws" ? "AWS access keys" : "Google Cloud service account";
}

export function getProviderCredentialKind(provider: Provider): "access-key" | "service-account" {
  return provider === "aws" ? "access-key" : "service-account";
}

export function defaultProviderCapabilities(provider: Provider): ProviderCapabilities {
  return {
    objectVersioning: getDefaultObjectVersioningCapability(provider),
    remoteBin: getDefaultRemoteBinCapability(provider),
    archiveStorage: getDefaultArchiveCapability(provider),
  };
}

function capabilityFromBoolean(value: boolean, unsupportedMessage: string): ProviderCapabilityStatus {
  return value
    ? createSupportedCapability()
    : {
      status: "unsupported",
      message: unsupportedMessage,
    };
}

export function normalizeProviderCapabilities(value: unknown, provider: Provider): ProviderCapabilities {
  const defaults = defaultProviderCapabilities(provider);
  if (!value || typeof value !== "object") {
    return defaults;
  }

  const record = value as Record<string, unknown>;

  if (
    typeof record.supportsObjectVersioning === "boolean"
    || typeof record.supports_object_versioning === "boolean"
    || typeof record.supportsRemoteBin === "boolean"
    || typeof record.supports_remote_bin === "boolean"
    || typeof record.supportsStorageClass === "boolean"
    || typeof record.supports_storage_class === "boolean"
  ) {
    const supportsObjectVersioning = typeof record.supportsObjectVersioning === "boolean"
      ? record.supportsObjectVersioning
      : typeof record.supports_object_versioning === "boolean"
        ? record.supports_object_versioning
        : provider === "aws" || provider === "gcs";
    const supportsRemoteBin = typeof record.supportsRemoteBin === "boolean"
      ? record.supportsRemoteBin
      : typeof record.supports_remote_bin === "boolean"
        ? record.supports_remote_bin
        : provider === "aws" || provider === "gcs";
    const supportsStorageClass = typeof record.supportsStorageClass === "boolean"
      ? record.supportsStorageClass
      : typeof record.supports_storage_class === "boolean"
        ? record.supports_storage_class
        : provider === "aws" || provider === "gcs";

    return {
      objectVersioning: supportsObjectVersioning ? defaults.objectVersioning : capabilityFromBoolean(false, "Not supported for this provider."),
      remoteBin: supportsRemoteBin ? defaults.remoteBin : capabilityFromBoolean(false, "Not supported for this provider."),
      archiveStorage: supportsStorageClass ? defaults.archiveStorage : capabilityFromBoolean(false, "Not supported for this provider."),
    };
  }

  return {
    objectVersioning: normalizeSingleCapability(
      record.objectVersioning ?? record.object_versioning ?? record.versioning,
      defaults.objectVersioning,
    ),
    remoteBin: normalizeSingleCapability(
      record.remoteBin ?? record.remote_bin,
      defaults.remoteBin,
    ),
    archiveStorage: normalizeSingleCapability(
      record.archiveStorage ?? record.archive_storage ?? record.storageClass ?? record.storageClasses ?? record.glacier,
      defaults.archiveStorage,
    ),
  };
}

export function capabilitiesFromProviderDefinition(definition: ProviderDefinition | null | undefined, provider: Provider): ProviderCapabilities {
  if (!definition) {
    return defaultProviderCapabilities(provider);
  }

  const capabilities = normalizeProviderCapabilities(definition, provider);
  return capabilities;
}

export function isCapabilityAvailable(capability: ProviderCapabilityStatus): boolean {
  return capability.status === "supported";
}

export function describeCapabilityAvailability(capability: ProviderCapabilityStatus): string {
  if (capability.message) {
    return capability.message;
  }

  switch (capability.status) {
    case "unsupported":
      return "Not supported for this provider.";
    case "permission-unavailable":
      return "Unavailable with the current permissions.";
    case "config-unavailable":
      return "Unavailable until the required configuration is completed.";
    case "runtime-unavailable":
      return "Temporarily unavailable in the current runtime.";
    case "supported":
    default:
      return "Available.";
  }
}

function normalizeText(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value.trim() : null;
}

function createAccessKeyPreview(value: unknown): string | null {
  const normalized = normalizeText(value);
  if (!normalized) return null;
  if (normalized.startsWith("••••")) return normalized;
  return normalized.length > 4 ? `••••${normalized.slice(-4)}` : normalized;
}

export function normalizeCredentialSummaryRecord(value: unknown): CredentialSummary | null {
  if (!value || typeof value !== "object") return null;

  const record = value as Record<string, unknown>;
  const id = normalizeText(record.id);
  if (!id) return null;

  const provider = normalizeProvider(normalizeText(record.provider) ?? undefined);
  const base = {
    id,
    name: normalizeText(record.name) ?? "",
    provider,
    ready: Boolean(record.ready),
    validationStatus: record.validationStatus === "passed" || record.validationStatus === "failed"
      ? record.validationStatus
      : "untested",
    lastTestedAt: normalizeText(record.lastTestedAt),
    lastTestMessage: normalizeText(record.lastTestMessage),
  } as const;

  const summaryRecord = (record.summary ?? record.details) as Record<string, unknown> | undefined;
  if (provider === "aws") {
    return {
      ...base,
      provider,
      summary: {
        accessKeyIdPreview: createAccessKeyPreview(
          summaryRecord?.accessKeyIdPreview
          ?? summaryRecord?.accessKeyId
          ?? record.accessKeyIdPreview
          ?? record.accessKeyId,
        ),
      },
    };
  }

  return {
    ...base,
    provider,
    summary: {
      clientEmail: normalizeText(
        summaryRecord?.clientEmail
        ?? summaryRecord?.client_email
        ?? record.clientEmail
        ?? record.client_email,
      ),
      projectId: normalizeText(
        summaryRecord?.projectId
        ?? summaryRecord?.project_id
        ?? record.projectId
        ?? record.project_id,
      ),
    },
  };
}

export type SyncPhase =
  | "unconfigured"
  | "idle"
  | "polling"
  | "syncing"
  | "paused"
  | "error";

export interface CredentialTestContext {
  provider: Provider;
  region: string;
  bucket: string;
}

export interface CredentialTestRequest {
  credentialId: string;
  context: CredentialTestContext;
}

export interface PermissionProbeResult {
  name: string;
  allowed: boolean;
  message: string;
}

export interface PermissionProbeSummary {
  checkedAt: string;
  bucket: string;
  probes: PermissionProbeResult[];
}

export interface CredentialTestResult {
  credential: CredentialSummary;
  ok: boolean;
  checkedAt: string;
  message: string;
  bucketCount: number;
  buckets: string[];
  permissions: PermissionProbeSummary | null;
}

export interface RemoteBinConfig {
  enabled: boolean;
  retentionDays: number;
}

export type BinEntryKind = "file" | "directory";

export type BinEntrySource = "remote-bin" | "object-versioning";

export interface VersionComparisonDetails {
  path: string;
  mode: "text" | "image" | "external";
  versionAId: string;
  versionBId: string;
  versionATempPath: string | null;
  versionBTempPath: string | null;
  versionAText: string | null;
  versionBText: string | null;
  versionAImageDataUrl: string | null;
  versionBImageDataUrl: string | null;
  fallbackReason: string | null;
}

export interface FileVersionEntry {
  versionId: string;
  isLatest: boolean;
  size: number;
  lastModifiedAt: string | null;
  storageClass: string | null;
  etag: string | null;
}

export interface VersionCountEntry {
  path: string;
  count: number;
}

export interface BinEntryRequest {
  path: string;
  kind: BinEntryKind;
  binKey?: string | null;
}

export interface BinEntryMutationResult {
  path: string;
  kind: BinEntryKind;
  binKey?: string | null;
  success: boolean;
  affectedCount: number;
  error?: string | null;
}

export interface BinEntryMutationSummary {
  results: BinEntryMutationResult[];
}

export interface StoredStorageProfile {
  provider: Provider;
  localFolder: string;
  region: string;
  bucket: string;
  remotePollingEnabled: boolean;
  pollIntervalSeconds: number;
  conflictStrategy: ConflictStrategy;
  activityDebugModeEnabled: boolean;
  credentialProfileId: string | null;
  selectedCredential: CredentialSummary | null;
  selectedCredentialAvailable: boolean;
  credentialsStoredSecurely: boolean;
  providerDefinition?: ProviderDefinition | null;
  capabilities?: ProviderCapabilities;
  syncLocations: SyncLocation[];
  activeLocationId?: string | null;
}

export type StorageProfileDraft = StoredStorageProfile;

export interface DeleteCredentialResult {
  deleted: boolean;
  profile: StoredStorageProfile;
}

export interface ConnectionValidationResult {
  ok: boolean;
  message: string;
  checkedAt: string;
}

export interface InventoryComparisonSummary {
  comparedAt: string;
  localFileCount: number;
  remoteObjectCount: number;
  exactMatchCount: number;
  localOnlyCount: number;
  remoteOnlyCount: number;
  sizeMismatchCount: number;
}

export interface SyncOverviewStats {
  localFiles: number;
  remoteFiles: number;
  inSync: number;
  notInSync: number;
}

export interface SyncStatus {
  phase: SyncPhase;
  lastSyncAt: string | null;
  lastRescanAt: string | null;
  lastRemoteRefreshAt: string | null;
  lastError: string | null;
  currentFolder: string | null;
  currentBucket: string | null;
  currentPrefix: string | null;
  remotePollingEnabled: boolean;
  pollIntervalSeconds: number;
  pendingOperations: number;
  indexedFileCount: number;
  indexedDirectoryCount: number;
  indexedTotalBytes: number;
  remoteObjectCount: number;
  remoteTotalBytes: number;
  comparison: InventoryComparisonSummary;
  overview?: SyncOverviewStats;
  plan: {
    lastPlannedAt: string | null;
    observedPathCount: number;
    uploadCount: number;
    downloadCount: number;
    conflictCount: number;
    noopCount: number;
    pendingOperationCount: number;
    credentialsAvailable: boolean;
  };
}

export interface ActivityItem {
  id: string;
  timestamp: string;
  level: "info" | "success" | "error";
  message: string;
  details?: string | null;
  source?: "ui" | "native";
}

export interface NativeActivityEvent {
  timestamp: string;
  level: "info" | "success" | "error";
  message: string;
  details: string | null;
}

export interface ActivityDebugLogState {
  enabled: boolean;
  logFilePath: string | null;
  logDirectoryPath: string | null;
}

export interface SyncLocation {
  id: string;
  label: string;
  provider: Provider;
  localFolder: string;
  region: string;
  bucket: string;
  credentialProfileId: string | null;
  objectVersioningEnabled: boolean;
  enabled: boolean;
  remotePollingEnabled: boolean;
  pollIntervalSeconds: number;
  conflictStrategy: ConflictStrategy;
  remoteBin: RemoteBinConfig;
  providerDefinition?: ProviderDefinition | null;
  capabilities?: ProviderCapabilities;
}

export interface SyncLocationDraft {
  id: string | null;
  label: string;
  provider: Provider;
  localFolder: string;
  region: string;
  bucket: string;
  credentialProfileId: string | null;
  objectVersioningEnabled: boolean;
  enabled: boolean;
  remotePollingEnabled: boolean;
  pollIntervalSeconds: number;
  conflictStrategy: ConflictStrategy;
  remoteBin: RemoteBinConfig;
  providerDefinition?: ProviderDefinition | null;
  capabilities?: ProviderCapabilities;
}

export interface ConflictResolutionDetails {
  locationId: string;
  path: string;
  mode: "image" | "text" | "external";
  localPath: string | null;
  remoteTempPath: string | null;
  localText: string | null;
  remoteText: string | null;
  localImageDataUrl: string | null;
  remoteImageDataUrl: string | null;
  fallbackReason: string | null;
}

export interface SyncStatusStats {
  exactMatchCount: number;
  localOnlyCount: number;
  remoteOnlyCount: number;
  sizeMismatchCount: number;
  uploadPendingCount: number;
  downloadPendingCount: number;
  conflictPendingCount: number;
}

type LocationSyncStatusIdentity =
  | {
    pairId: string;
    pairLabel: string;
    locationId?: string;
    locationLabel?: string;
  }
  | {
    locationId: string;
    locationLabel: string;
    pairId?: string;
    pairLabel?: string;
  };

export type LocationSyncStatus = LocationSyncStatusIdentity & {
  phase: SyncPhase;
  lastSyncAt: string | null;
  lastRescanAt: string | null;
  lastRemoteRefreshAt: string | null;
  lastError: string | null;
  currentFolder: string | null;
  currentBucket: string | null;
  currentPrefix: string | null;
  enabled: boolean;
  remotePollingEnabled: boolean;
  pollIntervalSeconds: number;
  pendingOperations: number;
  indexedFileCount: number;
  indexedDirectoryCount: number;
  indexedTotalBytes: number;
  remoteObjectCount: number;
  remoteTotalBytes: number;
  stats: SyncStatusStats;
  comparison: InventoryComparisonSummary;
  plan: {
    lastPlannedAt: string | null;
    observedPathCount: number;
    uploadCount: number;
    downloadCount: number;
    conflictCount: number;
    noopCount: number;
    pendingOperationCount: number;
    credentialsAvailable: boolean;
  };
};

export interface AggregateSyncStatus {
  locationCount: number;
  enabledLocationCount: number;
  configuredLocationCount: number;
  totalPendingOperations: number;
  totalIndexedFileCount: number;
  totalIndexedBytes: number;
  totalRemoteObjectCount: number;
  totalRemoteBytes: number;
  aggregatePhase: SyncPhase;
  locations: LocationSyncStatus[];
}
