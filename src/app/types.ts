export const CONFLICT_STRATEGIES = ["preserve-both", "prefer-local", "prefer-remote"] as const;

export type ConflictStrategy = typeof CONFLICT_STRATEGIES[number];

export type SyncPhase =
  | "unconfigured"
  | "idle"
  | "polling"
  | "syncing"
  | "paused"
  | "error";

export interface CredentialSummary {
  id: string;
  name: string;
  ready: boolean;
  validationStatus: "untested" | "passed" | "failed";
  lastTestedAt: string | null;
  lastTestMessage: string | null;
}

export interface CredentialDraft {
  name: string;
  accessKeyId: string;
  secretAccessKey: string;
}

export interface CredentialTestContext {
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
}

export interface SyncLocationDraft {
  id: string | null;
  label: string;
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
