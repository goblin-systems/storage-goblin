/**
 * The shape of the application's state, and the store that owns it
 * (backlog phase 4.1).
 *
 * This type was previously an anonymous annotation on a `const state` inside
 * `bootstrapStorageGoblin()`. Naming it and moving it out is the precondition
 * for extracting views: a view cannot subscribe to state it has no way to name.
 */

import type { FileTreeMode } from "../app/file-tree";
import type {
  ActivityDebugLogState,
  ActivityItem,
  CredentialSummary,
  LocationSyncStatus,
  ProviderDefinition,
  StorageProfileDraft,
  SyncLocation,
  SyncStatus,
} from "../app/types";
import { createStore, type Store } from "./store";

/** The dialogs the shell can open. Moved here from the bootstrap closure. */
export type DialogId =
  "credentials" | "locations" | "activity" | "polling" | "debug" | "conflict" | "about";

/**
 * Aggregate status plus the per-location breakdown.
 *
 * `locations` is optional because the browser-preview client synthesizes a
 * status without it.
 */
export type SyncStatusWithLocations = SyncStatus & {
  locations?: LocationSyncStatus[];
};

export interface AppState {
  /** The open dialog, or null when the home screen is showing. */
  activeDialog: DialogId | null;
  activeLocationId: string | null;
  activeLocationViewMode: FileTreeMode;
  profile: StorageProfileDraft;
  providerDefinitions: ProviderDefinition[];
  credentials: CredentialSummary[];
  syncLocations: SyncLocation[];
  status: SyncStatusWithLocations;
  activity: ActivityItem[];
  lastConnectAt: string | null;
  debugLogState: ActivityDebugLogState;
}

export type AppStore = Store<AppState>;

export function createAppStore(initial: AppState): AppStore {
  return createStore(initial);
}

// --- selectors -------------------------------------------------------------
//
// Named rather than inlined so a view subscribes to the same slice the rest of
// the app does, and so a state rename is one edit instead of a search.

export const selectActiveLocationId = (state: AppState): string | null => state.activeLocationId;

export const selectActiveLocation = (state: AppState): SyncLocation | undefined =>
  state.activeLocationId
    ? state.syncLocations.find((location) => location.id === state.activeLocationId)
    : undefined;

export const selectSyncLocations = (state: AppState): SyncLocation[] => state.syncLocations;

export const selectCredentials = (state: AppState): CredentialSummary[] => state.credentials;

export const selectStatus = (state: AppState): SyncStatusWithLocations => state.status;

export const selectActivity = (state: AppState): ActivityItem[] => state.activity;

export const selectProfile = (state: AppState): StorageProfileDraft => state.profile;

export const selectActiveDialog = (state: AppState): DialogId | null => state.activeDialog;

export const selectDebugLogState = (state: AppState): ActivityDebugLogState => state.debugLogState;

export const selectProviderDefinitions = (state: AppState): ProviderDefinition[] =>
  state.providerDefinitions;

export const selectActiveLocationViewMode = (state: AppState): FileTreeMode =>
  state.activeLocationViewMode;
