import { DEFAULT_STORED_PROFILE, normalizeStoredProfile } from "./profile";
import type { StoredStorageProfile, StorageProfileDraft } from "./types";

export const LOCAL_PROFILE_STORAGE_KEY = "storage-goblin.profile";

export interface NativeProfilePersistence {
  readonly supportsNativeProfilePersistence: boolean;
  loadProfile(): Promise<StoredStorageProfile>;
  saveProfile(profile: StorageProfileDraft): Promise<StoredStorageProfile>;
  saveProfileSettings(profile: StoredStorageProfile): Promise<StoredStorageProfile>;
}

export interface ProfilePersistence {
  load(): Promise<StoredStorageProfile>;
  save(profile: StorageProfileDraft): Promise<StoredStorageProfile>;
  saveSettings(profile: StoredStorageProfile): Promise<StoredStorageProfile>;
}

function getBrowserStorage(): Storage | null {
  if (typeof window === "undefined") return null;
  return window.localStorage;
}

export function loadStoredProfileFromBrowserStorage(): StoredStorageProfile {
  const storage = getBrowserStorage();
  if (!storage) return DEFAULT_STORED_PROFILE;

  const raw = storage.getItem(LOCAL_PROFILE_STORAGE_KEY);
  if (!raw) return DEFAULT_STORED_PROFILE;

  try {
    return normalizeStoredProfile(JSON.parse(raw) as Partial<StoredStorageProfile>);
  } catch {
    return DEFAULT_STORED_PROFILE;
  }
}

export function saveStoredProfileToBrowserStorage(profile: StoredStorageProfile | StorageProfileDraft): StoredStorageProfile {
  const storage = getBrowserStorage();
  const normalized = normalizeStoredProfile(profile);
  const sanitized = {
    ...normalized,
    credentialProfileId: normalized.credentialProfileId,
    selectedCredential: normalized.selectedCredential,
    selectedCredentialAvailable: false,
    credentialsStoredSecurely: false,
  };
  if (storage) {
    storage.setItem(LOCAL_PROFILE_STORAGE_KEY, JSON.stringify(sanitized));
  }
  return sanitized;
}

export function createProfilePersistence(nativePersistence: NativeProfilePersistence): ProfilePersistence {
  return {
    async load() {
      if (nativePersistence.supportsNativeProfilePersistence) {
        return normalizeStoredProfile(await nativePersistence.loadProfile());
      }
      return loadStoredProfileFromBrowserStorage();
    },
    async save(profile) {
      if (nativePersistence.supportsNativeProfilePersistence) {
        try {
          return normalizeStoredProfile(await nativePersistence.saveProfile(profile));
        } catch {
          return saveStoredProfileToBrowserStorage(profile);
        }
      }
      return saveStoredProfileToBrowserStorage(profile);
    },
    async saveSettings(profile) {
      if (nativePersistence.supportsNativeProfilePersistence) {
        try {
          return normalizeStoredProfile(await nativePersistence.saveProfileSettings(profile));
        } catch {
          return saveStoredProfileToBrowserStorage(profile);
        }
      }
      return saveStoredProfileToBrowserStorage(profile);
    },
  };
}
