import { describe, expect, it } from "vitest";
import {
  applyStoredProfile,
  describeProfileTarget,
  hasSelectedCredential,
  normalizeProfileDraft,
  normalizeStoredProfile,
  toStoredProfile,
} from "./profile";

describe("profile helpers", () => {
  it("drops unsupported endpoint and prefix fields during normalization", () => {
    const profile = normalizeStoredProfile({
      localFolder: "  C:/sync  ",
      bucket: "  demo-bucket  ",
      endpointUrl: " https://s3.example.test ",
      prefix: "/nested/path/",
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          endpointUrl: "https://s3.example.test",
          region: "us-east-1",
          bucket: "demo-bucket",
          prefix: "docs",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "preserve-both",
          deleteSafetyHours: 24,
        },
      ],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile).not.toHaveProperty("endpointUrl");
    expect(profile).not.toHaveProperty("prefix");
    expect(profile.syncLocations[0]).not.toHaveProperty("endpointUrl");
    expect(profile.syncLocations[0]).not.toHaveProperty("prefix");
  });

  it("normalizes and clamps stored profile values", () => {
    const profile = normalizeStoredProfile({
      localFolder: "  C:/sync  ",
      bucket: "  demo-bucket  ",
      remotePollingEnabled: false,
      pollIntervalSeconds: 1,
      deleteSafetyHours: 999,
    });

    expect(profile.localFolder).toBe("C:/sync");
    expect(profile.bucket).toBe("demo-bucket");
    expect(profile.pollIntervalSeconds).toBe(15);
    expect(profile.deleteSafetyHours).toBe(168);
    expect(profile.remotePollingEnabled).toBe(false);
    expect(profile.activityDebugModeEnabled).toBe(false);
    expect(profile.credentialProfileId).toBeNull();
    expect(profile.selectedCredential).toBeNull();
    expect(profile.selectedCredentialAvailable).toBe(false);
    expect(profile.credentialsStoredSecurely).toBe(false);
  });

  it("strips credentials from persisted profile", () => {
    const stored = toStoredProfile(normalizeProfileDraft({
      localFolder: "C:/sync",
      bucket: "demo",
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: true,
    }));

    expect(stored).toMatchObject({
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: true,
      credentialsStoredSecurely: true,
    });
  });

  it("rebuilds editable state from stored profile without raw secret fields", () => {
    const draft = applyStoredProfile({
      localFolder: "C:/sync",
      region: "",
      bucket: "demo",
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      deleteSafetyHours: 24,
      activityDebugModeEnabled: true,
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: true,
      credentialsStoredSecurely: true,
      syncLocations: [],
    });

    expect(draft.activityDebugModeEnabled).toBe(true);
    expect(draft.credentialProfileId).toBe("cred-1");
    expect(draft.selectedCredential?.name).toBe("Primary");
    expect(draft.selectedCredentialAvailable).toBe(true);
    expect(draft.credentialsStoredSecurely).toBe(true);
  });

  it("keeps secure storage state separate from selected availability", () => {
    const draft = applyStoredProfile({
      localFolder: "C:/sync",
      region: "",
      bucket: "demo",
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      deleteSafetyHours: 24,
      activityDebugModeEnabled: false,
      credentialProfileId: "cred-1",
      selectedCredential: { id: "cred-1", name: "Primary", ready: true, validationStatus: "untested", lastTestedAt: null, lastTestMessage: null },
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: true,
      syncLocations: [],
    });

    expect(draft.selectedCredential?.ready).toBe(true);
    expect(draft.selectedCredentialAvailable).toBe(false);
    expect(draft.credentialsStoredSecurely).toBe(true);
  });

  it("describes combined local and remote target", () => {
    expect(describeProfileTarget({
      localFolder: "C:/sync",
      region: "",
      bucket: "demo",
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
    })).toBe("demo ↔ C:/sync");
  });

  it("detects when a selected credential is ready to use", () => {
    expect(hasSelectedCredential(normalizeProfileDraft({
      credentialProfileId: "cred-1",
      selectedCredentialAvailable: true,
    }))).toBe(true);
    expect(hasSelectedCredential(normalizeProfileDraft({
      credentialProfileId: "cred-1",
      selectedCredentialAvailable: false,
    }))).toBe(false);
  });

  it("migrates legacy syncPairs to syncLocations", () => {
    const legacyLocation = {
      id: "loc-1",
      label: "Photos",
      localFolder: "C:/photos",
      region: "us-east-1",
      bucket: "my-bucket",
      credentialProfileId: null,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both" as const,
      deleteSafetyHours: 24,
    };

    // Old persisted data with syncPairs and no syncLocations
    const fromLegacy = normalizeStoredProfile({
      localFolder: "C:/sync",
      bucket: "demo",
      syncPairs: [legacyLocation],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(fromLegacy.syncLocations).toEqual([legacyLocation]);

    // When both exist, syncLocations takes precedence
    const withBoth = normalizeStoredProfile({
      localFolder: "C:/sync",
      bucket: "demo",
      syncLocations: [],
      syncPairs: [legacyLocation],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(withBoth.syncLocations).toEqual([]);

    // When neither exists, defaults to []
    const withNeither = normalizeStoredProfile({
      localFolder: "C:/sync",
      bucket: "demo",
    });

    expect(withNeither.syncLocations).toEqual([]);
  });
});
