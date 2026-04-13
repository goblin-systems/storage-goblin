import { describe, expect, it } from "vitest";
import {
  applyStoredProfile,
  DEFAULT_REMOTE_BIN_RETENTION_DAYS,
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
          remoteBin: {
            enabled: true,
            retentionDays: 7,
          },
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
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "preserve-both",
          remoteBin: {
            enabled: false,
            retentionDays: 9999,
          },
        },
      ],
    } as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.localFolder).toBe("C:/sync");
    expect(profile.bucket).toBe("demo-bucket");
    expect(profile.pollIntervalSeconds).toBe(15);
    expect(profile.remotePollingEnabled).toBe(false);
    expect(profile).not.toHaveProperty("deleteSafetyHours");
    expect(profile.activityDebugModeEnabled).toBe(false);
    expect(profile.credentialProfileId).toBeNull();
    expect(profile.selectedCredential).toBeNull();
    expect(profile.selectedCredentialAvailable).toBe(false);
    expect(profile.credentialsStoredSecurely).toBe(false);
    expect(profile.syncLocations[0]?.remoteBin).toEqual({
      enabled: false,
      retentionDays: 3650,
    });
  });

  it("preserves valid conflict strategies and falls back invalid values", () => {
    const profile = normalizeStoredProfile({
      conflictStrategy: "prefer-remote",
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "prefer-local",
          remoteBin: {
            enabled: true,
            retentionDays: 7,
          },
        },
        {
          id: "loc-2",
          label: "Media",
          localFolder: "C:/sync/media",
          region: "us-east-1",
          bucket: "media-bucket",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "not-a-strategy",
          remoteBin: {
            enabled: true,
            retentionDays: 7,
          },
        },
      ],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.conflictStrategy).toBe("prefer-remote");
    expect(profile.syncLocations[0]?.conflictStrategy).toBe("prefer-local");
    expect(profile.syncLocations[1]?.conflictStrategy).toBe("preserve-both");
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
      objectVersioningEnabled: false,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both" as const,
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
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

  it("maps legacy deleteSafetyHours values to sync location remoteBin", () => {
    const profile = normalizeStoredProfile({
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "preserve-both",
          deleteSafetyHours: 72,
        },
      ],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.syncLocations[0]?.remoteBin).toEqual({
      enabled: true,
      retentionDays: 3,
    });
  });

  it("defaults missing sync location remote bin retention to 7 days", () => {
    const profile = normalizeStoredProfile({
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          conflictStrategy: "preserve-both",
          remoteBin: {
            enabled: true,
          },
        },
      ],
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.syncLocations[0]?.remoteBin).toEqual({
      enabled: true,
      retentionDays: DEFAULT_REMOTE_BIN_RETENTION_DAYS,
    });
  });

  it("defaults sync location object versioning to false", () => {
    const profile = normalizeStoredProfile({
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
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
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.syncLocations[0]?.objectVersioningEnabled).toBe(false);
  });

  it("disables remote bin when object versioning is enabled", () => {
    const profile = normalizeStoredProfile({
      syncLocations: [
        {
          id: "loc-1",
          label: "Docs",
          localFolder: "C:/sync/docs",
          region: "us-east-1",
          bucket: "demo-bucket",
          credentialProfileId: null,
          objectVersioningEnabled: true,
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
    } as unknown as Parameters<typeof normalizeStoredProfile>[0]);

    expect(profile.syncLocations[0]).toMatchObject({
      objectVersioningEnabled: true,
      remoteBin: {
        enabled: false,
        retentionDays: 7,
      },
    });
  });
});
