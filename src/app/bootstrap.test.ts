import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CredentialSummary, DeleteCredentialResult, LocationSyncStatus, StoredStorageProfile, SyncLocation, SyncStatus } from "./types";
import type { FileEntry } from "./file-tree";

type ModalBackdrop = HTMLElement & { __onClose?: () => void };

const {
  closeModalMock,
  confirmModalMock,
  chooseLocalFolderMock,
  createCredentialMock,
  deleteCredentialMock,
  getSyncStatusMock,
  listCredentialsMock,
  loadProfileMock,
  listSyncLocationsMock,
  addSyncLocationMock,
  updateSyncLocationMock,
  removeSyncLocationMock,
  listFileEntriesMock,
  bindCheckboxTreeMock,
  openActivityDebugLogFolderMock,
  openModalMock,
  saveProfileSettingsMock,
  testCredentialMock,
  toggleLocalCopyMock,
} = vi.hoisted(() => ({
  confirmModalMock: vi.fn().mockResolvedValue(true),
  openModalMock: vi.fn(({ backdrop, onClose }: { backdrop: ModalBackdrop; onClose?: () => void }) => {
    backdrop.__onClose = onClose;
    backdrop.removeAttribute("hidden");
    document.body.classList.add("modal-open");
  }),
  closeModalMock: vi.fn(({ backdrop, onClose }: { backdrop: ModalBackdrop; onClose?: () => void }) => {
    backdrop.setAttribute("hidden", "");
    document.body.classList.remove("modal-open");
    backdrop.__onClose?.();
    onClose?.();
  }),
  chooseLocalFolderMock: vi.fn(),
  createCredentialMock: vi.fn(),
  deleteCredentialMock: vi.fn(),
  getSyncStatusMock: vi.fn(),
  listCredentialsMock: vi.fn(),
  loadProfileMock: vi.fn(),
  listSyncLocationsMock: vi.fn(),
  addSyncLocationMock: vi.fn(),
  updateSyncLocationMock: vi.fn(),
  removeSyncLocationMock: vi.fn(),
  listFileEntriesMock: vi.fn(),
  openActivityDebugLogFolderMock: vi.fn(),
  saveProfileSettingsMock: vi.fn(),
  testCredentialMock: vi.fn(),
  toggleLocalCopyMock: vi.fn(),
  bindCheckboxTreeMock: vi.fn(() => ({
    expand: vi.fn(),
    collapse: vi.fn(),
    expandAll: vi.fn(),
    collapseAll: vi.fn(),
    destroy: vi.fn(),
  })),
}));

vi.mock("@goblin-systems/goblin-design-system", () => ({
  applyIcons: vi.fn(),
  bindCheckboxTree: bindCheckboxTreeMock,
  bindNavigation: vi.fn(({ root, onSelect }: { root: HTMLElement; onSelect: (id: string) => void }) => {
    root.querySelectorAll<HTMLElement>("[data-nav-id]").forEach((item) => {
      item.addEventListener("click", () => {
        const id = item.dataset.navId;
        if (id) onSelect(id);
      });
    });

    return {
      closeAll: vi.fn(),
      closeItem: vi.fn(),
      openItem: vi.fn(),
    };
  }),
  byId: <T extends HTMLElement>(id: string, root: Document | HTMLElement = document) => {
    const element = root instanceof Document ? root.getElementById(id) : root.querySelector(`#${id}`);
    if (!element) {
      throw new Error(`Missing element: ${id}`);
    }
    return element as T;
  },
  closeModal: closeModalMock,
  confirmModal: confirmModalMock,
  openModal: openModalMock,
  setupWindowControls: vi.fn(),
  showToast: vi.fn(),
}));

vi.mock("./client", () => ({
  createStorageGoblinClient: () => ({
    supportsNativeProfilePersistence: true,
    chooseLocalFolder: chooseLocalFolderMock,
    connectAndSync: vi.fn(),
    validateS3Connection: vi.fn(),
    listCredentials: listCredentialsMock,
    createCredential: createCredentialMock,
    testCredential: testCredentialMock,
    deleteCredential: deleteCredentialMock,
    loadProfile: loadProfileMock,
    saveProfile: vi.fn(),
    saveProfileSettings: saveProfileSettingsMock,
    getSyncStatus: getSyncStatusMock,
    startSync: vi.fn(),
    pauseSync: vi.fn(),
    runFullRescan: vi.fn(),
    refreshRemoteInventory: vi.fn(),
    buildSyncPlan: vi.fn(),
    executePlannedUploads: vi.fn(),
    listenSyncStatus: vi.fn().mockResolvedValue(() => undefined),
    listenNativeActivity: vi.fn().mockResolvedValue(() => undefined),
    getActivityDebugLogState: vi.fn().mockResolvedValue({
      enabled: false,
      logFilePath: null,
      logDirectoryPath: null,
    }),
    openActivityDebugLogFolder: openActivityDebugLogFolderMock,
    listSyncLocations: listSyncLocationsMock,
    listFileEntries: listFileEntriesMock,
    toggleLocalCopy: toggleLocalCopyMock,
    addSyncLocation: addSyncLocationMock,
    updateSyncLocation: updateSyncLocationMock,
    removeSyncLocation: removeSyncLocationMock,
  }),
}));

import { bootstrapStorageGoblin } from "./bootstrap";

function renderAppShell() {
  const html = readFileSync(resolve(process.cwd(), "index.html"), "utf8");
  const parsed = new DOMParser().parseFromString(html, "text/html");
  document.body.innerHTML = parsed.body.innerHTML;
}

function baseCredential(id = "cred-1", name = "Primary"): CredentialSummary {
  return {
    id,
    name,
    ready: true,
    validationStatus: "untested",
    lastTestedAt: null,
    lastTestMessage: null,
  };
}

function baseStoredProfile(overrides: Partial<StoredStorageProfile> = {}): StoredStorageProfile {
  return {
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
    ...overrides,
  };
}

function baseSyncLocation(id: string, label: string, overrides: Partial<SyncLocation> = {}): SyncLocation {
  return {
    id,
    label,
    localFolder: `C:/${label.toLowerCase().replace(/\s+/g, "-")}`,
    region: "us-east-1",
    bucket: `${id}-bucket`,
    credentialProfileId: null,
    enabled: true,
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    conflictStrategy: "preserve-both",
    deleteSafetyHours: 24,
    ...overrides,
  };
}

function baseConnectedStatus(): SyncStatus {
  return {
    phase: "idle",
    lastSyncAt: "2026-04-04T12:00:00.000Z",
    lastRescanAt: null,
    lastRemoteRefreshAt: null,
    lastError: null,
    currentFolder: "C:/sync",
    currentBucket: "demo-bucket",
    currentPrefix: "archive/2026",
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    pendingOperations: 0,
    indexedFileCount: 12,
    indexedDirectoryCount: 2,
    indexedTotalBytes: 1024,
    remoteObjectCount: 12,
    remoteTotalBytes: 1024,
    comparison: {
      comparedAt: "2026-04-04T12:00:00.000Z",
      localFileCount: 12,
      remoteObjectCount: 12,
      exactMatchCount: 12,
      localOnlyCount: 0,
      remoteOnlyCount: 0,
      sizeMismatchCount: 0,
    },
    overview: {
      localFiles: 12,
      remoteFiles: 12,
      inSync: 12,
      notInSync: 0,
    },
    plan: {
      lastPlannedAt: null,
      observedPathCount: 12,
      uploadCount: 0,
      downloadCount: 0,
      conflictCount: 0,
      noopCount: 12,
      pendingOperationCount: 0,
      credentialsAvailable: true,
    },
  };
}

function baseUnconfiguredStatus(): SyncStatus {
  return {
    phase: "unconfigured",
    lastSyncAt: null,
    lastRescanAt: null,
    lastRemoteRefreshAt: null,
    lastError: null,
    currentFolder: null,
    currentBucket: null,
    currentPrefix: null,
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    pendingOperations: 0,
    indexedFileCount: 0,
    indexedDirectoryCount: 0,
    indexedTotalBytes: 0,
    remoteObjectCount: 0,
    remoteTotalBytes: 0,
    comparison: {
      comparedAt: "",
      localFileCount: 0,
      remoteObjectCount: 0,
      exactMatchCount: 0,
      localOnlyCount: 0,
      remoteOnlyCount: 0,
      sizeMismatchCount: 0,
    },
    overview: {
      localFiles: 0,
      remoteFiles: 0,
      inSync: 0,
      notInSync: 0,
    },
    plan: {
      lastPlannedAt: null,
      observedPathCount: 0,
      uploadCount: 0,
      downloadCount: 0,
      conflictCount: 0,
      noopCount: 0,
      pendingOperationCount: 0,
      credentialsAvailable: false,
    },
  };
}

function fileEntry(overrides: Partial<FileEntry> & Pick<FileEntry, "path">): FileEntry {
  return {
    kind: "file",
    status: "synced",
    hasLocalCopy: true,
    ...overrides,
  };
}

function directoryEntry(overrides: Partial<FileEntry> & Pick<FileEntry, "path">): FileEntry {
  return {
    kind: "directory",
    status: "synced",
    hasLocalCopy: true,
    ...overrides,
  };
}

async function flushTasks() {
  await new Promise((resolve) => setTimeout(resolve, 0));
}

function listItemByText(selector: string, text: string): HTMLElement {
  const item = Array.from(document.querySelectorAll<HTMLElement>(selector))
    .find((element) => element.textContent?.includes(text));

  expect(item).toBeTruthy();
  return item as HTMLElement;
}

describe("bootstrapStorageGoblin", () => {
  let cleanup: (() => void) | undefined;

  beforeEach(() => {
    renderAppShell();
    openModalMock.mockClear();
    closeModalMock.mockClear();
    bindCheckboxTreeMock.mockClear();
    chooseLocalFolderMock.mockReset().mockResolvedValue(null);
    openActivityDebugLogFolderMock.mockReset().mockResolvedValue(undefined);
    loadProfileMock.mockReset().mockResolvedValue(baseStoredProfile());
    saveProfileSettingsMock.mockReset().mockResolvedValue({});
    toggleLocalCopyMock.mockReset().mockResolvedValue(undefined);
    getSyncStatusMock.mockReset().mockResolvedValue(baseUnconfiguredStatus());
    listCredentialsMock.mockReset().mockResolvedValue([baseCredential()]);
    createCredentialMock.mockReset().mockResolvedValue(baseCredential("cred-2", "Archive"));
    testCredentialMock.mockReset().mockImplementation(async ({ credentialId }: { credentialId: string }) => ({
      credential: {
        id: credentialId,
        name: credentialId === "cred-2" ? "Archive" : "Primary",
        ready: true,
        validationStatus: "passed",
        lastTestedAt: "2026-04-04T12:05:00.000Z",
        lastTestMessage: "Validated access to bucket 'demo-bucket' and sampled 1 remote object(s).",
      },
      ok: true,
      checkedAt: "2026-04-04T12:05:00.000Z",
      message: "Validated access to bucket 'demo-bucket' and sampled 1 remote object(s).",
      bucketCount: 1,
      buckets: ["demo-bucket"],
      permissions: null,
    }));
    deleteCredentialMock.mockReset().mockResolvedValue({
      deleted: true,
      profile: baseStoredProfile(),
    } satisfies DeleteCredentialResult);
    listSyncLocationsMock.mockReset().mockResolvedValue([]);
    listFileEntriesMock.mockReset().mockResolvedValue([]);
    addSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    updateSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    removeSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    confirmModalMock.mockReset().mockResolvedValue(true);
  });

  afterEach(() => {
    cleanup?.();
    cleanup = undefined;
    vi.restoreAllMocks();
    document.body.innerHTML = "";
    document.body.className = "";
  });

  it("keeps the status surface visible while menu items open and close dialogs", async () => {
    cleanup = await bootstrapStorageGoblin();

    const home = document.getElementById("screen-home");
    const credentials = document.getElementById("screen-credentials");
    const activity = document.getElementById("screen-activity");
    const settings = document.getElementById("screen-settings");

    expect(home?.hidden).toBe(false);
    expect(credentials?.hidden).toBe(true);
    expect(activity?.hidden).toBe(true);
    expect(settings?.hidden).toBe(true);

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    expect(home?.hidden).toBe(false);
    expect(credentials?.hidden).toBe(false);

    document.querySelector<HTMLElement>("[data-nav-id='nav-home']")?.click();
    expect(credentials?.hidden).toBe(true);
    expect(activity?.hidden).toBe(true);
    expect(settings?.hidden).toBe(true);
    expect(home?.hidden).toBe(false);
  });

  it("creates credentials from the dedicated dialog", async () => {
    listCredentialsMock.mockResolvedValueOnce([]).mockResolvedValueOnce([baseCredential("cred-2", "Archive")]);

    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    (document.getElementById("credential-name-input") as HTMLInputElement).value = "Archive";
    (document.getElementById("credential-access-key-input") as HTMLInputElement).value = "AKIA123";
    (document.getElementById("credential-secret-key-input") as HTMLInputElement).value = "secret";

    document.getElementById("create-credential-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(createCredentialMock).toHaveBeenCalledWith({
      name: "Archive",
      accessKeyId: "AKIA123",
      secretAccessKey: "secret",
    });
    expect((document.getElementById("credential-name-input") as HTMLInputElement).value).toBe("");
    expect((document.getElementById("credential-access-key-input") as HTMLInputElement).value).toBe("");
    expect((document.getElementById("credential-secret-key-input") as HTMLInputElement).value).toBe("");
    expect(document.getElementById("credentials-result")?.textContent).toContain("Saved credential \"Archive\" securely.");
    expect(document.getElementById("credentials-result")?.textContent).toContain("It was not tested yet.");
  });

  it("surfaces tested-on-create credentials when bucket context exists", async () => {
    listCredentialsMock.mockResolvedValueOnce([]).mockResolvedValueOnce([{
      ...baseCredential("cred-2", "Archive"),
      validationStatus: "passed",
      lastTestedAt: "2026-04-04T12:05:00.000Z",
      lastTestMessage: "Validated access to bucket 'demo-bucket' and sampled 1 remote object(s).",
    }]);
    createCredentialMock.mockResolvedValueOnce({
      ...baseCredential("cred-2", "Archive"),
      validationStatus: "passed",
      lastTestedAt: "2026-04-04T12:05:00.000Z",
      lastTestMessage: "Validated access to bucket 'demo-bucket' and sampled 1 remote object(s).",
    });

    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    (document.getElementById("credential-name-input") as HTMLInputElement).value = "Archive";
    (document.getElementById("credential-access-key-input") as HTMLInputElement).value = "AKIA123";
    (document.getElementById("credential-secret-key-input") as HTMLInputElement).value = "secret";

    document.getElementById("create-credential-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(document.getElementById("credentials-result")?.textContent).toContain("tested and is valid");
  });

  it("offers re-test actions and updates credential test state", async () => {
    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    const testButton = Array.from(document.querySelectorAll<HTMLButtonElement>("#credentials-list button"))
      .find((button) => button.textContent === "Test");

    expect(testButton).toBeTruthy();
    testButton?.click();
    await flushTasks();

    expect(testCredentialMock).toHaveBeenCalledWith({
      credentialId: "cred-1",
      context: {
        region: "",
        bucket: "",
      },
    });
    expect(document.getElementById("credentials-result")?.textContent).toContain("test passed. Can access 1 bucket(s)");
    expect(Array.from(document.querySelectorAll<HTMLButtonElement>("#credentials-list button")).some((button) => button.textContent === "Re-test")).toBe(true);
  });

  it("deletes credentials from the dedicated dialog and clears selected state", async () => {
    listCredentialsMock
      .mockResolvedValueOnce([baseCredential()])
      .mockResolvedValueOnce([baseCredential()])
      .mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    Array.from(document.querySelectorAll<HTMLButtonElement>("#credentials-list button"))
      .find((button) => button.textContent === "Delete")
      ?.click();
    await flushTasks();

    expect(deleteCredentialMock).toHaveBeenCalledWith("cred-1");
  });

  it("keeps a deleted active sync location gone after bootstrap reload when the stored profile no longer includes it", async () => {
    const photos = baseSyncLocation("loc-1", "My photos", {
      localFolder: "C:/photos",
      bucket: "photo-bucket",
    });
    const documents = baseSyncLocation("loc-2", "Documents", {
      localFolder: "C:/docs",
      region: "eu-west-1",
      bucket: "doc-bucket",
    });
    const storedAfterDelete = baseStoredProfile({
      syncLocations: [documents],
      activeLocationId: "loc-2",
    });

    listSyncLocationsMock.mockResolvedValue([photos, documents]);
    removeSyncLocationMock.mockResolvedValueOnce(storedAfterDelete);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    listItemByText("#locations-list li", "My photos")
      .querySelectorAll<HTMLButtonElement>("button")
      .forEach((button) => {
        if (button.textContent === "Delete") {
          button.click();
        }
      });
    await flushTasks();

    const selectAfterDelete = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(removeSyncLocationMock).toHaveBeenCalledWith("loc-1");
    expect(Array.from(selectAfterDelete.options).map((option) => option.value)).toEqual(["", "loc-2"]);
    expect(selectAfterDelete.value).toBe("loc-2");
    expect(document.getElementById("locations-count-badge")?.textContent).toBe("1 sync location");

    cleanup?.();
    cleanup = undefined;
    document.body.className = "";
    renderAppShell();

    loadProfileMock.mockResolvedValueOnce(storedAfterDelete);
    listSyncLocationsMock.mockResolvedValueOnce([photos, documents]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const selectAfterReload = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(Array.from(selectAfterReload.options).map((option) => option.value)).toEqual(["", "loc-2"]);
    expect(selectAfterReload.value).toBe("loc-2");
  });

  it("opens the sync locations dialog from the menu and creates a sync location", async () => {
    const createdLocation = {
      id: "loc-1",
      label: "My photos",
      localFolder: "C:/photos",
      region: "us-east-1",
      bucket: "photo-bucket",
      credentialProfileId: null,
      enabled: true,
      remotePollingEnabled: true,
      pollIntervalSeconds: 60,
      conflictStrategy: "preserve-both",
      deleteSafetyHours: 24,
    };
    addSyncLocationMock.mockResolvedValueOnce({ syncLocations: [createdLocation] });

    cleanup = await bootstrapStorageGoblin();

    // Open the sync locations dialog
    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    expect(document.getElementById("screen-locations")?.hidden).toBe(false);

    // Fill in the form
    (document.getElementById("location-label-input") as HTMLInputElement).value = "My photos";
    (document.getElementById("location-local-folder-input") as HTMLInputElement).value = "C:/photos";
    (document.getElementById("location-bucket-input") as HTMLInputElement).value = "photo-bucket";
    (document.getElementById("location-region-select") as HTMLSelectElement).value = "us-east-1";

    // Click create
    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(addSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      id: null,
      label: "My photos",
      localFolder: "C:/photos",
      bucket: "photo-bucket",
      region: "us-east-1",
    }));
    expect(document.getElementById("locations-result")?.textContent).toContain('Created sync location "My photos"');
    expect(document.getElementById("locations-count-badge")?.textContent).toBe("1 sync location");
  });

  it("does not hydrate legacy endpoint or prefix values into the sync location form", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        ...baseSyncLocation("loc-1", "My photos"),
        endpointUrl: "https://s3.example.test",
        prefix: "archive/2026",
      } as unknown as SyncLocation,
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    listItemByText("#locations-list li", "My photos")
      .querySelectorAll<HTMLButtonElement>("button")
      .forEach((button) => {
        if (button.textContent === "Edit") {
          button.click();
        }
      });

    expect(document.getElementById("location-endpoint-input")).toBeNull();
    expect(document.getElementById("location-prefix-input")).toBeNull();
  });

  it("does not write endpoint or prefix values from the sync location form", async () => {
    addSyncLocationMock.mockResolvedValueOnce({
      syncLocations: [baseSyncLocation("loc-1", "My photos")],
    });

    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    (document.getElementById("location-label-input") as HTMLInputElement).value = "My photos";
    (document.getElementById("location-local-folder-input") as HTMLInputElement).value = "C:/photos";
    (document.getElementById("location-bucket-input") as HTMLInputElement).value = "photo-bucket";
    (document.getElementById("location-region-select") as HTMLSelectElement).value = "us-east-1";

    expect(document.getElementById("location-endpoint-input")).toBeNull();
    expect(document.getElementById("location-prefix-input")).toBeNull();

    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    const sentDraft = addSyncLocationMock.mock.calls[0]?.[0] as Record<string, unknown>;
    expect(sentDraft).not.toHaveProperty("endpointUrl");
    expect(sentDraft).not.toHaveProperty("prefix");
  });

  it("renders sync location summaries with bucket only", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        ...baseSyncLocation("loc-1", "My photos"),
        localFolder: "C:/photos",
        bucket: "photo-bucket",
        prefix: "archive/2026",
      } as unknown as SyncLocation,
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    const item = listItemByText("#locations-list li", "My photos");

    expect(item.textContent).toContain("C:/photos → photo-bucket");
    expect(item.textContent).not.toContain("photo-bucket/archive/2026");
  });

  it("surfaces unsupported legacy sync location load errors", async () => {
    listSyncLocationsMock.mockRejectedValueOnce(new Error(
      "Unsupported legacy sync location config: prefix and endpointUrl are no longer supported.",
    ));

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    expect(document.getElementById("locations-result")?.textContent).toContain(
      "Unsupported legacy sync location config",
    );
  });

  it("populates the active location dropdown from sync locations", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
      {
        id: "loc-2",
        label: "Documents",
        localFolder: "C:/docs",
        region: "eu-west-1",
        bucket: "doc-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ]);

    cleanup = await bootstrapStorageGoblin();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(select).toBeTruthy();
    // Default option + 2 locations = 3 options
    expect(select.options.length).toBe(3);
    // First location should be auto-selected
    expect(select.value).toBe("loc-1");
  });

  it("persists active location changes through saveProfileSettings and reload", async () => {
    const photos = baseSyncLocation("loc-1", "My photos", {
      localFolder: "C:/photos",
      bucket: "photo-bucket",
    });
    const documents = baseSyncLocation("loc-2", "Documents", {
      localFolder: "C:/docs",
      region: "eu-west-1",
      bucket: "doc-bucket",
    });

    listSyncLocationsMock.mockResolvedValue([photos, documents]);
    saveProfileSettingsMock.mockImplementation(async (profile: StoredStorageProfile) => profile);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "loc-2";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(saveProfileSettingsMock).toHaveBeenCalledWith(expect.objectContaining({
      activeLocationId: "loc-2",
    }));

    cleanup?.();
    cleanup = undefined;
    document.body.className = "";
    renderAppShell();

    loadProfileMock.mockResolvedValueOnce(baseStoredProfile({
      syncLocations: [photos, documents],
      activeLocationId: "loc-2",
    }));
    listSyncLocationsMock.mockResolvedValueOnce([photos, documents]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    expect((document.getElementById("active-location-select") as HTMLSelectElement).value).toBe("loc-2");
  });

  it("updates an existing sync location and refreshes the visible wiring", async () => {
    const original = baseSyncLocation("loc-1", "My photos", {
      localFolder: "C:/photos",
      bucket: "photo-bucket",
      region: "us-east-1",
    });
    const updated = {
      ...original,
      label: "Updated photos",
      localFolder: "D:/photos-archive",
      bucket: "archive-bucket",
      region: "eu-west-1",
      credentialProfileId: "cred-1",
    };

    listSyncLocationsMock.mockResolvedValueOnce([original]);
    updateSyncLocationMock.mockResolvedValueOnce(baseStoredProfile({ syncLocations: [updated] }));

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    listItemByText("#locations-list li", "My photos")
      .querySelectorAll<HTMLButtonElement>("button")
      .forEach((button) => {
        if (button.textContent === "Edit") {
          button.click();
        }
      });

    expect((document.getElementById("location-editing-id") as HTMLInputElement).value).toBe("loc-1");
    expect(document.getElementById("location-form-title")?.textContent).toBe("Edit sync location");

    (document.getElementById("location-label-input") as HTMLInputElement).value = "Updated photos";
    (document.getElementById("location-local-folder-input") as HTMLInputElement).value = "D:/photos-archive";
    (document.getElementById("location-bucket-input") as HTMLInputElement).value = "archive-bucket";
    (document.getElementById("location-region-select") as HTMLSelectElement).value = "eu-west-1";
    (document.getElementById("location-credential-select") as HTMLSelectElement).value = "cred-1";

    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(updateSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      id: "loc-1",
      label: "Updated photos",
      localFolder: "D:/photos-archive",
      bucket: "archive-bucket",
      region: "eu-west-1",
      credentialProfileId: "cred-1",
    }));
    expect(document.getElementById("locations-result")?.textContent).toContain('Updated sync location "Updated photos"');
    expect(listItemByText("#locations-list li", "Updated photos").textContent).toContain("D:/photos-archive → archive-bucket");
    expect(Array.from((document.getElementById("active-location-select") as HTMLSelectElement).options).some(
      (option) => option.value === "loc-1" && option.textContent === "Updated photos",
    )).toBe(true);
  });

  it("keeps selected credential UI cleared after deletion and bootstrap reload", async () => {
    const selectedCredential = baseCredential();
    const storedWithSelection = baseStoredProfile({
      credentialProfileId: selectedCredential.id,
      selectedCredential,
      selectedCredentialAvailable: true,
      credentialsStoredSecurely: true,
    });
    const storedAfterDelete = baseStoredProfile();

    loadProfileMock.mockResolvedValueOnce(storedWithSelection);
    listCredentialsMock.mockResolvedValueOnce([selectedCredential]).mockResolvedValueOnce([]);
    deleteCredentialMock.mockResolvedValueOnce({
      deleted: true,
      profile: storedAfterDelete,
    } satisfies DeleteCredentialResult);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    expect(Array.from(document.querySelectorAll("#credentials-list li .badge")).some(
      (badge) => badge.textContent === "selected",
    )).toBe(true);

    listItemByText("#credentials-list li", "Primary")
      .querySelectorAll<HTMLButtonElement>("button")
      .forEach((button) => {
        if (button.textContent === "Delete") {
          button.click();
        }
      });
    await flushTasks();

    expect(deleteCredentialMock).toHaveBeenCalledWith("cred-1");
    expect(Array.from(document.querySelectorAll("#credentials-list li .badge")).some(
      (badge) => badge.textContent === "selected",
    )).toBe(false);
    expect(document.getElementById("credentials-empty-state")?.hidden).toBe(false);

    cleanup?.();
    cleanup = undefined;
    document.body.className = "";
    renderAppShell();

    loadProfileMock.mockResolvedValueOnce(storedAfterDelete);
    listCredentialsMock.mockResolvedValueOnce([selectedCredential]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-credentials']")?.click();
    expect(Array.from(document.querySelectorAll("#credentials-list li .badge")).some(
      (badge) => badge.textContent === "selected",
    )).toBe(false);
    expect(listItemByText("#credentials-list li", "Primary").textContent).not.toContain("Selected for this bucket");
  });

  it("renders selected location status instead of aggregate status", async () => {
    const locations = [
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
      {
        id: "loc-2",
        label: "Documents",
        localFolder: "C:/docs",
        region: "eu-west-1",
        bucket: "doc-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: false,
        pollIntervalSeconds: 120,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ];
    const locationStatuses: LocationSyncStatus[] = [
      {
        pairId: "loc-1",
        pairLabel: "My photos",
        phase: "polling",
        lastSyncAt: "2026-04-04T12:00:00.000Z",
        lastRescanAt: null,
        lastRemoteRefreshAt: null,
        lastError: null,
        currentFolder: "C:/photos",
        currentBucket: "photo-bucket",
        currentPrefix: "",
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        pendingOperations: 0,
        indexedFileCount: 5,
        indexedDirectoryCount: 1,
        indexedTotalBytes: 1024,
        remoteObjectCount: 7,
        remoteTotalBytes: 2048,
        stats: {
          exactMatchCount: 4,
          localOnlyCount: 1,
          remoteOnlyCount: 1,
          sizeMismatchCount: 1,
          uploadPendingCount: 0,
          downloadPendingCount: 0,
          conflictPendingCount: 0,
        },
        comparison: {
          comparedAt: "2026-04-04T12:00:00.000Z",
          localFileCount: 5,
          remoteObjectCount: 7,
          exactMatchCount: 4,
          localOnlyCount: 1,
          remoteOnlyCount: 1,
          sizeMismatchCount: 1,
        },
        plan: {
          lastPlannedAt: null,
          observedPathCount: 5,
          uploadCount: 1,
          downloadCount: 1,
          conflictCount: 1,
          noopCount: 4,
          pendingOperationCount: 3,
          credentialsAvailable: true,
        },
      },
      {
        pairId: "loc-2",
        pairLabel: "Documents",
        phase: "paused",
        lastSyncAt: "2026-04-04T12:10:00.000Z",
        lastRescanAt: null,
        lastRemoteRefreshAt: null,
        lastError: null,
        currentFolder: "C:/docs",
        currentBucket: "doc-bucket",
        currentPrefix: "",
        enabled: true,
        remotePollingEnabled: false,
        pollIntervalSeconds: 120,
        pendingOperations: 0,
        indexedFileCount: 9,
        indexedDirectoryCount: 3,
        indexedTotalBytes: 4096,
        remoteObjectCount: 10,
        remoteTotalBytes: 5120,
        stats: {
          exactMatchCount: 8,
          localOnlyCount: 1,
          remoteOnlyCount: 0,
          sizeMismatchCount: 1,
          uploadPendingCount: 0,
          downloadPendingCount: 0,
          conflictPendingCount: 0,
        },
        comparison: {
          comparedAt: "2026-04-04T12:10:00.000Z",
          localFileCount: 9,
          remoteObjectCount: 10,
          exactMatchCount: 8,
          localOnlyCount: 1,
          remoteOnlyCount: 0,
          sizeMismatchCount: 1,
        },
        plan: {
          lastPlannedAt: null,
          observedPathCount: 9,
          uploadCount: 1,
          downloadCount: 0,
          conflictCount: 1,
          noopCount: 8,
          pendingOperationCount: 2,
          credentialsAvailable: true,
        },
      },
    ];

    listSyncLocationsMock.mockResolvedValueOnce(locations);
    getSyncStatusMock.mockResolvedValueOnce({
      ...baseUnconfiguredStatus(),
      locations: locationStatuses,
    });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    const topBadge = document.getElementById("sync-phase-badge");
    const summary = document.getElementById("status-summary");
    const windowSubtitle = document.getElementById("window-subtitle");
    const local = document.getElementById("status-overview-local");
    const remote = document.getElementById("status-overview-remote");
    const inSync = document.getElementById("status-overview-in-sync");
    const notInSync = document.getElementById("status-overview-not-in-sync");

    expect(select.value).toBe("loc-1");
    expect(topBadge?.textContent).toBe("Polling");
    expect(summary?.textContent).toContain("monitoring this folder and checking the bucket every 60s");
    expect(windowSubtitle?.textContent).toContain("monitoring this folder and checking the bucket every 60s");
    expect(local?.textContent).toBe("5");
    expect(remote?.textContent).toBe("7");
    expect(inSync?.textContent).toBe("4");
    expect(notInSync?.textContent).toBe("3");

    select.value = "loc-2";
    select.dispatchEvent(new Event("change", { bubbles: true }));

    expect(topBadge?.textContent).toBe("Paused");
    expect(summary?.textContent).toBe("Setup is saved, but automatic sync work is paused right now.");
    expect(windowSubtitle?.textContent).toBe("Setup is saved, but automatic sync work is paused right now.");
    expect(local?.textContent).toBe("9");
    expect(remote?.textContent).toBe("10");
    expect(inSync?.textContent).toBe("8");
    expect(notInSync?.textContent).toBe("2");

    select.value = "";
    select.dispatchEvent(new Event("change", { bubbles: true }));

    expect(summary?.textContent).toBe("Choose a folder, bucket target, and saved credential, then run Connect and sync.");
    expect(local?.textContent).toBe("0");
    expect(remote?.textContent).toBe("0");
    expect(inSync?.textContent).toBe("0");
    expect(notInSync?.textContent).toBe("0");
  });

  it("renders actionable selected-location error copy when backend error text is generic", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ]);
    getSyncStatusMock.mockResolvedValueOnce({
      ...baseConnectedStatus(),
      locations: [
        {
          pairId: "loc-1",
          pairLabel: "My photos",
          phase: "error",
          lastSyncAt: "2026-04-04T12:00:00.000Z",
          lastRescanAt: null,
          lastRemoteRefreshAt: null,
          lastError: "Error",
          currentFolder: "C:/photos",
          currentBucket: "photo-bucket",
          currentPrefix: "",
          enabled: true,
          remotePollingEnabled: true,
          pollIntervalSeconds: 60,
          pendingOperations: 0,
          indexedFileCount: 5,
          indexedDirectoryCount: 1,
          indexedTotalBytes: 1024,
          remoteObjectCount: 7,
          remoteTotalBytes: 2048,
          stats: {
            exactMatchCount: 4,
            localOnlyCount: 1,
            remoteOnlyCount: 1,
            sizeMismatchCount: 1,
            uploadPendingCount: 0,
            downloadPendingCount: 0,
            conflictPendingCount: 0,
          },
          comparison: {
            comparedAt: "2026-04-04T12:00:00.000Z",
            localFileCount: 5,
            remoteObjectCount: 7,
            exactMatchCount: 4,
            localOnlyCount: 1,
            remoteOnlyCount: 1,
            sizeMismatchCount: 1,
          },
          plan: {
            lastPlannedAt: null,
            observedPathCount: 5,
            uploadCount: 1,
            downloadCount: 1,
            conflictCount: 1,
            noopCount: 4,
            pendingOperationCount: 3,
            credentialsAvailable: true,
          },
        },
      ],
    });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const topBadge = document.getElementById("sync-phase-badge");
    const summary = document.getElementById("status-summary");
    const windowSubtitle = document.getElementById("window-subtitle");

    expect(topBadge?.textContent).toBe("Error");
    expect(summary?.textContent).toBe("Sync failed. Open Activity for details.");
    expect(windowSubtitle?.textContent).toBe("Sync failed. Open Activity for details.");
  });

  it("renders file tree when location is selected and file entries exist", async () => {
    const locations = [
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ];

    listSyncLocationsMock.mockResolvedValueOnce(locations);
    listFileEntriesMock.mockResolvedValue([
      fileEntry({ path: "photos/img001.jpg" }),
      fileEntry({ path: "photos/img002.jpg", status: "local-only" }),
      fileEntry({ path: "readme.txt", status: "remote-only", hasLocalCopy: false }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    // The dropdown should auto-select the first location
    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(select.value).toBe("loc-1");

    // The file tree should be populated (not hidden)
    const fileTree = document.getElementById("file-tree") as HTMLUListElement;
    const emptyState = document.getElementById("file-tree-empty-state") as HTMLElement;

    expect(emptyState.hidden).toBe(true);
    expect(fileTree.hidden).toBe(false);
    expect(fileTree.querySelectorAll(".tree-item").length).toBeGreaterThan(0);

    // Check that bindCheckboxTree was called
    expect(bindCheckboxTreeMock).toHaveBeenCalledWith(
      expect.objectContaining({ el: fileTree }),
    );
  });

  it("renders explicit empty directories in the file tree", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ]);
    listFileEntriesMock.mockResolvedValue([
      directoryEntry({ path: "photos/empty-folder" }),
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const fileTree = document.getElementById("file-tree") as HTMLUListElement;
    expect(fileTree.querySelector('.tree-item[data-value="photos/empty-folder"]')).not.toBeNull();
  });

  it("ignores directory entries when toggling local copy actions", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      {
        id: "loc-1",
        label: "My photos",
        localFolder: "C:/photos",
        region: "us-east-1",
        bucket: "photo-bucket",
        credentialProfileId: null,
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        conflictStrategy: "preserve-both",
        deleteSafetyHours: 24,
      },
    ]);
    listFileEntriesMock.mockResolvedValue([
      directoryEntry({ path: "photos/empty-folder", hasLocalCopy: false }),
      fileEntry({ path: "photos/img001.jpg", hasLocalCopy: false }),
      fileEntry({ path: "photos/img002.jpg", hasLocalCopy: true }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    vi.useFakeTimers();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    const onChange = latestBindCall?.[0].onChange;
    expect(onChange).toBeTypeOf("function");

    onChange?.(["photos/empty-folder", "photos/img001.jpg"]);
    await vi.advanceTimersByTimeAsync(500);
    await Promise.resolve();
    await Promise.resolve();

    expect(toggleLocalCopyMock).toHaveBeenCalledTimes(2);
    expect(toggleLocalCopyMock).toHaveBeenNthCalledWith(1, "loc-1", ["photos/img001.jpg"], true);
    expect(toggleLocalCopyMock).toHaveBeenNthCalledWith(2, "loc-1", ["photos/img002.jpg"], false);

    vi.useRealTimers();
  });
});
