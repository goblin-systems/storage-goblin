import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { CredentialSummary, DeleteCredentialResult, LocationSyncStatus, StoredStorageProfile, SyncLocation, SyncStatus } from "./types";
import type { FileEntry } from "./file-tree";

type ModalBackdrop = HTMLElement & { __onClose?: () => void };

const {
  closeModalMock,
  changeStorageClassMock,
  confirmModalMock,
  chooseLocalFolderMock,
  createCredentialMock,
  deleteFileMock,
  deleteFolderMock,
  deleteCredentialMock,
  getSyncStatusMock,
  listCredentialsMock,
  loadProfileMock,
  listSyncLocationsMock,
  addSyncLocationMock,
  updateSyncLocationMock,
  removeSyncLocationMock,
  listFileEntriesMock,
  listBinEntriesMock,
  revealTreeEntryMock,
  restoreBinEntryMock,
  restoreBinEntriesMock,
  purgeBinEntriesMock,
  prepareConflictComparisonMock,
  openPathMock,
  resolveConflictMock,
  bindCheckboxTreeMock,
  openActivityDebugLogFolderMock,
  openModalMock,
  saveProfileSettingsMock,
  showToastMock,
  testCredentialMock,
  toggleLocalCopyMock,
} = vi.hoisted(() => ({
  changeStorageClassMock: vi.fn(),
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
  deleteFileMock: vi.fn(),
  deleteFolderMock: vi.fn(),
  deleteCredentialMock: vi.fn(),
  getSyncStatusMock: vi.fn(),
  listCredentialsMock: vi.fn(),
  loadProfileMock: vi.fn(),
  listSyncLocationsMock: vi.fn(),
  addSyncLocationMock: vi.fn(),
  updateSyncLocationMock: vi.fn(),
  removeSyncLocationMock: vi.fn(),
  listFileEntriesMock: vi.fn(),
  listBinEntriesMock: vi.fn(),
  revealTreeEntryMock: vi.fn(),
  restoreBinEntryMock: vi.fn(),
  restoreBinEntriesMock: vi.fn(),
  purgeBinEntriesMock: vi.fn(),
  prepareConflictComparisonMock: vi.fn(),
  openPathMock: vi.fn(),
  resolveConflictMock: vi.fn(),
  openActivityDebugLogFolderMock: vi.fn(),
  saveProfileSettingsMock: vi.fn(),
  showToastMock: vi.fn(),
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
  showToast: showToastMock,
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
    listBinEntries: listBinEntriesMock,
    revealTreeEntry: revealTreeEntryMock,
    toggleLocalCopy: toggleLocalCopyMock,
    deleteFile: deleteFileMock,
    deleteFolder: deleteFolderMock,
    restoreBinEntry: restoreBinEntryMock,
    restoreBinEntries: restoreBinEntriesMock,
    purgeBinEntries: purgeBinEntriesMock,
    prepareConflictComparison: prepareConflictComparisonMock,
    openPath: openPathMock,
    resolveConflict: resolveConflictMock,
    addSyncLocation: addSyncLocationMock,
    updateSyncLocation: updateSyncLocationMock,
    removeSyncLocation: removeSyncLocationMock,
    changeStorageClass: changeStorageClassMock,
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
    objectVersioningEnabled: false,
    enabled: true,
    remotePollingEnabled: true,
    pollIntervalSeconds: 60,
    conflictStrategy: "preserve-both",
    remoteBin: {
      enabled: true,
      retentionDays: 7,
    },
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

function createDeferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function listItemByText(selector: string, text: string): HTMLElement {
  const item = Array.from(document.querySelectorAll<HTMLElement>(selector))
    .find((element) => element.textContent?.includes(text));

  expect(item).toBeTruthy();
  return item as HTMLElement;
}

function getAsyncConfirmModal(): HTMLElement | null {
  return document.querySelector<HTMLElement>(".storage-async-confirm-modal");
}

function getAsyncConfirmMessage(): HTMLElement | null {
  return getAsyncConfirmModal()?.querySelector<HTMLElement>(".modal-body-text") ?? null;
}

function getVisibleAsyncConfirmModals(): HTMLElement[] {
  return Array.from(document.querySelectorAll<HTMLElement>(".storage-async-confirm-modal:not([hidden])"));
}

function getAsyncConfirmAcceptButton(): HTMLButtonElement {
  const button = getAsyncConfirmModal()?.querySelector<HTMLButtonElement>(".modal-btn-accept");
  expect(button).toBeTruthy();
  return button as HTMLButtonElement;
}

function getAsyncConfirmAcceptSpinner(): HTMLElement {
  const spinner = getAsyncConfirmAcceptButton().querySelector<HTMLElement>(".modal-btn-spinner");
  expect(spinner).toBeTruthy();
  return spinner as HTMLElement;
}

function getAsyncConfirmAcceptBusyText(): HTMLElement {
  const busyText = getAsyncConfirmAcceptButton().querySelector<HTMLElement>(".modal-btn-busy-text");
  expect(busyText).toBeTruthy();
  return busyText as HTMLElement;
}

function getAsyncConfirmRejectButton(): HTMLButtonElement {
  const button = getAsyncConfirmModal()?.querySelector<HTMLButtonElement>(".modal-footer .secondary-btn");
  expect(button).toBeTruthy();
  return button as HTMLButtonElement;
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
    showToastMock.mockReset();
    loadProfileMock.mockReset().mockResolvedValue(baseStoredProfile());
    saveProfileSettingsMock.mockReset().mockResolvedValue({});
    toggleLocalCopyMock.mockReset().mockResolvedValue(undefined);
    deleteFileMock.mockReset().mockResolvedValue(undefined);
    deleteFolderMock.mockReset().mockResolvedValue(undefined);
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
    listBinEntriesMock.mockReset().mockResolvedValue([]);
    revealTreeEntryMock.mockReset().mockResolvedValue(undefined);
    restoreBinEntryMock.mockReset().mockResolvedValue(undefined);
    restoreBinEntriesMock.mockReset().mockResolvedValue({ results: [] });
    purgeBinEntriesMock.mockReset().mockResolvedValue({ results: [] });
    prepareConflictComparisonMock.mockReset().mockResolvedValue({
      locationId: "loc-1",
      path: "photos/conflict.txt",
      mode: "external",
      localPath: "C:/my-photos/photos/conflict.txt",
      remoteTempPath: "C:/temp/photos-conflict-remote.txt",
      localText: null,
      remoteText: null,
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: null,
    });
    openPathMock.mockReset().mockResolvedValue(undefined);
    resolveConflictMock.mockReset().mockResolvedValue(undefined);
    addSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    updateSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    removeSyncLocationMock.mockReset().mockResolvedValue({ syncLocations: [] });
    changeStorageClassMock.mockReset().mockResolvedValue(undefined);
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

    getAsyncConfirmAcceptButton().click();
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

    getAsyncConfirmAcceptButton().click();
    await flushTasks();

    const selectAfterDelete = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(removeSyncLocationMock).toHaveBeenCalledWith("loc-1");
    expect(Array.from(selectAfterDelete.options).map((option) => option.value)).toEqual(["", "live:loc-2", "bin:loc-2"]);
    expect(selectAfterDelete.value).toBe("live:loc-2");
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
    expect(Array.from(selectAfterReload.options).map((option) => option.value)).toEqual(["", "live:loc-2", "bin:loc-2"]);
    expect(selectAfterReload.value).toBe("live:loc-2");
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
      remoteBin: { enabled: true, retentionDays: 7 },
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
    (document.getElementById("location-conflict-strategy-select") as HTMLSelectElement).value = "prefer-local";

    // Click create
    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(addSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      id: null,
      label: "My photos",
      localFolder: "C:/photos",
      bucket: "photo-bucket",
      region: "us-east-1",
      conflictStrategy: "prefer-local",
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
    }));
    expect(document.getElementById("locations-result")?.textContent).toContain('Created sync location "My photos"');
    expect(document.getElementById("locations-count-badge")?.textContent).toBe("1 sync location");
  });

  it("exposes all conflict strategies in settings and sync location forms", async () => {
    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const settingsOptions = Array.from(
      (document.getElementById("conflict-strategy-select") as HTMLSelectElement).options,
    ).map((option) => option.value);
    const locationOptions = Array.from(
      (document.getElementById("location-conflict-strategy-select") as HTMLSelectElement).options,
    ).map((option) => option.value);

    expect(settingsOptions).toEqual(["preserve-both", "prefer-local", "prefer-remote"]);
    expect(locationOptions).toEqual(["preserve-both", "prefer-local", "prefer-remote"]);
  });

  it("saves the selected default conflict strategy", async () => {
    saveProfileSettingsMock.mockImplementation(async (profile: StoredStorageProfile) => profile);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    (document.getElementById("conflict-strategy-select") as HTMLSelectElement).value = "prefer-remote";
    document.getElementById("save-settings-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(saveProfileSettingsMock).toHaveBeenCalledWith(expect.objectContaining({
      conflictStrategy: "prefer-remote",
    }));
  });

  it("uses the saved default conflict strategy when resetting the new location form", async () => {
    saveProfileSettingsMock.mockImplementation(async (profile: StoredStorageProfile) => profile);
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    (document.getElementById("conflict-strategy-select") as HTMLSelectElement).value = "prefer-remote";
    document.getElementById("save-settings-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    listItemByText("#locations-list li", "My photos")
      .querySelectorAll<HTMLButtonElement>("button")
      .forEach((button) => {
        if (button.textContent === "Edit") {
          button.click();
        }
      });

    expect((document.getElementById("location-conflict-strategy-select") as HTMLSelectElement).value).toBe("preserve-both");

    document.getElementById("cancel-edit-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect((document.getElementById("location-conflict-strategy-select") as HTMLSelectElement).value).toBe("prefer-remote");
  });

  it("defaults new sync locations to a 7 day remote bin retention and shows that default clearly", async () => {
    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();

    const retentionInput = document.getElementById("location-remote-bin-retention-input") as HTMLInputElement;
    const retentionHint = retentionInput.parentElement?.querySelector("small.hint");

    expect(retentionInput.value).toBe("7");
    expect(retentionHint?.textContent).toContain("Default is 7 days");
  });

  it("shows remote bin controls in the sync location form and updates helper copy", async () => {
    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();

    const enabledInput = document.getElementById("location-remote-bin-enabled-input") as HTMLInputElement;
    const retentionInput = document.getElementById("location-remote-bin-retention-input") as HTMLInputElement;
    const hint = document.getElementById("location-remote-bin-hint");

    expect(document.getElementById("location-delete-safety-input")).toBeNull();
    expect(enabledInput.checked).toBe(true);
    expect(retentionInput.value).toBe("7");
    expect(hint?.textContent).toContain("moves the remote object into the remote bin for 7 days");

    enabledInput.checked = false;
    enabledInput.dispatchEvent(new Event("change", { bubbles: true }));

    expect(retentionInput.disabled).toBe(true);
    expect(hint?.textContent).toContain("permanently deletes the remote object");
  });

  it("defaults new sync locations to object versioning disabled", async () => {
    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();

    const objectVersioningInput = document.getElementById("location-object-versioning-enabled-input") as HTMLInputElement;
    expect(objectVersioningInput.checked).toBe(false);
  });

  it("disables remote bin controls when object versioning is enabled", async () => {
    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();

    const objectVersioningInput = document.getElementById("location-object-versioning-enabled-input") as HTMLInputElement;
    const enabledInput = document.getElementById("location-remote-bin-enabled-input") as HTMLInputElement;
    const retentionInput = document.getElementById("location-remote-bin-retention-input") as HTMLInputElement;
    const hint = document.getElementById("location-remote-bin-hint");

    objectVersioningInput.checked = true;
    objectVersioningInput.dispatchEvent(new Event("change", { bubbles: true }));

    expect(enabledInput.checked).toBe(false);
    expect(enabledInput.disabled).toBe(true);
    expect(retentionInput.disabled).toBe(true);
    expect(hint?.textContent).toContain("Remote bin is unavailable in this mode");
  });

  it("round-trips object versioning and disables remote bin in saved drafts", async () => {
    addSyncLocationMock.mockResolvedValueOnce({
      syncLocations: [baseSyncLocation("loc-1", "My photos", {
        objectVersioningEnabled: true,
        remoteBin: { enabled: false, retentionDays: 7 },
      })],
    });

    cleanup = await bootstrapStorageGoblin();

    document.querySelector<HTMLElement>("[data-nav-id='nav-locations']")?.click();
    (document.getElementById("location-label-input") as HTMLInputElement).value = "My photos";
    (document.getElementById("location-local-folder-input") as HTMLInputElement).value = "C:/photos";
    (document.getElementById("location-bucket-input") as HTMLInputElement).value = "photo-bucket";
    (document.getElementById("location-region-select") as HTMLSelectElement).value = "us-east-1";
    (document.getElementById("location-object-versioning-enabled-input") as HTMLInputElement).checked = true;
    (document.getElementById("location-object-versioning-enabled-input") as HTMLInputElement).dispatchEvent(new Event("change", { bubbles: true }));

    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(addSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      objectVersioningEnabled: true,
      remoteBin: {
        enabled: false,
        retentionDays: 7,
      },
    }));
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
    expect(sentDraft).not.toHaveProperty("deleteSafetyHours");
    expect(sentDraft).toHaveProperty("remoteBin");
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
        remoteBin: { enabled: true, retentionDays: 7 },
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
        remoteBin: { enabled: true, retentionDays: 7 },
      },
    ]);

    cleanup = await bootstrapStorageGoblin();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(select).toBeTruthy();
    // Default option + live/bin variants for 2 locations = 5 options
    expect(select.options.length).toBe(5);
    // First location should be auto-selected
    expect(select.value).toBe("live:loc-1");
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
    select.value = "live:loc-2";
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

    expect((document.getElementById("active-location-select") as HTMLSelectElement).value).toBe("live:loc-2");
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
    (document.getElementById("location-conflict-strategy-select") as HTMLSelectElement).value = "prefer-remote";

    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(updateSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      id: "loc-1",
      label: "Updated photos",
      localFolder: "D:/photos-archive",
      bucket: "archive-bucket",
      region: "eu-west-1",
      credentialProfileId: "cred-1",
      conflictStrategy: "prefer-remote",
      remoteBin: {
        enabled: true,
        retentionDays: 7,
      },
    }));
    expect(document.getElementById("locations-result")?.textContent).toContain('Updated sync location "Updated photos"');
    expect(listItemByText("#locations-list li", "Updated photos").textContent).toContain("D:/photos-archive → archive-bucket");
    expect(Array.from((document.getElementById("active-location-select") as HTMLSelectElement).options).some(
      (option) => option.value === "live:loc-1" && option.textContent === "Updated photos",
    )).toBe(true);
  });

  it("round-trips edited remote bin retention values through the sync location form", async () => {
    const original = baseSyncLocation("loc-1", "My photos", {
      localFolder: "C:/photos",
      bucket: "photo-bucket",
      remoteBin: { enabled: true, retentionDays: 14 },
    });
    const updated = {
      ...original,
      remoteBin: { enabled: true, retentionDays: 30 },
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

    const retentionInput = document.getElementById("location-remote-bin-retention-input") as HTMLInputElement;
    expect(retentionInput.value).toBe("14");

    retentionInput.value = "30";
    retentionInput.dispatchEvent(new Event("input", { bubbles: true }));

    document.getElementById("save-location-btn")?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    await flushTasks();

    expect(updateSyncLocationMock).toHaveBeenCalledWith(expect.objectContaining({
      id: "loc-1",
      remoteBin: {
        enabled: true,
        retentionDays: 30,
      },
    }));
    expect(listItemByText("#locations-list li", "My photos").textContent).toContain("remote bin 30d");
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

    getAsyncConfirmAcceptButton().click();
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
        remoteBin: { enabled: true, retentionDays: 7 },
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
        remoteBin: { enabled: true, retentionDays: 14 },
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
    const localLabel = document.getElementById("status-overview-local-label");
    const local = document.getElementById("status-overview-local");
    const remoteLabel = document.getElementById("status-overview-remote-label");
    const remote = document.getElementById("status-overview-remote");
    const inSyncLabel = document.getElementById("status-overview-in-sync-label");
    const inSync = document.getElementById("status-overview-in-sync");
    const notInSyncLabel = document.getElementById("status-overview-not-in-sync-label");
    const notInSync = document.getElementById("status-overview-not-in-sync");

    expect(select.value).toBe("live:loc-1");
    expect(topBadge?.textContent).toBe("Polling");
    expect(summary?.textContent).toContain("monitoring this folder and checking the bucket every 60s");
    expect(windowSubtitle?.textContent).toContain("monitoring this folder and checking the bucket every 60s");
    expect(localLabel?.textContent).toBe("Local");
    expect(local?.textContent).toBe("5");
    expect(remoteLabel?.textContent).toBe("Remote");
    expect(remote?.textContent).toBe("7");
    expect(inSyncLabel?.textContent).toBe("In sync");
    expect(inSync?.textContent).toBe("4");
    expect(notInSyncLabel?.textContent).toBe("Changes");
    expect(notInSync?.textContent).toBe("3");

    select.value = "live:loc-2";
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
        remoteBin: { enabled: true, retentionDays: 7 },
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
        remoteBin: { enabled: true, retentionDays: 7 },
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
    expect(select.value).toBe("live:loc-1");

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

  it("opens live view successfully when backend returns review-required entries", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/review.jpg", status: "review-required", hasLocalCopy: false }),
      fileEntry({ path: "photos/synced.jpg", status: "synced", hasLocalCopy: true }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const fileTree = document.getElementById("file-tree") as HTMLUListElement;
    const reviewRow = fileTree.querySelector('.tree-item[data-value="photos/review.jpg"]');
    const reviewCheckbox = reviewRow?.querySelector<HTMLInputElement>(".tree-check");
    const reviewIndicator = reviewRow?.querySelector<HTMLElement>(".status-indicator");

    expect(fileTree.hidden).toBe(false);
    expect(reviewRow).not.toBeNull();
    expect(reviewIndicator?.getAttribute("title")).toBe("Requires review before syncing changes");
    expect(reviewCheckbox?.disabled).toBe(true);
    expect(reviewCheckbox?.checked).toBe(false);
    expect(document.getElementById("status-overview-not-in-sync-label")?.textContent).toBe("Changes");
    expect(document.getElementById("status-overview-not-in-sync")?.textContent).toBe("1");
  });

  it("shows both live and bin variants for each location in the dropdown", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
      baseSyncLocation("loc-2", "Documents"),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    const options = Array.from(select.options).map((option) => ({ value: option.value, text: option.textContent }));

    expect(options).toEqual([
      { value: "", text: "Select a sync location" },
      { value: "live:loc-1", text: "My photos" },
      { value: "bin:loc-1", text: "My photos Bin" },
      { value: "live:loc-2", text: "Documents" },
      { value: "bin:loc-2", text: "Documents Bin" },
    ]);
  });

  it("loads bin entries and shows restore actions when bin view is selected", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/deleted.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(saveProfileSettingsMock).toHaveBeenCalledWith(expect.objectContaining({ activeLocationId: "loc-1" }));
    expect(listBinEntriesMock).toHaveBeenCalledWith("loc-1");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(1);

    const fileTreeSection = document.getElementById("file-tree-section") as HTMLElement;
    expect(fileTreeSection.classList.contains("is-bin-view")).toBe(true);
    expect(document.querySelector(".tree-restore-btn")?.textContent).toBe("Restore");
    expect((document.getElementById("bin-toolbar") as HTMLElement).hidden).toBe(false);
    expect(document.querySelector(".tree-delete-btn")).toBeNull();
    expect(document.querySelector(".status-indicator.error")).not.toBeNull();
    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Bin items");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("1");
    expect(document.getElementById("status-overview-remote-label")?.textContent).toBe("Retention");
    expect(document.getElementById("status-overview-remote")?.textContent).toBe("7d");
    expect(document.getElementById("status-overview-in-sync-label")?.textContent).toBe("Live phase");
    expect(document.getElementById("status-overview-not-in-sync-label")?.textContent).toBe("Pending");
  });

  it("supports folder restore and lifecycle visibility in bin mode", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      {
        path: "photos/2026",
        kind: "directory",
        status: "deleted",
        hasLocalCopy: false,
        deletedFrom: "object-versioning",
        deletedAt: "2026-04-12T08:00:00.000Z",
        expiresAt: null,
        retentionDays: 30,
      },
    ] satisfies FileEntry[]);
    restoreBinEntriesMock.mockResolvedValueOnce({ results: [] });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(document.querySelector('.tree-item[data-value="photos/2026"] .tree-restore-btn')).not.toBeNull();
    expect(document.querySelector('.tree-item[data-value="photos/2026"] .tree-bin-lifecycle')?.textContent).toContain("Object versioning");

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/2026"] .tree-restore-btn')?.click();
    await flushTasks();
    await flushTasks();

    expect(restoreBinEntriesMock).toHaveBeenCalledWith("loc-1", [{ path: "photos/2026", kind: "directory", binKey: null }]);
  });

  it("supports bulk restore and purge from bin selection", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", { objectVersioningEnabled: true }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValue([
      fileEntry({ path: "photos/a.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-a", deletedFrom: "object-versioning" }),
      fileEntry({ path: "photos/b.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-b", deletedFrom: "object-versioning" }),
    ]);
    restoreBinEntriesMock.mockResolvedValue({ results: [] });
    purgeBinEntriesMock.mockResolvedValue({ results: [] });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    latestBindCall?.[0].onChange?.(["photos/a.jpg", "photos/b.jpg"]);
    await flushTasks();

    expect(document.getElementById("bin-selection-summary")?.textContent).toContain("2 bin entries selected");

    document.getElementById("restore-selected-btn")?.dispatchEvent(new Event("click", { bubbles: true }));
    await flushTasks();
    await flushTasks();

    expect(restoreBinEntriesMock).toHaveBeenCalledWith("loc-1", [
      { path: "photos/a.jpg", kind: "file", binKey: "bin-a" },
      { path: "photos/b.jpg", kind: "file", binKey: "bin-b" },
    ]);

    latestBindCall?.[0].onChange?.(["photos/a.jpg", "photos/b.jpg"]);
    await flushTasks();

    document.getElementById("purge-selected-btn")?.dispatchEvent(new Event("click", { bubbles: true }));
    await flushTasks();
    expect(getAsyncConfirmMessage()?.textContent).toContain("permanently deletes the selected object versions");
    getAsyncConfirmAcceptButton().click();
    await flushTasks();
    await flushTasks();

    expect(purgeBinEntriesMock).toHaveBeenCalledWith("loc-1", [
      { path: "photos/a.jpg", kind: "file", binKey: "bin-a" },
      { path: "photos/b.jpg", kind: "file", binKey: "bin-b" },
    ]);
  });

  it("keeps failed bulk restore selections and reports partial mutation results", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValue([
      fileEntry({ path: "photos/a.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-a", deletedFrom: "remote-bin" }),
      fileEntry({ path: "photos/b.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-b", deletedFrom: "remote-bin" }),
    ]);
    restoreBinEntriesMock.mockResolvedValueOnce({
      results: [
        { path: "photos/a.jpg", kind: "file", binKey: "bin-a", success: true, affectedCount: 1, error: null },
        { path: "photos/b.jpg", kind: "file", binKey: "bin-b", success: false, affectedCount: 0, error: "destination exists" },
      ],
    });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    latestBindCall?.[0].onChange?.(["photos/a.jpg", "photos/b.jpg"]);
    await flushTasks();

    document.getElementById("restore-selected-btn")?.dispatchEvent(new Event("click", { bubbles: true }));
    await flushTasks();
    await flushTasks();
    await new Promise((resolve) => setTimeout(resolve, 200));
    await flushTasks();

    expect(showToastMock).toHaveBeenCalledWith("Restored 1 of 2 bin entries; 1 failed.", "info", 2200, "app-toast");
    expect(document.getElementById("bin-selection-summary")?.textContent).toContain("1 bin entry selected");
    expect(document.getElementById("activity-list")?.textContent).toContain("destination exists");
  });

  it("keeps failed bulk purge selections and reports total failure cleanly", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", { objectVersioningEnabled: true }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValue([
      fileEntry({ path: "photos/a.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-a", deletedFrom: "object-versioning" }),
      fileEntry({ path: "photos/b.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-b", deletedFrom: "object-versioning" }),
    ]);
    purgeBinEntriesMock.mockResolvedValueOnce({
      results: [
        { path: "photos/a.jpg", kind: "file", binKey: "bin-a", success: false, affectedCount: 0, error: "access denied" },
        { path: "photos/b.jpg", kind: "file", binKey: "bin-b", success: false, affectedCount: 0, error: "version locked" },
      ],
    });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    latestBindCall?.[0].onChange?.(["photos/a.jpg", "photos/b.jpg"]);
    await flushTasks();

    document.getElementById("purge-selected-btn")?.dispatchEvent(new Event("click", { bubbles: true }));
    await flushTasks();
    getAsyncConfirmAcceptButton().click();
    await flushTasks();
    await flushTasks();
    await new Promise((resolve) => setTimeout(resolve, 200));
    await flushTasks();

    expect(showToastMock).toHaveBeenCalledWith("Purge failed for 2 bin entries.", "error", 2200, "app-toast");
    expect(document.getElementById("bin-selection-summary")?.textContent).toContain("2 bin entries selected");
    expect(document.getElementById("activity-list")?.textContent).toContain("access denied");
    expect(document.getElementById("activity-list")?.textContent).toContain("version locked");
  });

  it("restores synthetic grouping rows through batch restore", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      {
        path: "photos/2026",
        kind: "directory",
        status: "deleted",
        hasLocalCopy: false,
        deletedFrom: "remote-bin",
      },
      fileEntry({ path: "photos/2026/img001.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-1", deletedFrom: "remote-bin" }),
    ] satisfies FileEntry[]);
    restoreBinEntriesMock.mockResolvedValueOnce({ results: [{ path: "photos/2026", kind: "directory", binKey: null, success: true, affectedCount: 1, error: null }] });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/2026"] .tree-restore-btn')?.click();
    await flushTasks();
    await flushTasks();

    expect(restoreBinEntriesMock).toHaveBeenCalledWith("loc-1", [{ path: "photos/2026", kind: "directory", binKey: null }]);
  });

  it("shows bin retention from the saved selected location config when listed locations are stale", async () => {
    loadProfileMock.mockResolvedValueOnce(baseStoredProfile({
      syncLocations: [
        baseSyncLocation("loc-1", "My photos", {
          remoteBin: { enabled: true, retentionDays: 7 },
        }),
      ],
      activeLocationId: "loc-1",
    }));
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: true, retentionDays: 1 },
      }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(document.getElementById("status-overview-remote-label")?.textContent).toBe("Retention");
    expect(document.getElementById("status-overview-remote")?.textContent).toBe("7d");
  });

  it("restores cached live status metrics when switching from bin back to live", async () => {
    const locationStatuses: LocationSyncStatus[] = [
      {
        pairId: "loc-1",
        pairLabel: "My photos",
        phase: "idle",
        lastSyncAt: "2026-04-04T12:00:00.000Z",
        lastRescanAt: null,
        lastRemoteRefreshAt: null,
        lastError: null,
        currentFolder: "C:/my-photos",
        currentBucket: "photo-bucket",
        currentPrefix: "",
        enabled: true,
        remotePollingEnabled: true,
        pollIntervalSeconds: 60,
        pendingOperations: 0,
        indexedFileCount: 3,
        indexedDirectoryCount: 1,
        indexedTotalBytes: 2048,
        remoteObjectCount: 3,
        remoteTotalBytes: 2048,
        stats: {
          exactMatchCount: 1,
          localOnlyCount: 1,
          remoteOnlyCount: 1,
          sizeMismatchCount: 0,
          uploadPendingCount: 0,
          downloadPendingCount: 0,
          conflictPendingCount: 0,
        },
        comparison: {
          comparedAt: "2026-04-04T12:10:00.000Z",
          localFileCount: 3,
          remoteObjectCount: 3,
          exactMatchCount: 1,
          localOnlyCount: 1,
          remoteOnlyCount: 1,
          sizeMismatchCount: 0,
        },
        plan: {
          lastPlannedAt: null,
          observedPathCount: 3,
          uploadCount: 1,
          downloadCount: 1,
          conflictCount: 0,
          noopCount: 1,
          pendingOperationCount: 2,
          credentialsAvailable: true,
        },
      },
    ];

    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    getSyncStatusMock.mockResolvedValueOnce({
      ...baseUnconfiguredStatus(),
      locations: locationStatuses,
    });
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/a.jpg", status: "synced", hasLocalCopy: true }),
        fileEntry({ path: "photos/b.jpg", status: "local-only", hasLocalCopy: true }),
        fileEntry({ path: "photos/c.jpg", status: "remote-only", hasLocalCopy: false }),
      ])
      .mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    expect(document.getElementById("status-overview-local")?.textContent).toBe("3");
    expect(document.getElementById("status-overview-remote")?.textContent).toBe("3");
    expect(document.getElementById("status-overview-in-sync")?.textContent).toBe("1");
    expect(document.getElementById("status-overview-not-in-sync")?.textContent).toBe("2");

    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Bin items");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("0");

    select.value = "live:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Local");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("3");
    expect(document.getElementById("status-overview-remote")?.textContent).toBe("3");
    expect(document.getElementById("status-overview-in-sync")?.textContent).toBe("1");
    expect(document.getElementById("status-overview-not-in-sync")?.textContent).toBe("2");
  });

  it("does not count cold-storage-only mismatches as live Changes when no work is pending", async () => {
    const locationStatuses: LocationSyncStatus[] = [
      {
        pairId: "loc-1",
        pairLabel: "My photos",
        phase: "idle",
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
        indexedFileCount: 1,
        indexedDirectoryCount: 1,
        indexedTotalBytes: 1024,
        remoteObjectCount: 1,
        remoteTotalBytes: 1024,
        stats: {
          exactMatchCount: 0,
          localOnlyCount: 0,
          remoteOnlyCount: 0,
          sizeMismatchCount: 1,
          uploadPendingCount: 0,
          downloadPendingCount: 0,
          conflictPendingCount: 0,
        },
        comparison: {
          comparedAt: "2026-04-04T12:00:00.000Z",
          localFileCount: 1,
          remoteObjectCount: 1,
          exactMatchCount: 0,
          localOnlyCount: 0,
          remoteOnlyCount: 0,
          sizeMismatchCount: 1,
        },
        plan: {
          lastPlannedAt: "2026-04-04T12:00:00.000Z",
          observedPathCount: 1,
          uploadCount: 0,
          downloadCount: 0,
          conflictCount: 0,
          noopCount: 1,
          pendingOperationCount: 0,
          credentialsAvailable: true,
        },
      },
    ];

    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    getSyncStatusMock.mockResolvedValueOnce({
      ...baseUnconfiguredStatus(),
      locations: locationStatuses,
    });

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Local");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("1");
    expect(document.getElementById("status-overview-remote")?.textContent).toBe("1");
    expect(document.getElementById("status-overview-in-sync")?.textContent).toBe("0");
    expect(document.getElementById("status-overview-not-in-sync-label")?.textContent).toBe("Changes");
    expect(document.getElementById("status-overview-not-in-sync")?.textContent).toBe("0");
  });

  it("renders the bottom status bar metrics without forced wide spacing", async () => {
    const stylesheet = readFileSync(resolve(process.cwd(), "src/styles.css"), "utf8");
    const metricBlock = stylesheet.match(/\.home-status-metric\s*\{[^}]*\}/)?.[0] ?? "";

    expect(metricBlock).toContain("justify-content: flex-start;");
    expect(metricBlock).not.toContain("justify-content: space-between;");
    expect(metricBlock).toContain("gap: var(--space-2);");
  });

  it("ignores stale live responses and keeps status keyed by location plus mode", async () => {
    const liveDeferred = createDeferred<FileEntry[]>();

    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockImplementationOnce(() => liveDeferred.promise);
    listBinEntriesMock.mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(listFileEntriesMock).toHaveBeenCalledTimes(1);
    expect(listBinEntriesMock).toHaveBeenCalledTimes(1);
    expect(document.getElementById("status-phase-inline")?.textContent).toBe("Bin");
    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Bin items");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("0");

    liveDeferred.resolve([
      fileEntry({ path: "photos/from-live.jpg" }),
    ]);
    await flushTasks();
    await flushTasks();

    expect(document.getElementById("status-phase-inline")?.textContent).toBe("Bin");
    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Bin items");
    expect(document.getElementById("status-overview-local")?.textContent).toBe("0");
    expect(document.querySelector(".tree-delete-btn")).toBeNull();
    expect(document.querySelector(".tree-restore-btn")).toBeNull();

    select.value = "live:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(document.getElementById("status-overview-local-label")?.textContent).toBe("Local");
    expect(document.getElementById("status-overview-remote-label")?.textContent).toBe("Remote");
  });

  it("reveals live tree files and folders for the active sync location", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      directoryEntry({ path: "photos" }),
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const revealButtons = document.querySelectorAll<HTMLButtonElement>(".tree-reveal-btn");
    expect(revealButtons.length).toBeGreaterThan(1);

    revealButtons[0]?.click();
    await flushTasks();

    expect(revealTreeEntryMock).toHaveBeenCalledWith("loc-1", expect.any(String));
  });

  it("surfaces reveal errors clearly to the user", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);
    revealTreeEntryMock.mockRejectedValueOnce(new Error("Local path 'C:/my-photos/photos/img001.jpg' does not exist for sync location 'My photos'."));

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-reveal-btn")?.click();
    await flushTasks();

    expect(showToastMock).toHaveBeenCalledWith(
      "Local path 'C:/my-photos/photos/img001.jpg' does not exist for sync location 'My photos'.",
      "error",
      2200,
      "app-toast",
    );
  });

  it("restores bin entries using opaque binKey and surfaces backend errors", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/deleted.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ]);
    restoreBinEntryMock.mockRejectedValueOnce(new Error("restore conflict: destination already exists"));

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/deleted.jpg"] .tree-restore-btn')?.click();
    await flushTasks();
    await new Promise((resolve) => setTimeout(resolve, 200));
    await flushTasks();

    expect(restoreBinEntryMock).toHaveBeenCalledWith("loc-1", "opaque-bin-key");
    expect(document.getElementById("activity-list")?.textContent).toContain("restore conflict: destination already exists");
  });

  it("shows restore button loading state while bin restore is pending and clears it on failure", async () => {
    const restore = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/deleted.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
    ]);
    restoreBinEntryMock.mockImplementationOnce(() => restore.promise.catch((error) => { throw error; }));

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    const restoreButton = document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/deleted.jpg"] .tree-restore-btn');
    expect(restoreButton).toBeTruthy();

    restoreButton!.click();
    await flushTasks();

    expect(restoreBinEntryMock).toHaveBeenCalledTimes(1);
    expect(restoreButton?.classList.contains("is-loading")).toBe(true);
    expect(restoreButton?.disabled).toBe(true);
    expect(restoreButton?.getAttribute("aria-busy")).toBe("true");

    restoreButton!.click();
    expect(restoreBinEntryMock).toHaveBeenCalledTimes(1);

    restore.reject(new Error("restore conflict: destination already exists"));
    await flushTasks();
    await flushTasks();

    expect(restoreButton?.classList.contains("is-loading")).toBe(false);
    expect(restoreButton?.disabled).toBe(false);
    expect(restoreButton?.getAttribute("aria-busy")).toBe("false");
  });

  it("shows delayed file-tree loading indicator for slow view switches without leaving it stuck", async () => {
    vi.useFakeTimers();

    const binDeferred = createDeferred<FileEntry[]>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockImplementationOnce(() => binDeferred.promise);

    cleanup = await bootstrapStorageGoblin();
    await vi.runAllTimersAsync();

    const loadingIndicator = document.getElementById("file-tree-loading-indicator") as HTMLElement;
    const fileTreeSection = document.getElementById("file-tree-section") as HTMLElement;

    expect(loadingIndicator.hidden).toBe(true);

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await Promise.resolve();

    expect(loadingIndicator.hidden).toBe(true);

    await vi.advanceTimersByTimeAsync(149);
    expect(loadingIndicator.hidden).toBe(true);

    await vi.advanceTimersByTimeAsync(150);
    expect(loadingIndicator.hidden).toBe(false);
    expect(fileTreeSection.classList.contains("is-loading-tree")).toBe(true);
    expect(fileTreeSection.getAttribute("aria-busy")).toBe("true");

    binDeferred.resolve([]);
    await vi.runAllTimersAsync();
    await Promise.resolve();
    await Promise.resolve();

    expect(loadingIndicator.hidden).toBe(true);
    expect(fileTreeSection.classList.contains("is-loading-tree")).toBe(false);
    expect(fileTreeSection.getAttribute("aria-busy")).toBe("false");

    vi.useRealTimers();
  });

  it("does not show file-tree loading indicator for effectively instant view switches", async () => {
    vi.useFakeTimers();

    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();
    await vi.runAllTimersAsync();

    const loadingIndicator = document.getElementById("file-tree-loading-indicator") as HTMLElement;
    const fileTreeSection = document.getElementById("file-tree-section") as HTMLElement;

    expect(loadingIndicator.hidden).toBe(true);
    expect(fileTreeSection.classList.contains("is-loading-tree")).toBe(false);

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await Promise.resolve();
    await Promise.resolve();

    expect(loadingIndicator.hidden).toBe(true);

    await vi.advanceTimersByTimeAsync(150);
    expect(loadingIndicator.hidden).toBe(true);
    expect(fileTreeSection.classList.contains("is-loading-tree")).toBe(false);

    vi.useRealTimers();
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
        remoteBin: { enabled: true, retentionDays: 7 },
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
        remoteBin: { enabled: true, retentionDays: 7 },
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

  it("does not toggle local copies for review-required entries", async () => {
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
        remoteBin: { enabled: true, retentionDays: 7 },
      },
    ]);
    listFileEntriesMock.mockResolvedValue([
      fileEntry({ path: "photos/review.jpg", status: "review-required", hasLocalCopy: false }),
      fileEntry({ path: "photos/img001.jpg", status: "remote-only", hasLocalCopy: false }),
      fileEntry({ path: "photos/img002.jpg", status: "synced", hasLocalCopy: true }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    vi.useFakeTimers();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    const onChange = latestBindCall?.[0].onChange;
    expect(onChange).toBeTypeOf("function");

    onChange?.(["photos/review.jpg", "photos/img001.jpg"]);
    await vi.advanceTimersByTimeAsync(500);
    await Promise.resolve();
    await Promise.resolve();

    expect(toggleLocalCopyMock).toHaveBeenCalledTimes(2);
    expect(toggleLocalCopyMock).toHaveBeenNthCalledWith(1, "loc-1", ["photos/img001.jpg"], true);
    expect(toggleLocalCopyMock).toHaveBeenNthCalledWith(2, "loc-1", ["photos/img002.jpg"], false);

    vi.useRealTimers();
  });

  it("refreshes live entries after toggling local copies instead of relying on stale cache", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/img001.jpg", hasLocalCopy: false }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/img001.jpg", hasLocalCopy: true }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    vi.useFakeTimers();

    const latestBindCall = bindCheckboxTreeMock.mock.calls[
      bindCheckboxTreeMock.mock.calls.length - 1
    ] as unknown as [{ onChange?: (paths: string[]) => void }] | undefined;
    const onChange = latestBindCall?.[0].onChange;

    onChange?.(["photos/img001.jpg"]);
    await vi.advanceTimersByTimeAsync(500);
    await Promise.resolve();
    await Promise.resolve();

    expect(toggleLocalCopyMock).toHaveBeenCalledWith("loc-1", ["photos/img001.jpg"], true);
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);

    vi.useRealTimers();
  });

  it("shows resolve controls for conflict rows and disables their checkboxes", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/conflict.txt", status: "conflict", localKind: "file", remoteKind: "file" }),
      fileEntry({ path: "photos/unsupported.txt", status: "conflict", localKind: "file", remoteKind: "directory" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const checkbox = document.querySelector<HTMLInputElement>('.tree-item[data-value="photos/conflict.txt"] .tree-check');
    const resolveButton = document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/conflict.txt"] .tree-resolve-btn');
    const unsupportedResolveButton = document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/unsupported.txt"] .tree-resolve-btn');
    const unsupportedCheckbox = document.querySelector<HTMLInputElement>('.tree-item[data-value="photos/unsupported.txt"] .tree-check');

    expect(checkbox?.disabled).toBe(true);
    expect(resolveButton?.classList.contains("icon-btn")).toBe(true);
    expect(resolveButton?.classList.contains("icon-btn-sm")).toBe(true);
    expect(resolveButton?.textContent).toContain("Resolve");
    expect(resolveButton?.getAttribute("title")).toBe("Resolve file conflict");
    expect(resolveButton?.getAttribute("aria-label")).toBe("Resolve file conflict");
    expect(resolveButton?.querySelector('[data-lucide="triangle-alert"]')).not.toBeNull();
    expect(unsupportedResolveButton).toBeNull();
    expect(unsupportedCheckbox?.disabled).toBe(true);
  });

  it("compares non-inline conflict files by opening local and downloaded remote copies", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "photos/conflict.txt",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
        localSize: 12,
        remoteSize: 14,
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    expect(modal?.hidden).toBe(false);
    expect(modal?.textContent).toContain("photos/conflict.txt");
    expect(modal?.textContent).toContain("ETag");
    expect(modal?.textContent).toContain("Unavailable");

    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();
    await flushTasks();

    expect(prepareConflictComparisonMock).toHaveBeenCalledWith("loc-1", "photos/conflict.txt");
    expect(openPathMock).toHaveBeenNthCalledWith(1, "C:/my-photos/photos/conflict.txt");
    expect(openPathMock).toHaveBeenNthCalledWith(2, "C:/temp/photos-conflict-remote.txt");
  });

  it("renders inline text compare inside the conflict modal", async () => {
    prepareConflictComparisonMock.mockResolvedValueOnce({
      locationId: "loc-1",
      path: "docs/conflict.txt",
      mode: "text",
      localPath: "C:/sync/docs/conflict.txt",
      remoteTempPath: "C:/temp/docs-conflict.txt",
      localText: "local line 1\nlocal line 2",
      remoteText: "remote line 1\nremote line 2",
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: null,
    });
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My docs"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "docs/conflict.txt",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();
    await flushTasks();

    expect(modal?.textContent).toContain("Showing inline text comparison.");
    const panes = modal?.querySelectorAll<HTMLElement>(".storage-conflict-text-pane") ?? [];
    expect(panes).toHaveLength(2);
    expect(panes[0]?.textContent).toContain("local line 1");
    expect(panes[1]?.textContent).toContain("remote line 1");
    expect(openPathMock).not.toHaveBeenCalled();
  });

  it("renders inline image compare inside the conflict modal", async () => {
    prepareConflictComparisonMock.mockResolvedValueOnce({
      locationId: "loc-1",
      path: "photos/conflict.png",
      mode: "image",
      localPath: "C:/sync/photos/conflict.png",
      remoteTempPath: "C:/temp/photos-conflict.png",
      localText: null,
      remoteText: null,
      localImageDataUrl: "data:image/png;base64,AAA",
      remoteImageDataUrl: "data:image/png;base64,BBB",
      fallbackReason: null,
    });
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "photos/conflict.png",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();
    await flushTasks();

    expect(modal?.textContent).toContain("Showing inline image previews.");
    const images = modal?.querySelectorAll<HTMLImageElement>(".storage-conflict-image-preview") ?? [];
    expect(images).toHaveLength(2);
    expect(images[0]?.getAttribute("src")).toBe("data:image/png;base64,AAA");
    expect(images[1]?.getAttribute("src")).toBe("data:image/png;base64,BBB");
    expect(openPathMock).not.toHaveBeenCalled();
  });

  it("keeps the modal coherent while compare data is loading", async () => {
    const deferred = createDeferred<{
      locationId: string;
      path: string;
      mode: "text";
      localPath: string;
      remoteTempPath: string;
      localText: string;
      remoteText: string;
      localImageDataUrl: null;
      remoteImageDataUrl: null;
      fallbackReason: null;
    }>();
    prepareConflictComparisonMock.mockReturnValueOnce(deferred.promise);
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My docs"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "docs/conflict.txt",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();

    expect(modal?.textContent).toContain("Loading conflict comparison…");
    expect(compareButton?.classList.contains("is-loading")).toBe(true);
    expect(compareButton?.getAttribute("aria-busy")).toBe("true");
    expect(compareButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(false);

    deferred.resolve({
      locationId: "loc-1",
      path: "docs/conflict.txt",
      mode: "text",
      localPath: "C:/sync/docs/conflict.txt",
      remoteTempPath: "C:/temp/docs-conflict.txt",
      localText: "local body",
      remoteText: "remote body",
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: null,
    });
    await flushTasks();
    await flushTasks();

    expect(modal?.textContent).toContain("Showing inline text comparison.");
    expect(compareButton?.classList.contains("is-loading")).toBe(false);
    expect(compareButton?.getAttribute("aria-busy")).toBe("false");
    expect(compareButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(true);
  });

  it("shows remote etag in the conflict modal when available", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My docs"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "docs/conflict.txt",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
        remoteEtag: "etag-remote-123",
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    expect(modal?.textContent).toContain("ETag");
    expect(modal?.textContent).toContain("etag-remote-123");
  });

  it("shows a clear inline fallback message when compare reverts to external mode", async () => {
    prepareConflictComparisonMock.mockResolvedValueOnce({
      locationId: "loc-1",
      path: "docs/large.txt",
      mode: "external",
      localPath: "C:/sync/docs/large.txt",
      remoteTempPath: "C:/temp/docs-large.txt",
      localText: null,
      remoteText: null,
      localImageDataUrl: null,
      remoteImageDataUrl: null,
      fallbackReason: "File exceeded the 128 KB inline text compare limit.",
    });
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My docs"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "docs/large.txt",
        status: "conflict",
        localKind: "file",
        remoteKind: "file",
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();
    await flushTasks();

    expect(modal?.textContent).toContain("File exceeded the 128 KB inline text compare limit.");
    expect(openPathMock).toHaveBeenNthCalledWith(1, "C:/sync/docs/large.txt");
    expect(openPathMock).toHaveBeenNthCalledWith(2, "C:/temp/docs-large.txt");
  });

  it("resolves conflicts with keep local and refreshes the live view", async () => {
    const resolution = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    resolveConflictMock.mockReturnValueOnce(resolution.promise);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({
          path: "photos/conflict.txt",
          status: "conflict",
          localKind: "file",
          remoteKind: "file",
        }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/conflict.txt", status: "synced" }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const keepLocalButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Keep local"));
    keepLocalButton?.click();
    await flushTasks();

    expect(keepLocalButton?.classList.contains("is-loading")).toBe(true);
    expect(keepLocalButton?.getAttribute("aria-busy")).toBe("true");
    expect(keepLocalButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(false);

    resolution.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(resolveConflictMock).toHaveBeenCalledWith("loc-1", "photos/conflict.txt", "keep-local");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(showToastMock).toHaveBeenCalledWith(
      "Conflict resolved by keeping the local version.",
      "success",
      2200,
      "app-toast",
    );
  });

  it("resolves conflicts with keep remote and refreshes the live view", async () => {
    const resolution = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    resolveConflictMock.mockReturnValueOnce(resolution.promise);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({
          path: "photos/conflict.txt",
          status: "conflict",
          localKind: "file",
          remoteKind: "file",
        }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/conflict.txt", status: "synced" }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const keepRemoteButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Keep remote"));
    keepRemoteButton?.click();
    await flushTasks();

    expect(keepRemoteButton?.classList.contains("is-loading")).toBe(true);
    expect(keepRemoteButton?.getAttribute("aria-busy")).toBe("true");
    expect(keepRemoteButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(false);

    resolution.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(resolveConflictMock).toHaveBeenCalledWith("loc-1", "photos/conflict.txt", "keep-remote");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(showToastMock).toHaveBeenCalledWith(
      "Conflict resolved by keeping the remote version.",
      "success",
      2200,
      "app-toast",
    );
  });

  it("compares review-required file entries through the same resolution modal", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({
        path: "photos/review.txt",
        status: "review-required",
        hasLocalCopy: false,
        localKind: "file",
        remoteKind: "file",
        localSize: 12,
        remoteSize: 14,
      }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    expect(modal?.hidden).toBe(false);
    expect(modal?.textContent).toContain("photos/review.txt");

    const compareButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Compare"));
    compareButton?.click();
    await flushTasks();
    await flushTasks();

    expect(prepareConflictComparisonMock).toHaveBeenCalledWith("loc-1", "photos/review.txt");
    expect(openPathMock).toHaveBeenNthCalledWith(1, "C:/my-photos/photos/conflict.txt");
    expect(openPathMock).toHaveBeenNthCalledWith(2, "C:/temp/photos-conflict-remote.txt");
  });

  it("clears review-required entries with keep local and refreshes the live view", async () => {
    const resolution = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    resolveConflictMock.mockReturnValueOnce(resolution.promise);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({
          path: "photos/review.txt",
          status: "review-required",
          hasLocalCopy: false,
          localKind: "file",
          remoteKind: "file",
        }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/review.txt", status: "synced" }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const keepLocalButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Keep local"));
    keepLocalButton?.click();
    await flushTasks();

    expect(keepLocalButton?.classList.contains("is-loading")).toBe(true);
    expect(keepLocalButton?.getAttribute("aria-busy")).toBe("true");
    expect(keepLocalButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(false);

    resolution.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(resolveConflictMock).toHaveBeenCalledWith("loc-1", "photos/review.txt", "keep-local");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(showToastMock).toHaveBeenCalledWith(
      "Review cleared by keeping the local version.",
      "success",
      2200,
      "app-toast",
    );
  });

  it("clears review-required entries with keep remote and refreshes the live view", async () => {
    const resolution = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    resolveConflictMock.mockReturnValueOnce(resolution.promise);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({
          path: "photos/review.txt",
          status: "review-required",
          hasLocalCopy: false,
          localKind: "file",
          remoteKind: "file",
        }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/review.txt", status: "synced" }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-resolve-btn")?.click();
    await flushTasks();

    const modal = document.querySelector<HTMLElement>(".storage-conflict-resolution-modal");
    const keepRemoteButton = Array.from(modal?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .find((button) => button.textContent?.includes("Keep remote"));
    keepRemoteButton?.click();
    await flushTasks();

    expect(keepRemoteButton?.classList.contains("is-loading")).toBe(true);
    expect(keepRemoteButton?.getAttribute("aria-busy")).toBe("true");
    expect(keepRemoteButton?.querySelector<HTMLElement>(".storage-conflict-action-spinner")?.hidden).toBe(false);

    resolution.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(resolveConflictMock).toHaveBeenCalledWith("loc-1", "photos/review.txt", "keep-remote");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(showToastMock).toHaveBeenCalledWith(
      "Review cleared by keeping the remote version.",
      "success",
      2200,
      "app-toast",
    );
  });

  it("uses remote bin delete confirmation and delete action copy for the active location", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: true, retentionDays: 14 },
      }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const deleteButton = document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn');
    expect(deleteButton).toBeTruthy();

    deleteButton?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain(
      '"photos/img001.jpg" will be removed from local storage immediately. The remote object will be moved into this sync location\'s remote bin for 14 days.',
    );

    getAsyncConfirmAcceptButton().click();
    await flushTasks();

    expect(deleteFileMock).toHaveBeenCalledWith("loc-1", "photos/img001.jpg");
  });

  it("uses folder-specific remote bin confirmation and delete action copy for safe live directories", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: true, retentionDays: 14 },
      }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/2026/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const deleteButton = document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos"] .tree-delete-btn');
    expect(deleteButton).toBeTruthy();

    deleteButton?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain(
      'Folder "photos" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be moved into this sync location\'s remote bin for 14 days.',
    );

    getAsyncConfirmAcceptButton().click();
    await flushTasks();

    expect(deleteFolderMock).toHaveBeenCalledWith("loc-1", "photos");
  });

  it("uses hard delete confirmation copy when the active location remote bin is disabled", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: false, retentionDays: 30 },
      }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain(
      '"photos/img001.jpg" will be removed from local storage immediately and permanently deleted from the remote bucket. This cannot be undone.',
    );

    getAsyncConfirmAcceptButton().click();
    await flushTasks();

    expect(deleteFileMock).toHaveBeenCalledWith("loc-1", "photos/img001.jpg");
  });

  it("uses versioned delete confirmation copy when object versioning is enabled", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        objectVersioningEnabled: true,
        remoteBin: { enabled: false, retentionDays: 7 },
      }),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain(
      '"photos/img001.jpg" will be removed from local storage immediately. The remote object will be deleted using S3 object versioning so it can be restored from version history.',
    );
  });

  it("does not expose directory delete in blocked live directory states", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/review.jpg", status: "review-required", hasLocalCopy: false }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    expect(document.querySelector('.tree-item[data-value="photos"] .tree-delete-btn')).toBeNull();
  });

  it("refreshes live and bin views after deleting a folder into the remote bin", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: true, retentionDays: 7 },
      }),
    ]);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/2026/img001.jpg" }),
      ])
      .mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/2026/img001.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-key-folder-1" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos"] .tree-delete-btn')?.click();
    await flushTasks();
    getAsyncConfirmAcceptButton().click();
    await flushTasks();
    await flushTasks();

    expect(deleteFolderMock).toHaveBeenCalledWith("loc-1", "photos");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(listBinEntriesMock).toHaveBeenCalledWith("loc-1");
    expect(document.querySelector('.tree-item[data-value="photos/2026/img001.jpg"]')).not.toBeNull();
  });

  it("keeps the file delete confirm modal open and loading until deletion succeeds", async () => {
    const deletion = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);
    deleteFileMock.mockReturnValueOnce(deletion.promise);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    const acceptButton = getAsyncConfirmAcceptButton();
    acceptButton.click();
    await flushTasks();

    expect(deleteFileMock).toHaveBeenCalledWith("loc-1", "photos/img001.jpg");
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);
    expect(acceptButton.classList.contains("is-loading")).toBe(true);
    expect(acceptButton.getAttribute("aria-busy")).toBe("true");
    expect(acceptButton.disabled).toBe(true);
    expect(acceptButton.classList.contains("secondary-btn")).toBe(true);
    expect(acceptButton.classList.contains("modal-btn-accept")).toBe(true);
    expect(getAsyncConfirmAcceptSpinner().hidden).toBe(false);
    expect(getAsyncConfirmAcceptBusyText().hidden).toBe(false);
    expect(getAsyncConfirmAcceptBusyText().textContent).toContain("Loading");
    expect(getAsyncConfirmModal()?.hidden).toBe(false);

    deletion.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(getVisibleAsyncConfirmModals()).toHaveLength(0);
    expect(getAsyncConfirmModal()?.hidden).toBe(true);
    expect(document.querySelector('.tree-item[data-value="photos/img001.jpg"]')).toBeNull();
  });

  it("refreshes live and bin views after deleting a file into the remote bin", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos", {
        remoteBin: { enabled: true, retentionDays: 7 },
      }),
    ]);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/img001.jpg" }),
      ])
      .mockResolvedValueOnce([]);
    listBinEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg", status: "deleted", hasLocalCopy: false, binKey: "bin-key-1" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();
    getAsyncConfirmAcceptButton().click();
    await flushTasks();
    await flushTasks();

    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(listBinEntriesMock).toHaveBeenCalledWith("loc-1");
    expect(document.querySelector('.tree-item[data-value="photos/img001.jpg"]')).not.toBeNull();
  });

  it("refreshes both bin and live views after restoring from bin", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock
      .mockResolvedValueOnce([])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/restored.jpg", hasLocalCopy: false, status: "remote-only" }),
      ]);
    listBinEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/restored.jpg", status: "deleted", hasLocalCopy: false, binKey: "opaque-bin-key" }),
      ])
      .mockResolvedValueOnce([]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    const select = document.getElementById("active-location-select") as HTMLSelectElement;
    select.value = "bin:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-restore-btn")?.click();
    await flushTasks();
    await flushTasks();

    expect(listBinEntriesMock).toHaveBeenCalledTimes(2);

    select.value = "live:loc-1";
    select.dispatchEvent(new Event("change", { bubbles: true }));
    await flushTasks();

    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
    expect(document.querySelector('.tree-item[data-value="photos/restored.jpg"]')).not.toBeNull();
  });

  it("keeps the file delete confirm modal usable after deletion failure", async () => {
    const deletion = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);
    deleteFileMock.mockReturnValueOnce(deletion.promise);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    const acceptButton = getAsyncConfirmAcceptButton();
    acceptButton.click();
    await flushTasks();

    expect(acceptButton.classList.contains("is-loading")).toBe(true);
    expect(acceptButton.getAttribute("aria-busy")).toBe("true");
    expect(getAsyncConfirmAcceptSpinner().hidden).toBe(false);
    expect(getAsyncConfirmAcceptBusyText().hidden).toBe(false);

    deletion.reject(new Error("network down"));
    await flushTasks();
    await flushTasks();

    expect(getAsyncConfirmModal()).not.toBeNull();
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);
    expect(acceptButton.classList.contains("is-loading")).toBe(false);
    expect(acceptButton.getAttribute("aria-busy")).toBe("false");
    expect(acceptButton.disabled).toBe(false);
    expect(getAsyncConfirmAcceptSpinner().hidden).toBe(true);
    expect(getAsyncConfirmAcceptBusyText().hidden).toBe(true);
    expect(showToastMock).toHaveBeenCalledWith("Failed to delete file: network down", "error", 2200, "app-toast");

    getAsyncConfirmRejectButton().click();
    await flushTasks();

    expect(getVisibleAsyncConfirmModals()).toHaveLength(0);
    expect(getAsyncConfirmModal()?.hidden).toBe(true);
  });

  it("keeps the move-to-Glacier confirm modal open and loading until the storage class change succeeds", async () => {
    const change = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg", storageClass: "STANDARD" }),
    ]);
    changeStorageClassMock.mockReturnValueOnce(change.promise);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-storage-class-btn")?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain("Move to Glacier storage?");

    const acceptButton = getAsyncConfirmAcceptButton();
    acceptButton.click();
    await flushTasks();

    expect(changeStorageClassMock).toHaveBeenCalledWith("loc-1", "photos/img001.jpg", "GLACIER_IR");
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);
    expect(acceptButton.classList.contains("is-loading")).toBe(true);
    expect(acceptButton.getAttribute("aria-busy")).toBe("true");
    expect(acceptButton.classList.contains("secondary-btn")).toBe(true);
    expect(acceptButton.classList.contains("modal-btn-accept")).toBe(true);
    expect(getAsyncConfirmAcceptSpinner().hidden).toBe(false);
    expect(getAsyncConfirmModal()?.hidden).toBe(false);

    change.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(getVisibleAsyncConfirmModals()).toHaveLength(0);
    expect(getAsyncConfirmModal()?.hidden).toBe(true);
  });

  it("refreshes live entries after changing storage class", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/img001.jpg", storageClass: "STANDARD", hasLocalCopy: true }),
      ])
      .mockResolvedValueOnce([
        fileEntry({ path: "photos/img001.jpg", storageClass: "GLACIER_IR", status: "glacier", hasLocalCopy: false }),
      ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-storage-class-btn")?.click();
    await flushTasks();
    getAsyncConfirmAcceptButton().click();
    await flushTasks();
    await flushTasks();

    expect(changeStorageClassMock).toHaveBeenCalledWith("loc-1", "photos/img001.jpg", "GLACIER_IR");
    expect(listFileEntriesMock).toHaveBeenCalledTimes(2);
  });

  it("keeps the restore-from-Glacier confirm modal open and loading until the storage class change succeeds", async () => {
    const change = createDeferred<void>();
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/archive.zip", status: "glacier", hasLocalCopy: false, storageClass: "GLACIER_IR" }),
    ]);
    changeStorageClassMock.mockReturnValueOnce(change.promise);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>(".tree-storage-class-btn")?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()?.textContent).toContain("Restore from Glacier?");

    const acceptButton = getAsyncConfirmAcceptButton();
    acceptButton.click();
    await flushTasks();

    expect(changeStorageClassMock).toHaveBeenCalledWith("loc-1", "photos/archive.zip", "STANDARD");
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);
    expect(acceptButton.classList.contains("is-loading")).toBe(true);
    expect(acceptButton.getAttribute("aria-busy")).toBe("true");
    expect(acceptButton.classList.contains("secondary-btn")).toBe(true);
    expect(acceptButton.classList.contains("modal-btn-accept")).toBe(true);
    expect(getAsyncConfirmAcceptSpinner().hidden).toBe(false);
    expect(getAsyncConfirmModal()?.hidden).toBe(false);

    change.resolve(undefined);
    await flushTasks();
    await flushTasks();

    expect(getVisibleAsyncConfirmModals()).toHaveLength(0);
    expect(getAsyncConfirmModal()?.hidden).toBe(true);
  });

  it("reuses a single confirm modal instance across mutation actions", async () => {
    listSyncLocationsMock.mockResolvedValueOnce([
      baseSyncLocation("loc-1", "My photos"),
    ]);
    listFileEntriesMock.mockResolvedValueOnce([
      fileEntry({ path: "photos/img001.jpg" }),
    ]);

    cleanup = await bootstrapStorageGoblin();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    const firstModal = getAsyncConfirmModal();
    expect(firstModal).toBeTruthy();
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);

    getAsyncConfirmRejectButton().click();
    await flushTasks();

    document.querySelector<HTMLButtonElement>('.tree-item[data-value="photos/img001.jpg"] .tree-delete-btn')?.click();
    await flushTasks();

    expect(getAsyncConfirmModal()).toBe(firstModal);
    expect(getVisibleAsyncConfirmModals()).toHaveLength(1);
  });
});
