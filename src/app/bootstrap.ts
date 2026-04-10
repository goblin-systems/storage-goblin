import {
  applyIcons,
  bindNavigation,
  closeModal,
  confirmModal,
  openModal,
  setupWindowControls,
  showToast,
} from "@goblin-systems/goblin-design-system";
import { createNativeActivity, createUiActivity } from "./activity";
import { createStorageGoblinClient } from "./client";
import { createAppDom } from "./dom";
import { renderFileTree, type FileEntry, type FileTreeHandle } from "./file-tree";
import {
  applyStoredProfile,
  DEFAULT_PROFILE_DRAFT,
  normalizeProfileDraft,
  toStoredProfile,
} from "./profile";
import { createProfilePersistence } from "./persistence";
import { describeSyncStatus, formatTimestamp, getSyncOverviewStats } from "./status";
import type {
  ActivityDebugLogState,
  ActivityItem,
  CredentialDraft,
  CredentialSummary,
  CredentialTestContext,
  LocationSyncStatus,
  PermissionProbeSummary,
  StorageProfileDraft,
  SyncLocation,
  SyncLocationDraft,
  SyncStatus,
} from "./types";

type DialogId = "credentials" | "locations" | "activity" | "settings";

type SyncStatusWithLocations = SyncStatus & {
  locations?: LocationSyncStatus[];
};

function getLocationSyncStatusId(location: LocationSyncStatus): string {
  if (typeof location.pairId === "string") {
    return location.pairId;
  }

  if (typeof location.locationId === "string") {
    return location.locationId;
  }

  throw new Error("Location sync status is missing an identifier.");
}

function createInitialStatus(): SyncStatus {
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

function setButtonBusy(button: HTMLButtonElement, busy: boolean) {
  button.classList.toggle("is-loading", busy);
  button.disabled = busy;
}

function createUnavailableCredential(id: string, name?: string | null): CredentialSummary {
  return {
    id,
    name: name?.trim() || "Missing credential",
    ready: false,
    validationStatus: "untested",
    lastTestedAt: null,
    lastTestMessage: null,
  };
}

function getCredentialValidationLabel(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "test passed";
    case "failed":
      return "test failed";
    default:
      return "untested";
  }
}

function getCredentialTestActionLabel(credential: CredentialSummary): string {
  return credential.validationStatus === "untested" ? "Test" : "Re-test";
}

function getCredentialStorageLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored securely" : "stored secret missing";
}

function getCredentialStorageBadgeLabel(credential: CredentialSummary): string {
  return credential.ready ? "stored" : "needs repair";
}

function getCredentialStorageBadgeTone(credential: CredentialSummary): "success" | "danger" {
  return credential.ready ? "success" : "danger";
}

function getCredentialValidationBadgeTone(credential: CredentialSummary): "success" | "danger" | "default" {
  return credential.validationStatus === "passed"
    ? "success"
    : credential.validationStatus === "failed"
      ? "danger"
      : "default";
}

function getCredentialTestSentence(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "Its last test passed.";
    case "failed":
      return "Its last test failed.";
    default:
      return "It has not been tested yet.";
  }
}

function formatPermissionSummary(permissions: PermissionProbeSummary | null): string {
  if (!permissions) return "";

  const probeLabels: Record<string, string> = {
    put_object: "write",
    get_object: "read",
    delete_object: "delete",
  };

  const headBucket = permissions.probes.find((p) => p.name === "head_bucket");
  if (headBucket && !headBucket.allowed) {
    return `Bucket "${permissions.bucket}" is not accessible.`;
  }

  const labels = permissions.probes
    .filter((p) => p.name !== "head_bucket")
    .map((p) => `${probeLabels[p.name] ?? p.name} ${p.allowed ? "✓" : "✗"}`);

  return labels.length > 0 ? `Permissions: ${labels.join(" · ")}` : "";
}

function describeSelectedCredentialState(profile: StorageProfileDraft): string {
  if (!profile.credentialProfileId || !profile.selectedCredential) {
    return "Choose a saved credential before connecting";
  }

  if (!profile.selectedCredential.ready) {
    return "Selected credential reference exists, but its stored secret is missing. Recreate or replace it.";
  }

  return `Selected credential is stored securely. ${getCredentialTestSentence(profile.selectedCredential)}`;
}

function buildCredentialTestContext(profile: StorageProfileDraft): CredentialTestContext {
  return {
    region: profile.region,
    bucket: profile.bucket,
  };
}

function buildCredentialCreateMessage(credential: CredentialSummary): string {
  const savedState = credential.ready
    ? `Saved credential "${credential.name}" securely.`
    : `Saved credential "${credential.name}", but its stored secret needs attention.`;

  if (credential.validationStatus === "untested") {
    return `${savedState} It was not tested yet.`;
  }

  if (credential.validationStatus === "passed") {
    return `${savedState} It was tested and is valid.`;
  }

  return `${savedState} It was tested and failed.`;
}

function getCredentialDisplayName(profile: StorageProfileDraft): string {
  if (!profile.credentialProfileId) {
    return "No credential selected";
  }

  if (profile.selectedCredential) {
    return profile.selectedCredential.ready
      ? profile.selectedCredential.name
      : `${profile.selectedCredential.name} (stored secret missing)`;
  }

  return "Selected credential missing";
}

function syncProfileCredentialState(
  profile: StorageProfileDraft,
  credentials: CredentialSummary[],
): StorageProfileDraft {
  const credentialProfileId = profile.credentialProfileId?.trim() || null;

  if (!credentialProfileId) {
    return normalizeProfileDraft({
      ...profile,
      credentialProfileId: null,
      selectedCredential: null,
      selectedCredentialAvailable: false,
      credentialsStoredSecurely: false,
    });
  }

  const availableCredential = credentials.find((credential) => credential.id === credentialProfileId) ?? null;
  const selectedCredential = availableCredential
    ?? (profile.selectedCredential?.id === credentialProfileId
      ? createUnavailableCredential(credentialProfileId, profile.selectedCredential.name)
      : createUnavailableCredential(credentialProfileId));

  return normalizeProfileDraft({
    ...profile,
    credentialProfileId,
    selectedCredential,
    selectedCredentialAvailable: Boolean(availableCredential?.ready),
    credentialsStoredSecurely: Boolean(availableCredential?.ready),
  });
}

type BootstrapCleanup = () => void;

type DebouncedFn<T extends (...args: unknown[]) => void> = T & {
  cancel: () => void;
};

export async function bootstrapStorageGoblin(): Promise<BootstrapCleanup> {
  setupWindowControls();
  applyIcons();

  const dom = createAppDom();
  const client = createStorageGoblinClient();
  const persistence = createProfilePersistence(client);

  const state: {
    activeDialog: DialogId | null;
    activeLocationId: string | null;
    profile: StorageProfileDraft;
    credentials: CredentialSummary[];
    syncLocations: SyncLocation[];
    status: SyncStatusWithLocations;
    activity: ActivityItem[];
    lastConnectAt: string | null;
    debugLogState: ActivityDebugLogState;
  } = {
    activeDialog: null,
    activeLocationId: null,
    profile: DEFAULT_PROFILE_DRAFT,
    credentials: [],
    syncLocations: [],
    status: createInitialStatus(),
    activity: [],
    lastConnectAt: null,
    debugLogState: {
      enabled: false,
      logFilePath: null,
      logDirectoryPath: null,
    },
  };

  let fileTreeHandle: FileTreeHandle | null = null;
  let lastFileEntriesJson = "";

  let fileTreeChangeTimer: ReturnType<typeof setTimeout> | null = null;
  function debouncedFileTreeChange(checkedPaths: string[]) {
    if (fileTreeChangeTimer !== null) clearTimeout(fileTreeChangeTimer);
    fileTreeChangeTimer = setTimeout(() => {
      fileTreeChangeTimer = null;
      void handleFileTreeChange(checkedPaths);
    }, 500);
  }

  function debounce<T extends (...args: unknown[]) => void>(fn: T, ms: number): DebouncedFn<T> {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const debounced = ((...args: unknown[]) => {
      if (timer !== null) clearTimeout(timer);
      timer = setTimeout(() => { timer = null; fn(...args); }, ms);
    }) as DebouncedFn<T>;

    debounced.cancel = () => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    };

    return debounced;
  }

  const debouncedRefreshFileTree = debounce(() => void refreshFileTree(), 300);
  const debouncedRenderActivity = debounce(renderActivity, 150);

  function toast(message: string, variant: "success" | "error" | "info" = "info") {
    showToast(message, variant, 2200, "app-toast");
  }

  const dialogs: Record<DialogId, HTMLElement> = {
    credentials: dom.credentialsScreen,
    locations: dom.locationsScreen,
    activity: dom.activityScreen,
    settings: dom.settingsScreen,
  };

  function closeAllDialogs() {
    for (const dialog of Object.values(dialogs)) {
      if (!dialog.hidden) {
        closeModal({ backdrop: dialog });
      }
    }
    state.activeDialog = null;
  }

  function openDialog(dialogId: DialogId) {
    closeAllDialogs();
    state.activeDialog = dialogId;
    openModal({
      backdrop: dialogs[dialogId],
      onClose: () => {
        if (state.activeDialog === dialogId) {
          state.activeDialog = null;
        }
      },
    });
  }

  function addActivityItem(item: ActivityItem) {
    state.activity = [item, ...state.activity].slice(0, 36);
    debouncedRenderActivity();
  }

  function addActivity(level: ActivityItem["level"], message: string, details?: string | null) {
    addActivityItem(createUiActivity(level, message, details));
  }

  function renderDebugLogState() {
    const { enabled, logDirectoryPath, logFilePath } = state.debugLogState;

    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
    dom.debugLogStatusBadge.textContent = enabled ? "Enabled" : client.supportsNativeProfilePersistence ? "Disabled" : "Unavailable";
    dom.debugLogStatusBadge.className = `badge ${enabled ? "success" : "default"}`;
    dom.debugLogStatusText.textContent = client.supportsNativeProfilePersistence
      ? enabled
        ? "Detailed native activity logging is on. Open Activity from the menu to inspect richer event details."
        : "Detailed native activity logging is off. Turn it on and save settings to capture extra troubleshooting detail."
      : "Debug logging is not available in the browser preview.";
    dom.debugLogFilePath.textContent = logFilePath ?? logDirectoryPath ?? "Unavailable";
    dom.openDebugLogFolderBtn.disabled = !logDirectoryPath;
  }

  function renderActivity() {
    dom.activityList.innerHTML = "";
    const hasItems = state.activity.length > 0;
    dom.activityEmptyState.hidden = hasItems;
    dom.activityList.hidden = !hasItems;

    for (const item of state.activity) {
      const li = document.createElement("li");
      li.className = "activity-item";

      const message = document.createElement("span");
      message.className = "activity-message";
      message.textContent = item.message;

      const meta = document.createElement("span");
      meta.className = "activity-meta";
      meta.textContent = `${item.level.toUpperCase()} · ${formatTimestamp(item.timestamp)}`;

      li.append(message, meta);

      if (item.details) {
        const details = document.createElement("details");
        details.className = "activity-details";

        const summary = document.createElement("summary");
        summary.textContent = "Debug details";

        const pre = document.createElement("pre");
        pre.className = "activity-detail-text";
        pre.textContent = item.details;

        details.append(summary, pre);
        li.append(details);
      }

      dom.activityList.append(li);
    }
  }

  function renderCredentialsList() {
    dom.credentialsList.innerHTML = "";

    const count = state.credentials.length;
    dom.credentialsCountBadge.textContent = `${count} saved`;
    dom.credentialsSupportBadge.textContent = client.supportsNativeProfilePersistence ? "Desktop app" : "Preview only";
    dom.credentialsSupportBadge.className = `badge ${client.supportsNativeProfilePersistence ? "success" : "default"}`;
    dom.credentialsSupportText.textContent = client.supportsNativeProfilePersistence
      ? "Create a named credential once, then reuse it across sync locations without re-entering raw keys."
      : "Browser preview shows the credential workflow but does not create or store real credentials.";
    dom.createCredentialBtn.disabled = !client.supportsNativeProfilePersistence;

    dom.credentialsListStatus.textContent = count > 0
      ? "Saved credentials show secure storage state and test state separately."
      : client.supportsNativeProfilePersistence
        ? "Create your first named credential, then assign it to a sync location."
        : "Open the desktop app to create and manage credentials.";

    dom.credentialsEmptyState.hidden = count > 0;
    dom.credentialsList.hidden = count === 0;

    for (const credential of state.credentials) {
      const li = document.createElement("li");
      li.className = "credential-item";

      const meta = document.createElement("div");
      meta.className = "credential-item-meta";

      const name = document.createElement("strong");
      name.textContent = credential.name;

      const hint = document.createElement("span");
      hint.className = "hint";
      hint.textContent = credential.id === state.profile.credentialProfileId
        ? `Selected for this bucket · ${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}`
        : `${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}`;

      meta.append(name, hint);

      const actions = document.createElement("div");
      actions.className = "credential-item-actions";

      const availabilityBadge = document.createElement("span");
      availabilityBadge.className = `badge ${getCredentialStorageBadgeTone(credential)}`;
      availabilityBadge.textContent = getCredentialStorageBadgeLabel(credential);
      actions.append(availabilityBadge);

      const validationBadge = document.createElement("span");
      validationBadge.className = `badge ${getCredentialValidationBadgeTone(credential)}`;
      validationBadge.textContent = getCredentialValidationLabel(credential);
      actions.append(validationBadge);

      if (credential.id === state.profile.credentialProfileId) {
        const selectedBadge = document.createElement("span");
        selectedBadge.className = "badge default";
        selectedBadge.textContent = "selected";
        actions.append(selectedBadge);
      }

      const testButton = document.createElement("button");
      testButton.className = "secondary-btn slim-btn";
      testButton.type = "button";
      testButton.textContent = getCredentialTestActionLabel(credential);
      testButton.disabled = !client.supportsNativeProfilePersistence;
      testButton.title = !client.supportsNativeProfilePersistence
        ? "Credential testing is only available in the desktop app."
        : "";
      testButton.addEventListener("click", async () => {
        setButtonBusy(testButton, true);

        try {
          const result = await client.testCredential({
            credentialId: credential.id,
            context: buildCredentialTestContext(state.profile),
          });

          state.credentials = state.credentials.map((item) => item.id === result.credential.id ? result.credential : item);
          if (!state.credentials.some((item) => item.id === result.credential.id)) {
            state.credentials = [...state.credentials, result.credential];
          }

          state.profile = syncProfileCredentialState(normalizeProfileDraft({
            ...state.profile,
            selectedCredential: state.profile.credentialProfileId === result.credential.id
              ? result.credential
              : state.profile.selectedCredential,
          }), state.credentials);
          renderProfileSummary();

          const baseMessage = result.ok
            ? `Credential "${result.credential.name}" test passed. Can access ${result.bucketCount} bucket(s).`
            : `Credential "${result.credential.name}" test failed.`;

          const permissionLine = formatPermissionSummary(result.permissions);
          const displayMessage = permissionLine ? `${baseMessage} ${permissionLine}` : baseMessage;

          dom.credentialsResult.textContent = displayMessage;
          toast(displayMessage, result.ok ? "success" : "error");
          addActivity(result.ok ? "success" : "error", displayMessage);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const surfacedMessage = `Credential test failed: ${message}`;
          dom.credentialsResult.textContent = surfacedMessage;
          toast(surfacedMessage, "error");
          addActivity("error", surfacedMessage);
        } finally {
          setButtonBusy(testButton, false);
        }
      });
      actions.append(testButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.disabled = !client.supportsNativeProfilePersistence;
      deleteButton.addEventListener("click", async () => {
        const wasSelected = credential.id === state.profile.credentialProfileId;
        const confirmed = await confirmModal({
          title: "Delete credential?",
          message: wasSelected
            ? `"${credential.name}" will be deleted. This bucket will need a different credential before it can sync again.`
            : `"${credential.name}" will be permanently deleted.`,
          acceptLabel: "Delete",
          rejectLabel: "Cancel",
          variant: "danger",
        });

        if (!confirmed) return;

        try {
          const result = await client.deleteCredential(credential.id);
          if (!result.deleted) {
            const message = client.supportsNativeProfilePersistence
              ? `Could not delete credential \"${credential.name}\".`
              : "Credential deletion is only available in the desktop app.";
            dom.credentialsResult.textContent = message;
            toast(message, "info");
            return;
          }

          if (wasSelected) {
            state.profile = syncProfileCredentialState(normalizeProfileDraft({
              ...state.profile,
              credentialProfileId: result.profile.credentialProfileId,
              selectedCredential: result.profile.selectedCredential,
              selectedCredentialAvailable: result.profile.selectedCredentialAvailable,
              credentialsStoredSecurely: result.profile.credentialsStoredSecurely,
            }), state.credentials.filter((item) => item.id !== credential.id));
          }

          const message = `Deleted credential \"${credential.name}\".`;
          dom.credentialsResult.textContent = message;
          addActivity("info", message);
          toast(message, "success");
          await refreshCredentials();
          renderProfileSummary();
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          const surfacedMessage = `Delete credential failed: ${message}`;
          dom.credentialsResult.textContent = surfacedMessage;
          addActivity("error", surfacedMessage);
          toast(surfacedMessage, "error");
        }
      });
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.credentialsList.append(li);
    }
  }

  function renderStatus() {
    const activeLocationStatus = state.activeLocationId
      ? state.status.locations?.find((location) => getLocationSyncStatusId(location) === state.activeLocationId)
      : undefined;
    const effectiveStatus = activeLocationStatus ?? state.status;
    const aggregatePresentation = describeSyncStatus(state.status);
    const effectivePresentation = describeSyncStatus(effectiveStatus);
    const overview = getSyncOverviewStats(effectiveStatus);
    const headerPresentation = activeLocationStatus ? effectivePresentation : aggregatePresentation;

    dom.syncPhaseBadge.textContent = headerPresentation.badgeLabel;
    dom.syncPhaseBadge.className = `badge ${headerPresentation.badgeTone}`;
    dom.statusPhaseInline.textContent = effectivePresentation.badgeLabel;
    dom.statusPhaseInline.className = `badge ${effectivePresentation.badgeTone}`;
    dom.statusSummary.textContent = effectivePresentation.summary;
    dom.windowSubtitle.textContent = headerPresentation.summary;

    dom.statusOverviewLocal.textContent = new Intl.NumberFormat().format(overview.localFiles);
    dom.statusOverviewRemote.textContent = new Intl.NumberFormat().format(overview.remoteFiles);
    dom.statusOverviewInSync.textContent = new Intl.NumberFormat().format(overview.inSync);
    dom.statusOverviewNotInSync.textContent = new Intl.NumberFormat().format(overview.notInSync);
  }

  function renderProfileSummary() {
    renderCredentialsList();
    renderStatus();
    renderLocationCredentialOptions();
  }

  function writeSettingsToDom() {
    dom.remotePollingInput.checked = state.profile.remotePollingEnabled;
    dom.pollIntervalInput.value = String(state.profile.pollIntervalSeconds);
    dom.conflictStrategySelect.value = state.profile.conflictStrategy;
    dom.deleteSafetyInput.value = String(state.profile.deleteSafetyHours);
    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
  }

  function readSettingsFromDom() {
    state.profile = normalizeProfileDraft({
      ...state.profile,
      remotePollingEnabled: dom.remotePollingInput.checked,
      pollIntervalSeconds: Number(dom.pollIntervalInput.value),
      conflictStrategy: "preserve-both",
      deleteSafetyHours: Number(dom.deleteSafetyInput.value),
      activityDebugModeEnabled: dom.activityDebugModeInput.checked,
    });
  }

  async function refreshStatus() {
    state.status = await client.getSyncStatus();
    renderStatus();
  }

  async function refreshDebugLogState() {
    state.debugLogState = await client.getActivityDebugLogState();
    renderDebugLogState();
  }

  async function refreshCredentials() {
    state.credentials = await client.listCredentials();
    state.profile = syncProfileCredentialState(state.profile, state.credentials);
    renderProfileSummary();
  }

  function mergeSyncLocationsWithStoredProfile(listedLocations: SyncLocation[]): SyncLocation[] {
    const storedLocations = state.profile.syncLocations ?? [];
    const storedLocationIds = new Set(storedLocations.map((location) => location.id));

    if (storedLocationIds.size === 0 && !state.profile.activeLocationId) {
      return listedLocations;
    }

    const listedLocationsById = new Map(listedLocations.map((location) => [location.id, location]));
    return storedLocations.map((location) => listedLocationsById.get(location.id) ?? location);
  }

  function applySyncLocationState(syncLocations: SyncLocation[], preferredActiveLocationId: string | null = state.activeLocationId) {
    const activeLocationExists = preferredActiveLocationId
      ? syncLocations.some((location) => location.id === preferredActiveLocationId)
      : false;

    state.syncLocations = syncLocations;
    state.activeLocationId = syncLocations.length === 0
      ? null
      : activeLocationExists
        ? preferredActiveLocationId
        : syncLocations[0].id;
    state.profile = normalizeProfileDraft({
      ...state.profile,
      syncLocations,
      activeLocationId: state.activeLocationId,
    });
  }

  function renderLocationDropdown() {
    const select = dom.activeLocationSelect;
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Select a sync location";
    select.append(defaultOption);

    for (const location of state.syncLocations) {
      const option = document.createElement("option");
      option.value = location.id;
      option.textContent = location.label || `${location.bucket}`;
      select.append(option);
    }

    select.value = state.activeLocationId ?? "";
  }

  async function handleSaveSettings() {
    readSettingsFromDom();
    setButtonBusy(dom.saveSettingsBtn, true);

    try {
      const stored = await persistence.saveSettings(toStoredProfile(state.profile));
      state.profile = syncProfileCredentialState(normalizeProfileDraft({
        ...state.profile,
        ...stored,
      }), state.credentials);
      writeSettingsToDom();
      renderProfileSummary();
      await refreshStatus();
      await refreshDebugLogState();

      const message = client.supportsNativeProfilePersistence
        ? "Settings saved. Activity debug and polling preferences are updated."
        : "Settings saved locally in the browser preview.";

      dom.settingsResult.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } finally {
      setButtonBusy(dom.saveSettingsBtn, false);
    }
  }

  async function handleCreateCredential() {
    const draft: CredentialDraft = {
      name: dom.credentialNameInput.value.trim(),
      accessKeyId: dom.credentialAccessKeyInput.value.trim(),
      secretAccessKey: dom.credentialSecretKeyInput.value.trim(),
    };

    if (!client.supportsNativeProfilePersistence) {
      const message = "Credential management is only available in the desktop app.";
      dom.credentialsResult.textContent = message;
      toast(message, "info");
      return;
    }

    if (!draft.name || !draft.accessKeyId || !draft.secretAccessKey) {
      const message = "Enter a name, access key ID, and secret access key to create a credential.";
      dom.credentialsResult.textContent = message;
      toast(message, "error");
      return;
    }

    setButtonBusy(dom.createCredentialBtn, true);

    try {
      const created = await client.createCredential(draft);
      dom.credentialNameInput.value = "";
      dom.credentialAccessKeyInput.value = "";
      dom.credentialSecretKeyInput.value = "";
      await refreshCredentials();
      state.profile = syncProfileCredentialState(normalizeProfileDraft({
        ...state.profile,
        credentialProfileId: created.id,
        selectedCredential: created,
      }), state.credentials);
      renderProfileSummary();

      const message = buildCredentialCreateMessage(created);
      dom.credentialsResult.textContent = `${message} It is now selected for this bucket.`;
      toast(`Created credential \"${created.name}\".`, "success");
      addActivity("success", `Created credential \"${created.name}\".`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Create credential failed: ${message}`;
      dom.credentialsResult.textContent = surfacedMessage;
      toast(surfacedMessage, "error");
      addActivity("error", surfacedMessage);
    } finally {
      setButtonBusy(dom.createCredentialBtn, false);
    }
  }

  function renderLocationCredentialOptions() {
    const select = dom.locationCredentialSelect;
    const currentValue = select.value;
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Choose a credential";
    select.append(defaultOption);

    for (const credential of state.credentials) {
      const option = document.createElement("option");
      option.value = credential.id;
      option.textContent = credential.name;
      select.append(option);
    }

    select.value = currentValue;
    select.disabled = state.credentials.length === 0;
  }

  function resetLocationForm() {
    dom.locationEditingId.value = "";
    dom.locationFormTitle.textContent = "Create sync location";
    dom.locationLabelInput.value = "";
    dom.locationLocalFolderInput.value = "";
    dom.locationRegionSelect.value = "";
    dom.locationBucketInput.value = "";
    dom.locationCredentialSelect.value = "";
    dom.locationEnabledInput.checked = true;
    dom.locationPollingInput.checked = true;
    dom.locationPollIntervalInput.value = "60";
    dom.locationConflictStrategySelect.value = "preserve-both";
    dom.locationDeleteSafetyInput.value = "24";
    dom.cancelEditLocationBtn.hidden = true;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Create sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "plus");
    applyIcons();
  }

  function populateLocationForm(location: SyncLocation) {
    dom.locationEditingId.value = location.id;
    dom.locationFormTitle.textContent = "Edit sync location";
    dom.locationLabelInput.value = location.label;
    dom.locationLocalFolderInput.value = location.localFolder;
    dom.locationRegionSelect.value = location.region;
    dom.locationBucketInput.value = location.bucket;
    dom.locationCredentialSelect.value = location.credentialProfileId ?? "";
    dom.locationEnabledInput.checked = location.enabled;
    dom.locationPollingInput.checked = location.remotePollingEnabled;
    dom.locationPollIntervalInput.value = String(location.pollIntervalSeconds);
    dom.locationConflictStrategySelect.value = location.conflictStrategy;
    dom.locationDeleteSafetyInput.value = String(location.deleteSafetyHours);
    dom.cancelEditLocationBtn.hidden = false;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Update sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "save");
    applyIcons();
  }

  function readLocationDraftFromForm(): SyncLocationDraft {
    const editingId = dom.locationEditingId.value.trim() || null;
    return {
      id: editingId,
      label: dom.locationLabelInput.value.trim(),
      localFolder: dom.locationLocalFolderInput.value.trim(),
      region: dom.locationRegionSelect.value,
      bucket: dom.locationBucketInput.value.trim(),
      credentialProfileId: dom.locationCredentialSelect.value || null,
      enabled: dom.locationEnabledInput.checked,
      remotePollingEnabled: dom.locationPollingInput.checked,
      pollIntervalSeconds: Number(dom.locationPollIntervalInput.value) || 60,
      conflictStrategy: dom.locationConflictStrategySelect.value as SyncLocationDraft["conflictStrategy"],
      deleteSafetyHours: Number(dom.locationDeleteSafetyInput.value) || 24,
    };
  }

  function renderLocationsList() {
    dom.locationsList.innerHTML = "";

    const count = state.syncLocations.length;
    dom.locationsCountBadge.textContent = `${count} sync location${count !== 1 ? "s" : ""}`;

    dom.locationsEmptyState.hidden = count > 0;
    dom.locationsList.hidden = count === 0;

    for (const location of state.syncLocations) {
      const li = document.createElement("li");
      li.className = "credential-item";

      const meta = document.createElement("div");
      meta.className = "credential-item-meta";

      const name = document.createElement("strong");
      name.textContent = location.label || `${location.bucket}`;

      const hint = document.createElement("span");
      hint.className = "hint";
      const folder = location.localFolder || "No folder";
      const bucket = location.bucket || "No bucket";
      hint.textContent = `${folder} → ${bucket}`;

      meta.append(name, hint);

      const actions = document.createElement("div");
      actions.className = "credential-item-actions";

      const enabledBadge = document.createElement("span");
      enabledBadge.className = `badge ${location.enabled ? "success" : "default"}`;
      enabledBadge.textContent = location.enabled ? "enabled" : "paused";
      actions.append(enabledBadge);

      const editButton = document.createElement("button");
      editButton.className = "secondary-btn slim-btn";
      editButton.type = "button";
      editButton.textContent = "Edit";
      editButton.addEventListener("click", () => {
        renderLocationCredentialOptions();
        populateLocationForm(location);
      });
      actions.append(editButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.addEventListener("click", async () => {
        const confirmed = await confirmModal({
          title: "Delete sync location?",
          message: `"${location.label || location.bucket}" will be permanently deleted.`,
          acceptLabel: "Delete",
          rejectLabel: "Cancel",
          variant: "danger",
        });
        if (!confirmed) return;

        setButtonBusy(deleteButton, true);
        try {
          const updatedProfile = await client.removeSyncLocation(location.id);
          applySyncLocationState(updatedProfile.syncLocations, updatedProfile.activeLocationId ?? state.activeLocationId);
          renderLocationsList();
          renderLocationDropdown();
          resetLocationForm();

          const message = `Deleted sync location "${location.label || location.bucket}".`;
          dom.locationsResult.textContent = message;
          toast(message, "success");
          addActivity("info", message);
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          dom.locationsResult.textContent = `Delete failed: ${message}`;
          toast(`Delete failed: ${message}`, "error");
          addActivity("error", `Delete sync location failed: ${message}`);
        } finally {
          setButtonBusy(deleteButton, false);
        }
      });
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.locationsList.append(li);
    }
  }

  async function handleSaveLocation() {
    const draft = readLocationDraftFromForm();
    const isEditing = Boolean(draft.id);

    if (!draft.localFolder || !draft.bucket) {
      const message = "Enter a local folder and bucket name to create a sync location.";
      dom.locationsResult.textContent = message;
      toast(message, "error");
      return;
    }

    setButtonBusy(dom.saveLocationBtn, true);

    try {
      const updatedProfile = isEditing
        ? await client.updateSyncLocation(draft)
        : await client.addSyncLocation(draft);

      applySyncLocationState(updatedProfile.syncLocations, updatedProfile.activeLocationId ?? state.activeLocationId);
      renderLocationsList();
      renderLocationDropdown();
      resetLocationForm();

      const action = isEditing ? "Updated" : "Created";
      const message = `${action} sync location "${draft.label || draft.bucket}".`;
      dom.locationsResult.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `${isEditing ? "Update" : "Create"} sync location failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      toast(surfacedMessage, "error");
      addActivity("error", surfacedMessage);
    } finally {
      setButtonBusy(dom.saveLocationBtn, false);
    }
  }

  async function refreshSyncLocations() {
    try {
      const listedLocations = await client.listSyncLocations();
      applySyncLocationState(
        mergeSyncLocationsWithStoredProfile(listedLocations),
        state.profile.activeLocationId ?? state.activeLocationId,
      );
    } catch (error) {
      applySyncLocationState(state.profile.syncLocations ?? [], state.profile.activeLocationId ?? state.activeLocationId);
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Load sync locations failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      addActivity("error", surfacedMessage);
    }
    renderLocationsList();
    renderLocationDropdown();
    renderStatus();
    void refreshFileTree();
  }

  async function handleFileTreeChange(checkedPaths: string[]) {
    if (!state.activeLocationId) return;

    // Get current entries to know the full set of file paths
    const entries: FileEntry[] = lastFileEntriesJson ? JSON.parse(lastFileEntriesJson) : await client.listFileEntries(state.activeLocationId);
    const checkedSet = new Set(checkedPaths);

    // Files that are newly checked (want local copy) - were remote-only before
    const toDownload = entries
      .filter((e) => e.kind === "file" && checkedSet.has(e.path) && !e.hasLocalCopy)
      .map((e) => e.path);

    // Files that are newly unchecked (remove local copy) - had local copy before
    const toRemove = entries
      .filter((e) => e.kind === "file" && !checkedSet.has(e.path) && e.hasLocalCopy)
      .map((e) => e.path);

    try {
      if (toDownload.length > 0) {
        addActivity("info", `Downloading ${toDownload.length} file(s) to local storage...`);
        await client.toggleLocalCopy(state.activeLocationId, toDownload, true);
        addActivity("info", `Downloaded ${toDownload.length} file(s).`);
      }
      if (toRemove.length > 0) {
        addActivity("info", `Removing ${toRemove.length} local file(s)...`);
        await client.toggleLocalCopy(state.activeLocationId, toRemove, false);
        addActivity("info", `Removed ${toRemove.length} local file(s).`);
      }
    } catch (err) {
      addActivity("error", `Local copy toggle error: ${err}`);
    }

    // Optimistically update cached entries so the tree stays in sync
    // (the backend snapshots are stale after toggle, so refreshFileTree would revert checkboxes)
    if (lastFileEntriesJson) {
      const updated: FileEntry[] = JSON.parse(lastFileEntriesJson);
      const downloadSet = new Set(toDownload);
      const removeSet = new Set(toRemove);
      for (const entry of updated) {
        if (downloadSet.has(entry.path)) entry.hasLocalCopy = true;
        if (removeSet.has(entry.path)) entry.hasLocalCopy = false;
      }
      lastFileEntriesJson = JSON.stringify(updated);
    }
  }

  async function handleFileDelete(path: string) {
    const confirmed = await confirmModal({
      title: "Delete file?",
      message: `"${path}" will be permanently deleted from both local storage and the remote bucket. This cannot be undone.`,
      acceptLabel: "Delete",
      rejectLabel: "Cancel",
      variant: "danger",
    });

    if (!confirmed) return;

    if (!state.activeLocationId) {
      toast("No active sync location selected.", "error");
      return;
    }

    try {
      await client.deleteFile(state.activeLocationId, path);
      toast("File deleted.", "success");
      addActivity("info", "Deleted file", path);

      // Optimistically remove the deleted entry and re-render
      // (the backend snapshots are stale after delete, so refreshFileTree would still show it)
      if (lastFileEntriesJson) {
        const updated: FileEntry[] = JSON.parse(lastFileEntriesJson);
        const filtered = updated.filter((e) => e.path !== path);
        lastFileEntriesJson = JSON.stringify(filtered);

        if (fileTreeHandle) {
          fileTreeHandle.destroy();
          fileTreeHandle = null;
        }
        fileTreeHandle = renderFileTree({
          treeEl: dom.fileTree,
          emptyStateEl: dom.fileTreeEmptyState,
          entries: filtered,
          onChange: debouncedFileTreeChange,
          onDelete: handleFileDelete,
          onStorageClass: handleStorageClassChange,
        });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      toast(`Failed to delete file: ${message}`, "error");
      addActivity("error", "Failed to delete file", String(error));
    }
  }

  async function handleStorageClassChange(path: string, currentStorageClass: string | null) {
    const isGlacier =
      currentStorageClass === "GLACIER_IR" ||
      currentStorageClass === "DEEP_ARCHIVE" ||
      currentStorageClass === "GLACIER";

    if (isGlacier) {
      // Currently in Glacier — offer to restore to Standard
      const confirmed = await confirmModal({
        title: "Restore from Glacier?",
        message: `"${path}" is currently in Glacier storage. Restore it to Standard storage? This will make the file available for syncing again.`,
        acceptLabel: "Restore to Standard",
        rejectLabel: "Cancel",
      });

      if (!confirmed) return;

      if (!state.activeLocationId) {
        toast("No active sync location selected.", "error");
        return;
      }

      try {
        await client.changeStorageClass(state.activeLocationId, path, "STANDARD");
        toast("File restored to Standard storage.", "success");
        addActivity("info", "Restored file from Glacier", path);

        // Optimistically update the cached entry
        if (lastFileEntriesJson) {
          const updated: FileEntry[] = JSON.parse(lastFileEntriesJson);
          const entry = updated.find((e) => e.path === path);
          if (entry) {
            entry.storageClass = "STANDARD";
            entry.status = "remote-only";
          }
          lastFileEntriesJson = JSON.stringify(updated);

          if (fileTreeHandle) {
            fileTreeHandle.destroy();
            fileTreeHandle = null;
          }
          fileTreeHandle = renderFileTree({
            treeEl: dom.fileTree,
            emptyStateEl: dom.fileTreeEmptyState,
            entries: updated,
            onChange: debouncedFileTreeChange,
            onDelete: handleFileDelete,
            onStorageClass: handleStorageClassChange,
          });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        toast(`Failed to restore file: ${message}`, "error");
        addActivity("error", "Failed to restore file from Glacier", String(error));
      }
    } else {
      // Currently in Standard (or unknown) — offer to move to Glacier
      const confirmed = await confirmModal({
        title: "Move to Glacier storage?",
        message: `"${path}" will be moved to Amazon S3 Glacier Instant Retrieval. The local copy will not be available after this transition. The file will remain accessible on-demand from Glacier.`,
        acceptLabel: "Move to Glacier",
        rejectLabel: "Cancel",
        variant: "danger",
      });

      if (!confirmed) return;

      if (!state.activeLocationId) {
        toast("No active sync location selected.", "error");
        return;
      }

      try {
        await client.changeStorageClass(state.activeLocationId, path, "GLACIER_IR");
        toast("File moved to Glacier storage.", "success");
        addActivity("info", "Moved file to Glacier storage", path);

        // Optimistically update the cached entry
        if (lastFileEntriesJson) {
          const updated: FileEntry[] = JSON.parse(lastFileEntriesJson);
          const entry = updated.find((e) => e.path === path);
          if (entry) {
            entry.storageClass = "GLACIER_IR";
            entry.status = "glacier";
            entry.hasLocalCopy = false;
          }
          lastFileEntriesJson = JSON.stringify(updated);

          if (fileTreeHandle) {
            fileTreeHandle.destroy();
            fileTreeHandle = null;
          }
          fileTreeHandle = renderFileTree({
            treeEl: dom.fileTree,
            emptyStateEl: dom.fileTreeEmptyState,
            entries: updated,
            onChange: debouncedFileTreeChange,
            onDelete: handleFileDelete,
            onStorageClass: handleStorageClassChange,
          });
        }
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        toast(`Failed to move file to Glacier: ${message}`, "error");
        addActivity("error", "Failed to move file to Glacier", String(error));
      }
    }
  }

  async function refreshFileTree() {
    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    if (!state.activeLocationId) {
      if (fileTreeHandle) {
        fileTreeHandle.destroy();
        fileTreeHandle = null;
      }
      lastFileEntriesJson = "";
      dom.fileTreeEmptyState.hidden = false;
      dom.fileTree.hidden = true;
      dom.fileTree.innerHTML = "";
      return;
    }

    const entries = await client.listFileEntries(state.activeLocationId);
    const entriesJson = JSON.stringify(entries);
    if (entriesJson === lastFileEntriesJson && fileTreeHandle) {
      return; // Data hasn't changed, skip rebuild
    }
    lastFileEntriesJson = entriesJson;

    if (fileTreeHandle) {
      fileTreeHandle.destroy();
      fileTreeHandle = null;
    }

    fileTreeHandle = renderFileTree({
      treeEl: dom.fileTree,
      emptyStateEl: dom.fileTreeEmptyState,
      entries,
      onChange: debouncedFileTreeChange,
      onDelete: handleFileDelete,
      onStorageClass: handleStorageClassChange,
    });
  }

  dom.activeLocationSelect.addEventListener("change", () => {
    state.activeLocationId = dom.activeLocationSelect.value || null;
    state.profile = { ...state.profile, activeLocationId: state.activeLocationId };
    void persistence.saveSettings(toStoredProfile(state.profile));
    renderProfileSummary();
    void refreshFileTree();
  });

  dom.createCredentialBtn.addEventListener("click", () => void handleCreateCredential());
  dom.saveSettingsBtn.addEventListener("click", () => void handleSaveSettings());
  dom.saveLocationBtn.addEventListener("click", () => void handleSaveLocation());
  dom.cancelEditLocationBtn.addEventListener("click", () => {
    resetLocationForm();
    dom.locationsResult.textContent = "Edit cancelled.";
  });
  dom.locationBrowseFolderBtn.addEventListener("click", async () => {
    const selected = await client.chooseLocalFolder();
    if (!selected) {
      toast("Folder picker is available in the desktop app.", "info");
      return;
    }
    dom.locationLocalFolderInput.value = selected;
  });
  dom.openDebugLogFolderBtn.addEventListener("click", async () => {
    await client.openActivityDebugLogFolder();
  });

  bindNavigation({
    root: dom.nav,
    onSelect: (id) => {
      switch (id) {
        case "nav-home": closeAllDialogs(); break;
        case "nav-credentials": openDialog("credentials"); break;
        case "nav-locations": openDialog("locations"); break;
        case "nav-activity": openDialog("activity"); break;
        case "nav-settings": openDialog("settings"); break;
      }
    },
  });

  const unlistenStatus = await client.listenSyncStatus((status) => {
    state.status = status;
    renderStatus();
    addActivity("info", `Status updated: ${describeSyncStatus(status).badgeLabel}. ${new Intl.NumberFormat().format(getSyncOverviewStats(status).inSync)} files are in sync.`);
    debouncedRefreshFileTree();
  });

  const unlistenActivity = await client.listenNativeActivity((event) => {
    addActivityItem(createNativeActivity(event));
  });

  const handleBeforeUnload = () => {
    void unlistenStatus();
    void unlistenActivity();
  };

  window.addEventListener("beforeunload", handleBeforeUnload);

  const storedProfile = await persistence.load();
  state.profile = applyStoredProfile(storedProfile);
  state.activeLocationId = storedProfile.activeLocationId ?? null;
  writeSettingsToDom();
  await refreshCredentials();
  await refreshStatus();
  await refreshDebugLogState();
  await refreshSyncLocations();

  dom.homeScreen.hidden = false;
  dom.navTriggerLabel.textContent = "Menu";
  dom.settingsResult.textContent = client.supportsNativeProfilePersistence
    ? "Settings changes stay local until you save them."
    : "Settings changes stay local to this browser preview after you save them.";
  dom.credentialsResult.textContent = client.supportsNativeProfilePersistence
    ? "Create named credentials here. The UI will show whether each one was saved and tested."
    : "Credential management is shown here for preview, but real credentials are desktop-only.";
  if (!dom.locationsResult.textContent?.trim()) {
    dom.locationsResult.textContent = client.supportsNativeProfilePersistence
      ? "Create sync locations to connect local folders to remote buckets."
      : "Sync location management is shown here for preview, but real sync locations are desktop-only.";
  }

  addActivity("info", client.supportsNativeProfilePersistence
    ? "Ready to connect a folder, bucket, and named credential."
    : "Browser preview loaded. Credential management and sync stay desktop-only here.");

  return () => {
    debouncedRefreshFileTree.cancel();
    debouncedRenderActivity.cancel();

    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    if (fileTreeHandle) {
      fileTreeHandle.destroy();
      fileTreeHandle = null;
    }

    window.removeEventListener("beforeunload", handleBeforeUnload);
    void unlistenStatus();
    void unlistenActivity();
  };
}
