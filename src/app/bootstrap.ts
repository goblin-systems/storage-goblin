import {
  applyIcons,
  bindNavigation,
  closeDrawer,
  closeModal,
  confirmModal,
  openDrawer,
  openModal,
  setupWindowControls,
  showToast,
} from "@goblin-systems/goblin-design-system";
import { createNativeActivity, createUiActivity } from "./activity";
import { createStorageGoblinClient } from "./client";
import { createAppDom, type AppDom } from "./dom";
import {
  renderFileTree,
  type DeleteTarget,
  type FileEntry,
  type FileTreeHandle,
  type FileTreeMode,
} from "./file-tree";
import {
  applyStoredProfile,
  DEFAULT_REMOTE_BIN_RETENTION_DAYS,
  DEFAULT_PROFILE_DRAFT,
  normalizeProfileDraft,
  toStoredProfile,
} from "./profile";
import { createProfilePersistence } from "./persistence";
import { describeSyncStatus, formatTimestamp, getSyncOverviewStats } from "./status";
import {
  capabilitiesFromProviderDefinition,
  defaultProviderDefinition,
  defaultProviderCapabilities,
  describeCapabilityAvailability,
  getProviderCredentialKind,
  getProviderLabel,
  isCapabilityAvailable,
  normalizeProvider,
} from "./types";
import type {
  ActivityDebugLogState,
  ActivityItem,
  BinEntryMutationResult,
  BinEntryMutationSummary,
  BinEntryRequest,
  BinEntrySource,
  ConflictResolutionDetails,
  CredentialDraft,
  CredentialSummary,
  CredentialTestContext,
  FileVersionEntry,
  LocationSyncStatus,
  PermissionProbeSummary,
  Provider,
  ProviderDefinition,
  ProviderCapabilities,
  ProviderCapabilityStatus,
  StorageProfileDraft,
  SyncLocation,
  SyncLocationDraft,
  SyncStatus,
  VersionComparisonDetails,
  VersionCountEntry,
} from "./types";

type DialogId =
  "credentials" | "locations" | "activity" | "polling" | "debug" | "conflict" | "about";

type SyncStatusWithLocations = SyncStatus & {
  locations?: LocationSyncStatus[];
};

interface LocationViewSelection {
  locationId: string | null;
  mode: FileTreeMode;
}

interface StatusMetric {
  label: string;
  value: string;
}

interface FileTreeSnapshot {
  viewKey: string;
  entries: FileEntry[];
  entriesJson: string;
  versionCounts?: Map<string, number>;
  versionCountsJson?: string;
}

interface AsyncConfirmOptions {
  title: string;
  message: string;
  acceptLabel: string;
  rejectLabel: string;
  variant?: "danger";
  onAccept: () => Promise<void>;
}

interface AsyncConfirmController {
  open: (options: AsyncConfirmOptions) => Promise<boolean>;
  destroy: () => void;
}

type ConflictResolution = "keep-local" | "keep-remote";

type InlineCompareMode = ConflictResolutionDetails["mode"];

interface InlineCompareState {
  status: "idle" | "loading" | "ready" | "error";
  mode: InlineCompareMode | null;
  details: ConflictResolutionDetails | null;
  message: string;
}

interface ConflictResolutionModalOptions {
  locationLabel: string;
  entry: FileEntry;
  onCompare: (entry: FileEntry) => Promise<ConflictResolutionDetails>;
  onResolve: (entry: FileEntry, resolution: ConflictResolution) => Promise<void>;
}

interface ConflictResolutionModalController {
  open: (options: ConflictResolutionModalOptions) => void;
  close: () => void;
  destroy: () => void;
}

class HandledAsyncConfirmError extends Error {}

function encodeLocationSelectValue(locationId: string, mode: FileTreeMode): string {
  return `${mode}:${locationId}`;
}

function decodeLocationSelectValue(value: string): LocationViewSelection {
  if (!value) {
    return { locationId: null, mode: "live" };
  }

  const separatorIndex = value.indexOf(":");
  if (separatorIndex <= 0) {
    return { locationId: value, mode: "live" };
  }

  const mode = value.slice(0, separatorIndex) === "bin" ? "bin" : "live";
  const locationId = value.slice(separatorIndex + 1) || null;
  return { locationId, mode };
}

function describeRemoteBinBehavior(enabled: boolean, retentionDays: number): string {
  if (!enabled) {
    return "Deleting a file removes the local copy immediately and permanently deletes the remote object.";
  }

  return retentionDays === 1
    ? "Deleting a file removes the local copy immediately and moves the remote object into the remote bin for 1 day."
    : `Deleting a file removes the local copy immediately and moves the remote object into the remote bin for ${retentionDays} days.`;
}

function parseRemoteBinRetentionDays(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_REMOTE_BIN_RETENTION_DAYS;
  }

  return Math.min(3650, Math.max(1, Math.round(parsed)));
}

function getDeleteConfirmationMessage(path: string, location: SyncLocation): string {
  if (location.objectVersioningEnabled) {
    return location.provider === "aws"
      ? `"${path}" will be removed from local storage immediately. The remote object will be deleted using S3 object versioning so it can be restored from version history.`
      : `"${path}" will be removed from local storage immediately. The remote object will be deleted using object version history so it can be restored later.`;
  }

  if (!location.remoteBin.enabled) {
    return `"${path}" will be removed from local storage immediately and permanently deleted from the remote bucket. This cannot be undone.`;
  }

  const retentionDays = location.remoteBin.retentionDays;
  const retentionLabel = retentionDays === 1 ? "1 day" : `${retentionDays} days`;
  return `"${path}" will be removed from local storage immediately. The remote object will be moved into this sync location's remote bin for ${retentionLabel}.`;
}

function getFolderDeleteConfirmationMessage(path: string, location: SyncLocation): string {
  if (location.objectVersioningEnabled) {
    return location.provider === "aws"
      ? `Folder "${path}" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be deleted using S3 object versioning so they can be restored from version history.`
      : `Folder "${path}" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be deleted using object version history so they can be restored later.`;
  }

  if (!location.remoteBin.enabled) {
    return `Folder "${path}" and all nested contents will be removed from local storage immediately and permanently deleted from the remote bucket. This cannot be undone.`;
  }

  const retentionDays = location.remoteBin.retentionDays;
  const retentionLabel = retentionDays === 1 ? "1 day" : `${retentionDays} days`;
  return `Folder "${path}" and all nested contents will be removed from local storage immediately. Remote objects in this folder will be moved into this sync location's remote bin for ${retentionLabel}.`;
}

function getLocationBinLabel(location: SyncLocation): string {
  return location.objectVersioningEnabled ? "Deleted" : "Bin";
}

function canViewLocationBin(location: SyncLocation): boolean {
  const capabilities = getLocationCapabilities(location);
  return location.objectVersioningEnabled || isCapabilityAvailable(capabilities.remoteBin);
}

function getVersionedDeleteToastMessage(
  location: SyncLocation,
  subject: "file" | "folder",
): string {
  if (location.provider === "aws") {
    return subject === "file"
      ? "File deleted locally and marked deleted in S3 version history."
      : "Folder deleted locally and marked deleted in S3 version history.";
  }

  return subject === "file"
    ? "File deleted locally and marked deleted in object version history."
    : "Folder deleted locally and marked deleted in object version history.";
}

function getVersionedDeleteActivityMessage(
  location: SyncLocation,
  subject: "file" | "folder",
): string {
  if (location.provider === "aws") {
    return subject === "file"
      ? "Deleted file locally and added S3 delete marker"
      : "Deleted folder locally and added S3 delete markers";
  }

  return subject === "file"
    ? "Deleted file locally and added object-version history marker"
    : "Deleted folder locally and added object-version history markers";
}

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

const FILE_TREE_LOADING_DELAY_MS = 150;

function createHandledAsyncConfirmError(message: string): HandledAsyncConfirmError {
  return new HandledAsyncConfirmError(message);
}

function formatConflictSize(value: number | null | undefined): string {
  if (typeof value !== "number" || !Number.isFinite(value)) return "Unavailable";
  return new Intl.NumberFormat().format(value);
}

function formatConflictTimestamp(value: string | null | undefined): string {
  return value ? formatTimestamp(value) : "Unavailable";
}

function formatConflictEtag(value: string | null | undefined): string {
  return value?.trim() ? value : "Unavailable";
}

function describeConflictKind(value: string | null | undefined): string {
  return value === "directory" ? "Directory" : value === "file" ? "File" : "Unavailable";
}

function isResolvableConflictEntry(entry: FileEntry): boolean {
  return (
    entry.kind === "file" &&
    (entry.status === "conflict" || entry.status === "review-required") &&
    entry.localKind === "file" &&
    entry.remoteKind === "file"
  );
}

function createInitialInlineCompareState(): InlineCompareState {
  return {
    status: "idle",
    mode: null,
    details: null,
    message:
      "Select Compare to load inline previews or open external apps when inline compare is unavailable.",
  };
}

function getInlineCompareLoadingMessage(): string {
  return "Loading conflict comparison…";
}

function getInlineCompareExternalMessage(details: ConflictResolutionDetails | null): string {
  return details?.fallbackReason ?? "This file type uses your OS default app for comparison.";
}

function getInlineCompareErrorMessage(message: string): string {
  return `Compare failed: ${message}`;
}

function createConflictResolutionModalController(
  toastMessage: (message: string, variant?: "success" | "error" | "info") => void,
): ConflictResolutionModalController {
  const backdrop = document.createElement("section");
  backdrop.className = "modal-backdrop storage-modal storage-conflict-resolution-modal";
  backdrop.hidden = true;

  const dialog = document.createElement("div");
  dialog.className =
    "modal-card storage-modal-card storage-modal-card-wide storage-conflict-modal-card";

  const titleId = `storage-conflict-title-${Math.random().toString(36).slice(2)}`;
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", titleId);

  const header = document.createElement("div");
  header.className = "modal-header";

  const title = document.createElement("h3");
  title.id = titleId;

  const closeButton = document.createElement("button");
  closeButton.className = "icon-btn modal-close-btn";
  closeButton.type = "button";
  closeButton.setAttribute("aria-label", "Close conflict resolution dialog");
  closeButton.innerHTML = '<i data-lucide="x"></i>';

  header.append(title, closeButton);

  const body = document.createElement("div");
  body.className = "storage-conflict-modal-body";

  const intro = document.createElement("p");
  intro.className = "modal-body-text";

  const pathCallout = document.createElement("div");
  pathCallout.className = "callout storage-conflict-path-callout";

  const compareHint = document.createElement("p");
  compareHint.className = "hint storage-conflict-compare-hint";

  const compareState = document.createElement("div");
  compareState.className = "callout storage-conflict-compare-state";
  compareState.setAttribute("aria-live", "polite");

  const compareSurface = document.createElement("section");
  compareSurface.className = "storage-conflict-compare-surface";
  compareSurface.hidden = true;

  const compareColumns = document.createElement("div");
  compareColumns.className = "storage-conflict-compare-columns";

  const localComparePanel = document.createElement("section");
  localComparePanel.className = "settings-section mini-panel storage-conflict-compare-panel";
  const localCompareTitle = document.createElement("h4");
  localCompareTitle.textContent = "Local";
  const localCompareContent = document.createElement("div");
  localCompareContent.className = "storage-conflict-compare-content";
  localComparePanel.append(localCompareTitle, localCompareContent);

  const remoteComparePanel = document.createElement("section");
  remoteComparePanel.className = "settings-section mini-panel storage-conflict-compare-panel";
  const remoteCompareTitle = document.createElement("h4");
  remoteCompareTitle.textContent = "Remote";
  const remoteCompareContent = document.createElement("div");
  remoteCompareContent.className = "storage-conflict-compare-content";
  remoteComparePanel.append(remoteCompareTitle, remoteCompareContent);

  compareColumns.append(localComparePanel, remoteComparePanel);
  compareSurface.append(compareColumns);

  const metadataGrid = document.createElement("div");
  metadataGrid.className = "compact-list-grid details-grid storage-conflict-grid";

  const localPanel = document.createElement("section");
  localPanel.className = "settings-section mini-panel storage-conflict-panel";
  const localTitle = document.createElement("h4");
  localTitle.textContent = "Local";
  const localMeta = document.createElement("ul");
  localMeta.className = "compact-list status-list";
  localPanel.append(localTitle, localMeta);

  const remotePanel = document.createElement("section");
  remotePanel.className = "settings-section mini-panel storage-conflict-panel";
  const remoteTitle = document.createElement("h4");
  remoteTitle.textContent = "Remote";
  const remoteMeta = document.createElement("ul");
  remoteMeta.className = "compact-list status-list";
  remotePanel.append(remoteTitle, remoteMeta);

  metadataGrid.append(localPanel, remotePanel);
  body.append(intro, pathCallout, compareHint, compareState, compareSurface, metadataGrid);

  const footer = document.createElement("div");
  footer.className = "modal-footer storage-conflict-footer";

  const cancelButton = document.createElement("button");
  cancelButton.className = "secondary-btn";
  cancelButton.type = "button";
  cancelButton.textContent = "Cancel";

  const compareButton = document.createElement("button");
  compareButton.className = "secondary-btn";
  compareButton.type = "button";

  const keepLocalButton = document.createElement("button");
  keepLocalButton.className = "secondary-btn";
  keepLocalButton.type = "button";

  const keepRemoteButton = document.createElement("button");
  keepRemoteButton.className = "secondary-btn";
  keepRemoteButton.type = "button";

  const createConflictActionContent = (label: string) => {
    const content = document.createElement("span");
    content.className = "storage-conflict-action-content";

    const spinner = document.createElement("span");
    spinner.className = "storage-conflict-action-spinner";
    spinner.setAttribute("aria-hidden", "true");
    spinner.hidden = true;

    const labelEl = document.createElement("span");
    labelEl.className = "storage-conflict-action-label";
    labelEl.textContent = label;

    content.append(spinner, labelEl);
    return { content, spinner, labelEl };
  };

  const compareButtonContent = createConflictActionContent("Compare");
  const keepLocalButtonContent = createConflictActionContent("Keep local");
  const keepRemoteButtonContent = createConflictActionContent("Keep remote");

  compareButton.append(compareButtonContent.content);
  keepLocalButton.append(keepLocalButtonContent.content);
  keepRemoteButton.append(keepRemoteButtonContent.content);

  footer.append(cancelButton, compareButton, keepLocalButton, keepRemoteButton);
  dialog.append(header, body, footer);
  backdrop.append(dialog);
  document.body.append(backdrop);
  applyIcons();

  let visible = false;
  let busyAction: "compare" | ConflictResolution | null = null;
  let currentOptions: ConflictResolutionModalOptions | null = null;
  let inlineCompareState: InlineCompareState = createInitialInlineCompareState();

  const renderMetaList = (list: HTMLUListElement, values: [string, string][]) => {
    list.innerHTML = "";
    for (const [label, value] of values) {
      const item = document.createElement("li");
      const labelEl = document.createElement("span");
      labelEl.textContent = label;
      const valueEl = document.createElement("strong");
      valueEl.textContent = value;
      item.append(labelEl, valueEl);
      list.append(item);
    }
  };

  const setComparePanelText = (container: HTMLDivElement, text: string) => {
    container.innerHTML = "";
    const pre = document.createElement("pre");
    pre.className = "storage-conflict-text-pane";
    pre.textContent = text;
    container.append(pre);
  };

  const setComparePanelImage = (container: HTMLDivElement, src: string, alt: string) => {
    container.innerHTML = "";
    const frame = document.createElement("div");
    frame.className = "storage-conflict-image-frame";
    const image = document.createElement("img");
    image.className = "storage-conflict-image-preview";
    image.src = src;
    image.alt = alt;
    frame.append(image);
    container.append(frame);
  };

  const renderInlineCompareState = () => {
    compareState.textContent = inlineCompareState.message;
    compareState.classList.toggle("danger", inlineCompareState.status === "error");
    compareSurface.hidden =
      inlineCompareState.status !== "ready" || inlineCompareState.mode === "external";

    if (inlineCompareState.status !== "ready" || !inlineCompareState.details) {
      localCompareContent.innerHTML = "";
      remoteCompareContent.innerHTML = "";
      return;
    }

    if (inlineCompareState.mode === "image") {
      setComparePanelImage(
        localCompareContent,
        inlineCompareState.details.localImageDataUrl ?? "",
        `Local preview for ${inlineCompareState.details.path}`,
      );
      setComparePanelImage(
        remoteCompareContent,
        inlineCompareState.details.remoteImageDataUrl ?? "",
        `Remote preview for ${inlineCompareState.details.path}`,
      );
      return;
    }

    if (inlineCompareState.mode === "text") {
      setComparePanelText(localCompareContent, inlineCompareState.details.localText ?? "");
      setComparePanelText(remoteCompareContent, inlineCompareState.details.remoteText ?? "");
      return;
    }

    localCompareContent.innerHTML = "";
    remoteCompareContent.innerHTML = "";
  };

  const syncBusyState = () => {
    const busy = busyAction !== null;
    cancelButton.disabled = busy;
    closeButton.disabled = busy;
    compareButton.disabled = busy || compareButton.dataset.compareEnabled !== "true";
    keepLocalButton.disabled = busy;
    keepRemoteButton.disabled = busy;
    compareButton.classList.toggle("is-loading", busyAction === "compare");
    keepLocalButton.classList.toggle("is-loading", busyAction === "keep-local");
    keepRemoteButton.classList.toggle("is-loading", busyAction === "keep-remote");
    compareButton.setAttribute("aria-busy", busyAction === "compare" ? "true" : "false");
    keepLocalButton.setAttribute("aria-busy", busyAction === "keep-local" ? "true" : "false");
    keepRemoteButton.setAttribute("aria-busy", busyAction === "keep-remote" ? "true" : "false");
    compareButtonContent.spinner.hidden = busyAction !== "compare";
    keepLocalButtonContent.spinner.hidden = busyAction !== "keep-local";
    keepRemoteButtonContent.spinner.hidden = busyAction !== "keep-remote";
  };

  const close = () => {
    if (!visible || busyAction) return;
    visible = false;
    currentOptions = null;
    inlineCompareState = createInitialInlineCompareState();
    renderInlineCompareState();
    closeModal({ backdrop });
  };

  const runAction = async (action: "compare" | ConflictResolution) => {
    if (!currentOptions || busyAction) return;
    busyAction = action;
    syncBusyState();
    try {
      if (action === "compare") {
        inlineCompareState = {
          status: "loading",
          mode: null,
          details: null,
          message: getInlineCompareLoadingMessage(),
        };
        renderInlineCompareState();

        const details = await currentOptions.onCompare(currentOptions.entry);
        inlineCompareState = {
          status: "ready",
          mode: details.mode,
          details,
          message:
            details.mode === "image"
              ? "Showing inline image previews."
              : details.mode === "text"
                ? "Showing inline text comparison."
                : getInlineCompareExternalMessage(details),
        };
        renderInlineCompareState();
      } else {
        await currentOptions.onResolve(currentOptions.entry, action);
        visible = false;
        currentOptions = null;
        closeModal({ backdrop });
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      if (action === "compare") {
        inlineCompareState = {
          status: "error",
          mode: null,
          details: null,
          message: getInlineCompareErrorMessage(message),
        };
        renderInlineCompareState();
      }
      toastMessage(message, "error");
    } finally {
      busyAction = null;
      syncBusyState();
    }
  };

  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop) {
      close();
    }
  });
  closeButton.addEventListener("click", close);
  cancelButton.addEventListener("click", close);
  compareButton.addEventListener("click", () => void runAction("compare"));
  keepLocalButton.addEventListener("click", () => void runAction("keep-local"));
  keepRemoteButton.addEventListener("click", () => void runAction("keep-remote"));

  return {
    open(options) {
      currentOptions = options;
      busyAction = null;
      title.textContent = `Resolve file review · ${options.locationLabel}`;
      intro.textContent = "Choose which version should win for this file before syncing continues.";
      pathCallout.textContent = options.entry.path;

      renderMetaList(localMeta, [
        ["Kind", describeConflictKind(options.entry.localKind)],
        ["Size", formatConflictSize(options.entry.localSize)],
        ["Modified", formatConflictTimestamp(options.entry.localModifiedAt)],
      ]);

      renderMetaList(remoteMeta, [
        ["Kind", describeConflictKind(options.entry.remoteKind)],
        ["Size", formatConflictSize(options.entry.remoteSize)],
        ["Modified", formatConflictTimestamp(options.entry.remoteModifiedAt)],
        ["ETag", formatConflictEtag(options.entry.remoteEtag)],
      ]);

      const compareEnabled =
        options.entry.localKind === "file" && options.entry.remoteKind === "file";
      compareButton.dataset.compareEnabled = compareEnabled ? "true" : "false";
      compareHint.textContent =
        "Compare loads inline image/text previews when available and otherwise opens the local file plus a downloaded remote temp copy externally.";
      inlineCompareState = createInitialInlineCompareState();
      renderInlineCompareState();

      syncBusyState();
      visible = true;
      openModal({ backdrop });
      compareButton.focus();
    },
    close() {
      close();
    },
    destroy() {
      busyAction = null;
      currentOptions = null;
      backdrop.remove();
    },
  };
}

function createAsyncConfirmController(): AsyncConfirmController {
  const backdrop = document.createElement("section");
  backdrop.className = "modal-backdrop storage-modal storage-async-confirm-modal";
  backdrop.hidden = true;

  const dialog = document.createElement("div");
  dialog.className = "modal-card storage-modal-card";

  const titleId = `storage-async-confirm-title-${Math.random().toString(36).slice(2)}`;
  dialog.setAttribute("role", "dialog");
  dialog.setAttribute("aria-modal", "true");
  dialog.setAttribute("aria-labelledby", titleId);

  const header = document.createElement("div");
  header.className = "modal-header";

  const title = document.createElement("h3");
  title.id = titleId;

  const closeButton = document.createElement("button");
  closeButton.className = "icon-btn modal-close-btn modal-btn-reject";
  closeButton.type = "button";
  closeButton.setAttribute("aria-label", "Close confirmation dialog");
  closeButton.innerHTML = '<i data-lucide="x"></i>';

  header.append(title, closeButton);

  const message = document.createElement("p");
  message.className = "modal-body-text";

  const footer = document.createElement("div");
  footer.className = "modal-footer";

  const rejectButton = document.createElement("button");
  rejectButton.className = "secondary-btn modal-btn-reject";
  rejectButton.type = "button";

  const acceptButton = document.createElement("button");
  acceptButton.className = "secondary-btn modal-btn-accept";
  acceptButton.type = "button";

  const acceptButtonLabel = document.createElement("span");
  acceptButtonLabel.className = "modal-btn-label";

  const acceptButtonSpinner = document.createElement("span");
  acceptButtonSpinner.className = "modal-btn-spinner";
  acceptButtonSpinner.setAttribute("aria-hidden", "true");
  acceptButtonSpinner.hidden = true;

  const acceptButtonBusyText = document.createElement("span");
  acceptButtonBusyText.className = "modal-btn-busy-text";
  acceptButtonBusyText.textContent = "Loading";
  acceptButtonBusyText.hidden = true;

  acceptButton.append(acceptButtonSpinner, acceptButtonLabel, acceptButtonBusyText);

  footer.append(rejectButton, acceptButton);
  dialog.append(header, message, footer);
  backdrop.append(dialog);
  document.body.append(backdrop);
  applyIcons();

  let busy = false;
  let visible = false;
  let bodyWasModalOpen = false;
  let currentPromise: Promise<boolean> | null = null;
  let currentResolve: ((accepted: boolean) => void) | null = null;
  let currentOnAccept: (() => Promise<void>) | null = null;

  const syncBusyState = () => {
    setButtonBusy(acceptButton, busy);
    acceptButton.setAttribute("aria-busy", busy ? "true" : "false");
    acceptButtonSpinner.hidden = !busy;
    acceptButtonBusyText.hidden = !busy;
    rejectButton.disabled = busy;
    closeButton.disabled = busy;
  };

  const hide = () => {
    visible = false;
    busy = false;
    syncBusyState();
    backdrop.hidden = true;

    if (!bodyWasModalOpen) {
      document.body.classList.remove("modal-open");
    }

    currentPromise = null;
    currentResolve = null;
    currentOnAccept = null;
  };

  const resolveAndHide = (accepted: boolean) => {
    if (!visible || busy) return;
    const resolve = currentResolve;
    hide();
    resolve?.(accepted);
  };

  const handleKeyDown = (event: KeyboardEvent) => {
    if (event.key !== "Escape" || !visible || busy) return;
    event.preventDefault();
    resolveAndHide(false);
  };

  backdrop.addEventListener("click", (event) => {
    if (event.target === backdrop && visible && !busy) {
      resolveAndHide(false);
    }
  });

  rejectButton.addEventListener("click", () => {
    resolveAndHide(false);
  });

  closeButton.addEventListener("click", () => {
    resolveAndHide(false);
  });

  acceptButton.addEventListener(
    "click",
    () =>
      void (async () => {
        if (!visible || busy || !currentOnAccept) return;

        busy = true;
        syncBusyState();

        try {
          await currentOnAccept();
          const resolve = currentResolve;
          hide();
          resolve?.(true);
        } catch {
          busy = false;
          syncBusyState();
        }
      })(),
  );

  document.addEventListener("keydown", handleKeyDown, true);

  return {
    open(options) {
      if (currentPromise) {
        return currentPromise;
      }

      title.textContent = options.title;
      message.textContent = options.message;
      rejectButton.textContent = options.rejectLabel;
      acceptButtonLabel.textContent = options.acceptLabel;
      acceptButton.classList.toggle("danger", options.variant === "danger");

      busy = false;
      syncBusyState();
      currentOnAccept = options.onAccept;
      bodyWasModalOpen = document.body.classList.contains("modal-open");
      backdrop.hidden = false;
      document.body.classList.add("modal-open");
      visible = true;
      rejectButton.focus();

      currentPromise = new Promise<boolean>((resolve) => {
        currentResolve = resolve;
      });

      return currentPromise;
    },
    destroy() {
      document.removeEventListener("keydown", handleKeyDown, true);
      hide();
      backdrop.remove();
    },
  };
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

function describeCredentialSummary(credential: CredentialSummary): string | null {
  if (credential.provider === "aws") {
    return credential.summary?.accessKeyIdPreview ?? null;
  }

  return credential.summary?.clientEmail ?? credential.summary?.projectId ?? null;
}

function getSelectedCredentialContextLabel(profile: StorageProfileDraft): string {
  return profile.bucket ? `Selected for bucket "${profile.bucket}"` : "Selected for current setup";
}

function getEffectiveProfileProvider(profile: StorageProfileDraft): Provider {
  return profile.selectedCredential?.provider ?? profile.provider;
}

function getLocationCapabilities(
  location: Pick<SyncLocationDraft, "provider" | "providerDefinition" | "capabilities">,
): ProviderCapabilities {
  return (
    location.capabilities ??
    capabilitiesFromProviderDefinition(
      location.providerDefinition ?? defaultProviderDefinition(location.provider),
      location.provider,
    )
  );
}

function getLocationProviderDefinition(
  location: Pick<SyncLocationDraft, "provider" | "providerDefinition" | "capabilities">,
): ProviderDefinition {
  return location.providerDefinition ?? defaultProviderDefinition(location.provider);
}

function getProviderLocationLabel(provider: Provider): string {
  return provider === "aws" ? "Region" : "Bucket location";
}

function getProviderLocationHelp(provider: Provider): string {
  return provider === "aws"
    ? "Choose the AWS region for this bucket when creation or validation requires it."
    : "Use the bucket location or leave blank when Google Cloud Storage can infer it automatically.";
}

function describeCapabilityState(label: string, capability: ProviderCapabilityStatus): string {
  switch (capability.status) {
    case "unsupported":
      return `${label}: unsupported`;
    case "permission-unavailable":
      return `${label}: permission required`;
    case "config-unavailable":
      return `${label}: setup required`;
    case "runtime-unavailable":
      return `${label}: temporarily unavailable`;
    case "supported":
    default:
      return `${label}: available`;
  }
}

function describeVersionHistoryLabel(provider: Provider): string {
  return provider === "aws" ? "bucket version history" : "object version history";
}

function getArchiveActionLabel(provider: Provider): string {
  return provider === "aws" ? "Archive storage" : "Storage class";
}

function setControlDisabledState(control: HTMLElement, disabled: boolean, reason?: string | null) {
  if ("disabled" in control) {
    (
      control as HTMLInputElement | HTMLButtonElement | HTMLSelectElement | HTMLTextAreaElement
    ).disabled = disabled;
  }
  if (disabled && reason) {
    control.setAttribute("title", reason);
    control.setAttribute("aria-label", reason);
  } else {
    control.removeAttribute("title");
    control.removeAttribute("aria-label");
  }
}

function renderCredentialFormState(dom: AppDom, provider: Provider) {
  const isAws = getProviderCredentialKind(provider) === "access-key";
  dom.credentialProviderHelp.textContent = isAws
    ? "Use AWS access keys for S3."
    : "Paste a Google Cloud service account JSON document for GCS.";
  dom.credentialAccessKeyField.hidden = !isAws;
  dom.credentialSecretKeyField.hidden = !isAws;
  dom.credentialServiceAccountField.hidden = isAws;

  dom.credentialAccessKeyInput.placeholder = isAws ? "AKIA..." : "";
  dom.credentialSecretKeyInput.placeholder = isAws ? "Stored securely after creation" : "";
}

function getCapabilityBadgeText(capability: ProviderCapabilityStatus): string {
  switch (capability.status) {
    case "unsupported":
      return "Unsupported";
    case "permission-unavailable":
      return "Permission required";
    case "config-unavailable":
      return "Setup required";
    case "runtime-unavailable":
      return "Temporarily unavailable";
    case "supported":
    default:
      return "Available";
  }
}

function setLocationOptions(
  select: HTMLSelectElement,
  options: { value: string; label: string }[],
  currentValue: string,
) {
  const normalizedCurrent = currentValue.trim();
  select.innerHTML = "";
  for (const optionDef of options) {
    const option = document.createElement("option");
    option.value = optionDef.value;
    option.textContent = optionDef.label;
    select.append(option);
  }

  if (options.some((option) => option.value === normalizedCurrent)) {
    select.value = normalizedCurrent;
    return;
  }

  if (normalizedCurrent) {
    const option = document.createElement("option");
    option.value = normalizedCurrent;
    option.textContent = normalizedCurrent;
    select.append(option);
    select.value = normalizedCurrent;
    return;
  }

  select.value = options[0]?.value ?? "";
}

function getProviderLocationOptions(
  definition: ProviderDefinition,
): { value: string; label: string }[] {
  if (definition.provider === "gcs") {
    return [
      { value: "", label: "Auto-detect from bucket" },
      { value: "US", label: "US multi-region" },
      { value: "EU", label: "EU multi-region" },
      { value: "ASIA", label: "Asia multi-region" },
      { value: "us-central1", label: "Iowa — us-central1" },
      { value: "us-east1", label: "South Carolina — us-east1" },
      { value: "us-east4", label: "Northern Virginia — us-east4" },
      { value: "us-west1", label: "Oregon — us-west1" },
      { value: "us-west2", label: "Los Angeles — us-west2" },
      { value: "northamerica-northeast1", label: "Montréal — northamerica-northeast1" },
      { value: "southamerica-east1", label: "São Paulo — southamerica-east1" },
      { value: "europe-west1", label: "Belgium — europe-west1" },
      { value: "europe-west2", label: "London — europe-west2" },
      { value: "europe-west4", label: "Netherlands — europe-west4" },
      { value: "europe-central2", label: "Warsaw — europe-central2" },
      { value: "asia-east1", label: "Taiwan — asia-east1" },
      { value: "asia-northeast1", label: "Tokyo — asia-northeast1" },
      { value: "asia-southeast1", label: "Singapore — asia-southeast1" },
      { value: "australia-southeast1", label: "Sydney — australia-southeast1" },
    ];
  }

  return [
    { value: "", label: "Auto-detect" },
    { value: "us-east-1", label: "US East (N. Virginia) — us-east-1" },
    { value: "us-east-2", label: "US East (Ohio) — us-east-2" },
    { value: "us-west-1", label: "US West (N. California) — us-west-1" },
    { value: "us-west-2", label: "US West (Oregon) — us-west-2" },
    { value: "eu-west-1", label: "Europe (Ireland) — eu-west-1" },
    { value: "eu-west-2", label: "Europe (London) — eu-west-2" },
    { value: "eu-central-1", label: "Europe (Frankfurt) — eu-central-1" },
    { value: "ap-southeast-1", label: "Asia Pacific (Singapore) — ap-southeast-1" },
    { value: "ap-southeast-2", label: "Asia Pacific (Sydney) — ap-southeast-2" },
    { value: "ap-northeast-1", label: "Asia Pacific (Tokyo) — ap-northeast-1" },
    { value: "ca-central-1", label: "Canada (Central) — ca-central-1" },
    { value: "sa-east-1", label: "South America (São Paulo) — sa-east-1" },
  ];
}

function getCredentialValidationLabel(credential: CredentialSummary): string {
  switch (credential.validationStatus) {
    case "passed":
      return "test passed";
    case "failed":
      return "test failed";
    case "untested":
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

function getCredentialValidationBadgeTone(
  credential: CredentialSummary,
): "success" | "danger" | "default" {
  return credential.validationStatus === "passed"
    ? "success"
    : credential.validationStatus === "failed"
      ? "danger"
      : "default";
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

function buildCredentialTestContext(profile: StorageProfileDraft): CredentialTestContext {
  return {
    provider: getEffectiveProfileProvider(profile),
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

function syncProfileCredentialState(
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
    activeLocationViewMode: FileTreeMode;
    profile: StorageProfileDraft;
    providerDefinitions: ProviderDefinition[];
    credentials: CredentialSummary[];
    syncLocations: SyncLocation[];
    status: SyncStatusWithLocations;
    activity: ActivityItem[];
    lastConnectAt: string | null;
    debugLogState: ActivityDebugLogState;
  } = {
    activeDialog: null,
    activeLocationId: null,
    activeLocationViewMode: "live",
    profile: DEFAULT_PROFILE_DRAFT,
    providerDefinitions: [],
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
  const fileTreeSnapshots = new Map<string, FileTreeSnapshot>();
  let selectedBinPaths = new Set<string>();
  let fileTreeRequestSequence = 0;
  /** Version counts per file path for the active versioned location. */
  let activeVersionCounts: Map<string, number> | undefined;
  const asyncConfirm = createAsyncConfirmController();
  const conflictResolutionModal = createConflictResolutionModalController(toast);
  let fileTreeLoadingTimer: ReturnType<typeof setTimeout> | null = null;

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
      timer = setTimeout(() => {
        timer = null;
        fn(...args);
      }, ms);
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

  function formatCount(value: number): string {
    return new Intl.NumberFormat().format(value);
  }

  function getFileTreeViewKey(
    locationId: string | null = state.activeLocationId,
    mode: FileTreeMode = state.activeLocationViewMode,
  ): string {
    return `${locationId ?? "none"}:${mode}`;
  }

  function getActiveLocation() {
    return state.activeLocationId
      ? state.syncLocations.find((location) => location.id === state.activeLocationId)
      : undefined;
  }

  function findProviderDefinition(provider: Provider): ProviderDefinition {
    return (
      state.providerDefinitions.find((definition) => definition.provider === provider) ??
      (state.profile.providerDefinition?.provider === provider
        ? state.profile.providerDefinition
        : null) ??
      defaultProviderDefinition(provider)
    );
  }

  function hydrateSyncLocationMetadata(location: SyncLocation): SyncLocation {
    const providerDefinition =
      location.providerDefinition ?? findProviderDefinition(location.provider);
    return {
      ...location,
      providerDefinition,
      capabilities:
        location.capabilities ??
        capabilitiesFromProviderDefinition(providerDefinition, location.provider),
    };
  }

  function getSavedActiveLocation() {
    return state.activeLocationId
      ? state.profile.syncLocations.find((location) => location.id === state.activeLocationId)
      : undefined;
  }

  function getActiveLocationStatus() {
    return state.activeLocationId
      ? state.status.locations?.find(
          (location) => getLocationSyncStatusId(location) === state.activeLocationId,
        )
      : undefined;
  }

  function getViewSnapshot(viewKey: string = getFileTreeViewKey()): FileTreeSnapshot | null {
    return fileTreeSnapshots.get(viewKey) ?? null;
  }

  function getCurrentViewEntries(): FileEntry[] | null {
    return getViewSnapshot()?.entries ?? null;
  }

  function serializeVersionCounts(
    versionCounts: Map<string, number> | undefined,
  ): string | undefined {
    return versionCounts ? JSON.stringify(Array.from(versionCounts.entries())) : undefined;
  }

  function clearViewSnapshot(viewKey: string) {
    fileTreeSnapshots.delete(viewKey);
  }

  function clearLocationViewSnapshots(locationId: string) {
    clearViewSnapshot(getFileTreeViewKey(locationId, "live"));
    clearViewSnapshot(getFileTreeViewKey(locationId, "bin"));
  }

  async function refreshLocationViews(locationId: string, options: { clearCache?: boolean } = {}) {
    if (options.clearCache) {
      clearLocationViewSnapshots(locationId);
    }

    await refreshStatus();

    if (state.activeLocationId === locationId) {
      await refreshFileTree();
    }
  }

  function destroyFileTree() {
    if (fileTreeHandle) {
      fileTreeHandle.destroy();
      fileTreeHandle = null;
    }
  }

  function setFileTreeLoadingVisible(visible: boolean) {
    dom.fileTreeSection.classList.toggle("is-loading-tree", visible);
    dom.fileTreeSection.setAttribute("aria-busy", visible ? "true" : "false");
    dom.fileTreeLoadingIndicator.hidden = !visible;
  }

  function clearFileTreeLoadingTimer() {
    if (fileTreeLoadingTimer !== null) {
      clearTimeout(fileTreeLoadingTimer);
      fileTreeLoadingTimer = null;
    }
  }

  function beginFileTreeLoading(requestSequence: number) {
    clearFileTreeLoadingTimer();
    setFileTreeLoadingVisible(false);
    fileTreeLoadingTimer = setTimeout(() => {
      fileTreeLoadingTimer = null;
      if (requestSequence === fileTreeRequestSequence) {
        setFileTreeLoadingVisible(true);
      }
    }, FILE_TREE_LOADING_DELAY_MS);
  }

  function endFileTreeLoading(requestSequence?: number) {
    if (typeof requestSequence === "number" && requestSequence !== fileTreeRequestSequence) {
      return;
    }

    clearFileTreeLoadingTimer();
    setFileTreeLoadingVisible(false);
  }

  function renderFileTreeEntries(
    entries: FileEntry[],
    mode: FileTreeMode,
    versionCounts: Map<string, number> | undefined = mode === "live"
      ? activeVersionCounts
      : undefined,
  ) {
    const activeLocation = getActiveLocation();
    destroyFileTree();
    fileTreeHandle = renderFileTree({
      treeEl: dom.fileTree,
      emptyStateEl: dom.fileTreeEmptyState,
      entries,
      mode,
      checkedPaths: mode === "bin" ? Array.from(selectedBinPaths) : undefined,
      onChange:
        mode === "live"
          ? debouncedFileTreeChange
          : (checkedPaths) => {
              selectedBinPaths = new Set(checkedPaths);
              renderBinToolbar();
            },
      onReveal: handleReveal,
      onDelete: mode === "live" ? handleDelete : undefined,
      onRestore: mode === "bin" ? handleBinRestore : undefined,
      onStorageClass: mode === "live" ? handleStorageClassChange : undefined,
      getStorageClassActionState:
        mode === "live"
          ? (_entry) => {
              const archiveCapability = activeLocation
                ? getLocationCapabilities(activeLocation).archiveStorage
                : defaultProviderCapabilities("aws").archiveStorage;
              return {
                disabled: !isCapabilityAvailable(archiveCapability),
                title: isCapabilityAvailable(archiveCapability)
                  ? `Change ${getArchiveActionLabel(activeLocation?.provider ?? "aws").toLowerCase()}`
                  : describeCapabilityAvailability(archiveCapability),
              };
            }
          : undefined,
      onResolveConflict: mode === "live" ? handleResolveConflict : undefined,
      versionCounts: mode === "live" ? versionCounts : undefined,
      onViewVersions: mode === "live" ? handleViewVersions : undefined,
    });
    renderBinToolbar();
  }

  function renderStatusMetrics(metrics: [StatusMetric, StatusMetric, StatusMetric, StatusMetric]) {
    const [local, remote, inSync, notInSync] = metrics;
    dom.statusOverviewLocalLabel.textContent = local.label;
    dom.statusOverviewLocal.textContent = local.value;
    dom.statusOverviewRemoteLabel.textContent = remote.label;
    dom.statusOverviewRemote.textContent = remote.value;
    dom.statusOverviewInSyncLabel.textContent = inSync.label;
    dom.statusOverviewInSync.textContent = inSync.value;
    dom.statusOverviewNotInSyncLabel.textContent = notInSync.label;
    dom.statusOverviewNotInSync.textContent = notInSync.value;
  }

  function getLiveStatusMetrics(
    status: SyncStatus | LocationSyncStatus,
  ): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const overview = getSyncOverviewStats(status);
    return [
      { label: "Local", value: formatCount(overview.localFiles) },
      { label: "Remote", value: formatCount(overview.remoteFiles) },
      { label: "In sync", value: formatCount(overview.inSync) },
      { label: "Changes", value: formatCount(overview.notInSync) },
    ];
  }

  function isActionableLiveEntry(entry: FileEntry): boolean {
    return (
      entry.status === "local-only" ||
      entry.status === "remote-only" ||
      entry.status === "review-required" ||
      entry.status === "conflict"
    );
  }

  function getLiveStatusMetricsFromEntries(
    entries: FileEntry[],
  ): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const files = entries.filter((entry) => entry.kind === "file");
    const localFiles = files.filter((entry) => entry.hasLocalCopy).length;
    const remoteFiles = files.filter((entry) => entry.status !== "local-only").length;
    const inSync = files.filter((entry) => entry.status === "synced").length;
    const changes = files.filter(isActionableLiveEntry).length;

    return [
      { label: "Local", value: formatCount(localFiles) },
      { label: "Remote", value: formatCount(remoteFiles) },
      { label: "In sync", value: formatCount(inSync) },
      { label: "Changes", value: formatCount(changes) },
    ];
  }

  function getBinStatusMetrics(): [StatusMetric, StatusMetric, StatusMetric, StatusMetric] {
    const activeLocation = getActiveLocation();
    const savedActiveLocation = getSavedActiveLocation();
    const activeLocationStatus = getActiveLocationStatus();
    const binEntries = state.activeLocationId
      ? (getViewSnapshot(getFileTreeViewKey(state.activeLocationId, "bin"))?.entries ?? null)
      : null;
    const remoteBinConfig = savedActiveLocation?.remoteBin ?? activeLocation?.remoteBin;
    const retentionValue = remoteBinConfig?.enabled ? `${remoteBinConfig.retentionDays}d` : "Off";
    const livePhase = activeLocationStatus
      ? describeSyncStatus(activeLocationStatus).badgeLabel
      : describeSyncStatus(state.status).badgeLabel;
    const pendingCount =
      activeLocationStatus?.plan.pendingOperationCount ??
      activeLocationStatus?.pendingOperations ??
      0;

    return [
      { label: "Bin items", value: formatCount(binEntries?.length ?? 0) },
      { label: "Retention", value: retentionValue },
      { label: "Live phase", value: livePhase },
      { label: "Pending", value: formatCount(pendingCount) },
    ];
  }

  function getSelectedBinEntries(): FileEntry[] {
    const entries = getCurrentViewEntries() ?? [];
    return entries.filter(
      (entry) =>
        selectedBinPaths.has(entry.path) ||
        (entry.kind === "directory" && selectedBinPaths.has(entry.path)),
    );
  }

  function getBinSelectionSummaryText(): string {
    const count = selectedBinPaths.size;
    if (count === 0) {
      return "Select bin entries to restore or purge.";
    }

    return count === 1 ? "1 bin entry selected." : `${formatCount(count)} bin entries selected.`;
  }

  function renderBinToolbar() {
    const isBinView = state.activeLocationViewMode === "bin" && Boolean(state.activeLocationId);
    dom.binToolbar.hidden = !isBinView;
    dom.binSelectionSummary.textContent = getBinSelectionSummaryText();
    dom.restoreSelectedBtn.disabled = !isBinView || selectedBinPaths.size === 0;
    dom.purgeSelectedBtn.disabled = !isBinView || selectedBinPaths.size === 0;
  }

  function clearBinSelection() {
    selectedBinPaths = new Set<string>();
    renderBinToolbar();
  }

  function buildBinEntryRequest(entry: FileEntry): BinEntryRequest {
    return {
      path: entry.path,
      kind: entry.kind,
      binKey: entry.binKey ?? null,
    };
  }

  function getSelectedBinPathsForEntries(entries: FileEntry[]): Set<string> {
    return new Set(entries.map((entry) => entry.path));
  }

  function isBinMutationSummary(value: unknown): value is BinEntryMutationSummary {
    return (
      typeof value === "object" &&
      value !== null &&
      "results" in value &&
      Array.isArray((value as { results?: unknown }).results)
    );
  }

  function partitionBinMutationResults(
    requestedEntries: FileEntry[],
    summary: BinEntryMutationSummary | null,
  ): { successful: BinEntryMutationResult[]; failed: BinEntryMutationResult[] } {
    if (!summary) {
      return {
        successful: requestedEntries.map((entry) => ({
          path: entry.path,
          kind: entry.kind,
          binKey: entry.binKey ?? null,
          success: true,
          affectedCount: 1,
          error: null,
        })),
        failed: [],
      };
    }

    const successful = summary.results.filter((result) => result.success);
    const failed = summary.results.filter((result) => !result.success);
    return { successful, failed };
  }

  function getBinMutationOutcomeMessage(options: {
    action: "restore" | "purge";
    location: SyncLocation;
    requestedCount: number;
    successCount: number;
    failureCount: number;
  }): {
    toastMessage: string;
    toastVariant: "success" | "error" | "info";
    activityMessage: string;
  } {
    const { action, location, requestedCount, successCount, failureCount } = options;
    const noun = requestedCount === 1 ? "entry" : "entries";

    if (failureCount === 0) {
      if (action === "restore") {
        return {
          toastMessage: getBinRestoreToastMessage(location, successCount),
          toastVariant: "success",
          activityMessage: `Restored ${successCount} bin ${noun}`,
        };
      }

      return {
        toastMessage:
          successCount === 1
            ? "Purged 1 bin entry permanently."
            : `Purged ${successCount} bin entries permanently.`,
        toastVariant: "success",
        activityMessage: `Purged ${successCount} bin ${noun} permanently`,
      };
    }

    if (successCount === 0) {
      return {
        toastMessage:
          action === "restore"
            ? `Restore failed for ${failureCount} bin ${noun}.`
            : `Purge failed for ${failureCount} bin ${noun}.`,
        toastVariant: "error",
        activityMessage:
          action === "restore"
            ? `Restore failed for ${failureCount} bin ${noun}`
            : `Purge failed for ${failureCount} bin ${noun}`,
      };
    }

    return {
      toastMessage:
        action === "restore"
          ? `Restored ${successCount} of ${requestedCount} bin ${noun}; ${failureCount} failed.`
          : `Purged ${successCount} of ${requestedCount} bin ${noun}; ${failureCount} failed.`,
      toastVariant: "info",
      activityMessage:
        action === "restore" ? `Partially restored bin ${noun}` : `Partially purged bin ${noun}`,
    };
  }

  function formatBinMutationFailureDetails(failed: BinEntryMutationResult[]): string | null {
    if (failed.length === 0) {
      return null;
    }

    return failed.map((result) => `${result.path}: ${result.error ?? "Unknown error"}`).join("\n");
  }

  function getBinSourceLabel(source: BinEntrySource | null | undefined): string {
    return source === "object-versioning" ? "object version history" : "remote bin";
  }

  function getBinRestoreToastMessage(location: SyncLocation, count: number): string {
    const subject = count === 1 ? "entry" : "entries";
    return location.objectVersioningEnabled
      ? `Restored ${count} ${subject} from object version history.`
      : `Restored ${count} ${subject} from the remote bin.`;
  }

  function getBinPurgeConfirmationMessage(location: SyncLocation, count: number): string {
    const itemLabel = count === 1 ? "selected bin entry" : `${count} selected bin entries`;
    return location.objectVersioningEnabled
      ? `Purge ${itemLabel}? This permanently deletes the selected object versions from ${describeVersionHistoryLabel(location.provider)}. This cannot be undone.`
      : `Purge ${itemLabel}? This permanently deletes them from the remote bin. This cannot be undone.`;
  }

  function toast(message: string, variant: "success" | "error" | "info" = "info") {
    showToast(message, variant, 2200, "app-toast");
  }

  const dialogs: Record<DialogId, HTMLElement> = {
    credentials: dom.credentialsScreen,
    locations: dom.locationsScreen,
    activity: dom.activityScreen,
    polling: dom.pollingScreen,
    debug: dom.debugScreen,
    conflict: dom.conflictScreen,
    about: dom.aboutScreen,
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
    if (dialogId === "locations") {
      resetLocationForm();
    }
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
    dom.debugLogStatusBadge.textContent = enabled
      ? "Enabled"
      : client.supportsNativeProfilePersistence
        ? "Disabled"
        : "Unavailable";
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
    const selectedCredentialProvider = normalizeProvider(dom.credentialProviderSelect.value);
    dom.credentialsCountBadge.textContent = `${count} saved`;
    dom.credentialsSupportBadge.textContent = client.supportsNativeProfilePersistence
      ? "Desktop app"
      : "Preview only";
    dom.credentialsSupportBadge.className = `badge ${client.supportsNativeProfilePersistence ? "success" : "default"}`;
    dom.credentialsSupportText.textContent = client.supportsNativeProfilePersistence
      ? "Create provider-specific named credentials once, then reuse them across sync locations without re-entering secrets."
      : "Browser preview shows the credential workflow but does not create or store real credentials.";
    dom.createCredentialBtn.disabled = !client.supportsNativeProfilePersistence;
    renderCredentialFormState(dom, selectedCredentialProvider);

    dom.credentialsListStatus.textContent =
      count > 0
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
      const providerLabel = getProviderLabel(credential.provider);
      const summaryText = describeCredentialSummary(credential);
      hint.textContent =
        credential.id === state.profile.credentialProfileId
          ? `${getSelectedCredentialContextLabel(state.profile)} · ${providerLabel} · ${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}${summaryText ? ` · ${summaryText}` : ""}`
          : `${providerLabel} · ${getCredentialStorageLabel(credential)} · ${getCredentialValidationLabel(credential)}${summaryText ? ` · ${summaryText}` : ""}`;

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
      testButton.addEventListener(
        "click",
        () =>
          void (async () => {
            setButtonBusy(testButton, true);

            try {
              const result = await client.testCredential({
                credentialId: credential.id,
                context: buildCredentialTestContext(state.profile),
              });

              state.credentials = state.credentials.map((item) =>
                item.id === result.credential.id ? result.credential : item,
              );
              if (!state.credentials.some((item) => item.id === result.credential.id)) {
                state.credentials = [...state.credentials, result.credential];
              }

              state.profile = syncProfileCredentialState(
                normalizeProfileDraft({
                  ...state.profile,
                  selectedCredential:
                    state.profile.credentialProfileId === result.credential.id
                      ? result.credential
                      : state.profile.selectedCredential,
                }),
                state.credentials,
              );
              renderProfileSummary();

              const baseMessage = result.ok
                ? `Credential "${result.credential.name}" test passed. Can access ${result.bucketCount} bucket(s).`
                : `Credential "${result.credential.name}" test failed.`;

              const permissionLine = formatPermissionSummary(result.permissions);
              const displayMessage = permissionLine
                ? `${baseMessage} ${permissionLine}`
                : baseMessage;

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
          })(),
      );
      actions.append(testButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.disabled = !client.supportsNativeProfilePersistence;
      deleteButton.addEventListener("click", () => {
        const wasSelected = credential.id === state.profile.credentialProfileId;
        void asyncConfirm.open({
          title: "Delete credential?",
          message: wasSelected
            ? `"${credential.name}" will be deleted. This bucket will need a different credential before it can sync again.`
            : `"${credential.name}" will be permanently deleted.`,
          acceptLabel: "Delete",
          rejectLabel: "Cancel",
          variant: "danger",
          onAccept: async () => {
            try {
              const result = await client.deleteCredential(credential.id);
              if (!result.deleted) {
                const message = client.supportsNativeProfilePersistence
                  ? `Could not delete credential "${credential.name}".`
                  : "Credential deletion is only available in the desktop app.";
                dom.credentialsResult.textContent = message;
                toast(message, "info");
                throw createHandledAsyncConfirmError(message);
              }

              if (wasSelected) {
                state.profile = syncProfileCredentialState(
                  normalizeProfileDraft({
                    ...state.profile,
                    credentialProfileId: result.profile.credentialProfileId,
                    selectedCredential: result.profile.selectedCredential,
                    selectedCredentialAvailable: result.profile.selectedCredentialAvailable,
                    credentialsStoredSecurely: result.profile.credentialsStoredSecurely,
                  }),
                  state.credentials.filter((item) => item.id !== credential.id),
                );
              }

              const message = `Deleted credential "${credential.name}".`;
              dom.credentialsResult.textContent = message;
              addActivity("info", message);
              toast(message, "success");
              await refreshCredentials();
              renderProfileSummary();
            } catch (error) {
              if (error instanceof HandledAsyncConfirmError) {
                throw error;
              }

              const message = error instanceof Error ? error.message : String(error);
              const surfacedMessage = `Delete credential failed: ${message}`;
              dom.credentialsResult.textContent = surfacedMessage;
              addActivity("error", surfacedMessage);
              toast(surfacedMessage, "error");
              throw createHandledAsyncConfirmError(surfacedMessage);
            }
          },
        });
      });
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.credentialsList.append(li);
    }
  }

  function renderStatus() {
    const activeLocationStatus = getActiveLocationStatus();
    const activeLocation = getActiveLocation();
    const effectiveStatus = activeLocationStatus ?? state.status;
    const aggregatePresentation = describeSyncStatus(state.status);
    const effectivePresentation = describeSyncStatus(effectiveStatus);
    const headerPresentation = activeLocationStatus ? effectivePresentation : aggregatePresentation;

    dom.syncPhaseBadge.textContent = headerPresentation.badgeLabel;
    dom.syncPhaseBadge.className = `badge ${headerPresentation.badgeTone}`;
    dom.statusPhaseInline.textContent = effectivePresentation.badgeLabel;
    dom.statusPhaseInline.className = `badge ${effectivePresentation.badgeTone}`;
    dom.statusSummary.textContent = effectivePresentation.summary;
    dom.windowSubtitle.textContent = headerPresentation.summary;

    const cachedLiveEntries = state.activeLocationId
      ? (getViewSnapshot(getFileTreeViewKey(state.activeLocationId, "live"))?.entries ?? null)
      : null;

    renderStatusMetrics(
      activeLocationStatus || !cachedLiveEntries
        ? getLiveStatusMetrics(effectiveStatus)
        : getLiveStatusMetricsFromEntries(cachedLiveEntries),
    );

    if (state.activeLocationViewMode === "bin" && state.activeLocationId) {
      const label = activeLocation
        ? activeLocation.label || activeLocation.bucket
        : "selected location";
      dom.statusPhaseInline.textContent = activeLocation
        ? getLocationBinLabel(activeLocation)
        : "Deleted";
      dom.statusPhaseInline.className = "badge danger";
      const binLabel = activeLocation ? getLocationBinLabel(activeLocation) : "Deleted";
      dom.statusSummary.textContent = `Viewing ${label} ${binLabel}. Restore entries back into the live sync location.`;
      dom.windowSubtitle.textContent = `Viewing ${label} ${binLabel}.`;
      renderStatusMetrics(getBinStatusMetrics());
    }
  }

  function renderFileTreeViewState() {
    dom.fileTreeSection.classList.toggle("is-bin-view", state.activeLocationViewMode === "bin");
    renderBinToolbar();
    const emptyStateText =
      state.activeLocationViewMode === "bin"
        ? "Select a deleted-items view to browse recoverable files."
        : "Select a sync location to browse files.";
    const emptyStateCard = dom.fileTreeEmptyState.querySelector<HTMLElement>(".empty-state-card");
    if (emptyStateCard) {
      emptyStateCard.textContent = emptyStateText;
    } else {
      dom.fileTreeEmptyState.textContent = emptyStateText;
    }
  }

  function getSelectedLocationProvider(): Provider {
    const editingLocation = getEditingLocation();
    if (editingLocation) {
      return editingLocation.provider;
    }

    const credentialId = dom.locationCredentialSelect.value || null;
    const credential = state.credentials.find((item) => item.id === credentialId);
    return (
      credential?.provider ??
      normalizeProvider(
        dom.locationProviderSelect.value || getEffectiveProfileProvider(state.profile),
      )
    );
  }

  function getSelectedLocationCapabilities(): ProviderCapabilities {
    const provider = getSelectedLocationProvider();
    const editingLocation = getEditingLocation();

    return editingLocation?.provider === provider
      ? getLocationCapabilities(editingLocation)
      : capabilitiesFromProviderDefinition(
          state.providerDefinitions.find((definition) => definition.provider === provider) ??
            state.profile.providerDefinition ??
            null,
          provider,
        );
  }

  function getSelectedLocationProviderDefinition(): ProviderDefinition {
    const provider = getSelectedLocationProvider();
    const editingLocation = getEditingLocation();

    return editingLocation?.provider === provider
      ? getLocationProviderDefinition(editingLocation)
      : (state.providerDefinitions.find((definition) => definition.provider === provider) ??
          (state.profile.providerDefinition?.provider === provider
            ? state.profile.providerDefinition
            : null) ??
          defaultProviderDefinition(provider));
  }

  function renderLocationCapabilities(
    providerDefinition: ProviderDefinition,
    capabilities: ProviderCapabilities,
  ) {
    dom.locationCapabilityVersioningLabel.textContent =
      providerDefinition.provider === "aws" ? "Object versioning" : "Object versioning";
    dom.locationCapabilityRemoteBinLabel.textContent = "Remote bin";
    dom.locationCapabilityArchiveLabel.textContent = getArchiveActionLabel(
      providerDefinition.provider,
    );
    dom.locationCapabilityVersioning.textContent = getCapabilityBadgeText(
      capabilities.objectVersioning,
    );
    dom.locationCapabilityRemoteBin.textContent = getCapabilityBadgeText(capabilities.remoteBin);
    dom.locationCapabilityArchive.textContent = getCapabilityBadgeText(capabilities.archiveStorage);

    const versioningReason = describeCapabilityAvailability(capabilities.objectVersioning);
    const remoteBinReason = describeCapabilityAvailability(capabilities.remoteBin);
    const archiveReason = describeCapabilityAvailability(capabilities.archiveStorage);
    dom.locationCapabilityVersioning.title = versioningReason;
    dom.locationCapabilityRemoteBin.title = remoteBinReason;
    dom.locationCapabilityArchive.title = archiveReason;
    dom.locationCapabilityHelp.textContent = [
      describeCapabilityState("Object versioning", capabilities.objectVersioning),
      describeCapabilityState("Remote bin", capabilities.remoteBin),
      describeCapabilityState(
        getArchiveActionLabel(providerDefinition.provider),
        capabilities.archiveStorage,
      ),
    ].join(" · ");
    dom.locationProviderHelp.textContent =
      providerDefinition.provider === "aws"
        ? "AWS sync locations use S3 regions and S3-specific features when the backend reports them as available."
        : "GCS sync locations use bucket locations and GCS-native storage classes instead of AWS region semantics.";
  }

  function renderLocationProviderState() {
    const providerDefinition = getSelectedLocationProviderDefinition();
    const provider = providerDefinition.provider;
    const capabilities = getSelectedLocationCapabilities();
    const versioningAvailable = isCapabilityAvailable(capabilities.objectVersioning);
    const remoteBinAvailable = isCapabilityAvailable(capabilities.remoteBin);

    dom.locationRegionLabel.textContent = getProviderLocationLabel(provider);
    dom.locationRegionSelect.title = getProviderLocationHelp(provider);
    setLocationOptions(
      dom.locationRegionSelect,
      getProviderLocationOptions(providerDefinition),
      dom.locationRegionSelect.value,
    );
    renderLocationCapabilities(providerDefinition, capabilities);
    const archiveUnavailable = !isCapabilityAvailable(capabilities.archiveStorage);
    dom.locationCapabilitiesList
      .querySelector("#location-capability-archive")
      ?.parentElement?.classList.toggle("is-disabled", archiveUnavailable);
    dom.locationCapabilitiesList
      .querySelector("#location-capability-versioning")
      ?.parentElement?.classList.toggle("is-disabled", !versioningAvailable);
    dom.locationCapabilitiesList
      .querySelector("#location-capability-remote-bin")
      ?.parentElement?.classList.toggle("is-disabled", !remoteBinAvailable);
  }

  function renderProfileSummary() {
    renderCredentialsList();
    renderStatus();
    renderLocationCredentialOptions();
    renderLocationProviderState();
  }

  function getEditingLocation(): SyncLocation | null {
    const editingId = dom.locationEditingId.value.trim();
    return editingId
      ? (state.syncLocations.find((location) => location.id === editingId) ?? null)
      : null;
  }

  function syncCreateLocationFormProviderFromProfile() {
    if (dom.locationEditingId.value.trim()) {
      return;
    }

    dom.locationProviderSelect.value = getEffectiveProfileProvider(state.profile);
    dom.locationProviderInfo.hidden = true;
  }

  function writeSettingsToDom() {
    dom.remotePollingInput.checked = state.profile.remotePollingEnabled;
    dom.pollIntervalInput.value = String(state.profile.pollIntervalSeconds);
    dom.conflictStrategySelect.value = state.profile.conflictStrategy;
    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
  }

  function readSettingsFromDom() {
    state.profile = normalizeProfileDraft({
      ...state.profile,
      remotePollingEnabled: dom.remotePollingInput.checked,
      pollIntervalSeconds: Number(dom.pollIntervalInput.value),
      conflictStrategy: dom.conflictStrategySelect.value as StorageProfileDraft["conflictStrategy"],
      activityDebugModeEnabled: dom.activityDebugModeInput.checked,
    });
  }

  function isObjectVersioningEnabled(): boolean {
    return dom.locationObjectVersioningEnabledInput.value === "true";
  }

  function setObjectVersioningEnabled(enabled: boolean) {
    dom.locationObjectVersioningEnabledInput.value = enabled ? "true" : "false";
    dom.locationVersioningCheckbox.checked = enabled;
    renderObjectVersioningBtn();
  }

  function showVersioningCheckbox() {
    dom.locationVersioningCheckboxWrap.hidden = false;
    dom.locationVersioningBtnWrap.hidden = true;
  }

  function showVersioningButton() {
    dom.locationVersioningCheckboxWrap.hidden = true;
    dom.locationVersioningBtnWrap.hidden = false;
  }

  function renderObjectVersioningBtn() {
    const enabled = isObjectVersioningEnabled();
    dom.locationVersioningBtnIcon.setAttribute(
      "data-lucide",
      enabled ? "shield-off" : "shield-check",
    );
    dom.locationVersioningBtnLabel.textContent = enabled
      ? "Disable object versioning"
      : "Enable object versioning";
    dom.locationObjectVersioningBtn.classList.toggle("danger", enabled);
    applyIcons();
  }

  function renderLocationRemoteBinState() {
    const provider = getSelectedLocationProvider();
    const capabilities = getSelectedLocationCapabilities();
    const objectVersioningEnabled = isObjectVersioningEnabled();
    const objectVersioningAvailable = isCapabilityAvailable(capabilities.objectVersioning);
    const remoteBinAvailable = isCapabilityAvailable(capabilities.remoteBin);
    const enabled =
      objectVersioningEnabled || !remoteBinAvailable
        ? false
        : dom.locationRemoteBinEnabledInput.checked;
    const retentionDays = parseRemoteBinRetentionDays(dom.locationRemoteBinRetentionInput.value);
    if (objectVersioningEnabled || !remoteBinAvailable) {
      dom.locationRemoteBinEnabledInput.checked = false;
    }
    dom.locationRemoteBinEnabledInput.disabled = objectVersioningEnabled || !remoteBinAvailable;
    dom.locationRemoteBinRetentionInput.value = String(retentionDays);
    dom.locationRemoteBinRetentionInput.disabled =
      objectVersioningEnabled || !remoteBinAvailable || !enabled;
    dom.locationRemoteBinHint.textContent = !remoteBinAvailable
      ? describeCapabilityAvailability(capabilities.remoteBin)
      : objectVersioningEnabled
        ? `Object versioning is enabled for this sync location. Remote bin is unavailable in this mode; deleted objects will be recovered from ${describeVersionHistoryLabel(provider)} instead.`
        : describeRemoteBinBehavior(enabled, retentionDays);
    setControlDisabledState(
      dom.locationVersioningCheckbox,
      !objectVersioningAvailable,
      describeCapabilityAvailability(capabilities.objectVersioning),
    );
    setControlDisabledState(
      dom.locationObjectVersioningBtn,
      !objectVersioningAvailable,
      describeCapabilityAvailability(capabilities.objectVersioning),
    );
    setControlDisabledState(
      dom.locationRemoteBinEnabledInput,
      objectVersioningEnabled || !remoteBinAvailable,
      describeCapabilityAvailability(capabilities.remoteBin),
    );
    setControlDisabledState(
      dom.locationRemoteBinRetentionInput,
      objectVersioningEnabled || !remoteBinAvailable || !enabled,
      describeCapabilityAvailability(capabilities.remoteBin),
    );
  }

  async function refreshStatus() {
    state.status = await client.getSyncStatus();
    renderStatus();
  }

  async function refreshDebugLogState() {
    state.debugLogState = await client.getActivityDebugLogState();
    renderDebugLogState();
  }

  async function refreshProviderDefinitions() {
    const definitions = await client.listProviderCapabilities();
    state.providerDefinitions = definitions;
    const currentProvider = state.profile.provider;
    const currentDefinition =
      definitions.find((definition) => definition.provider === currentProvider) ??
      defaultProviderDefinition(currentProvider);
    state.profile = normalizeProfileDraft({
      ...state.profile,
      providerDefinition: currentDefinition,
      capabilities:
        state.profile.capabilities ??
        capabilitiesFromProviderDefinition(currentDefinition, currentProvider),
      syncLocations: state.profile.syncLocations.map((location) =>
        hydrateSyncLocationMetadata(location),
      ),
    });
  }

  async function refreshCredentials() {
    state.credentials = await client.listCredentials();
    state.profile = syncProfileCredentialState(state.profile, state.credentials);
    syncCreateLocationFormProviderFromProfile();
    renderProfileSummary();
    renderLocationRemoteBinState();
  }

  function mergeSyncLocationsWithStoredProfile(listedLocations: SyncLocation[]): SyncLocation[] {
    const storedLocations = state.profile.syncLocations;
    const storedLocationIds = new Set(storedLocations.map((location) => location.id));

    if (storedLocationIds.size === 0 && !state.profile.activeLocationId) {
      return listedLocations.map((location) => hydrateSyncLocationMetadata(location));
    }

    const listedLocationsById = new Map(listedLocations.map((location) => [location.id, location]));
    return storedLocations.map((storedLocation) => {
      const listedLocation = listedLocationsById.get(storedLocation.id);
      if (!listedLocation) {
        return storedLocation;
      }

      return hydrateSyncLocationMetadata({
        ...listedLocation,
        objectVersioningEnabled: storedLocation.objectVersioningEnabled,
        remoteBin: storedLocation.remoteBin,
        providerDefinition:
          listedLocation.providerDefinition ??
          storedLocation.providerDefinition ??
          defaultProviderDefinition(listedLocation.provider),
        capabilities:
          listedLocation.capabilities ??
          storedLocation.capabilities ??
          getLocationCapabilities(listedLocation),
      });
    });
  }

  function applySyncLocationState(
    syncLocations: SyncLocation[],
    preferredActiveLocationId: string | null = state.activeLocationId,
  ) {
    const activeLocationExists = preferredActiveLocationId
      ? syncLocations.some((location) => location.id === preferredActiveLocationId)
      : false;

    const hydratedSyncLocations = syncLocations.map((location) =>
      hydrateSyncLocationMetadata(location),
    );
    state.syncLocations = hydratedSyncLocations;
    state.activeLocationId =
      syncLocations.length === 0
        ? null
        : activeLocationExists
          ? preferredActiveLocationId
          : hydratedSyncLocations[0].id;
    if (state.activeLocationId === null) {
      state.activeLocationViewMode = "live";
    } else {
      const activeLocation =
        hydratedSyncLocations.find((location) => location.id === state.activeLocationId) ?? null;
      if (
        state.activeLocationViewMode === "bin" &&
        activeLocation &&
        !canViewLocationBin(activeLocation)
      ) {
        state.activeLocationViewMode = "live";
      }
    }
    state.profile = normalizeProfileDraft({
      ...state.profile,
      syncLocations: hydratedSyncLocations,
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
      const liveOption = document.createElement("option");
      liveOption.value = encodeLocationSelectValue(location.id, "live");
      liveOption.textContent = location.label || location.bucket;
      select.append(liveOption);

      if (canViewLocationBin(location)) {
        const binOption = document.createElement("option");
        binOption.value = encodeLocationSelectValue(location.id, "bin");
        binOption.textContent = `${location.label || location.bucket} ${getLocationBinLabel(location)}`;
        select.append(binOption);
      }
    }

    const desiredValue = state.activeLocationId
      ? encodeLocationSelectValue(state.activeLocationId, state.activeLocationViewMode)
      : "";
    select.value = Array.from(select.options).some((option) => option.value === desiredValue)
      ? desiredValue
      : state.activeLocationId
        ? encodeLocationSelectValue(state.activeLocationId, "live")
        : "";
  }

  async function handleSaveSettings(btn: HTMLButtonElement, resultEl: HTMLElement) {
    readSettingsFromDom();
    setButtonBusy(btn, true);

    try {
      const stored = await persistence.saveSettings(toStoredProfile(state.profile));
      state.profile = syncProfileCredentialState(
        normalizeProfileDraft({
          ...state.profile,
          ...stored,
        }),
        state.credentials,
      );
      writeSettingsToDom();
      renderProfileSummary();
      await refreshStatus();
      await refreshDebugLogState();

      const message = client.supportsNativeProfilePersistence
        ? "Settings saved. Preferences are updated."
        : "Settings saved locally in the browser preview.";

      resultEl.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } finally {
      setButtonBusy(btn, false);
    }
  }

  async function handleCreateCredential() {
    const provider = normalizeProvider(dom.credentialProviderSelect.value);
    const name = dom.credentialNameInput.value.trim();
    const draft: CredentialDraft =
      provider === "aws"
        ? {
            name,
            provider: "aws",
            accessKeyId: dom.credentialAccessKeyInput.value.trim(),
            secretAccessKey: dom.credentialSecretKeyInput.value.trim(),
          }
        : {
            name,
            provider: "gcs",
            credential: {
              kind: "gcsServiceAccount",
              serviceAccountJson: dom.credentialServiceAccountInput.value.trim(),
            },
          };

    if (!client.supportsNativeProfilePersistence) {
      const message = "Credential management is only available in the desktop app.";
      dom.credentialsResult.textContent = message;
      toast(message, "info");
      return;
    }

    const invalidAwsDraft =
      draft.provider === "aws" && (!draft.name || !draft.accessKeyId || !draft.secretAccessKey);
    const invalidGcsDraft =
      draft.provider === "gcs" && (!draft.name || !draft.credential.serviceAccountJson);
    if (invalidAwsDraft || invalidGcsDraft) {
      const message =
        draft.provider === "aws"
          ? "Enter a name, access key ID, and secret access key to create an AWS credential."
          : "Enter a name and paste the full service account JSON to create a GCS credential.";
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
      dom.credentialServiceAccountInput.value = "";
      await refreshCredentials();
      state.profile = syncProfileCredentialState(
        normalizeProfileDraft({
          ...state.profile,
          provider: created.provider,
          credentialProfileId: created.id,
          selectedCredential: created,
        }),
        state.credentials,
      );
      syncCreateLocationFormProviderFromProfile();
      renderProfileSummary();
      renderLocationRemoteBinState();

      const message = buildCredentialCreateMessage(created);
      dom.credentialsResult.textContent = `${message} It is now selected for this setup.`;
      toast(`Created credential "${created.name}".`, "success");
      addActivity("success", `Created credential "${created.name}".`);
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

  function renderLocationCredentialOptions(
    preferredValue: string | null = dom.locationCredentialSelect.value || null,
  ) {
    const select = dom.locationCredentialSelect;
    const editingLocation = getEditingLocation();
    select.innerHTML = "";

    const defaultOption = document.createElement("option");
    defaultOption.value = "";
    defaultOption.textContent = "Choose a credential";
    select.append(defaultOption);

    for (const credential of state.credentials) {
      if (editingLocation && credential.provider !== editingLocation.provider) {
        continue;
      }
      const option = document.createElement("option");
      option.value = credential.id;
      option.textContent = `${credential.name} — ${getProviderLabel(credential.provider)}`;
      select.append(option);
    }

    select.value =
      preferredValue && Array.from(select.options).some((option) => option.value === preferredValue)
        ? preferredValue
        : "";
    select.disabled = select.options.length <= 1;
  }

  function resetLocationForm() {
    dom.locationEditingId.value = "";
    dom.locationFormTitle.textContent = "Create sync location";
    dom.locationLabelInput.value = "";
    dom.locationProviderSelectField.hidden = true;
    dom.locationProviderSelect.disabled = false;
    dom.locationProviderSelect.value = getEffectiveProfileProvider(state.profile);
    dom.locationProviderInfo.hidden = true;
    dom.locationLocalFolderInput.value = "";
    dom.locationRegionSelect.value = "";
    dom.locationBucketInput.value = "";
    dom.locationCredentialSelect.value = "";
    setObjectVersioningEnabled(false);
    showVersioningCheckbox();
    dom.locationEnabledInput.checked = true;
    dom.locationPollingInput.checked = true;
    dom.locationPollIntervalInput.value = "60";
    dom.locationConflictStrategySelect.value = state.profile.conflictStrategy;
    dom.locationRemoteBinEnabledInput.checked = true;
    dom.locationRemoteBinRetentionInput.value = String(DEFAULT_REMOTE_BIN_RETENTION_DAYS);
    renderLocationProviderState();
    renderLocationCredentialOptions();
    renderLocationRemoteBinState();
    dom.cancelEditLocationBtn.hidden = true;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Create sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "plus");
    applyIcons();
  }

  function updateLocationProviderLabel(provider: Provider) {
    dom.locationProviderLabel.textContent =
      state.providerDefinitions.find((definition) => definition.provider === provider)
        ?.displayName ?? getProviderLabel(provider);
    dom.locationProviderInfo.hidden = false;
  }

  function populateLocationForm(location: SyncLocation) {
    dom.locationEditingId.value = location.id;
    dom.locationFormTitle.textContent = "Edit sync location";
    dom.locationLabelInput.value = location.label;
    dom.locationProviderSelect.value = location.provider;
    dom.locationProviderSelect.disabled = true;
    dom.locationProviderSelectField.hidden = true;
    updateLocationProviderLabel(location.provider);
    dom.locationCredentialSelect.value = "";
    dom.locationLocalFolderInput.value = location.localFolder;
    dom.locationRegionSelect.value = location.region;
    dom.locationBucketInput.value = location.bucket;
    setObjectVersioningEnabled(location.objectVersioningEnabled);
    renderLocationCredentialOptions(location.credentialProfileId);
    showVersioningButton();
    dom.locationEnabledInput.checked = location.enabled;
    dom.locationPollingInput.checked = location.remotePollingEnabled;
    dom.locationPollIntervalInput.value = String(location.pollIntervalSeconds);
    dom.locationConflictStrategySelect.value = location.conflictStrategy;
    dom.locationRemoteBinEnabledInput.checked = location.remoteBin.enabled;
    dom.locationRemoteBinRetentionInput.value = String(location.remoteBin.retentionDays);
    renderLocationProviderState();
    renderLocationRemoteBinState();
    dom.cancelEditLocationBtn.hidden = false;

    const saveBtnLabel = dom.saveLocationBtn.querySelector("span");
    if (saveBtnLabel) saveBtnLabel.textContent = "Update sync location";
    const saveBtnIcon = dom.saveLocationBtn.querySelector("i");
    if (saveBtnIcon) saveBtnIcon.setAttribute("data-lucide", "save");
    applyIcons();
  }

  function readLocationDraftFromForm(): SyncLocationDraft {
    const editingId = dom.locationEditingId.value.trim() || null;
    const credentialId = dom.locationCredentialSelect.value || null;
    const provider = getSelectedLocationProvider();
    const providerDefinition = getSelectedLocationProviderDefinition();
    const capabilities = getSelectedLocationCapabilities();

    return {
      id: editingId,
      label: dom.locationLabelInput.value.trim(),
      provider,
      localFolder: dom.locationLocalFolderInput.value.trim(),
      region: dom.locationRegionSelect.value,
      bucket: dom.locationBucketInput.value.trim(),
      credentialProfileId: credentialId,
      objectVersioningEnabled: isObjectVersioningEnabled(),
      enabled: dom.locationEnabledInput.checked,
      remotePollingEnabled: dom.locationPollingInput.checked,
      pollIntervalSeconds: Number(dom.locationPollIntervalInput.value) || 60,
      conflictStrategy: dom.locationConflictStrategySelect
        .value as SyncLocationDraft["conflictStrategy"],
      remoteBin: {
        enabled: isObjectVersioningEnabled() ? false : dom.locationRemoteBinEnabledInput.checked,
        retentionDays: parseRemoteBinRetentionDays(dom.locationRemoteBinRetentionInput.value),
      },
      providerDefinition,
      capabilities,
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
      name.textContent = location.label || location.bucket;

      const hint = document.createElement("span");
      hint.className = "hint";
      const folder = location.localFolder || "No folder";
      const bucket = location.bucket || "No bucket";
      const locationLabel = location.region
        ? `${getProviderLocationLabel(location.provider)} ${location.region}`
        : location.provider === "gcs"
          ? "bucket location auto-detect"
          : "region auto-detect";
      hint.textContent = `${folder} → ${bucket} · ${locationLabel}`;

      meta.append(name, hint);

      const actions = document.createElement("div");
      actions.className = "credential-item-actions";

      const enabledBadge = document.createElement("span");
      enabledBadge.className = `badge ${location.enabled ? "success" : "default"}`;
      enabledBadge.textContent = location.enabled ? "enabled" : "paused";
      actions.append(enabledBadge);

      const remoteBinBadge = document.createElement("span");
      remoteBinBadge.className = `badge ${location.objectVersioningEnabled || location.remoteBin.enabled ? "success" : "default"}`;
      remoteBinBadge.textContent = location.objectVersioningEnabled
        ? location.provider === "aws"
          ? "bucket versioning"
          : "object versioning"
        : location.remoteBin.enabled
          ? `remote bin ${location.remoteBin.retentionDays}d`
          : canViewLocationBin(location)
            ? "hard delete"
            : "no deleted-items view";
      actions.append(remoteBinBadge);

      const providerBadge = document.createElement("span");
      providerBadge.className = "badge default";
      providerBadge.textContent = location.provider === "aws" ? "AWS" : "GCS";
      actions.append(providerBadge);

      const editButton = document.createElement("button");
      editButton.className = "secondary-btn slim-btn";
      editButton.type = "button";
      editButton.textContent = "Edit";
      editButton.addEventListener("click", () => {
        populateLocationForm(location);
      });
      actions.append(editButton);

      const deleteButton = document.createElement("button");
      deleteButton.className = "secondary-btn slim-btn";
      deleteButton.type = "button";
      deleteButton.textContent = "Delete";
      deleteButton.addEventListener(
        "click",
        () =>
          void asyncConfirm.open({
            title: "Delete sync location?",
            message: `"${location.label || location.bucket}" will be permanently deleted.`,
            acceptLabel: "Delete",
            rejectLabel: "Cancel",
            variant: "danger",
            onAccept: async () => {
              try {
                const updatedProfile = await client.removeSyncLocation(location.id);
                applySyncLocationState(
                  updatedProfile.syncLocations,
                  updatedProfile.activeLocationId ?? state.activeLocationId,
                );
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
                throw createHandledAsyncConfirmError(message);
              }
            },
          }),
      );
      actions.append(deleteButton);

      li.append(meta, actions);
      dom.locationsList.append(li);
    }
  }

  async function handleSaveLocation() {
    const draft = readLocationDraftFromForm();
    const isEditing = Boolean(draft.id);
    const provider = draft.provider;

    if (!draft.localFolder || !draft.bucket) {
      const message = "Enter a local folder and bucket name to create a sync location.";
      dom.locationsResult.textContent = message;
      toast(message, "error");
      return;
    }

    if (draft.credentialProfileId) {
      const selectedCredential =
        state.credentials.find((credential) => credential.id === draft.credentialProfileId) ?? null;
      if (!selectedCredential) {
        const message =
          "Choose a saved credential for the selected provider or leave the location unassigned for now.";
        dom.locationsResult.textContent = message;
        toast(message, "error");
        return;
      }
      if (selectedCredential.provider !== provider) {
        const message = `The selected credential does not match ${getProviderLabel(provider)}.`;
        dom.locationsResult.textContent = message;
        toast(message, "error");
        return;
      }
    }

    setButtonBusy(dom.saveLocationBtn, true);

    try {
      const updatedProfile = isEditing
        ? await client.updateSyncLocation(draft)
        : await client.addSyncLocation(draft);

      applySyncLocationState(
        updatedProfile.syncLocations,
        updatedProfile.activeLocationId ?? state.activeLocationId,
      );
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

  async function handleVersioningToggle() {
    const editingId = dom.locationEditingId.value.trim();
    const capabilities = getSelectedLocationCapabilities();
    if (!isCapabilityAvailable(capabilities.objectVersioning)) {
      const message = describeCapabilityAvailability(capabilities.objectVersioning);
      dom.locationsResult.textContent = message;
      toast(message, "info");
      return;
    }
    const newEnabled = !isObjectVersioningEnabled();

    if (!editingId) {
      setObjectVersioningEnabled(newEnabled);
      renderLocationRemoteBinState();
      return;
    }

    dom.locationObjectVersioningBtn.disabled = true;
    dom.locationVersioningBtnSpinner.hidden = false;
    dom.locationVersioningBtnIcon.hidden = true;
    dom.locationVersioningBtnLabel.textContent = newEnabled ? "Enabling…" : "Disabling…";

    try {
      const updatedProfile = await client.setSyncLocationVersioning(editingId, newEnabled);
      state.syncLocations = updatedProfile.syncLocations;
      renderLocationsList();
      renderLocationDropdown();
      setObjectVersioningEnabled(newEnabled);
      renderLocationRemoteBinState();
      const message = newEnabled
        ? "Object versioning enabled on bucket."
        : "Object versioning suspended on bucket.";
      dom.locationsResult.textContent = message;
      toast(message, "success");
      addActivity("success", message);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Versioning update failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      toast(surfacedMessage, "error");
      addActivity("error", surfacedMessage);
    } finally {
      dom.locationObjectVersioningBtn.disabled = false;
      dom.locationVersioningBtnSpinner.hidden = true;
      dom.locationVersioningBtnIcon.hidden = false;
      renderObjectVersioningBtn();
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
      applySyncLocationState(
        state.profile.syncLocations,
        state.profile.activeLocationId ?? state.activeLocationId,
      );
      const message = error instanceof Error ? error.message : String(error);
      const surfacedMessage = `Load sync locations failed: ${message}`;
      dom.locationsResult.textContent = surfacedMessage;
      addActivity("error", surfacedMessage);
    }
    renderLocationsList();
    renderLocationDropdown();
    renderStatus();
    renderFileTreeViewState();
    void refreshFileTree();
  }

  async function handleFileTreeChange(checkedPaths: string[]) {
    if (!state.activeLocationId || state.activeLocationViewMode === "bin") return;

    // Get current entries to know the full set of file paths
    const cachedEntries = getCurrentViewEntries();
    const entries: FileEntry[] =
      cachedEntries ?? (await client.listFileEntries(state.activeLocationId));
    const checkedSet = new Set(checkedPaths);
    const mutableEntries = entries.filter(
      (entry) =>
        entry.kind === "file" &&
        entry.status !== "conflict" &&
        entry.status !== "review-required" &&
        entry.status !== "glacier",
    );

    // Files that are newly checked (want local copy) - were remote-only before
    const toDownload = mutableEntries
      .filter((e) => checkedSet.has(e.path) && !e.hasLocalCopy)
      .map((e) => e.path);

    // Files that are newly unchecked (remove local copy) - had local copy before
    const toRemove = mutableEntries
      .filter((e) => !checkedSet.has(e.path) && e.hasLocalCopy)
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
      addActivity(
        "error",
        `Local copy toggle error: ${err instanceof Error ? err.message : String(err)}`,
      );
      await refreshLocationViews(state.activeLocationId, { clearCache: true });
      return;
    }

    await refreshLocationViews(state.activeLocationId, { clearCache: true });
  }

  async function handleDelete(target: DeleteTarget) {
    if (target.kind === "directory") {
      await handleFolderDelete(target.path);
      return;
    }

    await handleFileDelete(target.path);
  }

  async function mutateBinEntries(options: {
    title: string;
    actionLabel: "restore" | "purge";
    entries: FileEntry[];
    location: SyncLocation;
    confirmMessage?: string;
    acceptLabel?: string;
    variant?: "danger";
    action: (entries: BinEntryRequest[]) => Promise<unknown>;
  }) {
    const run = async () => {
      const response = await options.action(options.entries.map(buildBinEntryRequest));
      const summary = isBinMutationSummary(response) ? response : null;
      const { successful, failed } = partitionBinMutationResults(options.entries, summary);
      const outcome = getBinMutationOutcomeMessage({
        action: options.actionLabel,
        location: options.location,
        requestedCount: options.entries.length,
        successCount: successful.length,
        failureCount: failed.length,
      });

      toast(outcome.toastMessage, outcome.toastVariant);
      addActivity(
        failed.length > 0 ? (successful.length > 0 ? "info" : "error") : "info",
        outcome.activityMessage,
        formatBinMutationFailureDetails(failed) ??
          options.entries.map((entry) => entry.path).join("\n"),
      );

      if (failed.length === 0) {
        clearBinSelection();
      } else {
        selectedBinPaths = getSelectedBinPathsForEntries(
          options.entries.filter((entry) =>
            failed.some((result) => result.path === entry.path && result.kind === entry.kind),
          ),
        );
        renderBinToolbar();
      }

      await refreshLocationViews(options.location.id, { clearCache: true });
    };

    if (options.confirmMessage) {
      await asyncConfirm.open({
        title: options.title,
        message: options.confirmMessage,
        acceptLabel: options.acceptLabel ?? "Confirm",
        rejectLabel: "Cancel",
        variant: options.variant,
        onAccept: async () => {
          try {
            await run();
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(message, "error");
            addActivity("error", message);
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
      return;
    }

    try {
      await run();
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      toast(message, "error");
      addActivity("error", message);
    }
  }

  async function handleFileDelete(path: string) {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    await asyncConfirm.open({
      title: "Delete file?",
      message: getDeleteConfirmationMessage(path, activeLocation),
      acceptLabel: "Delete",
      rejectLabel: "Cancel",
      variant: "danger",
      onAccept: async () => {
        try {
          await client.deleteFile(activeLocation.id, path);
          const toastMessage = activeLocation.objectVersioningEnabled
            ? getVersionedDeleteToastMessage(activeLocation, "file")
            : activeLocation.remoteBin.enabled
              ? "File deleted locally and moved to the remote bin."
              : "File permanently deleted.";
          const activityMessage = activeLocation.objectVersioningEnabled
            ? getVersionedDeleteActivityMessage(activeLocation, "file")
            : activeLocation.remoteBin.enabled
              ? "Deleted file locally and moved remote object to remote bin"
              : "Permanently deleted file";
          toast(toastMessage, "success");
          addActivity("info", activityMessage, path);
          await refreshLocationViews(activeLocation.id, { clearCache: true });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          toast(`Failed to delete file: ${message}`, "error");
          addActivity("error", "Failed to delete file", String(error));
          throw createHandledAsyncConfirmError(message);
        }
      },
    });
  }

  async function handleFolderDelete(path: string) {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    await asyncConfirm.open({
      title: "Delete folder?",
      message: getFolderDeleteConfirmationMessage(path, activeLocation),
      acceptLabel: "Delete folder",
      rejectLabel: "Cancel",
      variant: "danger",
      onAccept: async () => {
        try {
          await client.deleteFolder(activeLocation.id, path);
          const toastMessage = activeLocation.objectVersioningEnabled
            ? getVersionedDeleteToastMessage(activeLocation, "folder")
            : activeLocation.remoteBin.enabled
              ? "Folder deleted locally and moved to the remote bin."
              : "Folder permanently deleted.";
          const activityMessage = activeLocation.objectVersioningEnabled
            ? getVersionedDeleteActivityMessage(activeLocation, "folder")
            : activeLocation.remoteBin.enabled
              ? "Deleted folder locally and moved remote subtree to remote bin"
              : "Permanently deleted folder";
          toast(toastMessage, "success");
          addActivity("info", activityMessage, path);
          await refreshLocationViews(activeLocation.id, { clearCache: true });
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error);
          toast(`Failed to delete folder: ${message}`, "error");
          addActivity("error", "Failed to delete folder", String(error));
          throw createHandledAsyncConfirmError(message);
        }
      },
    });
  }

  async function handleBinRestore(entry: FileEntry) {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    if (entry.binKey && entry.kind === "file") {
      try {
        await client.restoreBinEntry(activeLocation.id, entry.binKey);
        toast(`Restored ${entry.kind} from ${getBinSourceLabel(entry.deletedFrom)}.`, "success");
        addActivity(
          "info",
          `Restored ${entry.kind} from ${getBinSourceLabel(entry.deletedFrom)}`,
          entry.path,
        );
        clearBinSelection();
        await refreshLocationViews(activeLocation.id, { clearCache: true });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        toast(message, "error");
        addActivity("error", message);
      }
      return;
    }

    await mutateBinEntries({
      title: "Restore bin entry?",
      actionLabel: "restore",
      entries: [entry],
      location: activeLocation,
      action: (entries) => client.restoreBinEntries(activeLocation.id, entries),
    });
  }

  async function handleBulkBinRestore() {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;
    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    const entries = getSelectedBinEntries();
    if (entries.length === 0) {
      toast("Select at least one bin entry to restore.", "info");
      return;
    }

    await mutateBinEntries({
      title: "Restore selected bin entries?",
      actionLabel: "restore",
      entries,
      location: activeLocation,
      action: (requests) => client.restoreBinEntries(activeLocation.id, requests),
    });
  }

  async function handleBulkBinPurge() {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;
    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    const entries = getSelectedBinEntries();
    if (entries.length === 0) {
      toast("Select at least one bin entry to purge.", "info");
      return;
    }

    await mutateBinEntries({
      title: "Purge selected bin entries?",
      actionLabel: "purge",
      entries,
      location: activeLocation,
      confirmMessage: getBinPurgeConfirmationMessage(activeLocation, entries.length),
      acceptLabel: activeLocation.objectVersioningEnabled
        ? "Purge permanently"
        : "Delete permanently",
      variant: "danger",
      action: (requests) => client.purgeBinEntries(activeLocation.id, requests),
    });
  }

  async function compareConflictEntry(entry: FileEntry): Promise<ConflictResolutionDetails> {
    if (!state.activeLocationId) {
      throw new Error("No active sync location selected.");
    }

    const details = await client.prepareConflictComparison(state.activeLocationId, entry.path);
    if (details.mode !== "external") {
      addActivity("info", `Loaded inline ${details.mode} comparison`, entry.path);
      return details;
    }

    const openTasks: Promise<void>[] = [];

    if (details.localPath) {
      openTasks.push(client.openPath(details.localPath));
    }

    if (details.remoteTempPath) {
      openTasks.push(client.openPath(details.remoteTempPath));
    }

    if (openTasks.length === 0) {
      throw new Error("No compare targets were available for this conflict.");
    }

    await Promise.all(openTasks);
    addActivity("info", "Opened conflict comparison targets", entry.path);
    return details;
  }

  function handleResolveConflict(entry: FileEntry) {
    if (!isResolvableConflictEntry(entry)) {
      toast("This MVP only resolves file-vs-file conflict or review-required entries.", "info");
      return;
    }

    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    conflictResolutionModal.open({
      locationLabel: activeLocation.label || activeLocation.bucket,
      entry,
      onCompare: async (currentEntry) => {
        const details = await compareConflictEntry(currentEntry);
        toast(
          details.mode === "external"
            ? "Opened local and remote versions for comparison."
            : details.mode === "image"
              ? "Loaded inline image comparison."
              : "Loaded inline text comparison.",
          "info",
        );
        return details;
      },
      onResolve: async (currentEntry, resolution) => {
        await client.resolveConflict(activeLocation.id, currentEntry.path, resolution);
        const entryLabel =
          currentEntry.status === "review-required" ? "Review cleared" : "Conflict resolved";
        const message =
          resolution === "keep-local"
            ? `${entryLabel} by keeping the local version.`
            : `${entryLabel} by keeping the remote version.`;
        toast(message, "success");
        addActivity("success", message, currentEntry.path);
        await refreshLocationViews(activeLocation.id, { clearCache: true });
      },
    });
  }

  async function handleReveal(path: string) {
    const activeLocation = state.activeLocationId
      ? (state.syncLocations.find((location) => location.id === state.activeLocationId) ?? null)
      : null;

    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    try {
      await client.revealTreeEntry(activeLocation.id, path);
      addActivity("info", "Revealed local path in file manager", path);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      toast(message, "error");
      addActivity("error", "Failed to reveal local path", message);
    }
  }

  async function handleStorageClassChange(path: string, currentStorageClass: string | null) {
    const activeLocation = getActiveLocation();
    if (!activeLocation) {
      toast("No active sync location selected.", "error");
      return;
    }

    const archiveCapability = getLocationCapabilities(activeLocation).archiveStorage;
    if (!isCapabilityAvailable(archiveCapability)) {
      const message = describeCapabilityAvailability(archiveCapability);
      toast(message, "info");
      addActivity("info", message, path);
      return;
    }

    const provider = activeLocation.provider;

    const isColdStorage =
      currentStorageClass === "GLACIER_IR" ||
      currentStorageClass === "DEEP_ARCHIVE" ||
      currentStorageClass === "GLACIER" ||
      currentStorageClass === "NEARLINE" ||
      currentStorageClass === "COLDLINE" ||
      currentStorageClass === "ARCHIVE";

    const coldConfig =
      provider === "gcs"
        ? {
            restoreTitle: "Restore to Standard?",
            restoreMessage: `"${path}" is currently in ${currentStorageClass} storage. Restore it to Standard storage?`,
            restoreAccept: "Restore to Standard",
            restoreToast: "File restored to Standard storage.",
            restoreActivity: "Restored file to Standard storage",
            restoreErrorLabel: "restore file",
            archiveTitle: "Move to Coldline storage?",
            archiveMessage: `"${path}" will be moved to Google Cloud Storage Coldline. The local copy will not be available after this transition.`,
            archiveAccept: "Move to Coldline",
            archiveTarget: "COLDLINE" as const,
            archiveToast: "File moved to Coldline storage.",
            archiveActivity: "Moved file to Coldline storage",
            archiveErrorLabel: "move file to Coldline",
          }
        : {
            restoreTitle: "Restore from Glacier?",
            restoreMessage: `"${path}" is currently in Glacier storage. Restore it to Standard storage? This will make the file available for syncing again.`,
            restoreAccept: "Restore to Standard",
            restoreToast: "File restored to Standard storage.",
            restoreActivity: "Restored file from Glacier",
            restoreErrorLabel: "restore file",
            archiveTitle: "Move to Glacier storage?",
            archiveMessage: `"${path}" will be moved to Amazon S3 Glacier Instant Retrieval. The local copy will not be available after this transition. The file will remain accessible on-demand from Glacier.`,
            archiveAccept: "Move to Glacier",
            archiveTarget: "GLACIER_IR" as const,
            archiveToast: "File moved to Glacier storage.",
            archiveActivity: "Moved file to Glacier storage",
            archiveErrorLabel: "move file to Glacier",
          };

    if (isColdStorage) {
      // Currently in cold storage — offer to restore to Standard
      await asyncConfirm.open({
        title: coldConfig.restoreTitle,
        message: coldConfig.restoreMessage,
        acceptLabel: coldConfig.restoreAccept,
        rejectLabel: "Cancel",
        onAccept: async () => {
          if (!state.activeLocationId) {
            toast("No active sync location selected.", "error");
            throw createHandledAsyncConfirmError("No active sync location selected.");
          }

          try {
            await client.changeStorageClass(state.activeLocationId, path, "STANDARD");
            toast(coldConfig.restoreToast, "success");
            addActivity("info", coldConfig.restoreActivity, path);
            await refreshLocationViews(state.activeLocationId, { clearCache: true });
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(`Failed to ${coldConfig.restoreErrorLabel}: ${message}`, "error");
            addActivity("error", `Failed to ${coldConfig.restoreErrorLabel}`, String(error));
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
    } else {
      // Currently in Standard (or unknown) — offer to move to cold storage
      await asyncConfirm.open({
        title: coldConfig.archiveTitle,
        message: coldConfig.archiveMessage,
        acceptLabel: coldConfig.archiveAccept,
        rejectLabel: "Cancel",
        variant: "danger",
        onAccept: async () => {
          if (!state.activeLocationId) {
            toast("No active sync location selected.", "error");
            throw createHandledAsyncConfirmError("No active sync location selected.");
          }

          try {
            await client.changeStorageClass(state.activeLocationId, path, coldConfig.archiveTarget);
            toast(coldConfig.archiveToast, "success");
            addActivity("info", coldConfig.archiveActivity, path);
            await refreshLocationViews(state.activeLocationId, { clearCache: true });
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            toast(`Failed to ${coldConfig.archiveErrorLabel}: ${message}`, "error");
            addActivity("error", `Failed to ${coldConfig.archiveErrorLabel}`, String(error));
            throw createHandledAsyncConfirmError(message);
          }
        },
      });
    }
  }

  // ---------------------------------------------------------------------------
  // Version history drawer
  // ---------------------------------------------------------------------------

  function formatVersionSize(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }

  function formatVersionDate(isoString: string | null): string {
    if (!isoString) return "Unknown date";
    const date = new Date(isoString);
    if (Number.isNaN(date.getTime())) return isoString;
    return new Intl.DateTimeFormat(undefined, {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
    }).format(date);
  }

  function formatVersionEtag(value: string | null | undefined): string {
    return value?.trim() ? value : "Unavailable";
  }

  interface VersionComparisonModalOptions {
    entryPath: string;
    versionA: FileVersionEntry;
    versionB: FileVersionEntry;
    onCompare: () => Promise<VersionComparisonDetails>;
  }

  type VersionCompareState =
    | { status: "idle"; mode: null; details: null; message: string }
    | { status: "loading"; mode: null; details: null; message: string }
    | {
        status: "ready";
        mode: "text" | "image" | "external";
        details: VersionComparisonDetails;
        message: string;
      }
    | { status: "error"; mode: null; details: null; message: string };

  function createVersionComparisonModalController(
    toastMessage: (message: string, variant?: "success" | "error" | "info") => void,
  ) {
    const backdrop = document.createElement("section");
    backdrop.className = "modal-backdrop storage-modal storage-conflict-resolution-modal";
    backdrop.hidden = true;

    const dialog = document.createElement("div");
    dialog.className =
      "modal-card storage-modal-card storage-modal-card-wide storage-conflict-modal-card";

    const titleId = `storage-version-compare-title-${Math.random().toString(36).slice(2)}`;
    dialog.setAttribute("role", "dialog");
    dialog.setAttribute("aria-modal", "true");
    dialog.setAttribute("aria-labelledby", titleId);

    const header = document.createElement("div");
    header.className = "modal-header";

    const title = document.createElement("h3");
    title.id = titleId;

    const closeButton = document.createElement("button");
    closeButton.className = "icon-btn modal-close-btn";
    closeButton.type = "button";
    closeButton.setAttribute("aria-label", "Close version comparison dialog");
    closeButton.innerHTML = '<i data-lucide="x"></i>';

    header.append(title, closeButton);

    const body = document.createElement("div");
    body.className = "storage-conflict-modal-body";

    const intro = document.createElement("p");
    intro.className = "modal-body-text";

    const pathCallout = document.createElement("div");
    pathCallout.className = "callout storage-conflict-path-callout";

    const compareHint = document.createElement("p");
    compareHint.className = "hint storage-conflict-compare-hint";

    const compareState = document.createElement("div");
    compareState.className = "callout storage-conflict-compare-state";
    compareState.setAttribute("aria-live", "polite");

    const compareSurface = document.createElement("section");
    compareSurface.className = "storage-conflict-compare-surface";
    compareSurface.hidden = true;

    const compareColumns = document.createElement("div");
    compareColumns.className = "storage-conflict-compare-columns";

    const versionAPanel = document.createElement("section");
    versionAPanel.className = "settings-section mini-panel storage-conflict-compare-panel";
    const versionATitle = document.createElement("h4");
    versionATitle.textContent = "Version A";
    const versionAContent = document.createElement("div");
    versionAContent.className = "storage-conflict-compare-content";
    versionAPanel.append(versionATitle, versionAContent);

    const versionBPanel = document.createElement("section");
    versionBPanel.className = "settings-section mini-panel storage-conflict-compare-panel";
    const versionBTitle = document.createElement("h4");
    versionBTitle.textContent = "Version B";
    const versionBContent = document.createElement("div");
    versionBContent.className = "storage-conflict-compare-content";
    versionBPanel.append(versionBTitle, versionBContent);

    compareColumns.append(versionAPanel, versionBPanel);
    compareSurface.append(compareColumns);

    const metadataGrid = document.createElement("div");
    metadataGrid.className = "compact-list-grid details-grid storage-conflict-grid";

    const metaAPanel = document.createElement("section");
    metaAPanel.className = "settings-section mini-panel storage-conflict-panel";
    const metaATitle = document.createElement("h4");
    metaATitle.textContent = "Version A";
    const metaAList = document.createElement("ul");
    metaAList.className = "compact-list status-list";
    metaAPanel.append(metaATitle, metaAList);

    const metaBPanel = document.createElement("section");
    metaBPanel.className = "settings-section mini-panel storage-conflict-panel";
    const metaBTitle = document.createElement("h4");
    metaBTitle.textContent = "Version B";
    const metaBList = document.createElement("ul");
    metaBList.className = "compact-list status-list";
    metaBPanel.append(metaBTitle, metaBList);

    metadataGrid.append(metaAPanel, metaBPanel);
    body.append(intro, pathCallout, compareHint, compareState, compareSurface, metadataGrid);

    const footer = document.createElement("div");
    footer.className = "modal-footer storage-conflict-footer";

    const cancelButton = document.createElement("button");
    cancelButton.className = "secondary-btn";
    cancelButton.type = "button";
    cancelButton.textContent = "Cancel";

    const compareButton = document.createElement("button");
    compareButton.className = "secondary-btn";
    compareButton.type = "button";

    const compareButtonContent = document.createElement("span");
    compareButtonContent.className = "storage-conflict-action-content";

    const spinner = document.createElement("span");
    spinner.className = "storage-conflict-action-spinner";
    spinner.setAttribute("aria-hidden", "true");
    spinner.hidden = true;

    const compareLabel = document.createElement("span");
    compareLabel.className = "storage-conflict-action-label";
    compareLabel.textContent = "Compare";

    compareButtonContent.append(spinner, compareLabel);
    compareButton.append(compareButtonContent);

    footer.append(cancelButton, compareButton);
    dialog.append(header, body, footer);
    backdrop.append(dialog);
    document.body.append(backdrop);
    applyIcons();

    let visible = false;
    let isComparing = false;
    let currentOptions: VersionComparisonModalOptions | null = null;
    let inlineCompareState: VersionCompareState = {
      status: "idle",
      mode: null,
      details: null,
      message: "Click Compare to load inline previews or open external diff tools.",
    };

    const renderMetaList = (list: HTMLUListElement, values: [string, string][]) => {
      list.innerHTML = "";
      for (const [label, value] of values) {
        const item = document.createElement("li");
        const labelEl = document.createElement("span");
        labelEl.textContent = label;
        const valueEl = document.createElement("strong");
        valueEl.textContent = value;
        item.append(labelEl, valueEl);
        list.append(item);
      }
    };

    const setComparePanelText = (container: HTMLDivElement, text: string) => {
      container.innerHTML = "";
      const pre = document.createElement("pre");
      pre.className = "storage-conflict-text-pane";
      pre.textContent = text;
      container.append(pre);
    };

    const setComparePanelImage = (container: HTMLDivElement, src: string, alt: string) => {
      container.innerHTML = "";
      const frame = document.createElement("div");
      frame.className = "storage-conflict-image-frame";
      const image = document.createElement("img");
      image.className = "storage-conflict-image-preview";
      image.src = src;
      image.alt = alt;
      frame.append(image);
      container.append(frame);
    };

    const renderInlineCompareState = () => {
      compareState.textContent = inlineCompareState.message;
      compareState.classList.toggle("danger", inlineCompareState.status === "error");
      compareSurface.hidden =
        inlineCompareState.status !== "ready" || inlineCompareState.mode === "external";

      if (inlineCompareState.status !== "ready") {
        versionAContent.innerHTML = "";
        versionBContent.innerHTML = "";
        return;
      }

      if (inlineCompareState.mode === "image") {
        setComparePanelImage(
          versionAContent,
          inlineCompareState.details.versionAImageDataUrl ?? "",
          `Version A preview`,
        );
        setComparePanelImage(
          versionBContent,
          inlineCompareState.details.versionBImageDataUrl ?? "",
          `Version B preview`,
        );
        return;
      }

      if (inlineCompareState.mode === "text") {
        setComparePanelText(versionAContent, inlineCompareState.details.versionAText ?? "");
        setComparePanelText(versionBContent, inlineCompareState.details.versionBText ?? "");
        return;
      }

      versionAContent.innerHTML = "";
      versionBContent.innerHTML = "";
    };

    const syncBusyState = () => {
      cancelButton.disabled = isComparing;
      closeButton.disabled = isComparing;
      compareButton.disabled = isComparing;
      compareButton.classList.toggle("is-loading", isComparing);
      compareButton.setAttribute("aria-busy", isComparing ? "true" : "false");
      spinner.hidden = !isComparing;
    };

    const close = () => {
      if (!visible || isComparing) return;
      visible = false;
      currentOptions = null;
      inlineCompareState = {
        status: "idle",
        mode: null,
        details: null,
        message: "Click Compare to load inline previews or open external diff tools.",
      };
      renderInlineCompareState();
      closeModal({ backdrop });
    };

    const runCompare = async () => {
      if (!currentOptions || isComparing) return;
      isComparing = true;
      syncBusyState();
      try {
        inlineCompareState = {
          status: "loading",
          mode: null,
          details: null,
          message: getInlineCompareLoadingMessage(),
        };
        renderInlineCompareState();

        const details = await currentOptions.onCompare();
        inlineCompareState = {
          status: "ready",
          mode: details.mode,
          details,
          message:
            details.mode === "image"
              ? "Showing inline image previews."
              : details.mode === "text"
                ? "Showing inline text comparison."
                : (details.fallbackReason ?? "Opened for external comparison."),
        };
        if (details.mode === "external") {
          if (details.versionATempPath)
            client.openPath(details.versionATempPath).catch(console.error);
          if (details.versionBTempPath)
            client.openPath(details.versionBTempPath).catch(console.error);
        }
        renderInlineCompareState();
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        inlineCompareState = {
          status: "error",
          mode: null,
          details: null,
          message: getInlineCompareErrorMessage(message),
        };
        renderInlineCompareState();
        toastMessage(message, "error");
      } finally {
        isComparing = false;
        syncBusyState();
      }
    };

    backdrop.addEventListener("click", (event) => {
      if (event.target === backdrop) {
        close();
      }
    });
    closeButton.addEventListener("click", close);
    cancelButton.addEventListener("click", close);
    compareButton.addEventListener("click", () => void runCompare());

    return {
      open(options: VersionComparisonModalOptions) {
        currentOptions = options;
        isComparing = false;
        title.textContent = `Compare Versions`;
        intro.textContent = "Review the differences between the two selected versions.";
        pathCallout.textContent = options.entryPath;

        renderMetaList(metaAList, [
          ["Date", formatVersionDate(options.versionA.lastModifiedAt)],
          ["Size", formatVersionSize(options.versionA.size)],
          ["ETag", formatVersionEtag(options.versionA.etag)],
        ]);

        renderMetaList(metaBList, [
          ["Date", formatVersionDate(options.versionB.lastModifiedAt)],
          ["Size", formatVersionSize(options.versionB.size)],
          ["ETag", formatVersionEtag(options.versionB.etag)],
        ]);

        compareHint.textContent =
          "Compare loads inline image/text previews when available and otherwise opens the downloaded remote temp copies externally.";
        inlineCompareState = {
          status: "idle",
          mode: null,
          details: null,
          message: "Click Compare to load inline previews or open external diff tools.",
        };
        renderInlineCompareState();

        syncBusyState();
        visible = true;
        openModal({ backdrop });
        compareButton.focus();
      },
      close() {
        close();
      },
      destroy() {
        backdrop.remove();
      },
    };
  }

  const versionComparisonModal = createVersionComparisonModalController(toast);

  function renderVersionsList(versions: FileVersionEntry[], locationId: string, filePath: string) {
    dom.fileVersionsDrawerList.innerHTML = "";
    dom.fileVersionsDrawerEmpty.hidden = versions.length > 0;

    dom.fileVersionsDrawerCompareToolbar.hidden = false;

    const selectedVersions = new Set<string>();

    const updateCompareToolbar = () => {
      dom.fileVersionsDrawerCompareBtn.disabled = selectedVersions.size !== 2;
    };

    updateCompareToolbar();

    dom.fileVersionsDrawerCompareBtn.onclick = () => {
      const sortedSelected = Array.from(selectedVersions).sort((a, b) => {
        const indexA = versions.findIndex((v) => v.versionId === a);
        const indexB = versions.findIndex((v) => v.versionId === b);
        return indexA - indexB;
      });
      const [versionAId, versionBId] = sortedSelected;
      const versionA = versions.find((v) => v.versionId === versionAId);
      const versionB = versions.find((v) => v.versionId === versionBId);
      if (!versionA || !versionB) return;

      versionComparisonModal.open({
        entryPath: filePath,
        versionA,
        versionB,
        onCompare: () =>
          client.prepareVersionComparison(locationId, filePath, versionAId, versionBId),
      });
    };

    for (const version of versions) {
      const li = document.createElement("li");
      li.className = "list-item";

      const checkboxWrap = document.createElement("label");
      checkboxWrap.className = "checkbox-label";
      checkboxWrap.style.marginRight = "1rem";
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.addEventListener("change", () => {
        if (checkbox.checked) {
          selectedVersions.add(version.versionId);
        } else {
          selectedVersions.delete(version.versionId);
        }
        updateCompareToolbar();
      });
      checkboxWrap.appendChild(checkbox);
      li.appendChild(checkboxWrap);

      const textDiv = document.createElement("div");
      textDiv.className = "list-item-text";

      const primaryLine = document.createElement("span");
      primaryLine.textContent = formatVersionDate(version.lastModifiedAt);
      textDiv.appendChild(primaryLine);

      const secondaryLine = document.createElement("span");
      secondaryLine.className = "list-item-secondary";
      const sizePart = formatVersionSize(version.size);
      const classPart = version.storageClass ? ` · ${version.storageClass}` : "";
      secondaryLine.textContent = sizePart + classPart;
      textDiv.appendChild(secondaryLine);

      li.appendChild(textDiv);

      const actionDiv = document.createElement("div");
      actionDiv.className = "list-item-action";

      if (version.isLatest) {
        const badge = document.createElement("span");
        badge.className = "badge success";
        badge.textContent = "Latest";
        actionDiv.appendChild(badge);
      } else {
        const restoreBtn = document.createElement("button");
        restoreBtn.className = "secondary-btn slim-btn";
        restoreBtn.type = "button";
        restoreBtn.textContent = "Restore";
        restoreBtn.addEventListener(
          "click",
          () =>
            void (async () => {
              const ok = await confirmModal({
                title: "Restore this version?",
                message: `This will make the version from ${formatVersionDate(version.lastModifiedAt)} the new latest version of "${filePath}". The current version will be kept as a previous version.`,
                acceptLabel: "Restore",
                rejectLabel: "Cancel",
              });
              if (!ok) return;

              restoreBtn.disabled = true;
              restoreBtn.textContent = "Restoring…";
              try {
                await client.restoreFileVersion(locationId, filePath, version.versionId);
                toast("Version restored successfully.", "success");
                addActivity(
                  "success",
                  `Restored version of "${filePath}"`,
                  `Version from ${formatVersionDate(version.lastModifiedAt)}`,
                );
                closeDrawer({ drawer: dom.fileVersionsDrawer });
                await refreshLocationViews(locationId, { clearCache: true });
              } catch (error) {
                const message = error instanceof Error ? error.message : String(error);
                toast(`Failed to restore version: ${message}`, "error");
                addActivity("error", `Failed to restore version of "${filePath}"`, message);
                restoreBtn.disabled = false;
                restoreBtn.textContent = "Restore";
              }
            })(),
        );
        actionDiv.appendChild(restoreBtn);
      }

      const deleteBtn = document.createElement("button");
      deleteBtn.className = "icon-btn icon-btn-sm danger-text";
      deleteBtn.type = "button";
      deleteBtn.title = "Delete version";
      deleteBtn.innerHTML = '<i data-lucide="trash-2"></i>';
      deleteBtn.addEventListener(
        "click",
        () =>
          void (async () => {
            const ok = await confirmModal({
              title: "Delete this version?",
              message: `Are you sure you want to permanently delete the version from ${formatVersionDate(version.lastModifiedAt)}? This cannot be undone.`,
              acceptLabel: "Delete",
              rejectLabel: "Cancel",
            });
            if (!ok) return;

            deleteBtn.disabled = true;
            try {
              await client.deleteFileVersion(locationId, filePath, version.versionId);
              toast("Version deleted successfully.", "success");
              addActivity(
                "success",
                `Deleted version of "${filePath}"`,
                `Version from ${formatVersionDate(version.lastModifiedAt)}`,
              );

              dom.fileVersionsDrawerLoading.hidden = false;
              dom.fileVersionsDrawerEmpty.hidden = true;
              dom.fileVersionsDrawerList.innerHTML = "";
              const updatedVersions = await client.listFileVersions(locationId, filePath);
              dom.fileVersionsDrawerLoading.hidden = true;
              renderVersionsList(updatedVersions, locationId, filePath);

              await refreshLocationViews(locationId, { clearCache: true });
            } catch (error) {
              const message = error instanceof Error ? error.message : String(error);
              toast(`Failed to delete version: ${message}`, "error");
              addActivity("error", `Failed to delete version of "${filePath}"`, message);
              deleteBtn.disabled = false;
            }
          })(),
      );
      actionDiv.appendChild(deleteBtn);

      li.appendChild(actionDiv);
      dom.fileVersionsDrawerList.appendChild(li);
    }
    applyIcons();
  }

  async function handleViewVersions(entry: FileEntry) {
    const locationId = state.activeLocationId;
    if (!locationId) return;

    dom.fileVersionsDrawerPath.textContent = entry.path;
    dom.fileVersionsDrawerLoading.hidden = false;
    dom.fileVersionsDrawerEmpty.hidden = true;
    dom.fileVersionsDrawerList.innerHTML = "";

    openDrawer({
      drawer: dom.fileVersionsDrawer,
      backdrop: dom.fileVersionsDrawerBackdrop,
      closeOnBackdrop: true,
      closeOnEscape: true,
    });

    try {
      const versions = await client.listFileVersions(locationId, entry.path);
      dom.fileVersionsDrawerLoading.hidden = true;
      renderVersionsList(versions, locationId, entry.path);
    } catch (error) {
      dom.fileVersionsDrawerLoading.hidden = true;
      dom.fileVersionsDrawerEmpty.hidden = false;
      const message = error instanceof Error ? error.message : String(error);
      addActivity("error", `Failed to load versions for "${entry.path}"`, message);
    }
  }

  dom.fileVersionsDrawerClose.addEventListener("click", () => {
    closeDrawer({
      drawer: dom.fileVersionsDrawer,
      backdrop: dom.fileVersionsDrawerBackdrop,
    });
  });

  async function refreshFileTree() {
    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    if (!state.activeLocationId) {
      endFileTreeLoading();
      destroyFileTree();
      dom.fileTreeEmptyState.hidden = false;
      dom.fileTree.hidden = true;
      dom.fileTree.innerHTML = "";
      renderStatus();
      return;
    }

    const locationId = state.activeLocationId;
    const mode = state.activeLocationViewMode;
    if (mode !== "bin") {
      clearBinSelection();
    }
    const viewKey = getFileTreeViewKey(locationId, mode);
    const requestSequence = ++fileTreeRequestSequence;
    beginFileTreeLoading(requestSequence);

    try {
      const activeLocation = getActiveLocation() ?? getSavedActiveLocation();
      const isVersioningEnabled =
        mode === "live" && (activeLocation?.objectVersioningEnabled ?? false);

      const [entries, versionCountEntries] = await Promise.all([
        mode === "bin" ? client.listBinEntries(locationId) : client.listFileEntries(locationId),
        isVersioningEnabled
          ? client.listVersionCounts(locationId).catch(() => [] as VersionCountEntry[])
          : Promise.resolve([] as VersionCountEntry[]),
      ]);

      if (requestSequence !== fileTreeRequestSequence) {
        return;
      }

      if (state.activeLocationId !== locationId || state.activeLocationViewMode !== mode) {
        return;
      }

      const nextVersionCounts = isVersioningEnabled
        ? new Map(versionCountEntries.map((e) => [e.path, e.count]))
        : undefined;
      const nextVersionCountsJson = serializeVersionCounts(nextVersionCounts);
      activeVersionCounts = nextVersionCounts;

      const entriesJson = JSON.stringify(entries);
      const cachedSnapshot = getViewSnapshot(viewKey);
      if (
        cachedSnapshot?.entriesJson === entriesJson &&
        cachedSnapshot.versionCountsJson === nextVersionCountsJson &&
        fileTreeHandle
      ) {
        renderStatus();
        return;
      }

      fileTreeSnapshots.set(viewKey, {
        viewKey,
        entries,
        entriesJson,
        versionCounts: nextVersionCounts,
        versionCountsJson: nextVersionCountsJson,
      });
      renderFileTreeEntries(entries, mode, nextVersionCounts);
      renderStatus();
    } finally {
      endFileTreeLoading(requestSequence);
    }
  }

  dom.activeLocationSelect.addEventListener("change", () => {
    conflictResolutionModal.close();
    const selection = decodeLocationSelectValue(dom.activeLocationSelect.value);
    clearBinSelection();
    state.activeLocationId = selection.locationId;
    const selectedLocation = selection.locationId
      ? (state.syncLocations.find((location) => location.id === selection.locationId) ?? null)
      : null;
    state.activeLocationViewMode =
      selection.mode === "bin" && selectedLocation && !canViewLocationBin(selectedLocation)
        ? "live"
        : selection.mode;
    state.profile = { ...state.profile, activeLocationId: state.activeLocationId };
    void persistence.saveSettings(toStoredProfile(state.profile));
    renderProfileSummary();
    renderFileTreeViewState();
    const cachedSnapshot = getViewSnapshot();
    if (cachedSnapshot) {
      renderFileTreeEntries(
        cachedSnapshot.entries,
        state.activeLocationViewMode,
        cachedSnapshot.versionCounts,
      );
      renderStatus();
    }
    void refreshFileTree();
  });

  dom.createCredentialBtn.addEventListener("click", () => void handleCreateCredential());
  dom.credentialProviderSelect.addEventListener("change", () => {
    renderCredentialFormState(dom, normalizeProvider(dom.credentialProviderSelect.value));
  });
  dom.restoreSelectedBtn.addEventListener("click", () => void handleBulkBinRestore());
  dom.purgeSelectedBtn.addEventListener("click", () => void handleBulkBinPurge());
  dom.savePollingBtn.addEventListener(
    "click",
    () => void handleSaveSettings(dom.savePollingBtn, dom.pollingResult),
  );
  dom.saveDebugBtn.addEventListener(
    "click",
    () => void handleSaveSettings(dom.saveDebugBtn, dom.debugResult),
  );
  dom.saveConflictBtn.addEventListener(
    "click",
    () => void handleSaveSettings(dom.saveConflictBtn, dom.conflictResult),
  );
  dom.locationProviderSelect.addEventListener("change", () => {
    renderLocationProviderState();
    renderLocationRemoteBinState();
  });
  dom.locationCredentialSelect.addEventListener("change", () => {
    const credentialId = dom.locationCredentialSelect.value;
    const credential = state.credentials.find((c) => c.id === credentialId);
    if (credential) {
      dom.locationProviderSelect.value = credential.provider;
    } else if (!dom.locationEditingId.value.trim()) {
      dom.locationProviderInfo.hidden = true;
    }
    renderLocationProviderState();
    renderLocationRemoteBinState();
  });
  dom.saveLocationBtn.addEventListener("click", () => void handleSaveLocation());
  dom.cancelEditLocationBtn.addEventListener("click", () => {
    resetLocationForm();
    dom.locationsResult.textContent = "Edit cancelled.";
  });
  dom.locationVersioningCheckbox.addEventListener("change", () => {
    dom.locationObjectVersioningEnabledInput.value = dom.locationVersioningCheckbox.checked
      ? "true"
      : "false";
    renderLocationRemoteBinState();
  });
  dom.locationObjectVersioningBtn.addEventListener("click", () => void handleVersioningToggle());
  dom.locationRemoteBinEnabledInput.addEventListener("change", renderLocationRemoteBinState);
  dom.locationRemoteBinRetentionInput.addEventListener("input", renderLocationRemoteBinState);
  resetLocationForm();
  dom.locationBrowseFolderBtn.addEventListener(
    "click",
    () =>
      void (async () => {
        const selected = await client.chooseLocalFolder();
        if (!selected) {
          toast("Folder picker is available in the desktop app.", "info");
          return;
        }
        dom.locationLocalFolderInput.value = selected;
      })(),
  );
  dom.openDebugLogFolderBtn.addEventListener(
    "click",
    () => void client.openActivityDebugLogFolder(),
  );

  bindNavigation({
    root: dom.nav,
    onSelect: (id) => {
      switch (id) {
        case "nav-home":
          closeAllDialogs();
          break;
        case "nav-credentials":
          openDialog("credentials");
          break;
        case "nav-locations":
          openDialog("locations");
          break;
        case "nav-activity":
          openDialog("activity");
          break;
        case "nav-polling-settings":
          openDialog("polling");
          break;
        case "nav-debug-settings":
          openDialog("debug");
          break;
        case "nav-conflict-settings":
          openDialog("conflict");
          break;
        case "open-debug-folder":
          client.openActivityDebugLogFolder().catch(console.error);
          break;
        case "open-about":
          openDialog("about");
          break;
        case "set-theme-goblin":
          document.documentElement.setAttribute("data-theme", "goblin");
          break;
        case "set-theme-dark":
          document.documentElement.setAttribute("data-theme", "dark");
          break;
        case "set-theme-light":
          document.documentElement.setAttribute("data-theme", "light");
          break;
      }
    },
  });

  const unlistenStatus = await client.listenSyncStatus((status) => {
    state.status = status;
    renderStatus();
    addActivity(
      "info",
      `Status updated: ${describeSyncStatus(status).badgeLabel}. ${new Intl.NumberFormat().format(getSyncOverviewStats(status).inSync)} files are in sync.`,
    );
    debouncedRefreshFileTree();
  });

  const unlistenActivity = await client.listenNativeActivity((event) => {
    addActivityItem(createNativeActivity(event));
  });

  const handleBeforeUnload = () => {
    unlistenStatus();
    unlistenActivity();
  };

  window.addEventListener("beforeunload", handleBeforeUnload);

  const storedProfile = await persistence.load();
  state.profile = applyStoredProfile(storedProfile);
  state.activeLocationId = storedProfile.activeLocationId ?? null;
  writeSettingsToDom();
  renderLocationRemoteBinState();
  renderFileTreeViewState();
  await refreshProviderDefinitions();
  await refreshCredentials();
  await refreshStatus();
  await refreshDebugLogState();
  await refreshSyncLocations();

  dom.homeScreen.hidden = false;
  const settingsInitMsg = client.supportsNativeProfilePersistence
    ? "Settings changes stay local until you save them."
    : "Settings changes stay local to this browser preview after you save them.";
  dom.pollingResult.textContent = settingsInitMsg;
  dom.debugResult.textContent = settingsInitMsg;
  dom.conflictResult.textContent = settingsInitMsg;
  dom.credentialsResult.textContent = client.supportsNativeProfilePersistence
    ? "Create named AWS or GCS credentials here. The UI will show whether each one was saved and tested."
    : "Credential management is shown here for preview, but real credentials are desktop-only.";
  if (!dom.locationsResult.textContent?.trim()) {
    dom.locationsResult.textContent = client.supportsNativeProfilePersistence
      ? "Create sync locations to connect local folders to remote buckets."
      : "Sync location management is shown here for preview, but real sync locations are desktop-only.";
  }

  addActivity(
    "info",
    client.supportsNativeProfilePersistence
      ? "Ready to connect a folder, remote bucket, and named credential."
      : "Browser preview loaded. Credential management and sync stay desktop-only here.",
  );

  return () => {
    debouncedRefreshFileTree.cancel();
    debouncedRenderActivity.cancel();

    if (fileTreeChangeTimer !== null) {
      clearTimeout(fileTreeChangeTimer);
      fileTreeChangeTimer = null;
    }

    endFileTreeLoading();

    destroyFileTree();
    asyncConfirm.destroy();
    conflictResolutionModal.destroy();

    window.removeEventListener("beforeunload", handleBeforeUnload);
    unlistenStatus();
    unlistenActivity();
  };
}
