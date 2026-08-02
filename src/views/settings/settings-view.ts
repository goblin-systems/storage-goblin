/**
 * The settings surfaces: polling, conflict defaults, and activity debug
 * (backlog phase 4.2).
 *
 * The first view extracted from `bootstrapStorageGoblin()`, chosen because it
 * is the simplest one that still exercises every part of the contract: it reads
 * store state, writes it back, calls IPC, and has to clean up after itself.
 *
 * ## The view contract
 *
 * `createSettingsView(deps) => { destroy() }`. A view:
 * - binds to the DOM subtree it owns, and no other,
 * - subscribes to the store slices it renders from,
 * - releases every listener and subscription in `destroy()`.
 *
 * It binds existing markup rather than generating it. The design system is
 * markup-driven by design ("keep behaviour headless and markup-driven"), and
 * regenerating DOM that phase 5 is about to redesign would be work done twice.
 *
 * ## Why the dependencies are an interface rather than the app
 *
 * The view takes narrow structural dependencies instead of `AppDom` and the
 * whole client. That is what lets it be tested with a handful of elements and a
 * fake, instead of booting the application — and it makes the coupling visible:
 * everything this screen touches is in one type.
 */

import { setButtonBusy } from "../../lib/dom";
import type { AppStore } from "../../state/app-state";
import type { StorageProfileDraft, StoredStorageProfile } from "../../app/types";

/** The elements this view owns. */
export interface SettingsDom {
  remotePollingInput: HTMLInputElement;
  pollIntervalInput: HTMLInputElement;
  conflictStrategySelect: HTMLSelectElement;
  activityDebugModeInput: HTMLInputElement;
  savePollingBtn: HTMLButtonElement;
  saveDebugBtn: HTMLButtonElement;
  saveConflictBtn: HTMLButtonElement;
  openDebugLogFolderBtn: HTMLButtonElement;
  pollingResult: HTMLElement;
  debugResult: HTMLElement;
  conflictResult: HTMLElement;
  debugLogStatusBadge: HTMLElement;
  debugLogStatusText: HTMLElement;
  debugLogFilePath: HTMLElement;
}

export interface SettingsViewDeps {
  dom: SettingsDom;
  store: AppStore;
  /** True on desktop; false in the browser preview, which changes the copy. */
  supportsNativePersistence: boolean;
  saveSettings(profile: StoredStorageProfile): Promise<StoredStorageProfile>;
  openDebugLogFolder(): Promise<void> | void;
  /** Rebuild the profile after a save (credential state, normalization). */
  applySavedProfile(stored: StoredStorageProfile): StorageProfileDraft;
  toStoredProfile(profile: StorageProfileDraft): StoredStorageProfile;
  normalizeProfileDraft(profile: StorageProfileDraft): StorageProfileDraft;
  /** Refresh anything outside this screen that a settings change affects. */
  onSaved(): Promise<void>;
  toast(message: string, variant: "success" | "error" | "info"): void;
  addActivity(level: "success" | "error" | "info", message: string): void;
}

export interface SettingsView {
  destroy(): void;
}

export function createSettingsView(deps: SettingsViewDeps): SettingsView {
  const { dom, store } = deps;
  const cleanups: (() => void)[] = [];

  function on<K extends keyof HTMLElementEventMap>(
    element: HTMLElement,
    type: K,
    handler: (event: HTMLElementEventMap[K]) => void,
  ): void {
    element.addEventListener(type, handler);
    cleanups.push(() => {
      element.removeEventListener(type, handler);
    });
  }

  /** Push profile values into the form. */
  function renderForm(profile: StorageProfileDraft): void {
    dom.remotePollingInput.checked = profile.remotePollingEnabled;
    dom.pollIntervalInput.value = String(profile.pollIntervalSeconds);
    dom.conflictStrategySelect.value = profile.conflictStrategy;
    dom.activityDebugModeInput.checked = profile.activityDebugModeEnabled;
  }

  /** Pull form values into the store, normalized. */
  function readForm(): void {
    store.setState((state) => ({
      profile: deps.normalizeProfileDraft({
        ...state.profile,
        remotePollingEnabled: dom.remotePollingInput.checked,
        pollIntervalSeconds: Number(dom.pollIntervalInput.value),
        conflictStrategy: dom.conflictStrategySelect
          .value as StorageProfileDraft["conflictStrategy"],
        activityDebugModeEnabled: dom.activityDebugModeInput.checked,
      }),
    }));
  }

  function renderDebugLogState(): void {
    const state = store.getState();
    const { enabled, logDirectoryPath, logFilePath } = state.debugLogState;

    dom.activityDebugModeInput.checked = state.profile.activityDebugModeEnabled;
    dom.debugLogStatusBadge.textContent = enabled
      ? "Enabled"
      : deps.supportsNativePersistence
        ? "Disabled"
        : "Unavailable";
    dom.debugLogStatusBadge.className = `badge ${enabled ? "success" : "default"}`;
    dom.debugLogStatusText.textContent = deps.supportsNativePersistence
      ? enabled
        ? "Detailed native activity logging is on. Open Activity from the menu to inspect richer event details."
        : "Detailed native activity logging is off. Turn it on and save settings to capture extra troubleshooting detail."
      : "Debug logging is not available in the browser preview.";
    dom.debugLogFilePath.textContent = logFilePath ?? logDirectoryPath ?? "Unavailable";
    // There is nothing to open when the backend never reported a directory.
    dom.openDebugLogFolderBtn.disabled = !logDirectoryPath;
  }

  async function save(button: HTMLButtonElement, resultEl: HTMLElement): Promise<void> {
    readForm();
    setButtonBusy(button, true);

    try {
      const stored = await deps.saveSettings(deps.toStoredProfile(store.getState().profile));
      store.setState({ profile: deps.applySavedProfile(stored) });
      renderForm(store.getState().profile);
      await deps.onSaved();

      const message = deps.supportsNativePersistence
        ? "Settings saved. Preferences are updated."
        : "Settings saved locally in the browser preview.";

      resultEl.textContent = message;
      deps.toast(message, "success");
      deps.addActivity("success", message);
    } catch (error) {
      // The previous `() => void handleSaveSettings(...)` wiring let a failed
      // save become an unhandled rejection: the button un-stuck itself and the
      // user was told nothing, so a save that did not happen looked like one
      // that did.
      const detail = error instanceof Error ? error.message : String(error);
      const message = `Could not save settings: ${detail}`;
      resultEl.textContent = message;
      deps.toast(message, "error");
      deps.addActivity("error", message);
    } finally {
      // In a `finally` so a failed save cannot leave the button stuck busy,
      // which would look like a frozen dialog.
      setButtonBusy(button, false);
    }
  }

  on(dom.savePollingBtn, "click", () => void save(dom.savePollingBtn, dom.pollingResult));
  on(dom.saveDebugBtn, "click", () => void save(dom.saveDebugBtn, dom.debugResult));
  on(dom.saveConflictBtn, "click", () => void save(dom.saveConflictBtn, dom.conflictResult));
  on(dom.openDebugLogFolderBtn, "click", () => void deps.openDebugLogFolder());

  // Re-render when state changes from anywhere else — a profile reload, another
  // screen's save, or the initial hydration.
  cleanups.push(store.select((state) => state.profile, renderForm));
  cleanups.push(store.select((state) => state.debugLogState, renderDebugLogState));

  renderForm(store.getState().profile);
  renderDebugLogState();

  return {
    destroy() {
      for (const cleanup of cleanups.splice(0)) cleanup();
    },
  };
}
