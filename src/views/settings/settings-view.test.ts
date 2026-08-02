import { beforeEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_PROFILE_DRAFT, normalizeProfileDraft, toStoredProfile } from "../../app/profile";
import type { StoredStorageProfile } from "../../app/types";
import { createAppStore, type AppState, type AppStore } from "../../state/app-state";
import { createSettingsView, type SettingsDom, type SettingsViewDeps } from "./settings-view";

function createDom(): SettingsDom {
  const make = <T extends HTMLElement>(tag: string, init?: (el: T) => void): T => {
    const element = document.createElement(tag) as T;
    init?.(element);
    document.body.append(element);
    return element;
  };

  return {
    remotePollingInput: make<HTMLInputElement>("input", (el) => (el.type = "checkbox")),
    pollIntervalInput: make<HTMLInputElement>("input", (el) => (el.type = "number")),
    conflictStrategySelect: make<HTMLSelectElement>("select", (el) => {
      for (const value of ["preserve-both", "prefer-local", "prefer-remote"]) {
        const option = document.createElement("option");
        option.value = value;
        el.append(option);
      }
    }),
    activityDebugModeInput: make<HTMLInputElement>("input", (el) => (el.type = "checkbox")),
    savePollingBtn: make<HTMLButtonElement>("button"),
    saveDebugBtn: make<HTMLButtonElement>("button"),
    saveConflictBtn: make<HTMLButtonElement>("button"),
    openDebugLogFolderBtn: make<HTMLButtonElement>("button"),
    pollingResult: make<HTMLElement>("div"),
    debugResult: make<HTMLElement>("div"),
    conflictResult: make<HTMLElement>("div"),
    debugLogStatusBadge: make<HTMLElement>("span"),
    debugLogStatusText: make<HTMLElement>("p"),
    debugLogFilePath: make<HTMLElement>("strong"),
  };
}

function createInitialState(overrides: Partial<AppState> = {}): AppState {
  return {
    activeDialog: null,
    activeLocationId: null,
    activeLocationViewMode: "live",
    profile: DEFAULT_PROFILE_DRAFT,
    providerDefinitions: [],
    credentials: [],
    syncLocations: [],
    status: {
      phase: "unconfigured",
    } as AppState["status"],
    activity: [],
    lastConnectAt: null,
    debugLogState: { enabled: false, logFilePath: null, logDirectoryPath: null },
    ...overrides,
  };
}

function harness(overrides: Partial<SettingsViewDeps> = {}) {
  const dom = createDom();
  const store: AppStore = createAppStore(createInitialState());

  const saveSettings = vi.fn(
    async (profile: StoredStorageProfile): Promise<StoredStorageProfile> => profile,
  );
  const onSaved = vi.fn(async () => undefined);
  const toast = vi.fn();
  const addActivity = vi.fn();
  const openDebugLogFolder = vi.fn();

  const deps: SettingsViewDeps = {
    dom,
    store,
    supportsNativePersistence: true,
    saveSettings,
    openDebugLogFolder,
    applySavedProfile: (stored) =>
      normalizeProfileDraft({ ...store.getState().profile, ...stored }),
    toStoredProfile,
    normalizeProfileDraft,
    onSaved,
    toast,
    addActivity,
    ...overrides,
  };

  const view = createSettingsView(deps);
  return { dom, store, view, saveSettings, onSaved, toast, addActivity, openDebugLogFolder };
}

beforeEach(() => {
  document.body.innerHTML = "";
});

describe("createSettingsView", () => {
  it("renders the current profile into the form on mount", () => {
    const dom = createDom();
    const store = createAppStore(
      createInitialState({
        profile: normalizeProfileDraft({
          ...DEFAULT_PROFILE_DRAFT,
          remotePollingEnabled: false,
          pollIntervalSeconds: 120,
          conflictStrategy: "prefer-local",
        }),
      }),
    );

    createSettingsView({
      dom,
      store,
      supportsNativePersistence: true,
      saveSettings: async (p) => p,
      openDebugLogFolder: vi.fn(),
      applySavedProfile: () => store.getState().profile,
      toStoredProfile,
      normalizeProfileDraft,
      onSaved: async () => undefined,
      toast: vi.fn(),
      addActivity: vi.fn(),
    });

    expect(dom.remotePollingInput.checked).toBe(false);
    expect(dom.pollIntervalInput.value).toBe("120");
    expect(dom.conflictStrategySelect.value).toBe("prefer-local");
  });

  it("re-renders when the profile changes from elsewhere", () => {
    // A save on another screen, or the initial profile load, must not leave
    // this form showing stale values.
    const { dom, store } = harness();

    store.setState({
      profile: normalizeProfileDraft({
        ...DEFAULT_PROFILE_DRAFT,
        pollIntervalSeconds: 300,
      }),
    });

    expect(dom.pollIntervalInput.value).toBe("300");
  });

  it("saves the values currently in the form, not the ones last stored", async () => {
    const { dom, saveSettings } = harness();

    dom.pollIntervalInput.value = "45";
    dom.remotePollingInput.checked = false;
    dom.savePollingBtn.click();
    await vi.waitFor(() => {
      expect(saveSettings).toHaveBeenCalled();
    });
    const saved = saveSettings.mock.calls[0][0];
    expect(saved.pollIntervalSeconds).toBe(45);
    expect(saved.remotePollingEnabled).toBe(false);
  });

  it("reports success on the result element belonging to the button pressed", async () => {
    // Three buttons share one handler; showing the message on the wrong panel
    // would look like nothing happened.
    const { dom, saveSettings } = harness();

    dom.saveConflictBtn.click();
    // Wait on the observable outcome, not the mock call — the text is written
    // after the save resolves.
    await vi.waitFor(() => {
      expect(dom.conflictResult.textContent).toMatch(/Settings saved/);
    });
    expect(saveSettings).toHaveBeenCalledTimes(1);
    expect(dom.pollingResult.textContent).toBe("");
    expect(dom.debugResult.textContent).toBe("");
  });

  it("marks the pressed button busy while saving and clears it after", async () => {
    let release!: (value: StoredStorageProfile) => void;
    const saveSettings = vi.fn(
      () =>
        new Promise<StoredStorageProfile>((resolve) => {
          release = resolve;
        }),
    );
    const { dom } = harness({ saveSettings });

    dom.savePollingBtn.click();
    await vi.waitFor(() => {
      expect(dom.savePollingBtn.disabled).toBe(true);
    });
    expect(dom.savePollingBtn.classList.contains("is-loading")).toBe(true);

    release(toStoredProfile(DEFAULT_PROFILE_DRAFT));
    await vi.waitFor(() => {
      expect(dom.savePollingBtn.disabled).toBe(false);
    });
    expect(dom.savePollingBtn.classList.contains("is-loading")).toBe(false);
  });

  it("reports a failed save instead of silently swallowing it", async () => {
    // The original wiring let a rejected save become an unhandled rejection:
    // the button un-stuck itself and the user was told nothing, so a save that
    // did not happen looked exactly like one that did.
    const saveSettings = vi.fn(async () => {
      throw new Error("disk unavailable");
    });
    const { dom, toast, addActivity } = harness({ saveSettings });

    dom.savePollingBtn.click();

    await vi.waitFor(() => {
      expect(dom.pollingResult.textContent).toMatch(/Could not save/);
    });
    expect(dom.pollingResult.textContent).toMatch(/disk unavailable/);
    expect(toast).toHaveBeenCalledWith(expect.stringMatching(/Could not save/), "error");
    expect(addActivity).toHaveBeenCalledWith("error", expect.stringMatching(/Could not save/));
  });

  it("clears the busy state when the save fails", async () => {
    // A stuck spinner reads as a frozen dialog, which is worse than an error.
    const saveSettings = vi.fn(async () => {
      throw new Error("disk unavailable");
    });
    const { dom } = harness({ saveSettings });

    dom.savePollingBtn.click();

    await vi.waitFor(() => {
      expect(dom.savePollingBtn.disabled).toBe(false);
    });
    expect(dom.savePollingBtn.classList.contains("is-loading")).toBe(false);
  });

  it("refreshes the rest of the app after a save", async () => {
    const { dom, onSaved, saveSettings } = harness();

    dom.saveDebugBtn.click();
    await vi.waitFor(() => {
      expect(onSaved).toHaveBeenCalledTimes(1);
    });
    expect(saveSettings).toHaveBeenCalledTimes(1);
  });

  it("shows the debug log path and enables the folder button when one exists", () => {
    const { dom, store } = harness();

    store.setState({
      debugLogState: {
        enabled: true,
        logFilePath: "C:/logs/goblin.log",
        logDirectoryPath: "C:/logs",
      },
    });

    expect(dom.debugLogStatusBadge.textContent).toBe("Enabled");
    expect(dom.debugLogStatusBadge.className).toBe("badge success");
    expect(dom.debugLogFilePath.textContent).toBe("C:/logs/goblin.log");
    expect(dom.openDebugLogFolderBtn.disabled).toBe(false);
  });

  it("disables the folder button when the backend reported no directory", () => {
    // Opening nothing is a dead click; the button must say so.
    const { dom } = harness();
    expect(dom.openDebugLogFolderBtn.disabled).toBe(true);
  });

  it("describes debug logging as unavailable in the browser preview", () => {
    const { dom } = harness({ supportsNativePersistence: false });

    expect(dom.debugLogStatusBadge.textContent).toBe("Unavailable");
    expect(dom.debugLogStatusText.textContent).toMatch(/not available in the browser preview/);
  });

  it("opens the log folder when asked", () => {
    const { dom, openDebugLogFolder, store } = harness();
    store.setState({
      debugLogState: { enabled: true, logFilePath: null, logDirectoryPath: "C:/logs" },
    });

    dom.openDebugLogFolderBtn.click();

    expect(openDebugLogFolder).toHaveBeenCalledTimes(1);
  });

  it("stops responding to clicks and state changes after destroy", () => {
    // A view that keeps rendering after teardown is how a closed dialog
    // resurrects itself, and how listeners leak across a reload.
    const { dom, store, view, saveSettings } = harness();
    view.destroy();

    dom.savePollingBtn.click();
    store.setState({
      profile: normalizeProfileDraft({ ...DEFAULT_PROFILE_DRAFT, pollIntervalSeconds: 900 }),
    });

    expect(saveSettings).not.toHaveBeenCalled();
    expect(dom.pollIntervalInput.value).not.toBe("900");
  });

  it("tolerates being destroyed twice", () => {
    const { view } = harness();
    view.destroy();
    expect(() => {
      view.destroy();
    }).not.toThrow();
  });
});
