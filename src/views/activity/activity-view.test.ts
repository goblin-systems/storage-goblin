import { beforeEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_PROFILE_DRAFT } from "../../app/profile";
import type { ActivityItem } from "../../app/types";
import { createAppStore, type AppState, type AppStore } from "../../state/app-state";
import { appendActivity, createActivityView, MAX_ACTIVITY_ITEMS } from "./activity-view";

function createState(activity: ActivityItem[] = []): AppState {
  return {
    activeDialog: null,
    activeLocationId: null,
    activeLocationViewMode: "live",
    profile: DEFAULT_PROFILE_DRAFT,
    providerDefinitions: [],
    credentials: [],
    syncLocations: [],
    status: { phase: "unconfigured" } as AppState["status"],
    activity,
    lastConnectAt: null,
    debugLogState: { enabled: false, logFilePath: null, logDirectoryPath: null },
  };
}

function item(overrides: Partial<ActivityItem> = {}): ActivityItem {
  return {
    id: crypto.randomUUID(),
    level: "info",
    message: "Something happened",
    timestamp: "2026-08-02T10:00:00Z",
    details: null,
    ...overrides,
  };
}

function harness(activity: ActivityItem[] = []) {
  const activityList = document.createElement("ul");
  const activityEmptyState = document.createElement("div");
  document.body.append(activityList, activityEmptyState);

  const store: AppStore = createAppStore(createState(activity));
  const view = createActivityView({
    dom: { activityList, activityEmptyState },
    store,
    formatTimestamp: (value) => `at ${value}`,
  });

  return { activityList, activityEmptyState, store, view };
}

beforeEach(() => {
  document.body.innerHTML = "";
});

describe("createActivityView", () => {
  it("shows the empty state when there is nothing to report", () => {
    const { activityList, activityEmptyState } = harness();

    expect(activityEmptyState.hidden).toBe(false);
    expect(activityList.hidden).toBe(true);
    expect(activityList.children).toHaveLength(0);
  });

  it("renders existing items on mount", () => {
    const { activityList, activityEmptyState } = harness([
      item({ message: "Uploaded a.txt" }),
      item({ message: "Downloaded b.txt" }),
    ]);

    expect(activityEmptyState.hidden).toBe(true);
    expect(activityList.hidden).toBe(false);
    expect(activityList.children).toHaveLength(2);
    expect(activityList.textContent).toContain("Uploaded a.txt");
  });

  it("renders the level and formatted timestamp", () => {
    const { activityList } = harness([item({ level: "error", timestamp: "2026-08-02T10:00:00Z" })]);

    const meta = activityList.querySelector(".activity-meta");
    expect(meta?.textContent).toBe("ERROR · at 2026-08-02T10:00:00Z");
  });

  it("renders details in a collapsed disclosure when present", () => {
    const { activityList } = harness([item({ details: "pair='x' error='boom'" })]);

    const details = activityList.querySelector("details.activity-details");
    expect(details).not.toBeNull();
    expect((details as HTMLDetailsElement).open).toBe(false);
    expect(details?.querySelector("pre")?.textContent).toBe("pair='x' error='boom'");
  });

  it("omits the disclosure when an item has no details", () => {
    const { activityList } = harness([item({ details: null })]);
    expect(activityList.querySelector("details")).toBeNull();
  });

  it("renders messages as text, never as markup", () => {
    // Activity messages carry provider errors and file paths, which are
    // attacker-influenced in the general case.
    const { activityList } = harness([item({ message: "<img src=x onerror=alert(1)>" })]);

    expect(activityList.querySelector("img")).toBeNull();
    expect(activityList.textContent).toContain("<img src=x onerror=alert(1)>");
  });

  it("coalesces a burst of events into a single render", async () => {
    // Native activity arrives in bursts during a sync cycle; rebuilding the
    // list per event is wasted work and a visible flicker.
    const { activityList, store, view } = harness();

    for (let index = 0; index < 5; index += 1) {
      appendActivity(store, item({ message: `event ${index}` }));
    }

    // Nothing drawn yet — the debounce is still open.
    expect(activityList.children).toHaveLength(0);

    view.flush();
    expect(activityList.children).toHaveLength(5);
  });

  it("renders newest first", async () => {
    const { activityList, store, view } = harness();

    appendActivity(store, item({ message: "older" }));
    appendActivity(store, item({ message: "newer" }));
    view.flush();

    expect(activityList.children[0].textContent).toContain("newer");
  });

  it("keeps the log bounded", () => {
    // Unbounded, a long-running sync grows the DOM without limit.
    const store = createAppStore(createState());

    for (let index = 0; index < MAX_ACTIVITY_ITEMS + 20; index += 1) {
      appendActivity(store, item({ message: `event ${index}` }));
    }

    expect(store.getState().activity).toHaveLength(MAX_ACTIVITY_ITEMS);
    // The oldest are the ones dropped.
    expect(store.getState().activity[0].message).toBe(`event ${MAX_ACTIVITY_ITEMS + 19}`);
  });

  it("stops rendering after destroy", () => {
    const { activityList, store, view } = harness();
    view.destroy();

    appendActivity(store, item({ message: "after teardown" }));

    expect(activityList.children).toHaveLength(0);
  });

  it("cancels a pending render on destroy", async () => {
    // A timer firing into a destroyed view touches DOM that may be gone.
    vi.useFakeTimers();
    const { activityList, store, view } = harness();

    appendActivity(store, item({ message: "queued" }));
    view.destroy();
    vi.runAllTimers();

    expect(activityList.children).toHaveLength(0);
    vi.useRealTimers();
  });
});
