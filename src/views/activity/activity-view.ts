/**
 * The activity log (backlog phase 4.2).
 *
 * Renders the rolling list of what the app has done. Currently the product's
 * primary feedback mechanism — phase 5 demotes it in favour of a status the
 * user can read at a glance, which is easier once it is a view rather than
 * three functions in a 4,000-line closure.
 *
 * Rendering is debounced. Native activity events arrive in bursts during a
 * sync cycle, and rebuilding a 36-item list per event is both wasted work and
 * a visible flicker.
 */

import { debounce, type DebouncedFn } from "../../lib/debounce";
import type { AppStore } from "../../state/app-state";
import type { ActivityItem } from "../../app/types";

/** How long activity events are coalesced before a re-render. */
const RENDER_DEBOUNCE_MS = 150;

/**
 * The most recent entries kept in memory.
 *
 * The log is a feedback surface, not an audit trail — the durable record is the
 * backend debug log. Keeping it bounded is what stops a long-running sync from
 * growing the DOM without limit.
 */
export const MAX_ACTIVITY_ITEMS = 36;

export interface ActivityDom {
  activityList: HTMLElement;
  activityEmptyState: HTMLElement;
}

export interface ActivityViewDeps {
  dom: ActivityDom;
  store: AppStore;
  /** Renders an item's timestamp for display. */
  formatTimestamp(timestamp: string): string;
}

export interface ActivityView {
  destroy(): void;
  /** Force a pending debounced render to happen now (tests, teardown). */
  flush(): void;
}

/**
 * Add an item to the log, trimming to [`MAX_ACTIVITY_ITEMS`].
 *
 * Exported as a plain function on the store rather than a view method: activity
 * is appended from all over the app, including while the activity dialog is
 * closed and the view is not mounted.
 */
export function appendActivity(store: AppStore, item: ActivityItem): void {
  store.setState((state) => ({
    activity: [item, ...state.activity].slice(0, MAX_ACTIVITY_ITEMS),
  }));
}

export function createActivityView(deps: ActivityViewDeps): ActivityView {
  const { dom, store } = deps;

  function buildItem(item: ActivityItem): HTMLLIElement {
    const li = document.createElement("li");
    li.className = "activity-item";

    const message = document.createElement("span");
    message.className = "activity-message";
    // textContent, never innerHTML: these strings carry provider errors and
    // file paths, which are attacker-influenced in the general case.
    message.textContent = item.message;

    const meta = document.createElement("span");
    meta.className = "activity-meta";
    meta.textContent = `${item.level.toUpperCase()} · ${deps.formatTimestamp(item.timestamp)}`;

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

    return li;
  }

  function render(): void {
    const items = store.getState().activity;
    const hasItems = items.length > 0;

    dom.activityEmptyState.hidden = hasItems;
    dom.activityList.hidden = !hasItems;

    // Build detached, then swap once: appending into the live list makes the
    // browser lay out 36 times instead of one.
    const fragment = document.createDocumentFragment();
    for (const item of items) fragment.append(buildItem(item));

    dom.activityList.replaceChildren(fragment);
  }

  const renderDebounced: DebouncedFn<() => void> = debounce(render, RENDER_DEBOUNCE_MS);

  const unsubscribe = store.select((state) => state.activity, renderDebounced);
  render();

  return {
    flush() {
      renderDebounced.cancel();
      render();
    },
    destroy() {
      renderDebounced.cancel();
      unsubscribe();
    },
  };
}
