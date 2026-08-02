import { describe, expect, it, vi } from "vitest";

import { createStore } from "./store";

interface TestState {
  count: number;
  label: string;
  items: string[];
}

const initial: TestState = { count: 0, label: "a", items: [] };

describe("createStore", () => {
  it("exposes the initial state", () => {
    expect(createStore(initial).getState()).toEqual(initial);
  });

  it("merges a patch rather than replacing the state", () => {
    const store = createStore(initial);
    store.setState({ count: 1 });
    expect(store.getState()).toEqual({ count: 1, label: "a", items: [] });
  });

  it("gives the updater the current state so derived writes cannot go stale", () => {
    const store = createStore(initial);
    store.setState({ count: 5 });
    store.setState((state) => ({ count: state.count + 1 }));
    expect(store.getState().count).toBe(6);
  });

  it("notifies subscribers with both the new and previous state", () => {
    const store = createStore(initial);
    const listener = vi.fn();
    store.subscribe(listener);

    store.setState({ count: 1 });

    expect(listener).toHaveBeenCalledTimes(1);
    expect(listener.mock.calls[0][0].count).toBe(1);
    expect(listener.mock.calls[0][1].count).toBe(0);
  });

  it("does not notify when the patch changes nothing", () => {
    // Re-applying a server response that matched is extremely common, and
    // notifying on it is how a render storm starts.
    const store = createStore(initial);
    const listener = vi.fn();
    store.subscribe(listener);

    store.setState({ count: 0 });
    store.setState({ count: 0, label: "a" });
    store.setState({});

    expect(listener).not.toHaveBeenCalled();
  });

  it("treats a new array with equal contents as a change", () => {
    // Reference equality is the contract: the store cannot know whether a new
    // array is meaningfully different, and claiming it is not would drop real
    // updates.
    const store = createStore(initial);
    const listener = vi.fn();
    store.subscribe(listener);

    store.setState({ items: [] });

    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("stops notifying after unsubscribe", () => {
    const store = createStore(initial);
    const listener = vi.fn();
    const unsubscribe = store.subscribe(listener);

    store.setState({ count: 1 });
    unsubscribe();
    store.setState({ count: 2 });

    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("does not skip listeners when one unsubscribes during a notification", () => {
    // Iterating the live set would shift it mid-pass and silently skip the
    // next listener — a bug that only shows up once views unmount each other.
    const store = createStore(initial);
    const calls: string[] = [];

    let unsubscribeSecond: () => void = () => undefined;
    store.subscribe(() => {
      calls.push("first");
      unsubscribeSecond();
    });
    unsubscribeSecond = store.subscribe(() => calls.push("second"));
    store.subscribe(() => calls.push("third"));

    store.setState({ count: 1 });

    // "second" was removed before it ran, so it must not be called — and
    // "third" must still run, which it would not if removal shifted the
    // iteration.
    expect(calls).toEqual(["first", "third"]);
  });

  it("keeps notifying the remaining listeners when one throws", () => {
    // One broken view must not freeze the rest of the UI.
    const store = createStore(initial);
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const after = vi.fn();

    store.subscribe(() => {
      throw new Error("view blew up");
    });
    store.subscribe(after);

    store.setState({ count: 1 });

    expect(after).toHaveBeenCalledTimes(1);
    expect(errorSpy).toHaveBeenCalled();
    errorSpy.mockRestore();
  });

  it("settles a setState issued from inside a listener instead of recursing", () => {
    const store = createStore(initial);
    const seen: number[] = [];

    store.subscribe((state) => {
      seen.push(state.count);
      if (state.count === 1) store.setState({ count: 2 });
    });

    store.setState({ count: 1 });

    // Both updates delivered, in order, without re-entering the notify loop.
    expect(seen).toEqual([1, 2]);
    expect(store.getState().count).toBe(2);
  });

  it("delivers a listener-issued update to every listener, not just the sender", () => {
    const store = createStore(initial);
    const other = vi.fn();

    store.subscribe((state) => {
      if (state.count === 1) store.setState({ label: "derived" });
    });
    store.subscribe(other);

    store.setState({ count: 1 });

    expect(other).toHaveBeenCalledTimes(2);
    expect(store.getState().label).toBe("derived");
  });
});

describe("select", () => {
  it("fires only when the selected slice changes", () => {
    const store = createStore(initial);
    const listener = vi.fn();
    store.select((state) => state.count, listener);

    store.setState({ label: "b" });
    expect(listener).not.toHaveBeenCalled();

    store.setState({ count: 1 });
    expect(listener).toHaveBeenCalledWith(1, 0);
  });

  it("reports the previous slice value", () => {
    const store = createStore(initial);
    const listener = vi.fn();
    store.select((state) => state.label, listener);

    store.setState({ label: "b" });
    store.setState({ label: "c" });

    expect(listener.mock.calls).toEqual([
      ["b", "a"],
      ["c", "b"],
    ]);
  });

  it("accepts a custom equality so derived objects do not fire every time", () => {
    // Without this, a selector returning a fresh object (a view model) would
    // notify on every unrelated state change.
    const store = createStore(initial);
    const listener = vi.fn();

    store.select(
      (state) => ({ count: state.count }),
      listener,
      (next, previous) => next.count === previous.count,
    );

    store.setState({ label: "b" });
    expect(listener).not.toHaveBeenCalled();

    store.setState({ count: 1 });
    expect(listener).toHaveBeenCalledTimes(1);
  });

  it("stops firing after unsubscribe", () => {
    const store = createStore(initial);
    const listener = vi.fn();
    const unsubscribe = store.select((state) => state.count, listener);

    unsubscribe();
    store.setState({ count: 1 });

    expect(listener).not.toHaveBeenCalled();
  });
});
