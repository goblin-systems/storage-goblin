import { describe, expect, it, vi } from "vitest";

import { createLatest, isSuperseded, SupersededError } from "./latest";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("createLatest", () => {
  it("resolves a call that was never superseded", async () => {
    const latest = createLatest();
    await expect(latest.run(async () => "value")).resolves.toBe("value");
  });

  it("rejects the earlier call when a newer one starts", async () => {
    // The bug this exists to prevent: the user switches location, the slower
    // first request resolves last, and the UI shows the wrong folder.
    const latest = createLatest("file tree");
    const first = deferred<string>();
    const second = deferred<string>();

    const firstCall = latest.run(() => first.promise);
    const secondCall = latest.run(() => second.promise);

    // First resolves *after* the second started — the out-of-order case.
    second.resolve("second");
    first.resolve("first");

    await expect(firstCall).rejects.toBeInstanceOf(SupersededError);
    await expect(secondCall).resolves.toBe("second");
  });

  it("rejects rather than hanging, so cleanup still runs", async () => {
    // A superseded call that simply never settles would leak every loading
    // spinner it started.
    const latest = createLatest();
    const first = deferred<string>();
    const cleanup = vi.fn();

    const firstCall = latest.run(() => first.promise).finally(cleanup);
    void latest.run(async () => "newer");
    first.resolve("first");

    await expect(firstCall).rejects.toThrow(/superseded/);
    expect(cleanup).toHaveBeenCalled();
  });

  it("names the stream in the error so a stray rejection is traceable", async () => {
    const latest = createLatest("bin listing");
    const first = deferred<string>();

    const call = latest.run(() => first.promise);
    void latest.run(async () => "newer");
    first.resolve("first");

    await expect(call).rejects.toThrow(/bin listing/);
  });

  it("tells a running operation that it has been superseded", async () => {
    // Long multi-step work should be able to bail rather than finish work whose
    // result is already unwanted.
    const latest = createLatest();
    const gate = deferred<void>();
    let sawSuperseded = false;

    const call = latest.run(async (signal) => {
      await gate.promise;
      sawSuperseded = signal.superseded;
      return "done";
    });

    void latest.run(async () => "newer");
    gate.resolve();

    await expect(call).rejects.toBeInstanceOf(SupersededError);
    expect(sawSuperseded).toBe(true);
  });

  it("does not report a lone call as superseded", async () => {
    const latest = createLatest();
    let sawSuperseded: boolean | null = null;

    await latest.run(async (signal) => {
      sawSuperseded = signal.superseded;
      return "done";
    });

    expect(sawSuperseded).toBe(false);
  });

  it("propagates a real failure instead of masking it as superseded", async () => {
    // Losing the real error would turn every network failure into a silent
    // no-op that looks like a race.
    const latest = createLatest();

    await expect(
      latest.run(async () => {
        throw new Error("network down");
      }),
    ).rejects.toThrow("network down");
  });

  it("invalidates in-flight work on cancel", async () => {
    const latest = createLatest();
    const first = deferred<string>();

    const call = latest.run(() => first.promise);
    latest.cancel();
    first.resolve("first");

    await expect(call).rejects.toBeInstanceOf(SupersededError);
  });

  it("keeps separate runners independent", async () => {
    // A slow file-tree load must not cancel an unrelated status refresh.
    const tree = createLatest("tree");
    const status = createLatest("status");
    const treeResult = deferred<string>();

    const treeCall = tree.run(() => treeResult.promise);
    await expect(status.run(async () => "status ok")).resolves.toBe("status ok");
    treeResult.resolve("tree ok");

    await expect(treeCall).resolves.toBe("tree ok");
  });

  it("identifies superseded errors without instanceof at the call site", () => {
    expect(isSuperseded(new SupersededError("x"))).toBe(true);
    expect(isSuperseded(new Error("network down"))).toBe(false);
    expect(isSuperseded(null)).toBe(false);
  });
});
