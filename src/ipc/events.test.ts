import { describe, expect, it, vi } from "vitest";

import {
  combineUnlisten,
  EVENT_CHANNELS,
  onTransferProgress,
  subscribe,
  type EventBackend,
  type TransferProgressEvent,
} from "./events";

function fakeBackend() {
  const unlisten = vi.fn();
  const listeners = new Map<string, (payload: { payload: unknown }) => void>();

  const backend: EventBackend = {
    listen: vi.fn(async (channel: string, handler: (payload: { payload: unknown }) => void) => {
      listeners.set(channel, handler);
      return unlisten;
    }),
  };

  return {
    backend,
    unlisten,
    emit(channel: string, payload: unknown) {
      listeners.get(channel)?.({ payload });
    },
  };
}

describe("subscribe", () => {
  it("unwraps the payload before handing it to the caller", async () => {
    const { backend, emit } = fakeBackend();
    const handler = vi.fn();

    await subscribe<{ value: number }>(backend, "test://channel", handler);
    emit("test://channel", { value: 7 });

    expect(handler).toHaveBeenCalledWith({ value: 7 });
  });

  it("releases the subscription when the returned handle is called", async () => {
    const { backend, unlisten } = fakeBackend();
    const release = await subscribe(backend, "test://channel", vi.fn());

    release();

    expect(unlisten).toHaveBeenCalledTimes(1);
  });

  it("ignores a second release", async () => {
    // Views unmounting during teardown can run their cleanup twice, and
    // Tauri's unlisten is not idempotent.
    const { backend, unlisten } = fakeBackend();
    const release = await subscribe(backend, "test://channel", vi.fn());

    release();
    release();

    expect(unlisten).toHaveBeenCalledTimes(1);
  });

  it("returns a usable no-op handle when there is no backend", async () => {
    // Browser preview has no events; callers must not have to branch on it.
    const release = await subscribe(null, "test://channel", vi.fn());
    expect(() => {
      release();
    }).not.toThrow();
  });
});

describe("combineUnlisten", () => {
  it("releases every handle", () => {
    const first = vi.fn();
    const second = vi.fn();

    combineUnlisten([first, second])();

    expect(first).toHaveBeenCalledTimes(1);
    expect(second).toHaveBeenCalledTimes(1);
  });

  it("releases the rest when one throws", () => {
    // One failed unlisten stranding the others would leak subscriptions for
    // the life of the window.
    const errorSpy = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const second = vi.fn();

    combineUnlisten([
      () => {
        throw new Error("already gone");
      },
      second,
    ])();

    expect(second).toHaveBeenCalledTimes(1);
    errorSpy.mockRestore();
  });

  it("ignores a second release", () => {
    const handle = vi.fn();
    const release = combineUnlisten([handle]);

    release();
    release();

    expect(handle).toHaveBeenCalledTimes(1);
  });
});

describe("onTransferProgress", () => {
  it("subscribes to the channel the backend actually emits on", async () => {
    // This string is the wire contract with storage/commands.rs; a typo here
    // is a silently dead progress bar.
    const { backend } = fakeBackend();
    await onTransferProgress(backend, vi.fn());

    expect(backend.listen).toHaveBeenCalledWith(
      "storage://transfer-progress",
      expect.any(Function),
    );
    expect(EVENT_CHANNELS.transferProgress).toBe("storage://transfer-progress");
  });

  it("passes through the indeterminate case unchanged", async () => {
    // S3 does not always report a size; nulls must survive so the UI can show
    // an indeterminate bar rather than computing a wrong percentage.
    const { backend, emit } = fakeBackend();
    const handler = vi.fn();
    await onTransferProgress(backend, handler);

    const event: TransferProgressEvent = {
      locationId: "loc-1",
      path: "big.iso",
      bytesDone: 1024,
      bytesTotal: null,
      bytesPerSecond: null,
      fraction: null,
    };
    emit(EVENT_CHANNELS.transferProgress, event);

    expect(handler).toHaveBeenCalledWith(event);
  });
});
