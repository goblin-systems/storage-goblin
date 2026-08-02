/**
 * Typed Tauri event subscriptions (backlog phase 4.1).
 *
 * Every backend event in one place, with its payload type and channel name
 * stated once. `client.ts` grew a bespoke `listen*` method per event, each
 * repeating the dynamic import, the payload unwrap, and the browser-preview
 * fallback; adding the phase-2.1 progress channel that way would have been a
 * fourth copy.
 *
 * The channel names are the wire contract with `storage/commands.rs`. They are
 * declared as one table so a rename is a single edit that TypeScript checks,
 * rather than a string literal buried in a method.
 */

/** Channel names, mirroring the `app.emit(...)` calls in the Rust backend. */
export const EVENT_CHANNELS = {
  syncStatus: "storage://sync-status-changed",
  activity: "storage://activity",
  transferProgress: "storage://transfer-progress",
} as const;

/**
 * Byte-level transfer progress (emitted by `queue_service`, phase 2.1).
 *
 * Mirrors `TransferProgressEvent` in `storage/commands.rs`. `bytesTotal` and
 * `fraction` are null when the provider did not report a size — the UI must
 * show an indeterminate state rather than inventing a denominator.
 */
export interface TransferProgressEvent {
  locationId: string;
  path: string;
  bytesDone: number;
  bytesTotal: number | null;
  bytesPerSecond: number | null;
  fraction: number | null;
}

export type Unlisten = () => void;

/** The subset of the Tauri event API this module needs. */
export interface EventBackend {
  listen(channel: string, handler: (payload: { payload: unknown }) => void): Promise<Unlisten>;
}

/**
 * Subscribe to `channel`, unwrapping the payload.
 *
 * Returns a no-op unlisten when no backend is available (browser preview), so
 * callers never branch on it and cleanup paths stay uniform.
 */
// T appears once by design: it is how a caller declares what the channel
// carries. Erasing it would hand every handler an `unknown` to cast itself.
// eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- see above
export async function subscribe<T>(
  backend: EventBackend | null,
  channel: string,
  handler: (payload: T) => void,
): Promise<Unlisten> {
  if (!backend) return () => undefined;

  // The backend hands back `unknown`; the channel constant is what pins the
  // payload type, so the cast is the single place that contract is asserted.
  const unlisten = await backend.listen(channel, (event) => {
    handler(event.payload as T);
  });

  // Guard against double-unlisten: views unmounting during teardown can call
  // their cleanup twice, and Tauri's unlisten is not idempotent.
  let released = false;
  return () => {
    if (released) return;
    released = true;
    unlisten();
  };
}

/**
 * Subscribe to several channels and release them together.
 *
 * A view that subscribes to three events needs one cleanup, not three — and
 * three separate handles is how one gets forgotten.
 */
export function combineUnlisten(handles: Unlisten[]): Unlisten {
  let released = false;
  return () => {
    if (released) return;
    released = true;
    for (const handle of handles) {
      try {
        handle();
      } catch (error) {
        // One failed unlisten must not strand the others.
        console.error("failed to release event subscription", error);
      }
    }
  };
}

/** Load Tauri's event API, or `null` outside the desktop shell. */
export async function loadEventBackend(): Promise<EventBackend | null> {
  try {
    const api = await import("@tauri-apps/api/event");
    return api satisfies EventBackend;
  } catch {
    // Browser preview: the module is not resolvable and events do not exist.
    return null;
  }
}

export function onTransferProgress(
  backend: EventBackend | null,
  handler: (event: TransferProgressEvent) => void,
): Promise<Unlisten> {
  return subscribe<TransferProgressEvent>(backend, EVENT_CHANNELS.transferProgress, handler);
}
