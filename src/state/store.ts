/**
 * The application store (backlog phase 4.1).
 *
 * `bootstrap.ts` holds all app state in a 4,300-line closure, which is why
 * nothing can be extracted from it: every render function reaches directly into
 * a `state` object that only exists inside that scope. Moving state here is
 * what makes views independently mountable and testable.
 *
 * Deliberately ~150 lines and dependency-free. The app is one window with a
 * handful of screens; a state library would add a dependency and a mental model
 * without solving a problem this does not have.
 *
 * The behaviours that matter are the ones that cause render storms and
 * heisenbugs, and each is pinned by a test:
 *
 * - A `setState` that changes nothing notifies nobody.
 * - A selector subscription fires only when its slice actually changes.
 * - Unsubscribing during a notification does not skip the remaining listeners.
 * - A listener that throws does not prevent the others from running.
 * - `setState` called from inside a listener settles rather than recursing.
 */

export type Listener<S> = (state: Readonly<S>, previous: Readonly<S>) => void;

export type Selector<S, T> = (state: Readonly<S>) => T;

/** Compares a selected slice; defaults to reference equality. */
export type EqualityFn<T> = (next: T, previous: T) => boolean;

export interface Store<S> {
  getState(): Readonly<S>;
  /**
   * Merge a patch into the state.
   *
   * The function form receives the current state, so callers deriving from it
   * cannot read a stale value between the read and the write.
   */
  setState(patch: Partial<S> | ((state: Readonly<S>) => Partial<S>)): void;
  subscribe(listener: Listener<S>): () => void;
  /** Subscribe to one slice. Fires only when that slice changes. */
  select<T>(
    selector: Selector<S, T>,
    listener: (value: T, previous: T) => void,
    isEqual?: EqualityFn<T>,
  ): () => void;
}

function shallowEqualPatch<S>(state: S, patch: Partial<S>): boolean {
  for (const key of Object.keys(patch) as (keyof S)[]) {
    if (!Object.is(state[key], patch[key])) return false;
  }
  return true;
}

export function createStore<S extends object>(initial: S): Store<S> {
  let state: Readonly<S> = Object.freeze({ ...initial });

  // A Set gives cheap unsubscribe; iteration order is insertion order, which
  // keeps notification deterministic.
  const listeners = new Set<Listener<S>>();

  /**
   * Notifications queued while another notification is in flight.
   *
   * A listener that calls `setState` (a render reacting to state by deriving
   * more state) would otherwise re-enter the notify loop and either recurse or
   * deliver updates out of order. Queueing makes the outer pass finish first,
   * so every listener sees a consistent sequence.
   */
  const pending: { state: Readonly<S>; previous: Readonly<S> }[] = [];
  let notifying = false;

  function notify(next: Readonly<S>, previous: Readonly<S>): void {
    pending.push({ state: next, previous });
    if (notifying) return;

    notifying = true;
    try {
      while (pending.length > 0) {
        const update = pending.shift();
        if (!update) break;

        // Snapshot the listener set: unsubscribing during notification must
        // not shift the iteration and skip a later listener.
        for (const listener of [...listeners]) {
          // A listener removed earlier in this same pass should not be called.
          if (!listeners.has(listener)) continue;
          try {
            listener(update.state, update.previous);
          } catch (error) {
            // One broken view must not stop the rest of the UI updating.
            console.error("store listener threw", error);
          }
        }
      }
    } finally {
      notifying = false;
      // Anything queued by a throwing listener is dropped with the loop; clear
      // so a later update does not replay stale pairs.
      pending.length = 0;
    }
  }

  return {
    getState() {
      return state;
    },

    setState(patch) {
      const resolved = typeof patch === "function" ? patch(state) : patch;

      // No-op writes are extremely common (re-applying a server response that
      // matched), and notifying on them is how a render storm starts.
      if (shallowEqualPatch(state, resolved)) return;

      const previous = state;
      state = Object.freeze({ ...state, ...resolved });
      notify(state, previous);
    },

    subscribe(listener) {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },

    select(selector, listener, isEqual = Object.is) {
      let current = selector(state);

      return this.subscribe((next) => {
        const value = selector(next);
        if (isEqual(value, current)) return;
        const previous = current;
        current = value;
        listener(value, previous);
      });
    },
  };
}
