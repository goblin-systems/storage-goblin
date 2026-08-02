/**
 * Request sequencing for out-of-order async results (backlog phase 4.1).
 *
 * The classic single-window bug: the user switches location, two `list_files`
 * calls are in flight, the slower *first* one resolves last, and the UI ends up
 * showing the previous location's files. `bootstrap.ts` guards one instance of
 * this by hand with a `fileTreeRequestSequence` counter and scattered
 * `if (sequence !== fileTreeRequestSequence) return` checks — which works, but
 * has to be remembered at every await point and is silently missing everywhere
 * else the same race exists.
 *
 * `latest()` makes the guard structural: a superseded call simply never
 * resolves its handler.
 */

/** Thrown into the `catch` of a superseded call so callers can ignore it. */
export class SupersededError extends Error {
  readonly superseded = true;

  constructor(label: string) {
    super(`${label} was superseded by a newer request`);
    this.name = "SupersededError";
  }
}

export function isSuperseded(error: unknown): boolean {
  return error instanceof SupersededError;
}

export interface LatestRunner {
  /**
   * Run `operation`; resolve only if no newer call started meanwhile.
   *
   * A superseded call rejects with [`SupersededError`] rather than hanging, so
   * a `finally` still runs and loading indicators cannot leak.
   */
  run<T>(operation: (signal: { superseded: boolean }) => Promise<T>): Promise<T>;
  /** Invalidate everything in flight without starting new work. */
  cancel(): void;
}

/**
 * Create a runner that keeps only the most recent call per key.
 *
 * One runner per logical stream — the file tree, the status refresh — so a slow
 * file-tree load cannot cancel an unrelated status refresh.
 */
export function createLatest(label = "request"): LatestRunner {
  let sequence = 0;

  return {
    async run<T>(operation: (signal: { superseded: boolean }) => Promise<T>): Promise<T> {
      const current = ++sequence;
      // Handed to the operation so long multi-step work can bail early rather
      // than finishing work whose result is already unwanted.
      const signal = {
        get superseded() {
          return current !== sequence;
        },
      };

      const value = await operation(signal);

      if (current !== sequence) throw new SupersededError(label);
      return value;
    },

    cancel() {
      sequence += 1;
    },
  };
}
