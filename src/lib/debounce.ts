/**
 * Trailing-edge debounce (backlog phase 4.1).
 *
 * Lifted out of the bootstrap closure so views can use it without depending on
 * the app. Trailing-edge on purpose: every caller here is coalescing a burst of
 * events into one render, where the *last* state is the one worth drawing.
 */

export interface DebouncedFn<T extends (...args: never[]) => void> {
  (...args: Parameters<T>): void;
  /** Drop a pending call. Required in view teardown, or a timer fires into a
   *  destroyed view and throws on DOM that is no longer there. */
  cancel(): void;
}

export function debounce<T extends (...args: never[]) => void>(fn: T, ms: number): DebouncedFn<T> {
  let timer: ReturnType<typeof setTimeout> | null = null;

  const debounced = ((...args: Parameters<T>) => {
    if (timer !== null) clearTimeout(timer);
    timer = setTimeout(() => {
      timer = null;
      fn(...args);
    }, ms);
  }) as DebouncedFn<T>;

  debounced.cancel = () => {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  };

  return debounced;
}
