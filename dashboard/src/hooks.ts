import { useCallback, useEffect, useRef, useState } from 'react';

/* usePoll — disciplined polling loop.
 *
 *  - never stacks requests (skips the tick while one is in flight)
 *  - pauses while the tab is hidden; fires immediately on re-focus
 *  - clears the interval on unmount
 *  - swallows rejections (fn is expected to set its own error state) so
 *    a failed poll can never become an unhandled rejection.
 */
export function usePoll(
  fn: () => Promise<void> | void,
  intervalMs: number,
  enabled = true,
): void {
  const fnRef = useRef(fn);
  fnRef.current = fn;

  useEffect(() => {
    if (!enabled) return;
    let stopped = false;
    let inflight = false;

    const tick = async () => {
      if (stopped || inflight || document.hidden) return;
      inflight = true;
      try {
        await fnRef.current();
      } catch {
        /* poll errors are handled inside fn; never escape */
      } finally {
        inflight = false;
      }
    };

    tick();
    const timer = window.setInterval(tick, intervalMs);
    const onVis = () => {
      if (!document.hidden) tick();
    };
    document.addEventListener('visibilitychange', onVis);
    return () => {
      stopped = true;
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', onVis);
    };
  }, [intervalMs, enabled]);
}

/* useTheme — light/dark with OS default + manual override.
 *
 * No stored preference → follow prefers-color-scheme live.  Toggling
 * stores the explicit choice in localStorage and stamps data-theme on
 * <html>, which the token overrides in styles.css honour in both
 * directions.  index.html applies the stored value pre-paint.
 */
const THEME_KEY = 'tsdb.theme';

function systemTheme(): 'light' | 'dark' {
  try {
    return window.matchMedia('(prefers-color-scheme: dark)').matches
      ? 'dark'
      : 'light';
  } catch {
    return 'light';
  }
}

function storedTheme(): 'light' | 'dark' | null {
  try {
    const v = localStorage.getItem(THEME_KEY);
    return v === 'light' || v === 'dark' ? v : null;
  } catch {
    return null;
  }
}

export function useTheme(): { theme: 'light' | 'dark'; toggle: () => void } {
  const [theme, setTheme] = useState<'light' | 'dark'>(
    () => storedTheme() ?? systemTheme(),
  );

  // Follow OS changes only while the user hasn't picked explicitly.
  useEffect(() => {
    let mq: MediaQueryList | null = null;
    try {
      mq = window.matchMedia('(prefers-color-scheme: dark)');
    } catch {
      return;
    }
    const onChange = () => {
      if (!storedTheme()) setTheme(systemTheme());
    };
    mq.addEventListener?.('change', onChange);
    return () => mq?.removeEventListener?.('change', onChange);
  }, []);

  const toggle = useCallback(() => {
    setTheme(prev => {
      const next = prev === 'dark' ? 'light' : 'dark';
      try {
        localStorage.setItem(THEME_KEY, next);
      } catch { /* private mode — in-memory only */ }
      document.documentElement.dataset.theme = next;
      return next;
    });
  }, []);

  return { theme, toggle };
}

/* useLocalFlag — persisted boolean (sidebar collapse etc.). */
export function useLocalFlag(key: string, initial: boolean): [boolean, () => void] {
  const [v, setV] = useState<boolean>(() => {
    try {
      const s = localStorage.getItem(key);
      return s === null ? initial : s === '1';
    } catch {
      return initial;
    }
  });
  const flip = useCallback(() => {
    setV(prev => {
      const next = !prev;
      try {
        localStorage.setItem(key, next ? '1' : '0');
      } catch { /* ignore */ }
      return next;
    });
  }, [key]);
  return [v, flip];
}
