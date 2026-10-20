// A page read that can recover.
//
// Before this, Sources and Updates read once per client: a read that failed
// stayed failed until the reader reloaded the browser tab, while the top bar
// above it retried on its own and came back -- a bar saying the read API was
// answering over a body saying it was not (audit 2026-09-23,
// `bar-recovered-body-still-failed.png`). Every console read now goes through
// `useRecoveringRead`, which gives it four ways back:
//
//   1. "Read again" on its failure (`readAgain`), which the ReadFailure beside
//      it renders.
//   2. The bar's bounded schedule: three tries at 5, 15 and 45 s after a
//      failure, then it stops. A read API that stays down is not polled.
//   3. Another read answering after a failure (`recoveryKey`): the bar's own
//      reads, or a sibling read on the page. Only reads that FAILED re-read.
//   4. A click on the current view's link in the bar (`reloadKey`): every
//      read on the view is issued again.
//
// Each read carries its own AbortController, so a read superseded by a new
// key, a reload or an unmount is cancelled in the browser and the server
// stops working on it.
import {
  createContext, useCallback, useContext, useEffect, useRef, useState, type ReactElement,
} from "react";

/** How long to wait before each automatic re-read of a read that failed.
 *  Three tries, then it stops; the top bar uses the same schedule, so the bar
 *  and the page come back together. */
export const READ_RETRY_DELAYS_MS: readonly number[] = [5_000, 15_000, 45_000];

/** When the "Still reading" line replaces "Reading". */
export const STILL_READING_AFTER_MS = 3_000;

export interface ConsoleReads {
  /** Changes when the reader asks for the whole view again. */
  reloadKey: number;
  /** Changes when some read answered after failing. Reads that failed read
   *  again; reads that answered are left alone. */
  recoveryKey: number;
  /** Tell the console that a read which had failed has now answered. */
  reportRecovered: () => void;
  /** Read the whole view again. The "Read again" of a failure whose read
   *  has no handle of its own. Null outside the live console. */
  readAgainView: (() => void) | null;
}

export const ConsoleReadsContext = createContext<ConsoleReads>({
  reloadKey: 0,
  recoveryKey: 0,
  reportRecovered: () => {},
  readAgainView: null,
});

const clientIds = new WeakMap<object, number>();
let nextClientId = 0;

/** A stable string for one client object, so a read keyed on it starts
 *  afresh when the page is handed a different client. */
export function clientKey(client: object): string {
  let id = clientIds.get(client);
  if (id === undefined) {
    nextClientId += 1;
    id = nextClientId;
    clientIds.set(client, id);
  }
  return `client${id}`;
}

export interface RecoveringRead<T> {
  value: T | null;
  error: unknown;
  /** No answer yet for this key: neither a value nor a failure. */
  pending: boolean;
  /** A re-read of a failed read is in flight; the failure stays up until
   *  the answer replaces it. */
  retrying: boolean;
  readAgain: () => void;
}

/**
 * One read, keyed. A new `key` (or a view reload) starts a fresh read and
 * clears what the old one said; a re-read of a failure keeps the failure on
 * screen until the answer replaces it. A null key issues nothing.
 */
export function useRecoveringRead<T>(
  key: string | null,
  start: (signal: AbortSignal) => Promise<T>,
  options: {
    /** True for an answer that carries a failed part -- a page whose minor
     *  read failed renders from the rest and still re-reads on the
     *  schedule until the part answers. */
    incomplete?: (value: T) => boolean;
    /** True for a failure reading again cannot change -- a policy refusal,
     *  an id the version does not hold. It stays on screen and is NOT
     *  re-read on the schedule or when another read recovers; "Read again"
     *  (`readAgain`) still issues it when the page offers one. */
    settled?: (reason: unknown) => boolean;
  } = {},
): RecoveringRead<T> {
  const incompleteRef = useRef(options.incomplete);
  incompleteRef.current = options.incomplete;
  const settledRef = useRef(options.settled);
  settledRef.current = options.settled;
  const { reloadKey, recoveryKey, reportRecovered } = useContext(ConsoleReadsContext);
  const startRef = useRef(start);
  startRef.current = start;
  const reportRef = useRef(reportRecovered);
  reportRef.current = reportRecovered;
  const [value, setValue] = useState<T | null>(null);
  const [error, setError] = useState<unknown>(null);
  const [retrying, setRetrying] = useState(false);
  const [attempt, setAttempt] = useState(0);
  // Bumped on every failed try -- a rejection, or an answer with a failed
  // part -- so the schedule below sees each one.
  const [failedTries, setFailedTries] = useState(0);
  const identity = `${key ?? ""}\u0000${reloadKey}`;
  const lastIdentity = useRef<string | null>(null);
  const tries = useRef(0);
  const failed = useRef(false);
  const hasValue = useRef(false);

  useEffect(() => {
    if (key === null) return undefined;
    const fresh = lastIdentity.current !== identity;
    lastIdentity.current = identity;
    if (fresh) {
      tries.current = 0;
      failed.current = false;
      hasValue.current = false;
      setValue(null);
      setError(null);
      setRetrying(false);
    } else {
      setRetrying(true);
    }
    const controller = new AbortController();
    let live = true;
    startRef.current(controller.signal).then(
      (next) => {
        if (!live) return;
        const recovered = failed.current;
        const partial = incompleteRef.current?.(next) ?? false;
        failed.current = partial;
        hasValue.current = true;
        setValue(next);
        setError(null);
        setRetrying(false);
        if (partial) setFailedTries((count) => count + 1);
        else tries.current = 0;
        if (recovered && !partial) reportRef.current();
      },
      (reason: unknown) => {
        if (!live) return;
        if (settledRef.current?.(reason)) {
          // Not a failure to recover from: shown, and left alone.
          failed.current = false;
          if (!hasValue.current) setError(reason);
          setRetrying(false);
          return;
        }
        failed.current = true;
        // A re-read that fails outright while the page is drawn from an
        // earlier answer keeps that answer on screen: its own failed parts
        // are still named there, and the schedule tries again.
        if (!hasValue.current) setError(reason);
        setRetrying(false);
        setFailedTries((count) => count + 1);
      },
    );
    return () => {
      live = false;
      controller.abort(new DOMException("superseded", "AbortError"));
    };
  }, [key, identity, attempt]);

  // The bounded schedule: three more tries after a failed one, then stop.
  useEffect(() => {
    if (!failed.current || retrying) return undefined;
    if (tries.current >= READ_RETRY_DELAYS_MS.length) return undefined;
    const timer = setTimeout(() => {
      tries.current += 1;
      setAttempt((count) => count + 1);
    }, READ_RETRY_DELAYS_MS[tries.current]);
    return () => clearTimeout(timer);
  }, [failedTries, retrying]);

  // Something else answered after failing: a failed read tries again now.
  const seenRecovery = useRef(recoveryKey);
  useEffect(() => {
    if (seenRecovery.current === recoveryKey) return;
    seenRecovery.current = recoveryKey;
    if (failed.current) setAttempt((count) => count + 1);
  }, [recoveryKey]);

  const readAgain = useCallback(() => {
    tries.current = 0;
    setAttempt((count) => count + 1);
  }, []);

  return {
    value,
    error,
    pending: key !== null && value === null && error === null,
    retrying,
    readAgain,
  };
}

/**
 * "Reading X…", and after three seconds "Still reading X (N s)", counting.
 *
 * Not red and not a failure: a slow read is not a failed one. Home took nine
 * seconds on Archiv3 with nothing on the page moving (audit 2026-09-23); a
 * reader could not tell a slow read from a hung page.
 */
export function ReadingLine({
  what, className = "okg-console__fact", testId,
}: { what: string; className?: string; testId?: string }): ReactElement {
  const [started] = useState(() => Date.now());
  const [now, setNow] = useState(started);
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), 1_000);
    return () => clearInterval(timer);
  }, []);
  const elapsed = now - started;
  return (
    // No live region: a line that changes every second would be read out
    // every second.
    <p className={className} data-testid={testId}>
      {elapsed < STILL_READING_AFTER_MS
        ? `Reading ${what}…`
        : `Still reading ${what} (${Math.floor(elapsed / 1000)} s)`}
    </p>
  );
}
