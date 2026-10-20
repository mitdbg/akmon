// The top bar's search box as a record picker, on the Graph view.
//
// Everywhere else the box opens Explore's results on Enter. On Graph the
// approved prototype lists the matching records under the box as the reader
// types, and picking one opens that record's neighbourhood. It is the same
// box and the same read Explore uses (console/records/search, first ten
// matches); only what a pick does differs. A read that fails says so in the
// list, by route and status, rather than showing an empty list.
import { type InputHTMLAttributes, type KeyboardEvent, type ReactElement, useEffect, useRef, useState } from "react";
import { SEARCH_ROUTE } from "@operator-ui/console/api/records";
import { parseSearchPage, type SearchHit } from "@operator-ui/console/api/searchData";
import { namedReadFailure, readFailureLine } from "@operator-ui/console/model/shared/readFailure";
import type { ApiClient } from "@operator-ui/shared/api/apiClient";

/** The top bar's props that turn its box into the picker (ConsoleTopBarProps extends these). */
export interface RecordPickProps {
  /** On Graph: the box lists matching records as the reader types, and a
   *  pick opens that record's neighbourhood instead of Explore's results. */
  pickRecord?: ((id: string) => void) | null;
  /** The version the page under the bar SHOWS (its reads' answer), which the
   *  record picker searches. Null until the page has read: the picker then
   *  searches the bar's fallback (the version on the URL, else the latest). */
  pickVersion?: string | null;
}

interface RecordPick {
  /** The version the search reads: the version the page shows. */
  version: string | null;
  onPick: (id: string) => void;
}

export const PICKER_LIMIT = 10;
export const PICKER_DEBOUNCE_MS = 150;
export const PICKER_LIST_ID = "console-record-picker";

type ListState =
  | { kind: "idle" }
  | { kind: "reading" }
  | { kind: "hits"; hits: SearchHit[] }
  | { kind: "failed"; line: string };

export interface RecordPicker {
  open: boolean;
  activeId: string | undefined;
  /** Handle a key in the box; true when the picker took it. */
  onKeyDown: (event: KeyboardEvent<HTMLInputElement>) => boolean;
  onFocus: () => void;
  onBlur: () => void;
  /** Spread onto the box: the combobox role, its ARIA wiring and focus handlers. */
  inputProps: InputHTMLAttributes<HTMLInputElement>;
  list: ReactElement | null;
}

export function useRecordPicker(
  api: ApiClient, props: RecordPickProps, fallbackVersion: string | null, query: string, clear: () => void,
): RecordPicker | null {
  // The picker searches the version the Graph page shows. Without
  // `?version=` the page reads the version the read API pinned at start-up,
  // which is not always the latest published one, so the page's own answer
  // comes first; before it has one, the bar's fallback.
  const pick: RecordPick | null = props.pickRecord
    ? { version: props.pickVersion ?? fallbackVersion, onPick: props.pickRecord } : null;
  const [focused, setFocused] = useState(false);
  const [state, setState] = useState<ListState>({ kind: "idle" });
  const [index, setIndex] = useState(-1);
  const q = query.trim();
  const version = pick?.version ?? null;
  const active = Boolean(pick) && focused && q.length > 0;
  // Enter pressed before the answer arrived (inside the debounce, or while
  // the read is out): honoured when the answer comes, for the query it was
  // pressed on.
  const pendingEnter = useRef<string | null>(null);

  useEffect(() => {
    if (!active) { setState({ kind: "idle" }); return undefined; }
    if (!version) { setState({ kind: "failed", line: "The published versions have not been read, so there is no version to search." }); return undefined; }
    let live = true;
    setState({ kind: "reading" });
    const timer = setTimeout(() => {
      api.get(SEARCH_ROUTE, { version, q, limit: PICKER_LIMIT, offset: 0 })
        .then(parseSearchPage)
        .then(
          (page) => { if (live) { setState({ kind: "hits", hits: page.payload.results }); setIndex(page.payload.results.length ? 0 : -1); } },
          (error: unknown) => { if (live) setState({ kind: "failed", line: readFailureLine(namedReadFailure(SEARCH_ROUTE, error)) }); },
        );
    }, PICKER_DEBOUNCE_MS);
    return () => { live = false; clearTimeout(timer); };
  }, [api, active, q, version]);

  const pickRef = useRef(pick);
  pickRef.current = pick;
  useEffect(() => {
    if (pendingEnter.current === null) return;
    if (state.kind === "reading") return;
    const asked = pendingEnter.current;
    pendingEnter.current = null;
    if (state.kind === "hits" && asked === q && state.hits.length && pickRef.current) {
      setFocused(false);
      clear();
      pickRef.current.onPick(state.hits[0].nodeId);
    }
  }, [state, q, clear]);

  if (!pick) return null;
  const hits = state.kind === "hits" ? state.hits : [];
  const choose = (id: string) => { pendingEnter.current = null; setFocused(false); clear(); pick.onPick(id); };
  const open = active && state.kind !== "idle";
  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): boolean => {
    const n = hits.length;
    if (event.key === "ArrowDown" || event.key === "ArrowUp") {
      event.preventDefault();
      if (n) setIndex((i) => (event.key === "ArrowDown" ? (i + 1) % n : (i <= 0 ? n - 1 : i - 1)));
      return true;
    }
    if (event.key === "Enter") {
      event.preventDefault();
      if (open && state.kind === "hits" && n) choose(hits[Math.max(0, index)].nodeId);
      else if (active && (state.kind === "reading" || state.kind === "idle")) pendingEnter.current = q;
      return true;
    }
    if (event.key === "Escape" && open) {
      event.preventDefault();
      setFocused(false);
      return true;
    }
    return false;
  };
  const list = open ? (
    <ul className="okg-console-bar__picker" id={PICKER_LIST_ID} role="listbox" aria-label="Matching records" data-testid="console-record-picker">
      {state.kind === "reading" ? <li className="okg-console-bar__picker-none" role="option" aria-disabled="true" aria-selected="false">Searching…</li> : null}
      {state.kind === "failed" ? (
        <li className="okg-console-bar__picker-none okg-console-bar__picker-failed" role="option" aria-disabled="true" aria-selected="false">
          The search could not be read. {state.line}
        </li>
      ) : null}
      {state.kind === "hits" && !hits.length ? (
        <li className="okg-console-bar__picker-none" role="option" aria-disabled="true" aria-selected="false">No match for “{q}”</li>
      ) : null}
      {hits.map((hit, i) => (
        <li
          key={hit.nodeId}
          id={`${PICKER_LIST_ID}-${i}`}
          role="option"
          aria-selected={i === index}
          onMouseDown={(event) => { event.preventDefault(); choose(hit.nodeId); }}
          onMouseMove={() => { if (i !== index) setIndex(i); }}
        >
          <span>{hit.label || hit.nodeId}</span>
          <code>{hit.type}</code>
        </li>
      ))}
    </ul>
  ) : null;
  const activeId = open && index >= 0 && hits.length ? `${PICKER_LIST_ID}-${index}` : undefined;
  const onFocus = () => setFocused(true);
  const onBlur = () => setFocused(false);
  return {
    open,
    activeId,
    onKeyDown,
    onFocus,
    onBlur,
    inputProps: {
      "aria-label": "Search records; opens the record's neighbourhood",
      role: "combobox",
      "aria-autocomplete": "list",
      "aria-expanded": open,
      "aria-controls": PICKER_LIST_ID,
      "aria-activedescendant": activeId,
      autoComplete: "off",
      onFocus,
      onBlur,
    },
    list,
  };
}
