// The rows of the Sources table: the filter, one row's cells, sorting and the
// shared-cause line.
import { type ProducerRecord, defaultRank } from "@operator-ui/console/model/sources/producers";
import { freshnessWords, producerGlyph, type ProducerState, stateFromParam, stateOf, stateParam, STATES } from "@operator-ui/console/model/shared/producerStates";
import { exact } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { Glyph } from "@operator-ui/console/model/shared/status";
import { relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";

/** One page of the producers table. */
export const SOURCES_PAGE = 100;

/** Run-history rows shown before "Load more". */
export const RUNS_PAGE = 20;

/** Lines of a recorded error shown before "show all". */
export const ERROR_FOLD = 12;

/** What the Last run cell says when no read recorded a run. */
export const NEVER_RUN = "never run";

/** Past this many characters a name is shortened in the middle, with the
 *  whole name in the cell's title. The tail of a long name is the part that
 *  tells two mirrors apart, so the tail is what stays. */
export const NAME_FOLD = 40;

// --- filters --------------------------------------------------------------

export interface SourcesFilter {
  kind: ProducerKind | null;
  state: ProducerState | null;
  /** A name, or part of one. Seeds the name box from `?producer=`. */
  name: string;
}

export const NO_SOURCES_FILTER: SourcesFilter = { kind: null, state: null, name: "" };

export function parseSourcesFilter(params: Record<string, string>): SourcesFilter {
  const kind = params.kind;
  return {
    kind: kind === "source" || kind === "enricher" || kind === "view" ? kind : null,
    state: stateFromParam(params.state),
    name: params.producer?.trim() ?? "",
  };
}

export function sourcesFilterParams(filter: SourcesFilter): Record<string, string> {
  const params: Record<string, string> = {};
  if (filter.kind) params.kind = filter.kind;
  if (filter.state) params.state = stateParam(filter.state);
  if (filter.name) params.producer = filter.name;
  return params;
}

/**
 * The sort, from the URL.
 *
 * Sort lived in component state, so reloading the page or sending someone the
 * link threw it away: the reader who sorted by records to find the empty
 * producers got the default order back, and had no way to hand anyone else
 * what they were looking at. It is `?sort=<column>&dir=asc|desc`, beside the
 * filter parameters that were already there.
 *
 * An unknown column or direction is the default order, not a refusal: a
 * hand-edited URL should not be able to break the page, and the header the
 * reader then clicks writes a valid one back. But it is not silent either --
 * see `ignoredSortParam` below.
 */
const SORT_KEYS: readonly SortKey[] = [
  "name", "state", "lastRun", "records", "relationships",
];

const SORT_DIRECTIONS: readonly SortDirection[] = ["asc", "desc"];

export function parseSourcesSort(params: Record<string, string>): Sort {
  const key = SORT_KEYS.find((candidate) => candidate === params.sort) ?? null;
  if (key === null) return DEFAULT_SORT;
  return { key, direction: params.dir === "desc" ? "desc" : "asc" };
}

/**
 * What to say about a `sort` or `dir` this table did not use.
 *
 * The substitution above is right -- a hand-edited URL must not break the
 * page -- and doing it SILENTLY was not. `?sort=recrods` returned the
 * default order and said nothing, so the rows a reader was looking at were
 * ranked by something other than what the link asked for, with nothing on
 * the page to tell them. The same for `?dir=sideways`, which became `asc`.
 *
 * The line names the parameter and the value the URL carried, and says what
 * the table did instead. It is the console's ignored-parameter line, the
 * same one the Updates table prints for a leftover `?version=`: a parameter
 * the page read and did not act on is said, once, above the table.
 *
 * Null when both are usable, which includes a `dir` beside no `sort` at all:
 * `?dir=desc` alone asks nothing of a table that is not sorted by a column.
 */
export function ignoredSortParam(params: Record<string, string>): string | null {
  const sort = params.sort?.trim() ?? "";
  const dir = params.dir?.trim() ?? "";
  const badSort = sort !== "" && !SORT_KEYS.includes(sort as SortKey);
  const badDir = dir !== "" && !SORT_DIRECTIONS.includes(dir as SortDirection);
  const named: string[] = [];
  if (badSort) named.push(`sort=${sort}`);
  // A direction is only ever read beside a column. With an unusable column
  // the whole sort is the default, so naming the direction as well would say
  // the page weighed something it never looked at.
  if (badDir && !badSort) named.push(`dir=${dir}`);
  if (!named.length) return null;
  const subject = named.length === 1 ? `${named[0]} is` : `${named.join(" and ")} are`;
  const columns = SORT_KEYS.join(", ");
  return `${subject} not something this table sorts by, so it is not sorted `
    + "by it: the rows are in the default order, worst state first and then "
    + "by last run, newest first. The columns it sorts by are "
    + `${columns}, each asc or desc.`;
}

export function sourcesSortParams(sort: Sort): Record<string, string> {
  if (!sort.key) return {};
  return { sort: sort.key, dir: sort.direction };
}

export function matchesSourcesFilter(record: ProducerRecord, filter: SourcesFilter): boolean {
  if (filter.kind && record.kind !== filter.kind) return false;
  if (filter.state && stateOf(record) !== filter.state) return false;
  if (filter.name && !record.name.toLowerCase().includes(filter.name.toLowerCase())) return false;
  return true;
}

// --- rows -------------------------------------------------------------------

export interface SourceRow {
  key: string;
  name: string;
  /** The name as printed: shortened in the middle past NAME_FOLD, so the
   *  tail that tells two mirrors apart stays visible. */
  displayName: string;
  kind: ProducerKind;
  /** "enricher", "view" or "" -- printed only when the table mixes kinds. */
  kindWord: string;
  configured: boolean;
  state: string;
  mark: Glyph;
  /** "2 h ago", or "" for a producer that has not run. */
  lastRun: string;
  lastRunExact: string;
  lastRunMs: number | null;
  /** The runtime's own freshness state, in words. Never blank: on a
   *  deployment whose runtime supplies no watermark this reads
   *  "not evaluated", which is a fact about the runtime, not about the
   *  producer. There is deliberately no "next run" column: no read records
   *  when a producer is next due. */
  freshness: string;
  /** The code the read served for `state`, for the cell's title and for the
   *  copy text. The cell itself prints the word. */
  stateCode: string;
  /** Null for a producer that has not run: no version holds its work. */
  records: number | null;
  /** Null for a view, which builds rows and emits no relationships, and for
   *  a producer that has not run. `relationshipsNote` says which. */
  relationships: number | null;
  relationshipsNote: string;
  href: string;
}

/** `a_view_emits_no_relationships` and its kin, as a person reads them. */
export function inWords(reason: string | undefined | null): string {
  if (!reason) return "";
  return reason.replace(/_/g, " ");
}

export function foldName(name: string, fold: number = NAME_FOLD): string {
  if (name.length <= fold) return name;
  const tail = 14;
  const head = fold - tail - 1;
  return `${name.slice(0, head)}…${name.slice(-tail)}`;
}

/** Whether any read recorded this producer running.
 *
 *  From the recorded latest run. It used to read `freshness.last_run`, which
 *  the live read does not carry for a source: the runtime's freshness is a
 *  state (`not_evaluated`), and the time a producer last ran is the run's. */
export function hasRun(record: ProducerRecord): boolean {
  return Boolean(record.latestRun?.iso);
}

export function buildSourceRow(
  record: ProducerRecord,
  { nowMs, link, mixedKinds }: { nowMs: number; link: LinkFn; mixedKinds: boolean },
): SourceRow {
  const ran = hasRun(record);
  // The time a producer last ran is the recorded run's, not a freshness
  // field: the live read carries freshness as the runtime's own state and
  // records no watermark for a source. An enricher's read records an age
  // instead of a time, which is printed as it stands.
  const lastRun = record.latestRun?.iso ?? null;
  const age = record.freshness.age ?? "";
  const state = stateOf(record);
  const isView = record.kind === "view";
  return {
    key: record.rowKey,
    name: record.name,
    displayName: foldName(record.name),
    kind: record.kind,
    kindWord: mixedKinds && record.kind !== "source" ? record.kind : "",
    configured: record.configured,
    state,
    mark: producerGlyph(state),
    // "never run" in words, from the read's recorded absence of a latest
    // run -- not a guess, and not a blank cell a reader has to interpret.
    // An enricher's read records an age instead of a time; that is printed
    // as it stands where there is one.
    lastRun: lastRun ? relativeAge(lastRun, nowMs) : (age || NEVER_RUN),
    lastRunExact: lastRun ? utcStamp(lastRun) : "",
    lastRunMs: lastRun ? Date.parse(lastRun) : null,
    // The code the read SERVED. `state` can be this console's reading of it
    // -- `overdue` for a `blocked` source with an overdue reason -- and the
    // cell title and the copy text promise the served code.
    stateCode: record.workflow.state ?? state,
    freshness: freshnessWords(record.freshness.status),
    records: ran ? record.contribution.records_in_gen : null,
    relationships: isView || !ran ? null : record.contribution.edges_in_gen,
    relationshipsNote: isView
      ? inWords(record.contribution.edges_unavailable_reason)
      : "",
    href: link(`sources/${encodeURIComponent(record.name)}`),
  };
}

// No "nextDue": no read records when a producer is next due, so the column
// it sorted is cut.
export type SortKey = "name" | "state" | "lastRun" | "records" | "relationships";
export type SortDirection = "asc" | "desc";

export interface Sort {
  key: SortKey | null;
  direction: SortDirection;
}

export const DEFAULT_SORT: Sort = { key: null, direction: "asc" };

/** Nulls last, whichever way the column is sorted. */
function compareNullable(a: number | null, b: number | null, sign: number): number {
  if (a === null && b === null) return 0;
  if (a === null) return 1;
  if (b === null) return -1;
  return (a - b) * sign;
}

/**
 * Order the rows.
 *
 * No sort key gives the default order: failed, never run, running, then the
 * rest by last run, newest first; not-configured rows last. A key sorts by
 * that column; an empty cell sorts after every value whichever way the
 * column is sorted, so the empty cells never lead.
 */
export function sortRows(
  rows: SourceRow[],
  sort: Sort,
  records: Map<string, ProducerRecord>,
): SourceRow[] {
  const sign = sort.direction === "desc" ? -1 : 1;
  const sorted = [...rows];
  const rankOf = (row: SourceRow) => defaultRank(records.get(row.key) as ProducerRecord);
  if (!sort.key) {
    sorted.sort((a, b) => {
      const rank = rankOf(a) - rankOf(b);
      if (rank !== 0) return rank;
      const byRun = compareNullable(a.lastRunMs, b.lastRunMs, -1);
      if (byRun !== 0) return byRun;
      return a.name.localeCompare(b.name);
    });
    return sorted;
  }
  const key = sort.key;
  const stateIndex = (row: SourceRow) => {
    const index = STATES.indexOf(row.state as ProducerState);
    return index === -1 ? STATES.length : index;
  };
  sorted.sort((a, b) => {
    let result = 0;
    if (key === "name") result = a.name.localeCompare(b.name) * sign;
    else if (key === "state") result = (stateIndex(a) - stateIndex(b)) * sign;
    else if (key === "lastRun") result = compareNullable(a.lastRunMs, b.lastRunMs, sign);
    else if (key === "records") result = compareNullable(a.records, b.records, sign);
    else if (key === "relationships") {
      result = compareNullable(a.relationships, b.relationships, sign);
    }
    return result !== 0 ? result : a.name.localeCompare(b.name);
  });
  return sorted;
}

/** The next sort when a header is clicked: a new column in its natural
 *  direction (names and states ascending, ages and counts descending), the
 *  same column flipped, and a third click back to the default order. */
export function nextSort(current: Sort, key: SortKey): Sort {
  const naturally: SortDirection = key === "name" || key === "state" ? "asc" : "desc";
  if (current.key !== key) return { key, direction: naturally };
  if (current.direction === naturally) {
    return { key, direction: naturally === "asc" ? "desc" : "asc" };
  }
  return DEFAULT_SORT;
}

/**
 * "9 failed", when more than one row failed.
 *
 * The reference put the shared cause here -- "repository not found (404) on
 * all 9". No read records one. `okg.v_source_run_diagnostics` has no message
 * column, so a failed source has an outcome and a phase and no text, and the
 * cause half of this line is cut. The count is still worth saying: it is the
 * difference between nine separate problems and one.
 */
export function sharedCauseLine(rows: SourceRow[]): string | null {
  // Two recorded words for one thing: the design fixtures say `failed`, the
  // enricher and view reads say `failing`.
  const failed = rows.filter((row) => row.state === "failed" || row.state === "failing");
  if (failed.length < 2) return null;
  return `${exact(failed.length)} failed`;
}
