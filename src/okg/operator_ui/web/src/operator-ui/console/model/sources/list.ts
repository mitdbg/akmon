// The Sources view's model: the producer reads in, one table of configured
// producers and one producer detail out.
//
// Pure, like consoleModel.ts and updatesModel.ts, and for the same reason: the
// rows, their order, the filters and the words on the page are asserted in a
// unit test without a browser.
//
// One thing this module does that the read does not: it never prints a count
// a read did not record. A producer that has not run has no contribution to
// print; its cells are empty, not "0".
//
// It does NOT collapse. An earlier version did: the three diagnostics reads
// returned one row per registry name and one per runtime key -- eleven rows
// for five sources on Archiv3 -- and the browser folded them by guessing
// which key belonged to which name. `console/producers` resolves a producer
// by the `ownership_id` its registry declares and returns it once, so there
// is nothing left to fold and no name to guess at.
import { type ProducerRecord, partialReadSentences, producerRecordsFrom } from "@operator-ui/console/model/sources/producers";
import { producerGlyph, stateOf, STATES } from "@operator-ui/console/model/shared/producerStates";
import type { RawProducersRead, SourcesInputs } from "@operator-ui/console/model/sources/reads";
import { DEFAULT_SORT, SOURCES_PAGE, type Sort, type SourceRow, type SourcesFilter, buildSourceRow, matchesSourcesFilter, sharedCauseLine, sortRows, sourcesFilterParams } from "@operator-ui/console/model/sources/rows";
import { exact } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";
import type { Glyph } from "@operator-ui/console/model/shared/status";
import { utcStamp } from "@operator-ui/console/model/shared/time";

// --- chips ------------------------------------------------------------------

export interface SourcesChip {
  key: string;
  label: string;
  value: number;
  mark: Glyph | null;
  selected: boolean;
  href: string;
}

export function noun(value: number, singular: string): string {
  return value === 1 ? singular : `${singular}s`;
}

// --- the whole view -----------------------------------------------------------

export interface SourcesModel {
  deployment: string;
  /** "37 producers" or "5 sources". */
  headerCount: string;
  /** "next run due in 4 h (13:00 UTC)", or null with no cadence. */
  segments: { state: string; count: number; share: number }[];
  kindChips: SourcesChip[];
  stateChips: SourcesChip[];
  /** Every matched row, in order, before paging. */
  rows: SourceRow[];
  total: number;
  mixedKinds: boolean;
  /** A column exists only where some row prints a value in it. */
  columns: {
    contribution: boolean;
    freshness: boolean;
  };
  /** True when the name box is worth its row. */
  nameFilter: boolean;
  sharedCause: string | null;
  /** One clause: when a row reads Unknown to the read, while Home's health
   *  verdict and this table count on different bases; null once the health
   *  read says it counts on the same basis. See STATE_BASIS_NOTE. */
  stateBasisNote: string | null;
  /** What the console's own readiness read covered, and how many checks
   *  it declined, or null when the read was the full composition. */
  readinessScopeNote: string | null;
  /** One sentence per producer kind the read could not list, because the
   *  composer behind it failed. Empty when the read answered in full. */
  partialReads: string[];
  noMatchSentence: string | null;
  filter: SourcesFilter;
  clearHref: string;
  footer: string;
  /** The clause that explains the RECORDS and RELATIONSHIPS columns, or
   *  null when no version is being read. Hidden with those columns at
   *  390 (review N4). */
  footerContribution: string | null;
  /** When the page was read. Always shown: it is true at every width. */
  /** Always a whole sentence: "As of 2026-09-22 05:36 UTC." The page shows
   *  this one where the clause before it is hidden. */
  footerAsOf: string;
  /** The same fact worded to follow the contribution clause, or null when
   *  there is no clause to follow. */
  footerAsOfJoined: string | null;
  readingVersion: string | null;
  nowMs: number;
}

function chip(
  key: string,
  label: string,
  value: number,
  mark: Glyph | null,
  selected: boolean,
  href: string,
): SourcesChip {
  return { key, label, value, mark, selected, href };
}

// The recorded vocabulary, in the order `producers.ts` ranks it: the states
// an operator acts on first, then work under way, then the rest. It used to be
// a second list of the fixtures' words alone, so on a deployment whose runtime
// records real states every segment counted zero and the bar was empty.
const SEGMENT_ORDER: readonly string[] = STATES;

function segmentsOf(producers: ProducerRecord[]) {
  const total = producers.length;
  if (total === 0) return [];
  return SEGMENT_ORDER
    .map((state) => ({
      state,
      count: producers.filter((record) => stateOf(record) === state).length,
    }))
    .filter((row) => row.count > 0)
    .map((row) => ({ ...row, share: (row.count / total) * 100 }));
}

/**
 * What this table's State column is, and when it reads "Unknown to the read".
 *
 * The column carries the source-sync convergence state the runtime records
 * live (`source_sync_convergence.py`). Convergence records `fresh` for every
 * source the registry does not require for readiness, with nothing behind
 * the word, and the Health read used to call a source healthy for a mere
 * `okg.source_progress` row. On cern-r3final the two disagreed on one
 * version; on okg-workspace 22 sources read "Fresh" on no recorded outcome.
 *
 * Issue #2205 put one rule under both: a source is healthy only on a
 * watermark the runtime judged fresh against its cadence or a latest run
 * recorded `OK` (`_has_recorded_health`, `console_views/source_diagnostics.py`),
 * and a good state that rule does not back reads "Unknown to the read"
 * (`stateOf`, `shared/producerStates.ts`). Home and this page both count
 * states through `stateOf` over the one producers read, so they agree. The
 * sentence says the rule so a reader need not find it here.
 */
// The console has four views -- Home, Updates, Sources and Explore -- and no
// Health page. The verdict this note once pointed at is on Home.
//
// One clause (review 2026-09-22): it states a fact, never that two pages
// "can disagree". Issue #2205 removed the fact the earlier clause stated --
// Home no longer counts a source healthy because this version recorded an
// update for it -- so the clause now states the rule both pages read. Once
// the health read serves a basis equal to this table's (`SHARED_STATE_BASIS`),
// it says nothing at all.
export const STATE_BASIS_NOTE =
  "A source reads Unknown to the read when no run outcome and no watermark "
  + "within its cadence is recorded for it, even where readiness calls it fresh.";

/** The `health.source_breakdown.basis` value under which Home's verdict
 *  counts sources from the same convergence state this table prints. The
 *  sibling read-layer change serves it; until then the note stays. */
export const SHARED_STATE_BASIS = "source_convergence";

/** The basis note, or null when the health read says it shares this
 *  table's basis. */
export function stateBasisNote(health: SourcesInputs["health"]): string | null {
  // Unread, the basis is unknown, and the health read's own failure is on
  // the page instead of a clause about a verdict nobody read.
  if (health === null) return null;
  const breakdown = health.payload.source_breakdown ?? {};
  // `status_basis: producer_state` is the key the health read serves once
  // its source rows ARE this table's state (PACT
  // console-archiv3-read-layer-fixes); then Home and this page cannot
  // disagree, and the note has nothing to explain.
  if (breakdown.status_basis === PRODUCER_STATE_BASIS) return null;
  return breakdown.basis === SHARED_STATE_BASIS ? null : STATE_BASIS_NOTE;
}

/** `health.source_breakdown.status_basis` when each source's status is the
 *  producers read's own state for it. */
export const PRODUCER_STATE_BASIS = "producer_state";

/**
 * What the console's readiness read covered, and what it did not.
 *
 * The Sources page reads three things out of readiness -- each source's sync
 * convergence, its freshness, and the published generation -- so the console
 * composes only those and declines the rest. On okg-workspace the rest was
 * most of the wait: 15.4 s in the Nomos source-lineage report and 5.6 s in
 * the runtime queue's dead-letter scan, neither of which this page can show.
 *
 * That is a fair trade only if the page SAYS so. A reader who takes this
 * table for a release gate would be reading a bounded check as a complete
 * one. The sentence names the two things it covers in words and points at the
 * command that runs the rest; the codes stay in the payload and in "Copy as
 * text", where a reader matching the page to a log needs them.
 */
export function readinessScopeNote(read: RawProducersRead): string | null {
  const authority = read.payload.read_authority as
    | { sources?: { runtime_readiness?: Record<string, unknown> } }
    | undefined;
  const readiness = authority?.sources?.runtime_readiness;
  if (!readiness || readiness.basis !== "console_bounded_readiness") return null;
  const declined = readiness.checks_not_run_count;
  if (typeof declined !== "number" || declined <= 0) return null;
  // In words: the command name was the reader's first sight of a CLI on a
  // page that offers none (review 2026-09-22).
  return `Readiness here is source sync and freshness; ${exact(declined)} further `
    + `${declined === 1 ? "check runs" : "checks run"} only in the full status check on the command line.`;
}

export function buildSourcesModel(
  inputs: SourcesInputs,
  filter: SourcesFilter,
  link: LinkFn,
  sort: Sort = DEFAULT_SORT,
): SourcesModel {
  // The read's own clock where the attempts read answered, this browser's
  // otherwise -- and the footer then says whose clock it is.
  const nowMs = inputs.attempts ? Date.parse(inputs.attempts.payload.now) : Date.now();
  // No collapsing: console/producers returns each producer once.
  const producers = producerRecordsFrom(inputs.producers);
  const kinds = new Set(producers.map((record) => record.kind));
  const mixedKinds = kinds.size > 1;
  // Keyed by `rowKey`, which is what `buildSourceRow` puts on the row and
  // what `sortRows` and the filter look a record back up by. Keying this by
  // `ownershipId` while the row carried `rowKey` would have silently emptied
  // both lookups.
  const byKey = new Map(producers.map((record) => [record.rowKey, record]));

  const allRows = producers.map((record) => buildSourceRow(record, { nowMs, link, mixedKinds }));
  const matched = allRows.filter(
    (row) => matchesSourcesFilter(byKey.get(row.key) as ProducerRecord, filter),
  );
  const rows = sortRows(matched, sort, byKey);

  const toggle = (next: Partial<SourcesFilter>) =>
    link("sources", sourcesFilterParams({ ...filter, ...next }));
  const countKind = (kind: ProducerKind) =>
    producers.filter((record) => record.kind === kind).length;
  const countState = (state: string) =>
    producers.filter((record) => stateOf(record) === state).length;

  // Kind chips only where the table mixes kinds: on a deployment of sources
  // alone, "5 sources" is what the header already says and a filter that
  // matches everything.
  const kindChips = mixedKinds
    ? (["source", "enricher", "view"] as const)
      .map((kind) => chip(
        kind, noun(countKind(kind), kind), countKind(kind), null,
        filter.kind === kind, toggle({ kind: filter.kind === kind ? null : kind }),
      ))
      .filter((row) => row.value > 0)
    : [];
  // The chip's label is the state's WORD, lowercased to sit inside "2
  // blocked" -- never the served code, which put `manual_required` in the
  // count strip above a table that had stopped printing it.
  const stateChips = STATES
    .map((state) => chip(
      state, producerGlyph(state).word.toLowerCase(), countState(state),
      producerGlyph(state), filter.state === state,
      toggle({ state: filter.state === state ? null : state }),
    ))
    .filter((row) => row.value > 0);

  // No "next run due" line. The reference put one in the header; no read
  // records when a producer is next due, so it is cut rather than guessed
  // from a cadence nothing declares.

  const readingVersion = inputs.health
    ? inputs.health.payload.generation_id
    : inputs.producers.payload.generation_id;
  const readAt = inputs.attempts
    ? utcStamp(inputs.attempts.payload.now)
    : `${utcStamp(new Date(nowMs).toISOString())} by this browser's clock`;
  // Three parts, because the first one explains COLUMNS. At 390 the table
  // drops RECORDS and RELATIONSHIPS, and the sentence explaining them used to
  // stay behind on its own -- a footnote to columns that are not there
  // (review N4). The page hides that clause with the columns.
  //
  // Which left the as-of fact beginning mid-sentence: what a phone showed was
  // "as of 2026-09-22 05:36 UTC.", lower-case, with nothing before it. So the
  // fact is worded twice -- joined to the clause, and standing alone -- and
  // the page shows whichever one its width leaves complete. `footer` is what
  // "Copy as text" carries; it has no viewport and takes the whole sentence.
  const footerContribution = readingVersion
    ? `Records and relationships are each producer's contribution to version `
      + `${shortVersion(readingVersion)}`
    : null;
  // The standalone wording carries the VERSION as well as the time, because
  // it is the only one of the two the phone shows: at 390 the contribution
  // clause is hidden with the columns it explains, and the top bar prints the
  // deployment name rather than the id. Without this, nothing on a 390 px
  // Sources page said which version the figures belonged to.
  const footerAsOf = readingVersion
    ? `As of ${readAt}, version ${shortVersion(readingVersion)}.`
    : `As of ${readAt}.`;
  const footerAsOfJoined = footerContribution ? `as of ${readAt}.` : null;
  const footer = footerContribution
    ? `${footerContribution} · ${footerAsOfJoined}`
    : footerAsOf;

  const sourceOnly = kinds.size === 1 && kinds.has("source");
  return {
    deployment: inputs.health?.payload.deployment ?? inputs.producers.payload.deployment,
    headerCount: sourceOnly
      ? `${exact(producers.length)} ${noun(producers.length, "source")}`
      : `${exact(producers.length)} ${noun(producers.length, "producer")}`,
    segments: segmentsOf(producers),
    kindChips,
    stateChips,
    rows,
    total: producers.length,
    mixedKinds,
    columns: {
      contribution: allRows.some((row) => row.records !== null),
      freshness: allRows.some((row) => row.freshness !== ""),
    },
    nameFilter: producers.length > 10 || filter.name !== "",
    sharedCause: sharedCauseLine(rows),
    stateBasisNote: stateBasisNote(inputs.health),
    readinessScopeNote: readinessScopeNote(inputs.producers),
    partialReads: partialReadSentences(inputs.producers),
    noMatchSentence: producers.length > 0 && rows.length === 0
      ? "No producer matches this filter."
      : null,
    filter,
    clearHref: link("sources"),
    footer,
    footerContribution,
    footerAsOf,
    footerAsOfJoined,
    readingVersion,
    nowMs,
  };
}

/** The rows one page shows: the first `pages` hundred. */
export function pageOf(rows: SourceRow[], pages: number): SourceRow[] {
  return rows.slice(0, pages * SOURCES_PAGE);
}

/** "100 of 220 producers" while the table is cut; "" once it is not. */
export function shownSentence(shown: number, total: number, unit: string): string {
  return shown < total ? `${exact(shown)} of ${exact(total)} ${unit}` : "";
}
