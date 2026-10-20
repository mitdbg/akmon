// The Updates view's model: the console's attempt reads in, one table of
// update attempts and one attempt detail out.
//
// Pure, like consoleModel.ts, and for the same reason: the numbers and the
// words on the page are asserted in a unit test without a browser, and the
// rules that matter -- rows collapse, the page is bounded, a fact the read does
// not carry is never invented -- are visible in code.
//
// What the fixtures actually record about an attempt is its outcome, its
// created_at, its duration, the stage and producer it stopped on, the verbatim
// error text, and (when it published) the records and relationships it added.
// They record NO per-stage counts and NO per-stage durations. Nothing here
// fills that in from another read: the stage list states the position of the
import { type Outcome, UPDATES_PAGE, type UpdatesFilter, filterParams, matchesFilter } from "@operator-ui/console/model/updates/filters";
import { type UpdateRow, buildUpdateRows } from "@operator-ui/console/model/updates/rows";
import { exact } from "@operator-ui/console/model/shared/format";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { RawAttempt, RawAttemptsByDay, RawGenerations, RawHealth, RawVersions } from "@operator-ui/console/model/shared/reads";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { type Glyph, outcomeGlyph } from "@operator-ui/console/model/shared/status";
import { relativeAge } from "@operator-ui/console/model/shared/time";

// --- the whole view -------------------------------------------------------

export interface OutcomeChip {
  outcome: Outcome;
  label: string;
  value: number;
  /** "all on cmssw_releases", "3 producers", or "" when no attempt the read
   *  returned names one. Printed after the count, so the repeat offender is
   *  readable without opening a row. */
  note: string;
  mark: Glyph;
  selected: boolean;
  /** Where the chip goes: its own filter, or back to no filter when it is
   *  already the one selected. */
  href: string;
}

export interface UpdatesModel {
  deployment: string;
  /** "166 attempts recorded · newest 2 h ago", split so the view can set the
   *  two halves in different ink. */
  total: number;
  newestAge: string | null;
  chips: OutcomeChip[];
  rows: UpdateRow[];
  /** How many attempts the read returned, before collapsing and filtering. */
  readCount: number;
  /** Set when the read says there are more attempts than it returned. */
  moreCursor: string | null;
  shownSentence: string;
  /** The one sentence a deployment with no attempt at all gets (story S20). */
  noAttemptsSentence: string | null;
  /** The second sentence it gets: what IS configured, and where the rest of
   *  the answer is. A dead end is not an answer to "what do I do next". */
  configuredSentence: string | null;
  configuredHref: string | null;
  configuredLink: string | null;
  /** Set when a filter matched nothing. Different from "no attempts". */
  noMatchSentence: string | null;
  filter: UpdatesFilter;
  clearHref: string;
  nowMs: number;
}

export interface UpdatesInputs {
  health: RawHealth;
  generations: RawGenerations;
  /** Null when the versions read failed; the page names that read. */
  versions: RawVersions | null;
  /** Null when this span's attempts read failed: no outcome chips, the
   *  chart's place names the read, and ages are by this browser's clock. */
  attempts: RawAttemptsByDay | null;
  /** Only story S20 reads it: how many sources are configured on a
   *  deployment that has never run anything. The producers read, counted by
   *  its rows of kind `source` -- the population the Sources page draws. Null
   *  when that read failed: the count is unknown, and the sentence says so
   *  rather than printing a number or a zero. Required, so no caller can
   *  leave it out and have "unknown" read as "none". */
  producers: RawProducersRead | null;
}

/** What blocked the blocked attempts the read returned. */
export function blockedNote(attempts: RawAttempt[]): string {
  const producers = new Set(
    attempts
      .filter((attempt) => attempt.status === "blocked" && attempt.stopped_producer)
      .map((attempt) => attempt.stopped_producer as string),
  );
  if (producers.size === 0) return "";
  if (producers.size === 1) return `all on ${[...producers][0]}`;
  return `${exact(producers.size)} producers`;
}

function chipFor(
  outcome: Outcome,
  value: number,
  note: string,
  filter: UpdatesFilter,
  link: LinkFn,
): OutcomeChip {
  const mark = outcomeGlyph(outcome);
  const selected = filter.outcome === outcome;
  const next = selected
    ? { ...filter, outcome: null }
    : { ...filter, outcome };
  return {
    outcome,
    label: mark.word,
    value,
    note,
    mark,
    selected,
    href: link("updates", filterParams(next)),
  };
}

/** Why the configured-source count is unknown, in the page's words. */
export const SOURCE_COUNT_UNKNOWN = {
  readFailed: "The number of configured sources is unknown because that read failed.",
  enrichersMissing: "The number of configured sources is unknown because the list of "
    + "enrichers could not be read, so some enrichers may be listed as sources.",
  sourcesMissing: "The number of configured sources is unknown because some sources "
    + "could not be read.",
} as const;

export type ConfiguredSourceCount = { count: number } | { unknown: string };

/** The configured-source count, or why it is unknown.
 *
 *  Counted from the producers read's rows of kind `source` -- the same rows
 *  the Sources page draws, so the two pages agree. Unknown, never a number,
 *  when the read failed, or when the read says it left out a whole kind that
 *  decides which rows are sources:
 *
 *  - `enricher`: the server folds each `_enricher:` source row into its
 *    enricher only when the enricher read answers
 *    (`console_views/producers.py`). When that read fails, those rows stay
 *    rows of kind `source`, and counting them is the miscount of issue #2257.
 *  - `source`: rows are missing, so any count is short.
 *
 *  An omission that cost only a FIELD (no `kind`), or the views, changes
 *  neither which rows are sources nor how many there are. */
export function configuredSourceCount(producers: RawProducersRead | null): ConfiguredSourceCount {
  if (!producers) return { unknown: SOURCE_COUNT_UNKNOWN.readFailed };
  const omittedKinds = new Set(
    (producers.payload.omitted_data ?? []).map((entry) => entry.kind).filter(Boolean),
  );
  if (omittedKinds.has("source")) return { unknown: SOURCE_COUNT_UNKNOWN.sourcesMissing };
  if (omittedKinds.has("enricher")) return { unknown: SOURCE_COUNT_UNKNOWN.enrichersMissing };
  const rows = producers.payload.producers ?? [];
  return { count: rows.filter((row) => row.kind === "source").length };
}

/** The second sentence a deployment with no attempt gets (story S20).
 *
 *  Unknown is said in words with no number: the page cannot tell "no source
 *  is configured" from "the read did not answer", so it prints neither. The
 *  link to Sources stays, because Sources is where the answer is. Zero
 *  sources prints nothing, as before: there is nothing configured to point
 *  at. */
function configuredSentenceFor(
  total: number,
  configured: ConfiguredSourceCount,
  link: LinkFn,
): { sentence: string | null; href: string | null; link: string | null } {
  if (total !== 0) return { sentence: null, href: null, link: null };
  if ("unknown" in configured) {
    return { sentence: configured.unknown, href: link("sources"), link: "View sources" };
  }
  const { count } = configured;
  if (count === 0) return { sentence: null, href: null, link: null };
  return {
    sentence: `${exact(count)} ${count === 1 ? "source is" : "sources are"} `
      + "configured and none has run yet.",
    href: link("sources"),
    link: `View sources (${exact(count)})`,
  };
}

export function buildUpdatesModel(
  inputs: UpdatesInputs,
  filter: UpdatesFilter,
  link: LinkFn,
): UpdatesModel {
  const nowMs = inputs.attempts ? Date.parse(inputs.attempts.payload.now) : Date.now();
  const all = inputs.generations.payload.generations;
  const readingVersion = inputs.health.payload.generation_id;
  const matched = all.filter((attempt) => matchesFilter(attempt, filter));
  const rows = buildUpdateRows(matched, {
    nowMs, readingVersion, limit: UPDATES_PAGE, link,
  });
  // No chips without the span's totals: a chip reading a count this page
  // did not read would be a number from nowhere.
  const totals = inputs.attempts?.payload.totals ?? { published: 0, blocked: 0, running: 0 };

  // Zeros are not printed. A chip reading "0 running" is a filter that can only
  // empty the table, in the same weight as the two that carry a number.
  const note = blockedNote(all);
  const chips = ([
    ["published", totals.published, ""],
    ["blocked", totals.blocked, note],
    ["running", totals.running, ""],
  ] as const)
    .filter(([, value]) => value > 0)
    .map(([outcome, value, chipNote]) => chipFor(outcome, value, chipNote, filter, link));

  const total = inputs.generations.payload.total;
  const collapsed = rows.reduce((sum, row) => sum + row.count, 0);
  // Unknown is never zero: a failed producers read is not a deployment with
  // no source configured.
  const configured = configuredSentenceFor(total, configuredSourceCount(inputs.producers), link);
  return {
    deployment: inputs.health.payload.deployment,
    total,
    newestAge: all.length ? relativeAge(all[0].created_at, nowMs) : null,
    chips,
    rows,
    readCount: all.length,
    moreCursor: inputs.generations.payload.next_cursor ?? null,
    // Only when the read did not return everything. "22 of 22 attempts" over a
    // count strip that already says so is the same fact twice.
    shownSentence: collapsed > 0 && collapsed < total
      ? `${exact(collapsed)} of ${exact(total)} attempts`
      : "",
    noAttemptsSentence: total === 0
      ? `No update attempt has been recorded for ${inputs.health.payload.deployment}.`
      : null,
    configuredSentence: configured.sentence,
    configuredHref: configured.href,
    configuredLink: configured.link,
    noMatchSentence: total > 0 && matched.length === 0
      ? "No attempt the read returned matches this filter."
      : null,
    filter,
    clearHref: link("updates"),
    nowMs,
  };
}
