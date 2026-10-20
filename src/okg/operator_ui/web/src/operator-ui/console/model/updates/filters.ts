// The Updates filter: outcome, producer and attempt, round-tripped through the
// URL.
//
// The attempt filter used to live under `?version=`, which is ALSO the top
// bar's version picker on every view. The picker writes a full generation id
// and this table matched a 12-hex short one, so choosing a version on Updates
// filtered the table to nothing and the page read "No attempt the read
// returned matches this filter." over an empty table. The two are different
// questions -- which version the console reads, and which attempt row to show
// -- so they no longer share a parameter name.
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { RawAttempt } from "@operator-ui/console/model/shared/reads";

export const UPDATES_PAGE = 50;

export const OUTCOMES = ["published", "blocked", "running"] as const;
export type Outcome = (typeof OUTCOMES)[number];

// --- filters --------------------------------------------------------------

export interface UpdatesFilter {
  /** From Home's count strip: `#updates?outcome=blocked`. */
  outcome: Outcome | null;
  /** From Home's Needs attention: the producer an attempt stopped on. */
  producer: string | null;
  /** A 12-hex short generation id: one attempt's row. */
  attempt: string | null;
}

export const EMPTY_FILTER: UpdatesFilter = { outcome: null, producer: null, attempt: null };

/**
 * Read the filter out of the route's parameters.
 *
 * An outcome the console does not have is dropped rather than carried: a table
 * filtered to a word nothing can match would render zero rows and look like a
 * deployment with no attempts, which is the one thing an empty table must not
 * be confused with.
 */
export function parseUpdatesFilter(params: Record<string, string>): UpdatesFilter {
  const outcome = params.outcome as Outcome | undefined;
  return {
    outcome: outcome && (OUTCOMES as readonly string[]).includes(outcome) ? outcome : null,
    producer: params.producer?.trim() || null,
    // NOT params.version: that one belongs to the top bar's picker.
    attempt: params.attempt?.trim() || null,
  };
}

/** The filter as route parameters again, dropping what is not set. */
export function filterParams(filter: UpdatesFilter): Record<string, string> {
  const params: Record<string, string> = {};
  if (filter.outcome) params.outcome = filter.outcome;
  if (filter.producer) params.producer = filter.producer;
  if (filter.attempt) params.attempt = filter.attempt;
  return params;
}

/** Does this attempt pass the filter? */
export function matchesFilter(attempt: RawAttempt, filter: UpdatesFilter): boolean {
  if (filter.outcome && attempt.status !== filter.outcome) return false;
  if (filter.producer && attempt.stopped_producer !== filter.producer) return false;
  if (filter.attempt && shortVersion(attempt.full_generation_id) !== filter.attempt) return false;
  return true;
}

/** True when any filter is set. */
export function filterIsSet(filter: UpdatesFilter): boolean {
  return Boolean(filter.outcome || filter.producer || filter.attempt);
}

/** A 12-hex short generation id, the only thing `?attempt=` ever held. */
const SHORT_ID = /^[0-9a-f]{6,12}$/i;

/**
 * What to say about a `version` parameter this page does not filter by.
 *
 * Null when the URL carries none, and null for anything carrying a colon --
 * the top bar's picker writes a full `okg:<deployment>:gen:<...>` id and
 * nothing it writes is bare, so a colon means the value was never this
 * table's filter. The version note answers those: by name when the value is a
 * published id, and as unrecognised when it is not.
 *
 * What is left is a bare value: an old link built when this table's filter
 * lived under `version`. The table is NOT filtered by it, and the line says so
 * rather than letting a complete table look like a filter that matched
 * everything.
 *
 * The value itself appears in the line only when it passed `SHORT_ID`, so the
 * only thing this can print is up to twelve hex characters. A `?version=` is
 * whatever a reader or an earlier link put on the URL, and a line that echoed
 * it would hand a page whatever arrived there.
 */
export function ignoredVersionParam(params: Record<string, string>): string | null {
  const value = params.version?.trim();
  if (!value) return null;
  if (value.includes(":")) return null;
  const meant = SHORT_ID.test(value) ? ` This link means attempt=${value}.` : "";
  return "The version parameter no longer filters this table \u2014 it names "
    + "the version the console reads \u2014 and the table filters by attempt "
    + `id, under attempt=.${meant} Every attempt the read returned is shown.`;
}

/**
 * What to say about a `?sort=` this list does not order by.
 *
 * The table draws the attempts in the order the read returns them -- newest
 * first -- and neither the read nor this page takes an ordering. A URL asking
 * for one used to change nothing and say nothing, so a reader who wrote or
 * followed `?sort=oldest` was shown newest first with no word about it.
 * Explore names `sort=` and `since=` for the same reason; this is that line
 * on the other view that can be asked for an order it has not got.
 */
export function ignoredSortParam(params: Record<string, string>): string | null {
  const value = params.sort?.trim();
  if (!value) return null;
  return `This link asked for sort=${value}, which this list cannot honour: `
    + "the read returns attempts in the order it holds them and takes no "
    + "ordering. Every attempt below is newest first.";
}
