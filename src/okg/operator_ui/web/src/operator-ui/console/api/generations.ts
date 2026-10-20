// Updates' reads: the generations read and its next page behind the cursor.
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import type { RawAttempt, RawAttemptsByDay, RawGenerations, RawHealth, RawVersions } from "@operator-ui/console/model/shared/reads";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import type { RawAttemptDetail } from "@operator-ui/console/model/updates/recorded";
import { RANGES, type Range, settle } from "@operator-ui/console/api/client";

/** What Updates reads. Four routes, not nine: the attempts table AND the
 *  attempt detail come from the generations read alone -- it now carries each
 *  attempt's own view-producer runs -- plus health for the version being read
 *  and the attempts read for the chart.
 *
 *  The enricher and view diagnostics reads are NOT here any more. They
 *  describe whatever generation they were last taken from, so attributing
 *  their rows to an attempt meant matching generation ids and printing
 *  nothing whenever they disagreed; and their row shape matches no live
 *  payload. The per-attempt runs are recorded against the attempt itself.
 *
 *  Health and generations are the page: the deployment it names and the
 *  table. The rest are settled on their own -- versions, the configured
 *  sources and each span of the chart -- so one of them failing costs its
 *  own panel, named there, and not the page. */
export interface ConsoleUpdatesData {
  health: RawHealth;
  generations: RawGenerations;
  versions: RawVersions | null;
  /** Only for the sentence a deployment with no attempt at all gets: how
   *  many sources are configured, so a dead end is not the answer to "what do
   *  I do next". It is the producers read -- the population the Sources page
   *  draws -- and only its rows of kind `source` are counted. It used to be
   *  `source-diagnostics`, which lists every key in the count rollup's
   *  `source` dimension, `_enricher:` keys no registry declares included, and
   *  so counted enrichers as sources (issue #2257). Null when the read
   *  failed: the count is then unknown and the page says so. */
  producers: RawProducersRead | null;
  /** A span whose read failed is absent; `failures.attempts` holds why. */
  attemptsByRange: Partial<Record<Range, RawAttemptsByDay>>;
  failures?: UpdatesReadFailures;
}

/** The minor reads that failed, each by its own error. */
export interface UpdatesReadFailures {
  versions?: unknown;
  producers?: unknown;
  attempts?: Partial<Record<Range, unknown>>;
}

export const UPDATES_VERSIONS_ROUTE = "/viz/api/console/versions";
/** The configured-source count's read: the producers read Sources and Home
 *  draw, so the three pages count one population. */
export const UPDATES_PRODUCERS_ROUTE = PRODUCERS_ROUTE;
export const UPDATES_ATTEMPTS_ROUTE = "/viz/api/console/attempts";

export async function loadConsoleUpdates(
  api: ApiClient,
  signal?: AbortSignal,
): Promise<ConsoleUpdatesData> {
  const opts = signal ? { signal } : {};
  const [health, generations, versions, producers, ranges] = await Promise.all([
    api.get<RawHealth>("/viz/api/console/health", {}, opts),
    api.get<RawGenerations>("/viz/api/console/generations", {}, opts),
    settle(api.get<RawVersions>(UPDATES_VERSIONS_ROUTE, {}, opts)),
    settle(api.get<RawProducersRead>(UPDATES_PRODUCERS_ROUTE, {}, opts)),
    Promise.all(RANGES.map((range) => settle(
      api.get<RawAttemptsByDay>(UPDATES_ATTEMPTS_ROUTE, { range }, opts),
    ))),
  ]);
  const failures: UpdatesReadFailures = {};
  if (versions.failure !== null) failures.versions = versions.failure;
  if (producers.failure !== null) failures.producers = producers.failure;
  const attemptsByRange: Partial<Record<Range, RawAttemptsByDay>> = {};
  RANGES.forEach((range, index) => {
    const read = ranges[index];
    if (read.value !== null) attemptsByRange[range] = read.value;
    else (failures.attempts ??= {})[range] = read.failure;
  });
  return {
    health,
    generations,
    versions: versions.value,
    producers: producers.value,
    attemptsByRange,
    failures,
  };
}

/** True when a minor read failed and the page is drawn without it. */
export function updatesIncomplete(data: ConsoleUpdatesData): boolean {
  return Object.keys(data.failures ?? {}).length > 0;
}

/**
 * The next page of attempts.
 *
 * The read is capped at 50 and hands back a cursor (requirements v2 section 10,
 * gap 5). Nothing here tolerates a page that repeats what the caller already
 * has: an API -- or a fixture -- that ignores the cursor and answers with the
 * first page again would otherwise silently double every row, and a table that
 * lists the same attempt twice is worse than one that stops at 50. It raises
 * instead, and the view renders the failure by name (rule 13).
 */
export async function loadMoreAttempts(
  api: ApiClient,
  cursor: string,
  have: readonly RawAttempt[],
): Promise<RawAttempt[]> {
  const next = await api.get<RawGenerations>("/viz/api/console/generations", { cursor });
  const rows = next.payload.generations;
  const seen = new Set(have.map((attempt) => attempt.full_generation_id));
  if (rows.length && seen.has(rows[0].full_generation_id)) {
    throw new Error(
      `The attempts read answered cursor ${cursor} with attempt `
      + `${rows[0].full_generation_id}, which is already in the table. The cursor is not `
      + "served yet (requirements v2 section 10, gap 5); no rows were added.",
    );
  }
  return rows;
}

/**
 * What ONE attempt recorded.
 *
 * A read of its own, issued only once an id in the hash has resolved to a
 * full generation id: the route takes the FULL id, and a suffix would either
 * be refused as malformed or -- worse -- match somebody else's attempt. It is
 * deliberately not folded into `loadConsoleUpdates`, because the list page
 * must not pay for a read only the detail page uses.
 *
 * Nothing here catches the failure. The page renders it by name (rule 13): a
 * detail read that 404s is a different answer from one that 500s, and both
 * are different from an attempt with nothing recorded.
 */
export async function loadAttemptDetail(
  api: ApiClient,
  fullGenerationId: string,
  signal?: AbortSignal,
): Promise<RawAttemptDetail> {
  return api.get<RawAttemptDetail>(
    "/viz/api/console/attempts/detail", { id: fullGenerationId },
    signal ? { signal } : {},
  );
}
