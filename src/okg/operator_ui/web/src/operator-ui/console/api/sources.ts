// Sources' reads: the one producers read, and one producer's own detail.
//
// Before this slice the view read `console/sources`, `source-diagnostics`,
// `enricher-diagnostics` and `view-diagnostics` and folded them in the
// browser. It now reads `console/producers`, which returns every producer
// once in one row shape. The detail comes from the same route with `?name=`;
// it deliberately does NOT come from `console/source-detail`, which composes
// `okg.source_progress` key/values, answers `found: false` for a source
// holding 988 records, and is a different read altogether (issue #2118).
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import type { RawAttemptsByDay, RawHealth, RawVersions } from "@operator-ui/console/model/shared/reads";
import type { ProducerDetailRead, RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { DEFAULT_RANGE, settle } from "@operator-ui/console/api/client";

/** The one producers route, named once: Sources reads it, and so does the
 *  record page, which needs the ownership-id-to-name map for its source link. */
export const PRODUCERS_ROUTE = "/viz/api/console/producers";

/** What Sources reads: the producers read it draws, health for the version
 *  being read, and the attempts read for its clock.
 *
 *  No generations read. It was here for one link, "Open in Updates", which
 *  matched an attempt whose `stopped_producer` is this producer. That key is
 *  null on every attempt the read returns, because no checkpoint column
 *  records it (issue #2120), so the link and this read are both cut.
 *
 *  Only the producers read is the page. The other three are settled on their
 *  own: health gives the basis clause, attempts the read time, versions the
 *  publish time. Each one that failed is null here, its failure is in
 *  `failures`, and the page names it in its own place and renders the rest. */
export interface ConsoleSourcesData {
  health: RawHealth | null;
  producers: RawProducersRead;
  attempts: RawAttemptsByDay | null;
  /** For the publish time of the version being read, in "Copy as text". */
  versions: RawVersions | null;
  failures?: SourcesReadFailures;
}

/** The minor reads that failed, each by its own error. */
export interface SourcesReadFailures {
  health?: unknown;
  attempts?: unknown;
  versions?: unknown;
}

export const SOURCES_HEALTH_ROUTE = "/viz/api/console/health";
export const SOURCES_ATTEMPTS_ROUTE = "/viz/api/console/attempts";
export const SOURCES_VERSIONS_ROUTE = "/viz/api/console/versions";

export async function loadConsoleSources(
  api: ApiClient,
  signal?: AbortSignal,
): Promise<ConsoleSourcesData> {
  const opts = signal ? { signal } : {};
  const [health, producers, attempts, versions] = await Promise.all([
    settle(api.get<RawHealth>(SOURCES_HEALTH_ROUTE, {}, opts)),
    // The page itself: its failure is the page's.
    api.get<RawProducersRead>(PRODUCERS_ROUTE, {}, opts),
    settle(api.get<RawAttemptsByDay>(SOURCES_ATTEMPTS_ROUTE, { range: DEFAULT_RANGE }, opts)),
    settle(api.get<RawVersions>(SOURCES_VERSIONS_ROUTE, {}, opts)),
  ]);
  const failures: SourcesReadFailures = {};
  if (health.failure !== null) failures.health = health.failure;
  if (attempts.failure !== null) failures.attempts = attempts.failure;
  if (versions.failure !== null) failures.versions = versions.failure;
  return {
    health: health.value,
    producers,
    attempts: attempts.value,
    versions: versions.value,
    failures,
  };
}

/** True when a minor read failed and the page is drawn without it. */
export function sourcesIncomplete(data: ConsoleSourcesData): boolean {
  return Object.keys(data.failures ?? {}).length > 0;
}

/** How many runs one detail read asks for. The composer refuses more than 50
 *  by name rather than clamping, so this stays under that. */
export const RUN_LIMIT = 20;

/**
 * One producer's own read: its row again, and its run history.
 *
 * One route for all three kinds, because the producers read speaks for all
 * three. A read that answers "not found" raises here by name rather than
 * rendering a producer with no runs, which would look like one that never
 * ran.
 */
export async function loadProducerDetail(
  api: ApiClient,
  name: string,
  signal?: AbortSignal,
): Promise<ProducerDetailRead> {
  const read = await api.get<RawProducersRead>(
    PRODUCERS_ROUTE,
    { name, run_limit: RUN_LIMIT },
    signal ? { signal } : {},
  );
  const selected = read.payload.selected;
  if (!selected || !selected.found || !selected.row) {
    throw new Error(
      `The producers read found no producer named ${name}`
      + (selected?.reason ? ` (${selected.reason})` : "")
      + ".",
    );
  }
  const truncated = Boolean(selected.run_history_truncated);
  return {
    row: selected.row,
    runs: selected.run_history ?? [],
    // Null when the read's own query was capped: the rows in hand are a floor
    // and printing them as the count would say "ran twice" of a producer that
    // ran three times.
    runTotal: truncated ? null : selected.row.run_count ?? null,
    runTotalUnavailableReason: truncated
      ? selected.row.run_count_unavailable_reason ?? null
      : null,
    moreRuns: Boolean(selected.more),
    recordsByType: selected.records_by_type ?? null,
    relationshipsByType: selected.relationships_by_type ?? null,
    enricherRunsWindow: read.payload.enricher_runs_window ?? null,
  };
}
