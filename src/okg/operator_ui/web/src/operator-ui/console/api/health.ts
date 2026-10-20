// Home's reads: health, the ontology rollup, the versions and the attempts
// for every span, all at once.
import type { ApiClient } from "@operator-ui/shared/api/apiClient";
import type { HomeInputs } from "@operator-ui/console/model/home";
import type { RawAttemptsByDay, RawGenerations, RawHealth, RawRollup, RawVersions } from "@operator-ui/console/model/shared/reads";
import type { RawProducersRead } from "@operator-ui/console/model/sources/reads";
import { PRODUCERS_ROUTE } from "@operator-ui/console/api/sources";
import { DEFAULT_RANGE, RANGES, type Range } from "@operator-ui/console/api/client";

/** The route that feeds the Contents panel, named once so the loader, the
 *  panel's failure and its test all say the same string. */
export const ROLLUP_ROUTE = "/viz/api/console/ontology-rollup";
export const VERSIONS_ROUTE = "/viz/api/console/versions";

/** The route behind Home's chart. New in this slice, and the likeliest of
 *  Home's reads to fail, so it is settled on its own too. */
export const ATTEMPTS_ROUTE = "/viz/api/console/attempts";

/**
 * ONE read behind the Sources block and the producer half of Needs attention:
 * `console/producers`, which is the read the Sources page draws.
 *
 * Home used to read `source-diagnostics`, `enricher-diagnostics` and
 * `view-diagnostics` and count the rows itself. Those three are inventories of
 * recorded keys, not of producers. `compose_sources` lists every key the count
 * rollup's `source` dimension holds, including the derived ones no registry
 * declares (`_cascade:`, `_enricher:`, `catalog:`) -- so on okg-workspace
 * `_enricher:code_reference` and
 * `_enricher:declarative:agent_message_mentions_pact_change_dir` arrived as
 * SOURCES, and Home's hero figure read "37 sources configured, 63 producers"
 * while Sources, three clicks away, said 35 and 61 of the same deployment.
 *
 * `console/producers` is where that identity is resolved: its composer folds
 * an `_enricher:` source row into the enricher it IS
 * (`console_views/producers.py::_fold_enricher_source_row`), keyed by the
 * `ownership_id` the registry declares, and returns every producer once. One
 * population, one basis, one `workflow.state` accessor -- the same three the
 * Sources page uses, so the two pages cannot disagree about the deployment.
 *
 * The three diagnostics reads are NOT kept beside it. Everything Home printed
 * from them is in this read's row: the state, the freshness verdict, the
 * recorded issue, the last run and the contribution. The one field that is
 * not is a source's `freshness.next_due`, which no LIVE diagnostics read fills
 * either -- only the design harness's fixtures did, and Home's only use of it
 * was a sentence nothing ever called.
 */
export const PRODUCERS_ROUTES = [PRODUCERS_ROUTE] as const;
export { PRODUCERS_ROUTE };

export interface ConsoleHomeData {
  inputs: HomeInputs;
  /** The ontology-rollup read's failure, when it failed. The Contents panel
   *  renders it by name and the rest of the page still renders: this read
   *  feeds one panel, and losing it is not losing the page. Every other read
   *  here feeds the header, the figures or the chart, so its failure
   *  propagates and the route renders the failure for the whole page --
   *  there is no honest Home without the health read. */
  rollupFailure: unknown | null;
  /** The attempts read's failure, when it failed. The chart's place says so
   *  and the rest of Home renders from the other reads. */
  attemptsFailure: unknown | null;
  /** The versions read's failure, when it failed. It contributes the publish
   *  time in the header and whether a second published version exists, and
   *  nothing else; losing it CUTS the publish time and the record and
   *  relationship deltas, and a sentence names the read. It does not take the
   *  page, which is what it used to do: health, generations, attempts and the
   *  three diagnostics still answered and Home rendered nothing. */
  versionsFailure: unknown | null;
  /** Every range, loaded up front so the control switches without a read and a
   *  capture is never taken mid-load. */
  /** Empty when the attempts read failed. */
  attemptsByRange: Partial<Record<Range, RawAttemptsByDay>>;
}

/** The three reads the header, the figures and the attempts list are built
 *  from. One group, because the chip states what the NEWEST ATTEMPT did and
 *  that is the generations read; splitting it out would draw a header that
 *  then changed under the reader a moment later. */
export interface HomeHeadReads {
  health: RawHealth;
  generations: RawGenerations;
  versions: RawVersions;
}

/** The one producers read behind the Sources block. */
export type HomeProducerReads = RawProducersRead;

/**
 * Home's reads, all in flight at once and each settled on its own.
 *
 * `loadConsoleHome` below awaits every one of them -- nine reads, of which
 * four were the same attempts route at four different spans -- before its
 * caller could draw anything. On Archiv3 that is a blank page until the
 * slowest of the nine answers, and the slowest is the source diagnostics
 * read, which is also the one Home needs least in order to say something
 * useful.
 *
 * This returns the promises instead. The caller renders each block when its
 * own group resolves, and prints that group's failure in that block's place
 * when it does not. Nothing here catches anything: a caller that ignores a
 * rejection still sees it.
 */
export function startHomeReads(api: ApiClient, range: Range, signal?: AbortSignal): {
  head: Promise<HomeHeadReads>;
  producers: Promise<HomeProducerReads>;
  rollup: Promise<RawRollup>;
  attempts: Promise<RawAttemptsByDay>;
} {
  return {
    head: loadHomeHead(api, signal),
    producers: loadHomeProducers(api, signal),
    rollup: loadHomeRollup(api, signal),
    // ONE span. The page opened by asking for all four and using one of them;
    // three of the four requests were paid for on every load and thrown away
    // unless the reader touched the control. The other three are fetched when
    // the control asks for them.
    attempts: loadAttemptsRange(api, range, signal),
  };
}

/** Each of Home's groups on its own, so the route can re-read one group --
 *  "Read again" on one block -- without re-reading the page. */
export function loadHomeHead(api: ApiClient, signal?: AbortSignal): Promise<HomeHeadReads> {
  const opts = signal ? { signal } : {};
  return Promise.all([
    api.get<RawHealth>("/viz/api/console/health", {}, opts),
    api.get<RawGenerations>("/viz/api/console/generations", {}, opts),
    api.get<RawVersions>("/viz/api/console/versions", {}, opts),
  ]).then(([health, generations, versions]) => ({ health, generations, versions }));
}

export function loadHomeProducers(api: ApiClient, signal?: AbortSignal): Promise<HomeProducerReads> {
  return api.get<RawProducersRead>(PRODUCERS_ROUTE, {}, signal ? { signal } : {});
}

export function loadHomeRollup(api: ApiClient, signal?: AbortSignal): Promise<RawRollup> {
  return api.get<RawRollup>(ROLLUP_ROUTE, {}, signal ? { signal } : {});
}

export function loadAttemptsRange(
  api: ApiClient, range: Range, signal?: AbortSignal,
): Promise<RawAttemptsByDay> {
  return api.get<RawAttemptsByDay>(ATTEMPTS_ROUTE, { range }, signal ? { signal } : {});
}

export async function loadConsoleHome(api: ApiClient): Promise<ConsoleHomeData> {
  const [health, generations, versionsRead, producers] = await Promise.all([
    api.get<RawHealth>("/viz/api/console/health"),
    api.get<RawGenerations>("/viz/api/console/generations"),
    // Settled beside the others rather than with them: issued at the same
    // time, but its failure is a value instead of a rejection, so it cannot
    // take the reads that answered down with it.
    api.get<RawVersions>(VERSIONS_ROUTE).then(
      (value) => ({ value: value as RawVersions | null, failure: null as unknown }),
      (failure: unknown) => ({ value: null as RawVersions | null, failure }),
    ),
    api.get<RawProducersRead>(PRODUCERS_ROUTE),
  ]);
  const versions = versionsRead.value;
  // Settled on its own, not caught in a way that hides it: the value is
  // either the payload or the reason, and the panel prints whichever it got.
  const rollupRead = await api.get<RawRollup>(ROLLUP_ROUTE).then(
    (value) => ({ value, failure: null as unknown }),
    (failure: unknown) => ({ value: null, failure }),
  );
  // Every span at once, settled together: one failure takes the chart, not
  // the page. Partial success would mean a span control whose options render
  // different things, so all or none.
  const attemptsRead = await Promise.all(
    RANGES.map((range) => api.get<RawAttemptsByDay>(ATTEMPTS_ROUTE, { range })),
  ).then(
    (ranges) => ({
      byRange: Object.fromEntries(
        RANGES.map((range, index) => [range, ranges[index]]),
      ) as Record<Range, RawAttemptsByDay>,
      failure: null as unknown,
    }),
    (failure: unknown) => ({ byRange: {} as Partial<Record<Range, RawAttemptsByDay>>, failure }),
  );
  const attemptsByRange = attemptsRead.byRange;

  return {
    inputs: {
      health,
      generations,
      versions,
      attempts: attemptsByRange[DEFAULT_RANGE] ?? null,
      producers,
      rollup: rollupRead.value,
    },
    rollupFailure: rollupRead.failure,
    attemptsFailure: attemptsRead.failure,
    versionsFailure: versionsRead.failure,
    attemptsByRange,
  };
}

/** One span's attempts payload from a loaded fixture, or a failure naming the
 *  span. Every fixture defines all five, so a missing one is a defect in the
 *  fixture rather than a state a test should tolerate. */
export function attemptsFor(
  data: { attemptsByRange: Partial<Record<Range, RawAttemptsByDay>> },
  range: Range,
): RawAttemptsByDay {
  const payload = data.attemptsByRange[range];
  if (!payload) throw new Error(`the fixture defines no attempts read for ${range}`);
  return payload;
}
