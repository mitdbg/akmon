// The console's reads all go through the injected ApiClient, so the design
// harness serves them from a fixture and the live console from the backend
// with no second code path. Nothing is caught in any loader: a read that
// fails or that the fixture does not define propagates, and the view renders
// the failure by name (rule 13). This file holds what every loader shares:
// the chart's span control and the page header's context.
import type { RawVersions } from "@operator-ui/console/model/shared/reads";

/** The chart's span control. 14 days is the default the requirement names;
 *  90 days is the widest span the attempts-by-day read answers. */
export const RANGES = ["7d", "14d", "30d", "90d"] as const;
export type Range = (typeof RANGES)[number];
export const DEFAULT_RANGE: Range = "14d";

/** What the control prints. */
export const RANGE_LABELS: Record<Range, string> = {
  "7d": "7 d",
  "14d": "14 d",
  "30d": "30 d",
  "90d": "90 d",
};

/** The page header's context for "Copy as text": which version the reads
 *  use and when it was published, from the versions read. */
export function pageContextFor(
  deployment: string,
  version: string | null,
  /** Null when the versions read failed: the publish time is then not
   *  known, and is printed as not known rather than guessed. */
  versions: RawVersions | null,
  nowMs: number,
): { deployment: string; version: string | null; publishedAt: string | null; nowMs: number } {
  const row = version && versions
    ? versions.payload.versions.find((candidate) => candidate.generation_id === version) ?? null
    : null;
  return { deployment, version, publishedAt: row?.published_at ?? null, nowMs };
}

/** A read settled into a value or its failure, so one read failing cannot
 *  take the reads beside it down (audit 2026-09-23: one minor read's
 *  `Promise.all` blanked all of Updates or Sources). Nothing is caught and
 *  dropped: the failure is carried to the panel that prints it. */
export type Settled<T> = { value: T; failure: null } | { value: null; failure: unknown };

export function settle<T>(promise: Promise<T>): Promise<Settled<T>> {
  return promise.then(
    (value) => ({ value, failure: null }),
    (failure: unknown) => ({ value: null, failure }),
  );
}
