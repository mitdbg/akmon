// How long an update usually takes on this deployment, and whether a running
// attempt is late against it. Used by Home's chip and the Updates rows.
import type { RawGenerations } from "@operator-ui/console/model/shared/reads";
import { duration } from "@operator-ui/console/model/shared/time";

/**
 * The median duration of the newest published attempts.
 *
 * What "too long" means for a running attempt is not a constant: seven
 * minutes is normal on one deployment and twenty-three on another. The
 * attempts read already carries every duration, so the page can say what
 * normal is here rather than leaving the reader to compare a running time
 * against nothing.
 */
export const RUNNING_SAMPLE = 10;

/** A running attempt is late past this multiple of the median. */
export const RUNNING_LATE_FACTOR = 3;

export function medianPublishedDuration(
  generations: RawGenerations,
  sample: number = RUNNING_SAMPLE,
): number | null {
  const durations = generations.payload.generations
    .filter((attempt) => attempt.status === "published" && attempt.duration_ms !== null)
    .slice(0, sample)
    .map((attempt) => attempt.duration_ms as number)
    .sort((a, b) => a - b);
  if (durations.length === 0) return null;
  const middle = Math.floor(durations.length / 2);
  return durations.length % 2 === 1
    ? durations[middle]
    : (durations[middle - 1] + durations[middle]) / 2;
}

/** What a running attempt's row or chip says about how long it has run. */
export interface RunningPace {
  /** "6 h \u00b7 usually 7 m", or "6 h" when nothing has published yet. */
  label: string;
  /** True past RUNNING_LATE_FACTOR times the median. */
  late: boolean;
}

export function runningPace(
  elapsedMs: number | null,
  generations: RawGenerations,
): RunningPace {
  const median = medianPublishedDuration(generations);
  const elapsed = duration(elapsedMs);
  if (median === null || elapsedMs === null) return { label: elapsed, late: false };
  return {
    label: `${elapsed} \u00b7 usually ${duration(median)}`,
    late: elapsedMs > median * RUNNING_LATE_FACTOR,
  };
}
