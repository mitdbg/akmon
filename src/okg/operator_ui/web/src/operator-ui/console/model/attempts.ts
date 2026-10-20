// The recent-attempts rows: consecutive identical attempts collapsed into one
// row that names how many there were.
import { changePhrase } from "@operator-ui/console/model/shared/change";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { RawAttempt, RawGenerations } from "@operator-ui/console/model/shared/reads";
import { outcomeGlyph, type Glyph } from "@operator-ui/console/model/shared/status";
import { duration, relativeAge } from "@operator-ui/console/model/shared/time";

export interface AttemptRow {
  key: string;
  /** What the attempt stopped on, in words: "on cmssw_releases", "in
   *  publication checks", or "" for a published attempt. */
  headline: string;
  /** "2 h ago \u00b7 42 s" for one attempt, "3 d ago to 2 h ago" for a run. */
  when: string;
  outcome: Glyph;
  /** The newest version id in the row, short. */
  version: string;
  /** The same id in full: what a copy control hands over (rule 15). */
  fullVersion: string;
  detail: string;
  count: number;
  /** A blocked attempt a later published one already replaced. It is history,
   *  not an open problem, and Needs attention leaves it out for that reason;
   *  the word says why the two lists disagree. */
  superseded: boolean;
}

function changeVersusPrevious(attempt: RawAttempt): string {
  // One rule, shared with the Updates table: retractions are part of what an
  // attempt changed, and this list used to drop them.
  if (attempt.status === "published") return changePhrase(attempt, ", ");
  if (attempt.status === "running") return "not finished";
  // A blocked attempt published no version: the outcome word already said so.
  return "";
}

/**
 * Collapse a run of identical attempts into one row.
 *
 * Five attempts blocked on the same producer are one fact, not five rows, and
 * printing them five times is what made the page read as a wall. Consecutive
 * attempts with the same outcome AND the same stopped-on producer become a
 * single row that names how many there were and the span they cover; anything
 * distinct stays its own row. The newest version id in the run is the one
 * printed, because that is the attempt an operator would go and describe.
 */
export function collapseAttempts(
  generations: RawGenerations,
  limit: number,
  nowMs: number,
): AttemptRow[] {
  const newestPublishedAt = generations.payload.generations
    .find((attempt) => attempt.status === "published")?.created_at ?? null;
  const runs: RawAttempt[][] = [];
  for (const attempt of generations.payload.generations) {
    const last = runs[runs.length - 1];
    const previous = last?.[0];
    const sameRun = previous
      && previous.status === attempt.status
      && (previous.stopped_producer ?? null) === (attempt.stopped_producer ?? null)
      && (previous.stopped_stage ?? null) === (attempt.stopped_stage ?? null)
      && attempt.status !== "published";
    if (sameRun) last.push(attempt);
    else runs.push([attempt]);
    if (runs.length > limit) break;
  }

  return runs.slice(0, limit).map((run) => {
    const newest = run[0];
    const oldest = run[run.length - 1];
    const mark = outcomeGlyph(newest.status);
    const stoppedOn = newest.stopped_producer
      ? ` on ${newest.stopped_producer}`
      : newest.stopped_stage ? ` in ${newest.stopped_stage}` : "";
    return {
      key: newest.full_generation_id,
      // The glyph and the word beside it already say the outcome; repeating it
      // here printed "Published  Published" on every published row, and "Blocked
      // 5 attempts blocked on" on a collapsed one. The count is its own field,
      // printed as "5\u00d7" the way the Updates table prints it.
      headline: stoppedOn.trim(),
      // A duration only when one was recorded. On a deployment that records
      // none, "10 h ago \u00b7 duration not recorded" on every row says the
      // same absent fact four times; the row is "10 h ago" and the absence is
      // stated once, where it belongs, rather than made into furniture. Never
      // a zero: a zero would read as a run that took no time.
      when: run.length === 1
        ? (newest.duration_ms === null || !Number.isFinite(newest.duration_ms)
          ? relativeAge(newest.created_at, nowMs)
          : `${relativeAge(newest.created_at, nowMs)} \u00b7 ${duration(newest.duration_ms)}`)
        : `${relativeAge(oldest.created_at, nowMs)} to ${relativeAge(newest.created_at, nowMs)}`,
      outcome: mark,
      version: shortVersion(newest.full_generation_id) ?? "no version",
      fullVersion: newest.full_generation_id,
      detail: changeVersusPrevious(newest),
      count: run.length,
      superseded: newest.status === "blocked"
        && newestPublishedAt !== null
        && Date.parse(newestPublishedAt) > Date.parse(newest.created_at),
    };
  });
}
