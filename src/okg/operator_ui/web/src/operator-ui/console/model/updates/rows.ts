// The attempts table's rows: consecutive identical attempts collapsed, with
// what each stopped on in words.
import { changePhrase } from "@operator-ui/console/model/shared/change";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { LinkFn } from "@operator-ui/console/model/shared/links";
import type { RawAttempt } from "@operator-ui/console/model/shared/reads";
import { type Glyph, outcomeGlyph } from "@operator-ui/console/model/shared/status";
import { relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";

// --- rows -----------------------------------------------------------------

/** One attempt inside a collapsed row, for the "show N" expansion. */
export interface CollapsedMember {
  attemptId: string;
  /** The full id, for the copy control (rule 15). */
  fullId: string;
  when: string;
  whenExact: string;
}

export interface UpdateRow {
  key: string;
  /** The 12-hex id the detail route is keyed by. */
  attemptId: string;
  /** The full generation id: what the copy control hands over (rule 15). */
  fullId: string;
  when: string;
  /** The absolute UTC time, for the row's `title` (rule 5). */
  whenExact: string;
  outcome: Glyph;
  /** "on cmssw_releases" or "in publication checks"; empty when the attempt
   *  records neither. Printed after the outcome word. */
  stoppedOn: string;
  /** The published version's 12-hex id, or null: a blocked or running attempt
   *  published none, and printing one would be a version that does not exist. */
  version: string | null;
  /** The published version's full id, or null, for its copy control. */
  versionFullId: string | null;
  /** "5\u00d7" on a collapsed row. It is the expander, and it sits in the
   *  Outcome cell: the Duration cell that used to hold it is cut, because no
   *  column records a publish's wall clock. */
  repeat: string;
  /** True for the version the console is reading right now. */
  reading: boolean;
  /** "+2,116 records \u00b7 +7,214 relationships", or "" for an attempt that
   *  published nothing. */
  change: string;
  /** The second line a NARROW row shows, where the Change column is hidden.
   *  A published attempt shows its change; a blocked one its recorded reason
   *  code; a running one that it is running. Without it two blocked rows read
   *  identically: same age, same outcome word, no version, no change. */
  narrowDetail: string;
  /** How many attempts this row stands for. 1 unless identical rows collapsed. */
  count: number;
  members: CollapsedMember[];
  href: string;
}

function memberOf(attempt: RawAttempt, nowMs: number): CollapsedMember {
  return {
    attemptId: shortVersion(attempt.full_generation_id) ?? attempt.full_generation_id,
    fullId: attempt.full_generation_id,
    when: relativeAge(attempt.created_at, nowMs),
    whenExact: utcStamp(attempt.created_at),
  };
}

/**
 * What the attempt changed, on one line.
 *
 * "+2,116 records \u00b7 +7,214 relationships" at 13 px, so the row is 44 px
 * like every other row on the page. A blocked or running attempt changed
 * nothing and gets no line -- the outcome word already said so.
 */
export function changeText(attempt: RawAttempt): string {
  return changePhrase(attempt);
}

/**
 * What a narrow row says under its outcome word.
 *
 * At <=760px the Version and Change columns have nowhere to go. A published
 * attempt still differs by its change; a blocked or running one had nothing
 * left at all, so two of them read identically. The blocker's recorded reason
 * code is the fact that tells them apart, and it is already allowlisted by
 * the read.
 */
export function narrowDetailFor(attempt: RawAttempt): string {
  if (attempt.status === "published") return changeText(attempt);
  if (attempt.status === "running") return "running";
  const code = attempt.blocked_reason;
  if (code) return code;
  const short = shortVersion(attempt.full_generation_id);
  // No code recorded: the id is the only thing that distinguishes it, and it
  // is better than two rows that read the same.
  return short ? `attempt ${short}` : "";
}

/** "on cmssw_releases", "in publication checks", or "". */
export function stoppedOnPhrase(attempt: RawAttempt): string {
  if (attempt.stopped_producer) return `on ${attempt.stopped_producer}`;
  if (attempt.stopped_stage) return `in ${attempt.stopped_stage}`;
  return "";
}

/**
 * Build the table's rows, newest first, with runs of identical blocked
 * attempts collapsed into one.
 *
 * archiv3-small's five attempts blocked on cmssw_releases are one fact. Five
 * rows of the same words is the wall Home already cut; the same rule holds
 * here, and the row can be opened in place to get the five back.
 *
 * Only CONSECUTIVE attempts collapse, and only when the outcome, the stage and
 * the producer all match: a published attempt between two blocked ones means
 * the blockage was fixed and returned, which is a different story from five in
 * a row and must read as one.
 */
export function buildUpdateRows(
  attempts: RawAttempt[],
  {
    nowMs, readingVersion, limit, link,
  }: {
    nowMs: number;
    readingVersion: string | null;
    limit: number;
    link: LinkFn;
  },
): UpdateRow[] {
  const runs: RawAttempt[][] = [];
  for (const attempt of attempts) {
    const last = runs[runs.length - 1];
    const previous = last?.[0];
    const sameRun = previous
      && attempt.status !== "published"
      && previous.status === attempt.status
      && (previous.stopped_producer ?? null) === (attempt.stopped_producer ?? null)
      && (previous.stopped_stage ?? null) === (attempt.stopped_stage ?? null);
    if (sameRun && last) last.push(attempt);
    else runs.push([attempt]);
  }

  return runs.slice(0, limit).map((run) => {
    const newest = run[0];
    const oldest = run[run.length - 1];
    const mark = outcomeGlyph(newest.status);
    const attemptId = shortVersion(newest.full_generation_id) ?? newest.full_generation_id;
    return {
      key: newest.full_generation_id,
      attemptId,
      fullId: newest.full_generation_id,
      when: run.length === 1
        ? relativeAge(newest.created_at, nowMs)
        : `${relativeAge(oldest.created_at, nowMs)} to ${relativeAge(newest.created_at, nowMs)}`,
      whenExact: run.length === 1
        ? utcStamp(newest.created_at)
        : `${utcStamp(oldest.created_at)} to ${utcStamp(newest.created_at)}`,
      outcome: mark,
      stoppedOn: stoppedOnPhrase(newest),
      version: newest.status === "published"
        ? shortVersion(newest.full_generation_id)
        : null,
      versionFullId: newest.status === "published" ? newest.full_generation_id : null,
      reading: newest.status === "published"
        && readingVersion !== null
        && newest.full_generation_id === readingVersion,
      change: changeText(newest),
      narrowDetail: narrowDetailFor(newest),
      /** The count a collapsed row stands for, as the expander's label. */
      repeat: run.length > 1 ? `${run.length}\u00d7` : "",
      count: run.length,
      members: run.length === 1 ? [] : run.map((one) => memberOf(one, nowMs)),
      href: link(`updates/${attemptId}`),
    };
  });
}
