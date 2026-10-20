// One attempt's page: what it was, what it changed, and -- when it did not
// publish -- the blocker the run recorded, verbatim.
//
// What this page does NOT show, and why:
//
//   * a duration. No column records a publish's wall clock; the attempts read
//     returns duration_ms null on every attempt of every deployment.
//   * the five stages (Sources, Extraction and enrichers, Publication checks,
//     Views, Publish) with the failure's position. `stopped_stage` is never
//     recorded either. Deriving a stage from the blocker's code would print a
//     position no run measured, and rows reading "Not recorded" state nothing.
//   * per-SOURCE results for this attempt. A source run is recorded against
//     its own run id, not against a generation, so no source run can honestly
//     be attributed to an attempt. The page says that in words rather than
//     leaving a silent gap.
//
// What it shows instead is what IS recorded per attempt: the checkpoint's
// blocked_reason, its excluded_sources, and the view producers' own runs.
import { changePhrase } from "@operator-ui/console/model/shared/change";
import { exact } from "@operator-ui/console/model/shared/format";
import { shortVersion } from "@operator-ui/console/model/shared/ids";
import type { RawAttempt, RawViewRun } from "@operator-ui/console/model/shared/reads";
import { type Glyph, outcomeGlyph } from "@operator-ui/console/model/shared/status";
import { duration, relativeAge, utcStamp } from "@operator-ui/console/model/shared/time";

/** One view producer's run on THIS attempt. */
export interface ViewRunRow {
  key: string;
  name: string;
  mark: Glyph;
  /** "took 1 m 4 s", or "" when the run recorded no finish time. */
  timing: string;
  /** The producer's kind, named rather than assumed: a deployment's only
   *  producer may be a consistency check rather than a view. */
  kind: string;
  /** The producer's own error, as recorded, or null. Rendered as text. */
  errorText: string | null;
  /** Multi-line strings inside that error, shown in full beneath it. */
  errorMultiline: MultilineField[];
}

export interface AttemptDetail {
  attemptId: string;
  fullId: string;
  /** The h1: "Blocked", "Published as eea4e0d4e88d", "Running". */
  headline: string;
  outcome: Glyph;
  /** Started, and the attempt before it. Each entry is a whole phrase. */
  meta: string[];
  /** "+2,116 records · -988 records", or "" when the outcome said it. */
  change: string;
  /** The sources the gate recorded as excluded, whatever the outcome. */
  excludedSources: string[];
  excludedLine: string | null;
  /** The blocker's code. Never on the list; rule 3 allows it here, beside the
   *  verbatim record. */
  code: string | null;
  /** The recorded blocker, as text, exactly as it was written. Null on an
   *  attempt that recorded none. */
  blockedText: string | null;
  blockedLineCount: number;
  /** Multi-line strings inside the record, shown in full beneath it. The JSON
   *  above keeps them escaped; these are the same values, displayed. */
  blockedMultiline: MultilineField[];
  /** Set when the server withheld the record for size: what it was and how
   *  large, so the page states what it is not showing. */
  blockedOversizeLine: string | null;
  viewRuns: ViewRunRow[];
  /** What the view-runs section says when there is no row to show. Two
   *  different facts, never merged: the deployment recorded none for this
   *  attempt, or this server does not serve them at all. */
  viewRunsNote: string | null;
  /** Set when THIS attempt's own run list was capped, in words. */
  viewRunsCapLine: string | null;
  /** True for the second case: the read carried no `view_runs` field, so the
   *  page states that rather than claiming none was recorded. */
  viewRunsUnavailable: boolean;
  /** Said once, on every attempt: source results are not recorded per attempt. */
  sourcesNote: string;
  /** Set only on a published attempt, for "Read this version". */
  publishedVersion: string | null;
  /** True when the console is already reading this attempt's version. */
  reading: boolean;
  /** Only a running attempt has nothing finished to show. */
  runningNote: string | null;
}

const VIEW_RUN_MARKS: Record<string, Glyph> = {
  completed: { icon: "check", word: "Completed", severity: "ok" },
  carried_forward: { icon: "check", word: "Carried forward", severity: "ok" },
  failed: { icon: "close", word: "Failed", severity: "failed" },
  blocked: { icon: "stop", word: "Blocked", severity: "failed" },
  running: { icon: "sync", word: "Running", severity: "running" },
};

function viewRunMark(status: string): Glyph {
  return VIEW_RUN_MARKS[status]
    ?? { icon: "minus", word: status, severity: "never-run" };
}

/**
 * A recorded value as text, unedited.
 *
 * Objects are pretty-printed JSON. Whatever comes back is DATA: the view sets
 * it as escaped text in a `<pre>`, never as markup and never interpreted.
 */
export function verbatim(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value || null;
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

/** One multi-line string inside a recorded value, shown in full. */
export interface MultilineField {
  /** Where it sits in the record: "detail", "gates.0.sample". */
  path: string;
  /** The value with its REAL line breaks, for a block that shows every line. */
  text: string;
  lineCount: number;
}

/**
 * The multi-line strings inside a recorded value.
 *
 * The JSON block above stays the record of truth, unchanged, and it is what
 * the copy button hands over -- so a copied blocker still round-trips. But
 * JSON escapes a newline as `\n`, so a forty-line error recorded inside a
 * string arrives as one very long line, and the pinned reference requires all
 * forty to render. These blocks are a DISPLAY of the same value, labelled
 * with its key path, beside the escaped form rather than instead of it.
 *
 * Nothing is unescaped inside the JSON itself, and every block is set as text
 * by the view, so a value that looks like markup is shown, not mounted.
 */
export function multilineFields(value: unknown, path = ""): MultilineField[] {
  if (typeof value === "string") {
    if (!value.includes("\n")) return [];
    return [{ path: path || "(value)", text: value, lineCount: value.split("\n").length }];
  }
  if (Array.isArray(value)) {
    return value.flatMap(
      (item, index) => multilineFields(item, path ? `${path}.${index}` : String(index)),
    );
  }
  if (value && typeof value === "object") {
    return Object.entries(value as Record<string, unknown>).flatMap(
      ([key, item]) => multilineFields(item, path ? `${path}.${key}` : key),
    );
  }
  return [];
}

/**
 * How long a view producer's run took.
 *
 * Unlike the attempt itself, a view run DOES record both ends, so this is a
 * measured span rather than a guess. A run with no completed_at has not
 * finished and gets no span.
 */
export function viewRunTiming(run: RawViewRun): string {
  if (!run.started_at || !run.completed_at) return "";
  const span = Date.parse(run.completed_at) - Date.parse(run.started_at);
  if (!Number.isFinite(span) || span < 0) return "";
  return `took ${duration(span)}`;
}

export function buildViewRuns(attempt: RawAttempt): ViewRunRow[] {
  return (attempt.view_runs ?? []).map((run) => ({
    key: run.run_id,
    name: run.definition_id,
    mark: viewRunMark(run.status),
    timing: viewRunTiming(run),
    kind: run.artifact_kind ?? "",
    errorText: verbatim(run.error),
    errorMultiline: multilineFields(run.error),
  }));
}

/** What the page says when the server withheld an oversized blocker. */
export function oversizeLine(oversize: unknown): string | null {
  if (!oversize || typeof oversize !== "object") return null;
  const row = oversize as { reason?: unknown; bytes?: unknown; keys?: unknown };
  const keys = Array.isArray(row.keys) ? row.keys.map(String) : [];
  const named = keys.length ? ` Its keys are ${keys.join(", ")}.` : "";
  if (row.reason === "blocked_detail_too_large" && typeof row.bytes === "number") {
    return `The recorded blocker is ${exact(row.bytes)} bytes and is not shown `
      + `here.${named} Read it with \`okg generation describe\`.`;
  }
  return "The recorded blocker could not be read as text and is not shown "
    + `here.${named} Read it with \`okg generation describe\`.`;
}

function headlineFor(attempt: RawAttempt): string {
  if (attempt.status === "published") {
    return `Published as ${shortVersion(attempt.full_generation_id)}`;
  }
  if (attempt.status === "running") return "Running";
  return "Blocked";
}

/**
 * One attempt, as the detail page reads it.
 *
 * `previous` is the attempt before it in time, whatever its outcome: "the
 * version before this one" is the wrong fact when the attempt published
 * nothing, and "the attempt before this one" is the one an operator asks about.
 */
export function buildAttemptDetail(
  attempt: RawAttempt,
  {
    previous, nowMs, readingVersion,
  }: {
    previous: RawAttempt | null;
    nowMs: number;
    readingVersion: string | null;
  },
): AttemptDetail {
  // No duration line: nothing records one. What triggered the run is not in
  // any read either, so it is not stated -- a guessed "schedule" would be the
  // one kind of sentence this console is not allowed to print.
  const meta = [
    `started ${utcStamp(attempt.created_at)}, ${relativeAge(attempt.created_at, nowMs)}`,
    previous
      ? `previous attempt: ${outcomeGlyph(previous.status).word.toLowerCase()}, `
        + `${relativeAge(previous.created_at, nowMs)}`
      : "no earlier attempt is recorded",
  ];

  const blockedText = verbatim(attempt.blocked_detail);
  const excluded = attempt.excluded_sources ?? [];
  const viewRuns = buildViewRuns(attempt);
  const running = attempt.status === "running";
  // Absent is not empty. A payload with `view_runs: []` says the deployment
  // recorded no producer result for this attempt; a payload with no
  // `view_runs` key at all says this server does not serve them, and
  // printing "none recorded" for it would be a claim nobody made.
  const runsUnavailable = attempt.view_runs === undefined;

  return {
    attemptId: shortVersion(attempt.full_generation_id) ?? attempt.full_generation_id,
    fullId: attempt.full_generation_id,
    headline: headlineFor(attempt),
    outcome: outcomeGlyph(attempt.status),
    meta,
    change: changePhrase(attempt),
    excludedSources: excluded,
    // The names, not just how many: "1 source excluded" that never says which
    // is a number an operator cannot act on. Singular only for exactly one --
    // the record can name several and does not single one out.
    excludedLine: excluded.length
      ? `${excluded.length === 1 ? "Source" : "Sources"} excluded by the `
        + `publication gate: ${excluded.join(", ")}`
      : null,
    code: attempt.blocked_reason ?? null,
    blockedText,
    blockedLineCount: blockedText ? blockedText.split("\n").length : 0,
    blockedMultiline: multilineFields(attempt.blocked_detail),
    blockedOversizeLine: oversizeLine(attempt.blocked_detail_oversize),
    viewRuns,
    // Empty is not zero. The deployment recorded no per-producer result for
    // this attempt; it did not measure zero producers.
    viewRunsNote: running || viewRuns.length > 0
      ? null
      : runsUnavailable
        ? "This deployment's attempts read did not return per-producer "
          + "results (no `view_runs` field). Whether any were recorded for "
          + "this attempt is unknown."
        // The read looked up nothing, because this attempt records no target
        // generation to join the runs table on. Its own code is printed,
        // because that is what an operator greps the read API's log for.
        : attempt.view_runs_unavailable
          ? "The producer runs for this attempt could not be looked up: "
            + `${attempt.view_runs_unavailable.reason}.`
          : "No per-producer result is recorded for this attempt.",
    viewRunsUnavailable: runsUnavailable && !running,
    // The cap is this attempt's own, so the sentence can state both numbers.
    viewRunsCapLine: attempt.view_runs_capped
      ? `Showing ${viewRuns.length} of ${attempt.view_runs_total ?? viewRuns.length} `
        + "recorded producer runs for this attempt."
      : null,
    // The same fact as before, without the plumbing. What the reader has to
    // take away is that this page will not show them a per-source result and
    // that nothing is hidden by its absence; the run id and the generation
    // were the mechanism, and a newcomer could not parse the sentence.
    sourcesNote:
      "Source runs are recorded on their own, not against this attempt, "
      + "so no per-source result is shown here.",
    publishedVersion: attempt.status === "published" ? attempt.full_generation_id : null,
    reading: readingVersion !== null && attempt.full_generation_id === readingVersion,
    runningNote: running
      ? "This attempt has not finished. It records no progress while it runs."
      : null,
  };
}

/** Which attempt an id in the hash names, if any. */
export type AttemptMatch =
  | { kind: "one"; attempt: RawAttempt; index: number }
  | { kind: "many"; matches: RawAttempt[] }
  | { kind: "none" };

/**
 * The attempt an id in the hash names.
 *
 * A SUFFIX match, so more than one attempt can answer to it. Taking the first
 * would open somebody else's attempt under the id the reader typed and say
 * nothing about it; two matches is a question, not a page. Shared by the
 * route -- which needs the FULL id to read the attempt's recorded detail --
 * and by the state component, so the two cannot resolve the same hash to
 * different attempts.
 */
export function resolveAttempt(all: RawAttempt[], attemptId: string): AttemptMatch {
  const matches = all.filter(
    (attempt) => attempt.full_generation_id.endsWith(attemptId),
  );
  if (matches.length > 1) return { kind: "many", matches };
  if (matches.length === 1) {
    return { kind: "one", attempt: matches[0], index: all.indexOf(matches[0]) };
  }
  return { kind: "none" };
}

/**
 * The newest finished attempt, when it is blocked, with the attempt before it.
 *
 * A running attempt is skipped: it has not finished, so it is not the answer
 * to "did the last update land". Null when the newest finished attempt
 * published, or when nothing has finished at all.
 */
export function newestBlockedAttempt(
  attempts: RawAttempt[],
): { attempt: RawAttempt; previous: RawAttempt | null } | null {
  const index = attempts.findIndex((attempt) => attempt.status !== "running");
  if (index === -1) return null;
  const attempt = attempts[index];
  if (attempt.status !== "blocked") return null;
  return { attempt, previous: attempts[index + 1] ?? null };
}
