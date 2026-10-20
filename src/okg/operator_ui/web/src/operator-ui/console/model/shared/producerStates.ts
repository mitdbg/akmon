// The producer-state vocabulary: the words three console reads record, the
// word and glyph each is printed as, and the URL form of each.
//
// WHY THIS FILE EXISTS. The vocabulary was written on the Sources branch
// (`claude/console-live-sources`, commit ded597f6, in
// `console/model/sources/producers.ts`) after a live read showed that the
// console matched a state list no runtime emits. Home reads the same three
// diagnostics routes and needs the same words, so the words live here -- in a
// module neither view owns -- rather than being copied into Home.
//
// The Sources branch's copy is gone: merging the two branches deleted it and
// pointed `sources/producers.ts`, `sources/rows.ts`, `sources/list.ts`,
// `sources/detail.ts` and the Sources view here, so there is one table and it
// cannot diverge. `producerStates.test.ts` holds this file to the live
// payloads that change captured, so a state a real deployment records cannot
// silently fall through again.
import type { Producer } from "@operator-ui/console/model/shared/reads";
import type { Glyph } from "@operator-ui/console/model/shared/status";

/** What a row's state reads as when the read recorded none.
 *
 *  Not "never run", which is a fact about the producer, and not "updated",
 *  which is a claim. The read did not say, and the row says that.
 *
 *  It is not hypothetical: on `lane-d-home-render` every one of the four
 *  sources comes back with `workflow.state: null`, because that deployment
 *  has no runtime to observe. */
export const UNDETERMINED_STATE = "not recorded";

/**
 * The states a console read RECORDS for a producer, most urgent first.
 *
 * Three reads fill this one column, each with its own recorded vocabulary,
 * and this list is the union of the three plus the design harness's own:
 *
 *   sources    `console/source-diagnostics` workflow state, which is the
 *              source-sync convergence state (`source_sync_convergence.py`):
 *              fresh, converging, stuck, blocked, manual_required, unsupported
 *   enrichers  `console/enricher-diagnostics` health status:
 *              healthy, failing, withheld, wired-but-silent, unknown
 *   views      `console/view-diagnostics` status: healthy, failing, unknown
 *   fixtures   the design harness's generated payloads, which predate the
 *              live reads: updated, failed, running, never run, excluded
 *
 * The order is the order the state chips, the state bar and the count strip
 * are drawn in.
 */
export const STATES = [
  "failed", "failing", "blocked", "stuck", "unsupported", "manual_required", "overdue",
  "withheld", "excluded", "wired-but-silent", "never run",
  "converging", "running", "unknown", "fresh", "healthy", "updated",
  // Last, because it is the least urgent thing a row can say and because it
  // is not a state a RUNTIME recorded. It is in the list all the same: the
  // count strip, the state bar and the state filter are all built by walking
  // STATES, so leaving it out made a deployment whose reads record no state
  // at all -- every source of `lane-d-home-render` -- draw no count strip,
  // no state bar and no way to filter by the one state its rows are in.
  UNDETERMINED_STATE,
] as const;
export type ProducerState = (typeof STATES)[number];

/** A recorded code as a person reads it: `wired-but-silent`, `not_evaluated`. */
export function words(value: string): string {
  return value.replace(/_/g, " ");
}

/** The state as a URL value: a recorded code can hold a space or an
 *  underscore, and a query parameter holds neither comfortably. */
export function stateParam(state: string): string {
  return state.replace(/[ _]/g, "-");
}

export function stateFromParam(value: string | undefined): ProducerState | null {
  if (!value) return null;
  return STATES.find((state) => stateParam(state) === value) ?? null;
}

/**
 * The state a row that CARRIES A WORKFLOW BLOCK records.
 *
 * That is the source diagnostics read's row shape and, for all three kinds,
 * the `console/producers` read's: its composer folds the enricher's and the
 * view's `status` into a `workflow.state` of its own before it serves them
 * (`console_views/producers.py:194`, `:257`, `:320`). The Sources view reads
 * that route, which is why Sources survived a deployment Home crashed on.
 */
export function stateOf(row: {
  workflow: { state?: string | null; reason?: string | null };
  health?: { status?: string | null } | null;
}): string {
  const state = row.workflow.state ?? UNDETERMINED_STATE;
  if (state === "blocked" && OVERDUE_REASONS.has(row.workflow.reason ?? "")) return "overdue";
  return backedByHealth(state, row.health?.status);
}

/**
 * A GOOD state is printed only where the health read backs it (issue #2205).
 *
 * A source's state is its readiness convergence state, and convergence
 * records `fresh` for every source the registry does not require for
 * readiness (`source_not_required_for_readiness`) -- with no run, no
 * watermark and no cadence behind it. The producers read's `health.status`
 * is judged from what the runtime recorded, and says `unknown` exactly when
 * nothing was. Where the two meet, a check-mark "Fresh" would claim what no
 * read recorded, so the row reads "Unknown to the read" instead.
 *
 * Only an ok-severity state is replaced. A recorded problem (`blocked`,
 * `unsupported`, ...) or a recorded absence is printed as recorded whatever
 * the health read says: `unknown` never hides a state that asks for action.
 */
function backedByHealth(state: string, health: string | null | undefined): string {
  return health === "unknown" && producerGlyph(state).severity === "ok" ? "unknown" : state;
}

/**
 * The convergence reasons that make a `blocked` source OVERDUE.
 *
 * `_source_convergence_row` records `blocked` for a source whose last
 * completed run is older than its freshness window. Archiv3's cmssw_releases
 * and docsite each had three completed runs and nothing wrong with them but
 * their age, and Home called them "2 sources are blocked" (review
 * 2026-09-22): a reader went looking for what was blocking them. Nothing is.
 * They are overdue, and the word says so. A source blocked for any OTHER
 * recorded reason keeps the word Blocked. A read that serves `overdue` as the
 * state itself reads the same.
 */
export const OVERDUE_REASONS: ReadonlySet<string> = new Set([
  "completed_scope_watermark_stale",
  "source_progress_freshness_stale",
]);

/**
 * The state a producer records, read from the field ITS OWN read fills.
 *
 * Home reads the three diagnostics routes directly, and they do not agree on
 * a shape. A source's state is in `workflow.state`; an enricher's and a
 * view's is in `status`, and neither row has a `workflow` key to read at all.
 * One accessor that assumed the first drew a blank page on okg-workspace,
 * whose eighteen enrichers and eight views are the first live rows of those
 * two kinds this console has met.
 *
 * Nothing is defaulted where a state WAS recorded. `UNDETERMINED_STATE` is
 * reached only when the read carried no state field at all, which is the same
 * absence `stateOf` names for a null `workflow.state`.
 */
export function producerState(producer: Producer): string {
  switch (producer.kind) {
    case "source":
      return backedByHealth(
        producer.workflow.state ?? UNDETERMINED_STATE, producer.health?.status,
      );
    // `status`, for both, and for the view deliberately NOT its `state`: that
    // field says `registered` or `materialized`, which is what the registry
    // holds rather than what a run recorded.
    //
    // The fall-through to `workflow.state` is NOT a default. It is the
    // DESIGN HARNESS's row: `scripts/operator-ui-design/lib/reads.mjs` builds
    // one row shape for all three kinds and records the state there, in the
    // fixtures' own vocabulary. Both are states something recorded, so both
    // are read, each named. Only a row that carries NEITHER field reaches
    // `UNDETERMINED_STATE`.
    case "enricher":
    case "view":
      return producer.status ?? producer.workflow?.state ?? UNDETERMINED_STATE;
  }
}

/**
 * One word and one glyph per recorded state.
 *
 * Two rules hold this table together. A state is printed as a WORD, never as
 * the code the read served -- the code stays on the producer page and in the
 * copy text, where a reader who needs to grep for it can find it. And one
 * glyph means one thing: `minus` is reserved for "no read recorded a state"
 * and is not the table's fallback, because a fallback glyph turns every
 * unmapped state into an absence.
 */
export const PRODUCER_GLYPHS: Record<string, Glyph> = {
  // Sources: the source-sync convergence state.
  fresh: { icon: "check", word: "Fresh", severity: "ok" },
  converging: { icon: "sync", word: "Converging", severity: "running" },
  stuck: { icon: "warning", word: "Stuck", severity: "never-run" },
  blocked: { icon: "stop", word: "Blocked", severity: "blocked" },
  // Not "Manual required", and not "Needs authorization" either: Archiv3's
  // jira had three completed runs under that word, which read as a source
  // that had never been allowed to run. What the runtime recorded is
  // `manual_or_credential_constraint` -- the source refreshes when a person
  // runs it or supplies a credential. The code is beside it on the producer
  // page.
  manual_required: { icon: "warning", word: "Needs a manual run", severity: "never-run" },
  // A source with no run inside its freshness window. See OVERDUE_REASONS.
  overdue: { icon: "warning", word: "Overdue", severity: "never-run" },
  unsupported: { icon: "stop", word: "Not supported", severity: "blocked" },
  // Enrichers and views.
  healthy: { icon: "check", word: "Healthy", severity: "ok" },
  failing: { icon: "close", word: "Failing", severity: "failed" },
  "wired-but-silent": { icon: "warning", word: "Wired, silent", severity: "never-run" },
  // `unknown` is the enricher and view reads' own word for "this read found
  // no status to classify", which is the same absence `not recorded` names
  // and is drawn the same way, under its own word.
  unknown: { icon: "minus", word: "Unknown to the read", severity: "unrecorded" },
  // The design harness's fixtures.
  updated: { icon: "check", word: "Updated", severity: "ok" },
  failed: { icon: "close", word: "Failed", severity: "failed" },
  running: { icon: "sync", word: "Running", severity: "running" },
  "never run": { icon: "minus", word: "Never run", severity: "never-run" },
  excluded: { icon: "stop", word: "Excluded", severity: "never-run" },
  withheld: { icon: "stop", word: "Withheld", severity: "never-run" },
  // Muted, not amber: no read recorded a state, which is a fact about the
  // deployment's runtime rather than a problem with this producer.
  [UNDETERMINED_STATE]: {
    icon: "minus", word: "State not recorded", severity: "unrecorded",
  },
};

/**
 * A state no read this console knows about IS still a recorded state.
 *
 * So it is drawn as something to look at -- a warning, with the code in
 * words -- and not as the absence `minus` names.
 */
export function producerGlyph(state: string): Glyph {
  return PRODUCER_GLYPHS[state]
    ?? { icon: "warning", word: words(state), severity: "never-run" };
}

/**
 * The runtime's freshness state, as a person reads it.
 *
 * `not_configured` is the one that has to be translated rather than spelled
 * out: the readiness read returns it for a source no freshness policy names,
 * and printing it as "not configured" beside a state of "Fresh" read as a
 * contradiction on cern-r3final. It is not one. Nothing declares a cadence
 * for those sources, which is also why their convergence state is `fresh`.
 */
const FRESHNESS_WORDS: Record<string, string> = {
  fresh: "fresh",
  stale: "stale",
  missing: "no watermark recorded",
  not_evaluated: "not evaluated",
  // "No schedule", not "no cadence": the word an operator uses for it.
  not_configured: "no schedule",
  last_run_age_only: "no watermark read",
};

/** Never blank while the read carried a status. */
export function freshnessWords(status: string | null | undefined): string {
  if (!status) return "";
  return FRESHNESS_WORDS[status] ?? words(status);
}

/** The severities that report something to act on. `unrecorded` is not one. */
export const ATTENTION_SEVERITIES = new Set(["failed", "blocked", "never-run"]);

/**
 * The recorded states Home lists under "Needs attention", worst first.
 *
 * Every one of them is a producer that is NOT going to update itself: it
 * failed, something is holding it off, or it is waiting on a person. A
 * producer that is merely converging or running is doing what it should and
 * is counted in the state bar, not listed as a problem.
 *
 * `withheld` and `excluded` are deliberately absent: both are states an
 * operator's own configuration put the producer in, so listing them as
 * something to attend to reports a decision back as a problem.
 */
export const ATTENTION_STATES: readonly string[] = [
  "failed", "failing", "blocked", "stuck", "unsupported", "manual_required",
  "overdue", "wired-but-silent", "never run",
];

/** Where a state sorts in Needs attention and in the default producer rank.
 *  Spaced by ten so a new live state can be slotted in without moving one. */
export const STATE_RANK: Record<string, number> = {
  failed: 0, failing: 0,
  blocked: 5, stuck: 5, unsupported: 5,
  manual_required: 8,
  overdue: 9,
  "never run": 10,
  "wired-but-silent": 15,
  running: 20, converging: 20,
};

export function stateRank(state: string): number {
  return STATE_RANK[state] ?? 30;
}
