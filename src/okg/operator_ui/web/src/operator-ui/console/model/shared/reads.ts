// The raw console read payloads, only the fields the views read. Shared by the
// Home, Updates and Sources models and the API loaders.

// --- raw payload shapes (only the fields Home reads) ----------------------

export interface RawCheck {
  severity: string;
  code: string;
  scope: string;
  summary: string;
  actual?: string | null;
  /** What the check required. Never printed on Home: it is the other half of
   *  the measurement and carries the same object names. */
  expected?: string | null;
  /** What to do about it, in the composer's own words. Printed on Home only
   *  where it reads as a sentence rather than as a command line. */
  fix_hint?: string | null;
}

export interface RawHealth {
  payload: {
    deployment: string;
    generation_id: string | null;
    generation_status: string;
    generation_age: string | null;
    /** `healthy`, `degraded` or `failing`: the read's own summary of its
     *  checks, over every scope. Required — the composer derives it from the
     *  checks on every answer (`_verdict` in console_views/health.py), and the
     *  header is not allowed to state a state this field contradicts. */
    verdict: string;
    vitals: {
      queue_total: number;
      dlq_count: number;
      nodes_live: number;
      edges_live: number;
    };
    checks: RawCheck[];
    /** The per-source breakdown's basis, where the read names one. Only
     *  `basis` is read here: it says whether the verdict counts a source on
     *  the same recorded basis the producers read states (sibling change
     *  console-archiv3-read-layer-fixes). Absent on a read that predates it. */
    source_breakdown?: { basis?: string | null; status_basis?: string | null } | null;
  };
}

/** One view producer's recorded run, for the attempt it names. */
export interface RawViewRun {
  definition_id: string;
  /** views, consistency_check, lens ... named, never assumed. */
  artifact_kind?: string;
  run_id: string;
  status: string;
  trigger_kind: string;
  /** The producer's own error, as it was recorded. Printed verbatim. */
  error: unknown;
  started_at: string | null;
  completed_at: string | null;
}

export interface RawAttempt {
  generation_id: number | string;
  full_generation_id: string;
  status: string;
  created_at: string;
  duration_ms: number | null;
  nodes_added: number | null;
  edges_added: number | null;
  stopped_stage: string | null;
  stopped_producer: string | null;
  /** What the attempt REMOVED. Required, not optional: a published attempt
   *  that retracted 988 records and added none is not "no change", and a
   *  parser that reads only the added pair prints exactly that. The live
   *  deployment holds such attempts. */
  nodes_retracted: number | null;
  edges_retracted: number | null;
  /** False when the read could not compute this attempt's delta at all, with
   *  the read's own reason beside it. The four counts are null in that case
   *  and the view prints the reason rather than a number. */
  delta_available?: boolean;
  delta_unavailable_reason?: string | null;
  /** The gate's exclusions, as the list the checkpoint recorded, on every
   *  attempt whatever its outcome. Never collapsed to one name: the record
   *  does not single one out. */
  excluded_sources?: string[];
  /** The recorded blocker itself, unflattened, for the verbatim block.
   *  `blocked_reason` below stays the one-line summary a list can hold. */
  blocked_detail?: unknown;
  /** Present only when the server withheld the record for size, with its
   *  byte count and top-level keys. The page states what it is not showing
   *  rather than rendering an empty block. */
  blocked_detail_oversize?: unknown;
  /** One row per VIEW producer recorded against this attempt's own target
   *  generation. Empty means the deployment recorded none for this attempt --
   *  which is a different statement from "zero producers ran". */
  view_runs?: RawViewRun[];
  /** How many runs this attempt actually has, against how many were served. */
  view_runs_total?: number;
  /** True when THIS attempt's own list was capped. */
  view_runs_capped?: boolean;
  /** Set when the read could not join this attempt to the runs table at all
   *  -- the attempt records no target generation to join on. "Could not
   *  look" is a different answer from "looked and found none", and an empty
   *  list would read as the second. */
  view_runs_unavailable?: { reason: string; [key: string]: unknown } | null;
  /** The composer's own code for the failure. A code is not shown on a list
   *  (rule 3); the attempt detail is where it may appear, in monospace. */
  blocked_reason?: string | null;
  /** The failure as the run recorded it, unedited, newline-separated. Rule 3
   *  prints it verbatim one level down; null on an attempt that did not fail. */
  error_text?: string | null;
}

export interface RawGenerations {
  payload: {
    generations: RawAttempt[];
    total: number;
    /** How many rows this read returned. `total` is how many exist. */
    shown?: number;
    /** The cursor for the next page, when the read is capped and there are
     *  more (requirements v2 section 10, gap 5). Null when there are not. */
    next_cursor?: string | null;
  };
}

export interface RawVersions {
  payload: {
    versions: { generation_id: string; published_at: string }[];
    latest: string | null;
  };
}

export interface RawBucket {
  day: string;
  published: number;
  blocked: number;
  running: number;
}

export interface RawAttemptsByDay {
  payload: {
    range: string;
    now: string;
    buckets: RawBucket[];
    totals: { published: number; blocked: number; running: number };
    /** Attempts the buckets do NOT account for, and the reason each is left
     *  out. REQUIRED, not optional: a chart drawn from `totals` alone on a
     *  deployment whose attempts carry some other status would show zeros
     *  with nothing saying how many were dropped. The chart states both
     *  whenever either is non-zero. */
    other_status_count: number;
    undated_count: number;
  };
}

/**
 * One producer, as the three diagnostics reads return it.
 *
 * Almost every field is OPTIONAL, and that is a measurement rather than
 * caution. The design harness's fixtures, written before the live reads, fill
 * `freshness.last_run`, `freshness.next_due` and `health.detail`; the live
 * `console/source-diagnostics` read fills none of the three and returns
 * `freshness.status` instead, which no fixture has. `workflow.state` is a
 * string on Archiv3 (`fresh`, `blocked`, `manual_required`) and `null` on
 * every source of `lane-d-home-render`, whose runtime records nothing.
 *
 * A required field the live read does not send is a lie the type system
 * cannot catch, so each is declared for what it is and every reader states
 * what it prints when the field is absent.
 */
export interface RawProducerRow {
  name: string;
  health: {
    status: string;
    /** The run's own error text. Fixtures record it; the live source read
     *  records an incident COUNT instead and no text at all. */
    detail?: string | null;
    incident_count?: number;
  };
  freshness: {
    /** The live read's freshness verdict: `fresh`, `stale`, `missing`,
     *  `not_evaluated`, `not_configured`. Absent on the fixtures. */
    status?: string | null;
    last_run?: string | null;
    age?: string | null;
    next_due?: string | null;
  };
  workflow: {
    /** Null on a deployment whose runtime observed nothing. Never defaulted
     *  to a state word: "not recorded" and "updated" ask the reader to do
     *  different things. */
    state: string | null;
    withheld_reason?: string | null;
    excluded_reason?: string | null;
  };
  contribution: { records_in_gen: number; edges_in_gen: number };
}

/**
 * One enricher, as `console/enricher-diagnostics` returns it.
 *
 * THERE IS NO `workflow` BLOCK, on any row, on any deployment. That is not a
 * field this read sometimes omits: the composer never writes one. The state
 * it records is `status`, and its vocabulary is the five words
 * `_health_status` returns in
 * `src/okg/substrate/console_views/enricher_diagnostics.py` -- `withheld`,
 * `failing`, `wired-but-silent`, `healthy`, `unknown`.
 *
 * The Sources view never met this shape because it reads a different route.
 * `console/producers` composes all three kinds server-side and copies this
 * same `status` into a `workflow.state` of its own
 * (`console_views/producers.py:257`).
 */
export interface RawEnricherRow {
  name: string;
  producer_key?: string;
  /** The recorded state. Optional only because a read too old to carry it
   *  must read as "none recorded" rather than as a crash. */
  status?: string | null;
  /** Already in words ("8 Sep 2026"), not a timestamp. Printed as it stands;
   *  it cannot go through a relative-age formatter. */
  last_run_age?: string | null;
  /** What the read recorded about this enricher, in the composer's words
   *  ("0 edges emitted since wiring"). This is the enricher's equivalent of a
   *  source's `health.detail`, and it is what Needs attention prints. */
  latest_issue?: string | null;
  next_check?: string | null;
  contribution?: {
    records_in_gen?: number | null;
    edges_in_gen?: number | null;
    [key: string]: unknown;
  };
  /**
   * The DESIGN HARNESS's own row shape, and only ever that.
   *
   * No live `console/{kind}-diagnostics` read carries a `workflow` block --
   * that is the whole reason this interface exists. But
   * `scripts/operator-ui-design/lib/reads.mjs` builds one `diagnosticsRow` for
   * all three kinds and gives every one of them a `workflow`, in the
   * fixtures' own state vocabulary (updated, failed, running, never run,
   * excluded). So it is declared here as what it is: optional, absent on
   * every deployment, and the only place a generated fixture records its
   * state. It was REQUIRED on all three kinds before this change, and that is
   * what made the first live enricher row throw.
   */
  workflow?: { state: string | null; [key: string]: unknown };
  /** The same harness row's health block. Absent on every live read of this
   *  kind, which records its issue in `latest_issue` instead. */
  health?: { status: string; detail?: string | null; [key: string]: unknown };
  /** The same harness row's freshness block. Absent on every live read of
   *  this kind, which records no watermark, cadence or verdict at all. */
  freshness?: {
    status?: string | null;
    last_run?: string | null;
    age?: string | null;
    next_due?: string | null;
  };
}

/**
 * One view, as `console/view-diagnostics` returns it. Again no `workflow`.
 *
 * This read carries TWO words, and only one of them is a state:
 *
 *   `status`  `healthy`, `failing` or `unknown`, from the latest recorded
 *             materialization run (`console_views/view_diagnostics.py`,
 *             `_status`). This is the state.
 *   `state`   `materialized` or `registered` -- whether the registry entry
 *             has any run at all (`view_diagnostics.py:134`). An inventory
 *             fact about the registry, not something a run recorded, and NOT
 *             what this console's state column reads. The server-side
 *             producers composer makes the same choice
 *             (`console_views/producers.py:320` reads `status`).
 */
export interface RawViewRow {
  name: string;
  display_name?: string | null;
  producer_key?: string;
  status?: string | null;
  /** `materialized` or `registered`. See above: not the state column's field. */
  state?: string | null;
  last_run_age?: string | null;
  latest_issue?: string | null;
  next_check?: string | null;
  contribution?: {
    records_in_gen?: number | null;
    edges_in_gen?: number | null;
    [key: string]: unknown;
  };
  /**
   * The DESIGN HARNESS's own row shape, and only ever that.
   *
   * No live `console/{kind}-diagnostics` read carries a `workflow` block --
   * that is the whole reason this interface exists. But
   * `scripts/operator-ui-design/lib/reads.mjs` builds one `diagnosticsRow` for
   * all three kinds and gives every one of them a `workflow`, in the
   * fixtures' own state vocabulary (updated, failed, running, never run,
   * excluded). So it is declared here as what it is: optional, absent on
   * every deployment, and the only place a generated fixture records its
   * state. It was REQUIRED on all three kinds before this change, and that is
   * what made the first live enricher row throw.
   */
  workflow?: { state: string | null; [key: string]: unknown };
  /** The same harness row's health block. Absent on every live read of this
   *  kind, which records its issue in `latest_issue` instead. */
  health?: { status: string; detail?: string | null; [key: string]: unknown };
  /** The same harness row's freshness block. Absent on every live read of
   *  this kind, which records no watermark, cadence or verdict at all. */
  freshness?: {
    status?: string | null;
    last_run?: string | null;
    age?: string | null;
    next_due?: string | null;
  };
}

export interface RawDiagnostics<K extends string, R = RawProducerRow> {
  payload: Record<K, R[]> & Record<string, unknown>;
}

export type RawSourceDiagnostics = RawDiagnostics<"sources">;
export type RawEnricherDiagnostics = RawDiagnostics<"enrichers", RawEnricherRow>;
export type RawViewDiagnostics = RawDiagnostics<"views", RawViewRow>;

export interface RawRollup {
  payload: {
    node_types: { type: string; count: number }[];
    relationship_types: { type: string; count: number }[];
    node_type_total: number;
    relationship_type_total: number;
    count_basis: string;
  };
}

export type ProducerKind = "source" | "enricher" | "view";

/**
 * One producer of any kind, as the kind's OWN read returns it.
 *
 * A union rather than one interface, because the three reads do not return
 * one shape and pretending they do is what made Home render blank on
 * okg-workspace: `Producer extends RawProducerRow` promised a `workflow`
 * block on all three, `stateOf` read `row.workflow.state`, and the first of
 * the deployment's eighteen enricher rows threw
 * `Cannot read properties of undefined (reading 'state')`.
 *
 * Nothing reads a field off a `Producer` directly any more. The accessors
 * below name, per kind, the field that kind's read actually fills.
 */
export type Producer =
  | (RawProducerRow & { kind: "source" })
  | (RawEnricherRow & { kind: "enricher" })
  | (RawViewRow & { kind: "view" });

/**
 * The freshness block, which only the SOURCE read composes.
 *
 * An empty block for the other two kinds, and that is the honest answer
 * rather than a substituted one: the enricher and view reads carry no
 * watermark, no cadence and no freshness verdict at all, so every field is
 * absent, and each caller already states what it prints for an absent field.
 */
export function producerFreshness(producer: Producer): RawProducerRow["freshness"] {
  if (producer.kind === "source") return producer.freshness;
  // A generated fixture's enricher or view row carries one; no live read of
  // either kind does, and an empty block is the honest answer for those --
  // every field absent, which each caller already states what it prints for.
  return producer.freshness ?? {};
}

/** The read's own health word for this producer: a source's
 *  `health.status`, an enricher's or a view's `status`. */
export function producerHealthStatus(producer: Producer): string {
  if (producer.kind === "source") return producer.health.status;
  return producer.status ?? producer.health?.status ?? "";
}

/**
 * What the read recorded about why this producer is where it is, verbatim.
 *
 * A source's read records the run's own error text in `health.detail`; the
 * enricher and view reads record a composed sentence in `latest_issue`
 * ("0 edges emitted since wiring"). Null where the read recorded neither --
 * which is a different statement from "the run recorded no error text", and
 * the callers say which.
 */
export function producerIssue(producer: Producer): string | null {
  if (producer.kind === "source") return producer.health.detail ?? null;
  return producer.latest_issue ?? producer.health?.detail ?? null;
}

/**
 * When the producer last ran, in the form its own read recorded.
 *
 * A source's read gives an ISO timestamp, which a relative age can be
 * computed from. The enricher and view reads give an age ALREADY IN WORDS
 * ("8 Sep 2026") and no timestamp, so it is printed as it stands. The two are
 * kept apart rather than squeezed into one string, because a caller that
 * hands the second to a date parser gets `NaN` and prints it.
 */
export function producerLastRun(
  producer: Producer,
): { iso: string | null; words: string } {
  if (producer.kind === "source") {
    return { iso: producer.freshness.last_run ?? null, words: "" };
  }
  const harness = producer.freshness?.last_run ?? null;
  if (harness) return { iso: harness, words: "" };
  return { iso: null, words: producer.last_run_age ?? "" };
}
