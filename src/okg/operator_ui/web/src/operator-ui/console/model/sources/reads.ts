// The raw shapes the Sources view reads.
//
// One read backs this view: `GET /viz/api/console/producers`, which returns
// every producer of the deployment once -- sources, enrichers and views -- in
// one row shape, keyed by the `ownership_id` its registry declares. The three
// diagnostics reads this view used to fold in the browser are gone from here;
// the fold is gone with them, because the read no longer returns a producer
// twice.
//
// Every field below is one the live read records. Fields the mock was drawn
// with that no read supplies -- a source's next run, a source's error line,
// the recorded registry entry -- are absent on purpose and listed as cuts in
// the change's mismatch ledger.
import type { RawAttemptsByDay, RawHealth } from "@operator-ui/console/model/shared/reads";
import type { ProducerKind } from "@operator-ui/console/model/shared/reads";

// --- the producers read ----------------------------------------------------

/** One run, as the read adapts `okg.v_source_run_diagnostics`.
 *
 *  There is no error text: that view records no message column, so a run that
 *  failed is named by its outcome and the phase it stopped in. The read says
 *  so in `error_text_unavailable_reason` rather than offering a null that
 *  would read as "this run recorded no message". */
export interface RawProducerRun {
  iso: string | null;
  outcome: string | null;
  items_in: number | null;
  items_out: number | null;
  duration_ms: number | null;
  /** The SLOWEST phase the run recorded. Served again as `slowest_phase`. */
  phase: string | null;
  slowest_phase?: string | null;
  /** Always null today, with the reason: no read records the phase order. */
  last_phase?: string | null;
  last_phase_unavailable_reason?: string | null;
  /** What the run wrote, split; null where the run row carries no count. */
  records_written?: number | null;
  relationships_written?: number | null;
  run_id?: string | null;
  error_text_unavailable_reason?: string;
}

/** What the convergence read names as the way back for a source. */
export interface RawRecovery {
  /** A command the read recorded (`okg ingest ... --include <source>`). */
  command: string | null;
  command_unavailable_reason?: string | null;
  needs_credential: boolean | null;
  needs_manual_run: boolean | null;
}

/** What the runtime records about a producer's freshness, verbatim.
 *
 *  `status` is always present and is printed by name. On a deployment whose
 *  runtime supplies no watermark it is the literal `not_evaluated`, which the
 *  view prints in words; it is never blanked, dashed, or shown as a colour
 *  alone. */
export interface RawProducerFreshness {
  status: string;
  /** Present only where the read records one (enrichers and views). */
  age?: string | null;
}

/** One producer, as `console/producers` returns it. */
export interface RawProducerRecord {
  name: string;
  /** The registry's declared `ownership_id`: what the runtime attributes work
   *  to, and the key a row is identified by. */
  ownership_id: string | null;
  /** `<kind>:<ownership_id>`, the key the table renders a row by. Served by
   *  the read rather than built here, so the browser and the composer agree on
   *  one producer's identity. Optional because a payload captured before the
   *  read served it still parses; the model composes the same string then. */
  row_key?: string;
  kind: ProducerKind;
  configured: boolean;
  health: { status: string | null; incident_count: number | null };
  workflow: {
    state: string | null;
    status?: string | null;
    active?: boolean | null;
    /** The issue the producer's own read recorded, where one does.
     *
     *  Only the enricher read records one, and what it records is a class or
     *  a fixed phrase -- `failed:<ExceptionClassName>`, the withheld prefix
     *  plus the sources the publisher held on, or "0 edges emitted since
     *  wiring" -- never a message. A source's workflow comes from runtime
     *  readiness `convergence`, which records no policy reason of any kind;
     *  the row carries null and the reason why, in place of the
     *  `withheld_reason` / `excluded_reason` pair that was null on every
     *  source of every deployment. */
    latest_issue: string | null;
    latest_issue_unavailable_reason?: string | null;
    /** The code the convergence read recorded BESIDE the state, for a source.
     *  Not a policy reason -- there is none -- but the other half of the
     *  verdict: `_source_convergence_row` decides `blocked` and
     *  `completed_scope_watermark_stale` together, so a page given the state
     *  alone can say a source is blocked and not say by what. Null with a
     *  reason for an enricher and a view, which have no convergence row. */
    reason?: string | null;
    reason_unavailable_reason?: string | null;
    /** Why the state is what it is, in the read's own words, where the read
     *  composes one. Served by the read layer for an overdue source (sibling
     *  change console-archiv3-read-layer-fixes); preferred over `reason`
     *  when present, and absent on a read that predates it. */
    state_reason?: string | null;
    /** The stage that same row named as the one that has to clear. */
    blocking_stage?: string | null;
    /** A source's recorded way back, where the read serves one. */
    recovery?: RawRecovery | null;
  };
  contribution: {
    records_in_gen: number;
    /** Null for a view, which builds rows and emits no relationships. */
    edges_in_gen: number | null;
    edges_unavailable_reason?: string;
    horizon?: string;
  };
  freshness: RawProducerFreshness;
  latest_run: RawProducerRun | null;
  latest_run_unavailable_reason?: string | null;
  /** Null on a list row, and on a detail whose run query was capped. */
  run_count: number | null;
  run_count_unavailable_reason?: string | null;
}

/** This producer's live records, grouped by record type. */
export interface RawRecordsByType {
  status: string;
  reason?: string;
  types: { type: string; records: number }[];
  /** The sum over every type, not only those listed; null when unavailable. */
  total: number | null;
  omitted: number;
}

/** Why there is no relationships-by-type beside it. */
export interface RawRelationshipsByType {
  status: string;
  reason?: string;
}

/** The detail answer: the same row, plus that producer's run history. */
export interface RawProducerSelected {
  found: boolean;
  reason?: string;
  row?: RawProducerRecord;
  run_history?: RawProducerRun[];
  run_offset?: number;
  run_limit?: number;
  /** Rows this read fetched, which is not the run total when it was capped. */
  run_history_fetched?: number;
  run_history_truncated?: boolean;
  more?: boolean;
  records_by_type?: RawRecordsByType;
  relationships_by_type?: RawRelationshipsByType;
}

/** What one authority behind the read says about itself.
 *
 *  `status` is "available" or it is not. Where it is not, `reason` names why
 *  in the composer's own words and `error_type` names the exception's class.
 *  Neither is typed here, and the read carries no message at all: the console
 *  prints the reason and nothing else. */
export interface RawReadAuthority {
  status?: string;
  reason?: string;
}

/** One authority whose rows are missing from `producers`.
 *
 *  The producers read appends a row here when the enricher or the view
 *  composer fails, so the table it returns is short by every producer of
 *  that kind. The page must say so; a short table with no line reads as a
 *  deployment that configures none. */
export interface RawOmittedData {
  authority?: string;
  /** The producer kind that is missing from the table entirely. Absent when
   *  the failed read cost the rows a FIELD rather than their existence. */
  kind?: string;
  reason?: string;
  /** What a field-level failure cost, where `kind` is absent. */
  affects?: string;
}

export interface RawProducersRead {
  ok?: boolean;
  generation_id?: string | null;
  payload: {
    schema?: string;
    deployment: string;
    generation_id: string | null;
    producer_count: number;
    /** How many published versions the enricher read looked back over.
     *  Optional: a payload captured before the read served it still parses,
     *  and the page then says the window is not recorded rather than
     *  printing a number it invented. */
    enricher_runs_window?: number | null;
    producers: RawProducerRecord[];
    read_authority?: Record<string, unknown>;
    omitted_data?: RawOmittedData[];
    /** Producers whose two recorded counts contradict each other. NOT an
     *  omission: both figures were read in full. Optional, so a payload
     *  captured before the read served the key still parses -- and absent
     *  means "this read is too old to have looked", which is why the page
     *  says nothing rather than "no disagreement". */
    count_disagreements?: RawCountDisagreement[];
    selected_producer?: string | null;
    selected?: RawProducerSelected | null;
  };
}

/**
 * One producer, two recorded counts, and no basis for choosing between them.
 *
 * The rollup's `source` dimension is a subset of its `enricher_node`
 * dimension by the expression that writes them, so the first measuring MORE
 * than the second means the rollup is not what the read believes it is. Both
 * figures are carried, because the page states both.
 */
export interface RawCountDisagreement {
  authority?: string | null;
  reason?: string | null;
  ownership_id?: string | null;
  name?: string | null;
  kind?: string | null;
  affects?: string | null;
  source_dimension_records?: number | null;
  enricher_dimension_records?: number | null;
}

/** What a producer page is built from, once the loader has found it. */
export interface ProducerDetailRead {
  row: RawProducerRecord;
  runs: RawProducerRun[];
  /** How many runs exist, when the read can say; null when its query was
   *  capped and the rows in hand are only a floor. */
  runTotal: number | null;
  /** Why `runTotal` is null, printed as a plain sentence. */
  runTotalUnavailableReason: string | null;
  /** True when older runs exist that this read did not fetch. */
  moreRuns: boolean;
  recordsByType: RawRecordsByType | null;
  relationshipsByType: RawRelationshipsByType | null;
  /** How many published versions the enricher read looked back over, or null
   *  when it did not answer. An enricher's runs come from a WINDOW of
   *  checkpoints, not from its whole history, so a page that prints the count
   *  without the window prints a bounded history as a complete one. */
  enricherRunsWindow: number | null;
}

export interface SourcesInputs {
  /** Null when the health read failed: the page then names that read, and
   *  the deployment and version come from the producers read, which carries
   *  both. */
  health: RawHealth | null;
  producers: RawProducersRead;
  /** Read only for its clock. Null when it failed: the page then times
   *  itself by this browser's clock and says so. */
  attempts: RawAttemptsByDay | null;
}
