// `lane-d-home-render`'s producers read, composed from its own captures.
//
// WHY THIS EXISTS, and what it is careful not to be. That deployment was
// disposable and is gone, so a `console/producers` capture of it cannot be
// taken -- and it is the only live payload this repository holds in which
// EVERY producer's `workflow.state` is null, which is the state Home draws as
// "State not recorded" and must never draw as a good one.
//
// So its `source-diagnostics` capture is put through the server composer's own
// field map, one field at a time, and nothing else. `_source_row` in
// `src/okg/substrate/console_views/producers.py:166` is that map, and its
// docstring lists it:
//
//     name          <- the registry document key
//     ownership_id  <- the row's `source_key`
//     configured    <- `in_registry`
//     health        <- source-diagnostics `health`
//     workflow      <- runtime readiness `convergence`, with `latest_issue`
//                      null and the reason why
//     contribution  <- the count rollup's records/edges in this version
//     freshness     <- runtime readiness `source_freshness`, verbatim
//     latest_run    <- the newest run-diagnostics row
//
// No value here is invented and none is defaulted: every field is copied from
// the capture, and the two keys the composer hard-nulls are hard-nulled.
import type {
  RawProducerRecord, RawProducersRead,
} from "@operator-ui/console/model/sources/reads";
import type { RawDiagnostics } from "@operator-ui/console/model/shared/reads";

type SourceRow = RawDiagnostics<"sources">["payload"]["sources"][number] & {
  source_key?: string;
  in_registry?: boolean;
  latest_run?: Record<string, unknown> | null;
  workflow: { state: string | null; status?: string | null; active?: boolean | null;
    reason?: string | null; blocking_stage?: string | null };
};

function run(latest: Record<string, unknown> | null | undefined) {
  if (!latest || latest.status !== "available") return null;
  return {
    iso: (latest.completed_at ?? latest.started_at ?? null) as string | null,
    outcome: (latest.outcome ?? null) as string | null,
    items_in: (latest.items_in ?? null) as number | null,
    items_out: (latest.items_out ?? null) as number | null,
    duration_ms: (latest.duration_ms ?? null) as number | null,
    phase: (latest.phase ?? null) as string | null,
    run_id: (latest.run_id ?? null) as string | null,
    error_text_unavailable_reason: "no_message_column_in_run_diagnostics",
  };
}

export function laneProducersRead(
  diagnostics: RawDiagnostics<"sources">,
): RawProducersRead {
  const rows = (diagnostics.payload.sources ?? []) as SourceRow[];
  const producers: RawProducerRecord[] = rows.map((row) => {
    const latest = row.latest_run;
    const key = row.source_key ?? row.name;
    return {
      name: row.name,
      ownership_id: key,
      kind: "source" as const,
      row_key: `source:${key}`,
      configured: Boolean(row.in_registry),
      health: {
        status: row.health.status,
        incident_count: row.health.incident_count ?? null,
      },
      workflow: {
        state: row.workflow.state,
        status: row.workflow.status ?? null,
        active: row.workflow.active ?? null,
        latest_issue: null,
        latest_issue_unavailable_reason:
          "no_read_records_a_policy_reason_for_a_source",
        reason: row.workflow.reason ?? null,
        reason_unavailable_reason: row.workflow.reason
          ? null
          : "convergence_recorded_no_reason_for_this_source",
        blocking_stage: row.workflow.blocking_stage ?? null,
      },
      contribution: {
        records_in_gen: row.contribution.records_in_gen,
        edges_in_gen: row.contribution.edges_in_gen,
        horizon: "pinned_generation",
      },
      freshness: { status: row.freshness.status ?? "not_evaluated" },
      latest_run: run(latest),
      latest_run_unavailable_reason: run(latest)
        ? null
        : ((latest?.unavailable_reason ?? null) as string | null),
      run_count: null,
      run_count_unavailable_reason: "run_count_is_a_detail_read",
    };
  });
  return {
    payload: {
      deployment: String(diagnostics.payload.deployment ?? ""),
      generation_id: (diagnostics.payload.generation_id ?? null) as string | null,
      producer_count: producers.length,
      producers,
    },
  };
}
