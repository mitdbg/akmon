/* The telemetry surface the benchmark flamegraph viewer reads, and nothing
 * else. The operator UI's Performance view carried the full telemetry contract
 * (snapshots, coverage, timelines, metric series) plus an ApiClient to fetch
 * it; the viewer reads one payload embedded in its own HTML page, so only the
 * aggregate-flamegraph shape and one failure formatter survive the move. */

export interface TelemetryUnavailable {
  kind: string;
  name: string;
  reason: string | null;
}

export interface TelemetryFlamegraphNode {
  name: string;
  value: number;
  raw_value?: number;
  trace_id?: string;
  span_id?: string;
  gap_id?: string;
  tooltip?: string;
  children?: TelemetryFlamegraphNode[];
}

export interface TelemetryAggregateFlamegraph {
  schema: "okg.telemetry.aggregate_flamegraph.v1";
  deployment: string;
  generated_at: string;
  since_seconds: number;
  window_start?: string;
  window_end?: string;
  limit: number | null;
  source?: "spans" | "profiles";
  group_by?: "run" | "stage" | "profile";
  profile_type: "evented" | "sampled";
  measure: "wall_time" | "samples";
  unit: string;
  flamegraph_semantics?: "additive_wall_time" | "additive_samples";
  normalization?: {
    mode: "none" | "scale_overlapping_children";
    overlap_node_count: number;
    overlap_duration_ms: number;
    reason: string;
  };
  status: string;
  unavailable_reason: string | null;
  candidate_profile_count: number;
  candidate_trace_count?: number;
  candidate_span_count?: number;
  profile_count: number;
  trace_count: number;
  span_count?: number;
  rendered_span_count?: number;
  covered_duration_ms?: number;
  gap_duration_ms?: number;
  unavailable_profile_count: number;
  truncated?: boolean;
  omitted_trace_count?: number;
  omitted_span_count?: number;
  root: TelemetryFlamegraphNode;
  unavailable: TelemetryUnavailable[];
}

/* The operator UI's `telemetryFailureDiagnostic` branched on `ApiRequestError`
 * to recover the failing route, HTTP status and query parameters from a live
 * read. The viewer resolves its payload from an embedded `<script>` element, so
 * no read can raise that error and only the non-API branch was ever reachable;
 * that branch is reproduced here verbatim rather than dragging the operator
 * UI's ApiClient into the benchmark bundle. The rendered string is unchanged. */
export function telemetryFailureDiagnostic(
  _error: unknown,
  fallbackRoute = "/viz/api/telemetry/performance",
): string {
  const route = fallbackRoute.startsWith("GET ") ? fallbackRoute : `GET ${fallbackRoute}`;
  return `${route} · status unknown · query none · Telemetry performance did not load. `
    + "Next check: confirm the selected OKG service is running.";
}
