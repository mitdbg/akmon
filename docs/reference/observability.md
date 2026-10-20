---
audience: operator, developer
type: reference
canonical_id: observability
---
# AKMON runtime observability


The AKMON runtime (the ingest and publish pipeline) emits queryable metrics,
unified trace rows, and profile bundles for performance investigations.

AKMON has a unified telemetry store in Postgres
(`okg.telemetry_traces`, `okg.telemetry_spans`, `okg.telemetry_events`,
`okg.telemetry_metrics`, and `okg.telemetry_artifacts`). Source runs,
Chronos-native publisher cycles, MCP tool calls, and LLM call telemetry
mirror bounded spans into that store, using an active OpenTelemetry context
when one exists and generating W3C-shaped trace/span ids otherwise.

Most observability arrives through Postgres-backed CLI surfaces and on-disk
profile artifacts, described below.
The machine-readable [alert registry](alerts.yaml) defines the alert names and
thresholds used by deployment monitoring.

## Metrics snapshot

There is no single `MetricsSnapshot` envelope. `okg metrics` is a
Postgres-backed reporting command, not a metrics-collector feed. A
consumer selects a mode and uses its fields; it must not assume that
LLM cost, publication quality, or source-sync fields appear together.

The command requires a mode flag and supports four modes plus
the usual scoping flags (`--generation`, `--since`, `--deployment`,
`--dsn`, `--json`):

- `okg metrics --llm [--since 24h] [--deployment <name>]` — rolls up
  the `okg.llm_calls` table grouped by caller × model (call count,
  prompt/completion tokens, cost). `--since` accepts a window such as
  `1h`, `24h`, or `7d` (default `24h`).
- `okg metrics --qa-snapshot --deployment <name> [--generation N]` —
  emits one `QAGateMetrics` JSON object for the deployment (latest
  published generation by default, or the `--generation` you name).
- `okg metrics --consistency --deployment <name>` — shows why updates
  fell back to a complete recomputation, grouped by cause.
- `okg metrics --disk --deployment <name> [--top N] [--deep]` — reports
  database size, the largest tables and indexes, dead-row percentages,
  compression ratio and unused indexes. `--deep` measures dead rows
  exactly by scanning the tables, which is slow.
  `okg doctor --check disk` reads the same report; its
  `index_larger_than_table` finding names okg-schema tables whose indexes
  outgrew their rows, and `okg db reclaim-index-space` rebuilds them
  (see [the storage runbook](../runbooks/troubleshooting/storage.md#reclaim-index-space-left-by-a-failed-publish)).
- After a successful publish, each path that runs the index rebuild logs one line from
  `okg.substrate.db.index_reclaim` when the post-publish index rebuild
  rebuilt indexes, stopped, or found a leftover invalid index: `post-publish index rebuild: deployment=<d> status=<s>
  reason=<r> indexes_rebuilt=<n> reclaimed_bytes=<bytes> seconds=<s>
  max_publisher_lock_hold_seconds=<s> leftover_invalid_indexes=[...]`, at
  INFO, or WARNING on an error or a leftover invalid index. The same fields are
  in the publish summary under `post_publish_index_rebuild` (see
  [the storage runbook](../runbooks/troubleshooting/storage.md#index-rebuild-after-a-publish)).
- A filtered read that an attribute's expression index did not narrow
  (the index is not ready, the attribute is on edges, or the read is pinned
  to an older generation) says so in the response's `notes`
  (`{"pushdown": "not_covered", ...}`, one reason per attribute); nothing
  is logged. `okg doctor --check declared-attr-indexes` reports each
  declared node attribute without a ready index (`attr_not_indexed`).
- At start, the MCP server logs one ERROR from
  `okg.substrate.mcp.attr_filter_source` when `OKG_ATTR_FILTER_INDEX` is
  `attr_index` or `dual` (removed values; filtered reads refuse
  them), and one WARNING when `OKG_ATTR_FILTER_DUAL_SAMPLE_EVERY` or
  `OKG_ATTR_FILTER_DUAL_TIMEOUT_MS` is set (no effect).

Source-sync metrics are not part of `okg metrics`; they moved to
`okg sync health --deployment <name>`, which reports source-sync
producer heartbeat age and queue/DLQ state.

### What `--qa-snapshot` contains

`QAGateMetrics` (`src/okg/substrate/qa/metrics.py`) is the canonical
per-deployment health snapshot. Every field is computed from existing
runtime state — `graph_generations` (status, `published_at`,
`invariant_summary`, `excluded_sources`), `nodes_live` / `edges_live`
generation deltas, the DBOS-backed `okg.v_runtime_queue_health` view,
and the `okg doctor` exit code — so the snapshot is a query over durable tables, not
a separate emission path. The fields include `deployment_name`,
`generation_id`, `nodes_live_delta`, `edges_live_delta`, `queue_depth`
(per job kind), `dlq_count`, the cross-source and source-local
invariant tallies, `excluded_sources`, `total_invariant_duration_ms` /
`total_invariant_budget_exceeded`, `generation_blocked_count`,
`doctor_exit_code`, and `publish_wallclock_seconds`.

### Internal publish phases

The publish path records one unified `publisher.cycle` span for a
Chronos-native publish, carrying the Chronos generation id and aggregate
timings. Internal phases such as projection, gate evaluation, and commit
remain timing details rather than individual `publish.phase_a` /
`publish.phase_b` / `publish.phase_c` spans.

### Publish lock headroom

`timing_details.lock` reports lock wait, hold, slowest phase, the hard
`statement_timeout` (`publisher.hard_statement_timeout_seconds`, 900s), and a
`headroom` verdict. The hold must fit `timeout / factor`
(`publisher.lock_hold_headroom_factor`, 2.0). Receipts name their source. A
longer hold fails publish proof and blocks `okg deployment ready`; an
unmeasured hold is unproven.

## Generation Correlation

Chronos-native publishing uses the Chronos checkpoint id as the
generation id. The publisher writes that id to the unified
`publisher.cycle` span and `okg.telemetry_traces.generation_id`, so MCP
and LLM spans can join to the same trace by generation id. Older
`okg.graph_generations` rows still carry `correlation_id` for legacy
publish paths and compatibility tests, but Chronos-native publishes do
not rely on a new `graph_generations` row.

## Profiling bundles

The metrics surfaces above answer "is the deployment healthy right
now?" Profiling answers "where did this command or request spend its
time?" It is a separate surface so operators keep lightweight
visibility always and pay for heavy artifacts only when policy or a CLI
override requests them.

The always-on path records bounded stage/query summaries and writes
compact live snapshots under:

```
profiles/<deployment>/_live/
```

Deep captures write immutable-ish bundles under:

```
profiles/<deployment>/<profile_run_id>/
```

The profile root is `profiles/` in okg's user data folder:
`~/.local/share/okg/profiles`, or `$XDG_DATA_HOME/okg/profiles` when that
variable is set. Nothing is written into the folder you run commands from. Set
`OKG_PROFILE_DIR` to move the profile root. A bundle contains
`bundle.json`, `timeline.json`, `speedscope.json`, and `report.json`.
`speedscope.json` is an evented span timeline. Sampled Python flamegraph
artifacts such as `flamegraph.svg` are emitted only when
`--capture-collector python-flamegraph` is requested and `py-spy` is
available; otherwise the bundle records the degraded or unavailable
collector state.

Operators can force a capture around real work:

```
okg telemetry --apply profile --deployment <name> --trace-id <trace-id> --deep
okg run --once --deployment <name> --apply --capture
okg ingest --deployment <name> --capture
okg run --once --deployment <name> --apply --capture --capture-collector python-flamegraph
```

`--capture=off` suppresses durable deep capture for one invocation.
Deployment config controls automatic capture:

```yaml
profiling:
  lightweight:
    enabled: true
    slow_query_ms: 250
  deep_capture:
    mode: on_slow   # off | manual | on_slow | sampled
    slow_publish_ms: 30000
    slow_viz_request_ms: 1000
    collectors: [python-flamegraph]
    max_bundles: 20
```

Read the artifacts with:

```
okg perf list --deployment <name>
okg perf show <profile_run_id> --deployment <name>
okg perf compare <left> <right> --deployment <name>
```

The operator UI has no profiling view; use `okg perf` to read captures.

## Unsupported integrations

- **OTLP / Prometheus / StatsD export.** The runtime does not ship a
  metrics-collector exporter or a `/metrics` scrape endpoint. The
  unified telemetry tables use OpenTelemetry-compatible trace/span
  concepts and can read an active OTel context, but AKMON does not
  export the complete runtime trace to an external collector.
- **A named time-series metric catalog.** There is no
  `okg.queue.depth` / `okg.publish.duration` / `okg.gate.timeout`
  series. Queue depth, DLQ count, and invariant timing are
  read from `okg metrics --qa-snapshot` and `okg sync health` against
  the live database, not scraped as time series.
- **Log shipping infrastructure.** The runtime writes logs to
  stdout/stderr; how they reach an aggregator is a deployment concern.
