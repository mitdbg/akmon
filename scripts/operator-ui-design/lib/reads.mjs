// Build the console read payloads for one mock deployment.
//
// Every shape here was taken from a payload the live Archiv3 copy actually
// served on 2026-09-16 (health, generations, sources, status, types,
// source-diagnostics) or from the parser in the application that reads it
// (searchData.ts for the record reads, workflowList.ts for /viz/api/workflows,
// deployments.ts for /viz/api/deployments). Where the spec in
// scripts/operator-ui-design/redesign-requirements-v2.md section 10 names a read the
// backend does not serve yet, the payload is marked `"gap": true` and the
// README says so -- a fixture that quietly invented a served read would make
// the mock un-implementable.

import { createHash } from "node:crypto";

const QUEUE_LANES = ["ingest", "link", "enrich", "publish", "embed"];

function sha256Hex(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

export function generationId(deployment, iso, suffix) {
  const stamp = iso.replace(/[-:]/g, "").replace(/\.\d+/, "").replace("Z", "");
  return `okg:${deployment}:branch:default:gen:${stamp}000000Z:${suffix}`;
}

export function shortId(fullId) {
  return fullId.slice(-12);
}

function ageFrom(nowMs, iso) {
  const deltaMinutes = Math.round((nowMs - Date.parse(iso)) / 60000);
  if (deltaMinutes < 1) return "just now";
  if (deltaMinutes < 60) return `${deltaMinutes}m ago`;
  const hours = Math.round(deltaMinutes / 60);
  if (hours < 48) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

const PUBLISHED = (attempt) => attempt.status === "published";

export function latestPublished(spec) {
  return [...spec.attempts].reverse().find(PUBLISHED) ?? null;
}

export function latestAttempt(spec) {
  return spec.attempts.length ? spec.attempts[spec.attempts.length - 1] : null;
}

// --- /viz/api/deployments -------------------------------------------------

export function deploymentsRead(spec, allNames) {
  return {
    entries: allNames.map((name) => ({
      name,
      description: name === spec.name ? spec.purpose : `Mock deployment ${name}`,
      url: `https://mock.invalid/${name}`,
      operator_ui_base: `https://mock.invalid/${name}/console/`,
      chat_site_base: name === "empty" ? null : `https://mock.invalid/${name}/chat/`,
      viz_theme: { status: "undeclared", tokens: null, polarity: null },
      is_current: name === spec.name,
    })),
  };
}

// --- /viz/api/health (liveness) -------------------------------------------

export function livenessRead(spec) {
  const published = latestPublished(spec);
  return {
    ok: true,
    ready: Boolean(published),
    deployment_name: spec.name,
    generation_id: published ? published.compatId : null,
    node_count: spec.nodesLive,
    edge_count: spec.edgesLive,
    count_estimated: false,
  };
}

// --- /viz/api/console/health ----------------------------------------------

function historyRow(spec, attempt) {
  return {
    generation_id: attempt.generationId,
    compat_generation_id: attempt.compatId,
    status: attempt.status,
    age: ageFrom(spec.nowMs, attempt.iso),
    created_at: attempt.iso,
    blocked_reason: attempt.blockedReason ?? null,
    describe_command: `okg generation describe ${attempt.generationId}`,
    nodes_delta: attempt.nodesDelta,
    edges_delta: attempt.edgesDelta,
  };
}

export function consoleHealthRead(spec) {
  const published = latestPublished(spec);
  const attempt = latestAttempt(spec);
  const sources = spec.producers.filter((p) => p.kind === "source");
  const failed = sources.filter((p) => p.state === "failed");
  const neverRun = sources.filter((p) => p.state === "never run");
  return {
    generation_id: published ? published.generationId : null,
    payload: {
      deployment: spec.name,
      generation_id: published ? published.generationId : null,
      compat_generation_id: published ? published.compatId : null,
      generation_status: published ? "published" : "none",
      generation_age: published ? ageFrom(spec.nowMs, published.iso) : null,
      verdict: spec.verdict,
      history: spec.attempts.slice(-6).reverse().map((row) => historyRow(spec, row)),
      history_total: spec.attempts.length,
      latest_attempt: attempt ? historyRow(spec, attempt) : null,
      vitals: {
        generation_blocked_count: spec.attempts.filter((a) => a.status === "blocked").length,
        queue_depth: Object.fromEntries(
          QUEUE_LANES.map((lane, index) => [lane, spec.runtime.queueByLane[index] ?? 0]),
        ),
        queue_total: spec.runtime.queueByLane.reduce((a, b) => a + b, 0),
        dlq_count: spec.runtime.dlqCount,
        terminal_history_dlq_count: spec.runtime.dlqCount,
        nodes_live: spec.nodesLive,
        edges_live: spec.edgesLive,
        nodes_live_delta: published ? published.nodesDelta : 0,
        edges_live_delta: published ? published.edgesDelta : 0,
        publish_wallclock_seconds: published ? published.durationMs / 1000 : 0,
        invariants: spec.invariants,
        excluded_sources: sources.filter((p) => p.state === "excluded").map((p) => p.name),
      },
      source_breakdown: {
        source_count: sources.length,
        summary: {
          total: sources.length,
          healthy: sources.filter((p) => p.state === "updated").length,
          needs_review: failed.length,
          unknown: neverRun.length,
          excluded: sources.filter((p) => p.state === "excluded").length,
        },
        sources: sources.map((p) => ({
          name: p.name,
          source_key: `${spec.name}.${p.name}`,
          status: p.state === "updated" ? "healthy"
            : p.state === "never run" ? "unknown"
              : p.state === "excluded" ? "excluded" : "needs_review",
          included: p.state !== "excluded",
          last_update: p.lastRunIso,
          latest_issue: p.error ? p.error.split("\n")[0] : null,
          next_check: p.error ? `okg source diagnose ${p.name}` : null,
        })),
      },
      resources: {
        status: spec.runtime.resourcesStatus,
        source: "okg.node_series + pg_database_size",
        hosts: [],
        database_size_bytes: spec.databaseSizeBytes,
        window: { start: null, end: null, period: "1h" },
        series: { nodes: [], edges: [], cpu: [], memory: [] },
        omissions: spec.runtime.resourceOmissions,
      },
      critical_reads: {
        generation_id: published ? published.generationId : null,
        ready: Boolean(published),
        prerequisites: [],
      },
      checks: spec.runtime.checks,
      doctor_note:
        "Substrate-state health from the pinned Chronos generation. This is a design "
        + "fixture; no deployment was read.",
    },
  };
}

// --- /viz/api/console/generations -----------------------------------------

export function generationsRead(spec) {
  const published = latestPublished(spec);
  return {
    generation_id: published ? published.generationId : null,
    payload: {
      deployment: spec.name,
      current_generation_id: published ? published.compatId : null,
      shown: Math.min(spec.attempts.length, 50),
      total: spec.attempts.length,
      next_cursor: spec.attempts.length > 50 ? String(spec.attempts.length - 50) : null,
      generations: [...spec.attempts].reverse().slice(0, 50).map((attempt) => ({
        generation_id: attempt.compatId,
        full_generation_id: attempt.generationId,
        status: attempt.status,
        age: ageFrom(spec.nowMs, attempt.iso),
        created_at: attempt.iso,
        is_current: published ? attempt.compatId === published.compatId : false,
        blocked_reason: attempt.blockedReason ?? null,
        // The recorded blocker itself, unflattened, as the live composer
        // serves it. `blocked_reason` beside it stays the one-line summary a
        // list can hold; this is the record the detail page prints. Built
        // from the same spec fields so the harness shows what the product
        // shows rather than a shape the parser never meets.
        blocked_detail: attempt.errorText
          ? { reason: attempt.blockedReason ?? "publish_blocked", detail: attempt.errorText }
          : null,
        // The gate's exclusions as the LIST the checkpoint records, on every
        // attempt whatever its outcome. The count is DERIVED from it so the
        // two can never disagree.
        excluded_sources: attempt.excludedSources ?? [],
        excluded_source_count: (attempt.excludedSources ?? []).length,
        delta_available: attempt.status === "published",
        delta_unavailable_reason:
          attempt.status === "published" ? null : "pinned generation is not published",
        nodes_added: attempt.status === "published" ? attempt.nodesDelta : null,
        nodes_retracted: attempt.status === "published" ? 0 : null,
        edges_added: attempt.status === "published" ? attempt.edgesDelta : null,
        edges_retracted: attempt.status === "published" ? 0 : null,
        zero_delta: attempt.status === "published" && attempt.nodesDelta === 0,
        duration_ms: attempt.durationMs,
        // Backend gap 1 (requirements v2 section 10): the live composer records
        // neither of these, so today a blocked attempt names no producer.
        stopped_stage: attempt.stoppedStage ?? null,
        stopped_producer: attempt.stoppedProducer ?? null,
        error_text: attempt.errorText ?? null,
        // One row per VIEW producer recorded against this attempt's own
        // target generation. An empty list means the deployment recorded
        // none for this attempt; the field's ABSENCE would mean the server
        // does not serve them, which the console reports differently, so it
        // is always present here.
        view_runs: attempt.viewRuns ?? [],
      })),
      // `next_cursor` is served now (PACT operator-console-live-updates), so
      // it is no longer a gap. The four that remain are the fields the live
      // composer hard-nulls because no column records them.
      gap: ["stopped_stage", "stopped_producer", "error_text", "duration_ms"],
    },
  };
}

// --- attempts by outcome over a window (backend gap 3) --------------------

/**
 * The attempts chart's span, in the shape `console/attempts` answers with.
 *
 * `days + 1` buckets, not `days`: the live composer's span reaches back
 * `days` whole days from the read and therefore holds the read's own day and
 * the `days` before it (`console_views/attempts_by_day.py::_days`). That
 * boundary is the one the console's age label uses, so an attempt the page
 * calls "14 d ago" is inside a 14-day span; a fixture one day short would
 * draw a chart the deployment cannot serve, which is the one thing this
 * harness exists to prevent.
 */
export function attemptsByDayRead(spec, days) {
  const dayMs = 86400000;
  const endDay = Math.floor(spec.nowMs / dayMs) * dayMs;
  const buckets = [];
  for (let index = days; index >= 0; index -= 1) {
    const start = endDay - index * dayMs;
    const inDay = spec.attempts.filter((a) => {
      const t = Date.parse(a.iso);
      return t >= start && t < start + dayMs;
    });
    buckets.push({
      day: new Date(start).toISOString().slice(0, 10),
      published: inDay.filter((a) => a.status === "published").length,
      blocked: inDay.filter((a) => a.status === "blocked").length,
      running: inDay.filter((a) => a.status === "running").length,
    });
  }
  return {
    generation_id: latestPublished(spec)?.generationId ?? null,
    payload: {
      deployment: spec.name,
      range: `${days}d`,
      now: new Date(spec.nowMs).toISOString(),
      buckets,
      totals: {
        published: buckets.reduce((a, b) => a + b.published, 0),
        blocked: buckets.reduce((a, b) => a + b.blocked, 0),
        running: buckets.reduce((a, b) => a + b.running, 0),
      },
      gap: true,
    },
  };
}

// --- /viz/api/console/sources ---------------------------------------------

export function sourcesRead(spec) {
  const published = latestPublished(spec);
  // Deliberately DUPLICATED the way the live read duplicates: one row per
  // registry name plus one per runtime key. Sources must collapse these by
  // design (requirements v2 section 8); a fixture that pre-collapsed them
  // would hide the defect the view exists to fix.
  const rows = [];
  for (const producer of spec.producers.filter((p) => p.kind === "source")) {
    rows.push({
      name: producer.name,
      source_key: `${spec.name}.${producer.name}`,
      in_registry: true,
      total_facts: null,
      total_edge_facts: null,
      fact_counts_available: false,
      fact_counts_unavailable_reason: "raw_fact_log_counts_unbounded",
      records_in_gen: producer.records,
      edges_in_gen: producer.edges,
      last_sync_age: producer.lastRunIso ? ageFrom(spec.nowMs, producer.lastRunIso) : null,
    });
    for (const runtimeKey of producer.runtimeKeys ?? []) {
      rows.push({
        name: runtimeKey,
        source_key: `${spec.name}.${runtimeKey}`,
        in_registry: false,
        total_facts: null,
        total_edge_facts: null,
        fact_counts_available: false,
        fact_counts_unavailable_reason: "raw_fact_log_counts_unbounded",
        records_in_gen: 0,
        edges_in_gen: 0,
        last_sync_age: null,
      });
    }
  }
  return {
    generation_id: published ? published.generationId : null,
    payload: {
      deployment: spec.name,
      generation_id: published ? published.generationId : null,
      source_count: rows.length,
      sources: rows,
    },
  };
}

// --- diagnostics reads ----------------------------------------------------

function runRows(spec, producer) {
  return (producer.runs ?? []).map((run) => ({
    started_at: run.iso,
    age: ageFrom(spec.nowMs, run.iso),
    outcome: run.outcome,
    items_in: run.itemsIn,
    items_out: run.itemsOut,
    duration_ms: run.durationMs,
    phase_reached: run.phase,
    error_text: run.errorText ?? null,
  }));
}

function diagnosticsRow(spec, producer) {
  return {
    name: producer.name,
    source_key: `${spec.name}.${producer.name}`,
    in_registry: true,
    health: {
      status: producer.state === "updated" ? "healthy"
        : producer.state === "never run" ? "unknown"
          : producer.state === "excluded" ? "excluded" : "degraded",
      detail: producer.error ? producer.error.split("\n")[0] : null,
    },
    freshness: {
      last_run: producer.lastRunIso,
      age: producer.lastRunIso ? ageFrom(spec.nowMs, producer.lastRunIso) : null,
      next_due: producer.nextDueIso ?? null,
    },
    producers: {
      adapter: producer.adapter,
      settings: producer.settings ?? {},
      scope: producer.scope ?? null,
      credentials: "omitted",
    },
    workflow: {
      state: producer.state,
      excluded_reason: producer.excludedReason ?? null,
      withheld_reason: producer.withheldReason ?? null,
      cadence_hours: producer.cadenceHours ?? null,
      run_count: (producer.runs ?? []).length,
    },
    contribution: {
      records_in_gen: producer.records,
      edges_in_gen: producer.edges,
      record_types: producer.recordTypes ?? [],
    },
    latest_run: (producer.runs ?? [])[0] ?? null,
    incidents: producer.error ? [{ first_line: producer.error.split("\n")[0] }] : [],
  };
}

function diagnosticsEnvelope(spec, kind, key, selectedName) {
  const published = latestPublished(spec);
  const producers = spec.producers.filter((p) => p.kind === kind);
  const selected = selectedName
    ? producers.find((p) => p.name === selectedName) ?? null
    : null;
  return {
    generation_id: published ? published.generationId : null,
    payload: {
      schema: `okg.console.${kind}_diagnostics.v1`,
      deployment: spec.name,
      generation_id: published ? published.generationId : null,
      selected_source: selectedName ?? null,
      summary: {
        [`${key}_count`]: producers.length,
        degraded_count: producers.filter((p) => p.state === "failed").length,
        stale_count: producers.filter((p) => p.state === "never run").length,
        active_workflow_count: producers.filter((p) => p.state === "running").length,
        omitted_data_count: 0,
      },
      // A detail read carries only the selected producer. The live composer
      // returns the whole list alongside it, which at 220 producers is 220
      // copies of the same table across the fixture; the views read the list
      // from the unparameterised read instead. Stated, not silent.
      [key]: selectedName ? [] : producers.map((p) => diagnosticsRow(spec, p)),
      list_omitted_reason: selectedName
        ? `the list is served by GET /viz/api/console/${kind}-diagnostics without a name`
        : null,
      selected: selected
        ? {
          source_name: selected.name,
          found: true,
          row: diagnosticsRow(spec, selected),
          run_history: runRows(spec, selected),
          run_summary: {
            total: (selected.runs ?? []).length,
            failed: (selected.runs ?? []).filter((r) => r.outcome === "failed").length,
          },
          phase_summary: [],
          selected_run: (selected.runs ?? [])[0] ?? null,
          omitted_data: [],
        }
        : null,
      omitted_data: [],
    },
  };
}

export const sourceDiagnosticsRead = (spec, name) =>
  diagnosticsEnvelope(spec, "source", "sources", name);
export const enricherDiagnosticsRead = (spec, name) =>
  diagnosticsEnvelope(spec, "enricher", "enrichers", name);
export const viewDiagnosticsRead = (spec, name) =>
  diagnosticsEnvelope(spec, "view", "views", name);

// --- /viz/api/console/producers -------------------------------------------

/**
 * One producer, in the shape `console/producers` answers with.
 *
 * Deliberately NOT the diagnostics row. The live read carries what the live
 * composers record and nothing else, so the fixture carries the same fields
 * or the harness would draw a page the deployment cannot serve:
 *
 *   - `freshness` is the runtime's own state. A source whose runtime supplies
 *     no watermark reads `not_evaluated`; it has no `last_run` and no
 *     `next_due`, because nothing records them.
 *   - a run has no error text: `okg.v_source_run_diagnostics` records no
 *     message column.
 *   - a view's `edges_in_gen` is null with a reason: a view builds rows and
 *     emits no relationships.
 */
/**
 * The newest run's time.
 *
 * A deployment spec carries both `lastRunIso` and a run list whose first
 * entry is the newest, and for several producers the two disagree. The live
 * read cannot disagree with itself: `latest_run` IS the newest row of
 * `okg.v_source_run_diagnostics`. So the newest run here takes `lastRunIso`
 * where the spec declares one, and the run history is stamped the same way,
 * which also keeps the staggered ordering the list's default sort relies on.
 */
function newestRunIso(producer) {
  const runs = producer.runs ?? [];
  return producer.lastRunIso ?? runs[0]?.iso ?? null;
}

/** What the view read records about a view, in ITS words.
 *
 *  `_issue` in `src/okg/substrate/console_views/view_diagnostics.py` composes
 *  exactly these three answers from the latest materialization run, and
 *  nothing else: a view read never serves the run's own exception text. The
 *  fixture states map onto the three statuses that composer classifies --
 *  `failed` is `failing`, `updated` is `healthy`, everything else is the
 *  `unknown` a view with no recorded run gets. */
function viewIssue(state) {
  if (state === "failed") return "Latest materialization failed.";
  if (state === "updated") return null;
  return "No materialization run is recorded.";
}

function producerRow(spec, producer) {
  const runs = producer.runs ?? [];
  const latest = runs[0] ?? null;
  const isView = producer.kind === "view";
  return {
    name: producer.name,
    // The registry's declared ownership_id, which is what the runtime
    // attributes work to and what the table keys a row by.
    ownership_id: producer.ownershipId ?? `${spec.name}.${producer.name}`,
    kind: producer.kind,
    configured: producer.configured !== false,
    health: {
      status: producer.state === "updated" ? "healthy"
        : producer.state === "never run" ? "unknown"
          : producer.state === "excluded" ? "excluded" : "degraded",
      incident_count: producer.error ? 1 : 0,
    },
    workflow: {
      state: producer.state,
      status: producer.state,
      active: producer.state === "running",
      // The enricher read records an issue in three of its states, and the
      // view read composes one of three fixed sentences from the latest
      // materialization run (`console_views/view_diagnostics.py`, `_issue`).
      // A source's workflow comes from runtime readiness `convergence`, which
      // carries no policy reason of any kind, so its issue is null beside the
      // reason it is -- exactly as the composer serves it.
      //
      // The view sentences are the read's OWN words, not the run's. No live
      // view read serves an exception text, which is why a mock that did was
      // drawing a page no deployment can.
      latest_issue: producer.kind === "enricher"
        ? producer.withheldReason ?? null
        : producer.kind === "view"
          ? viewIssue(producer.state)
          : null,
      latest_issue_unavailable_reason: producer.kind === "enricher"
        ? (producer.withheldReason
          ? null
          : "enricher_records_no_issue_in_this_state")
        : producer.kind === "view"
          ? (viewIssue(producer.state)
            ? null
            : "view_diagnostics_recorded_no_issue_for_this_view")
          : "no_read_records_a_policy_reason_for_a_source",
    },
    contribution: {
      records_in_gen: producer.records,
      edges_in_gen: isView ? null : producer.edges,
      ...(isView ? { edges_unavailable_reason: "a_view_emits_no_relationships" } : {}),
      horizon: "pinned_generation",
    },
    freshness: producer.kind === "source"
      ? { status: "not_evaluated" }
      : {
        status: "last_run_age_only",
        age: producer.lastRunIso ? ageFrom(spec.nowMs, producer.lastRunIso) : null,
      },
    latest_run: latest
      ? {
        iso: newestRunIso(producer),
        outcome: latest.outcome,
        items_in: latest.itemsIn,
        items_out: latest.itemsOut,
        duration_ms: latest.durationMs,
        phase: latest.phase,
        run_id: latest.runId ?? null,
        error_text_unavailable_reason: "no_message_column_in_run_diagnostics",
      }
      : null,
    latest_run_unavailable_reason: latest ? null : "no_source_run_diagnostics_rows",
    run_count: null,
    run_count_unavailable_reason: "run_count_is_a_detail_read",
  };
}

function producerRunRows(producer) {
  const newest = newestRunIso(producer);
  return (producer.runs ?? []).map((run, index) => ({
    iso: index === 0 ? newest : run.iso,
    outcome: run.outcome,
    items_in: run.itemsIn,
    items_out: run.itemsOut,
    duration_ms: run.durationMs,
    phase: run.phase,
    run_id: run.runId ?? null,
    error_text_unavailable_reason: "no_message_column_in_run_diagnostics",
  }));
}

/**
 * One producer's records, grouped by record type.
 *
 * The live read groups the count rollup's `source_subtype` dimension for this
 * producer; a mock declares the type names its records carry and the total it
 * contributed, so the split here is the total divided evenly over those names,
 * with the remainder on the first. It is a mock's split of a mock's own total:
 * no figure is invented, and the sum is always the producer's own
 * `records_in_gen`, which is what the page prints above the table.
 *
 * A producer that declares no type name has no rows and a total of zero, which
 * is the one shape that can be read as "holds no record" without lying.
 */
function recordsByType(producer) {
  const names = producer.recordTypes ?? [];
  const total = producer.records ?? 0;
  if (!names.length || !total) {
    return { status: "available", types: [], total: names.length ? total : 0, omitted: 0 };
  }
  const share = Math.floor(total / names.length);
  const types = names.map((type, index) => ({
    type,
    records: index === 0 ? total - share * (names.length - 1) : share,
  }));
  types.sort((a, b) => (b.records - a.records) || a.type.localeCompare(b.type));
  const shown = types.slice(0, RECORD_TYPE_LIMIT);
  return {
    status: "available",
    types: shown,
    total,
    omitted: types.length - shown.length,
  };
}

/** What the live read caps a producer's type list at. */
const RECORD_TYPE_LIMIT = 40;

export function producersRead(spec, name = null, runLimit = 20) {
  const published = latestPublished(spec);
  const rows = spec.producers.map((p) => producerRow(spec, p));
  const selectedSpec = name
    ? spec.producers.find((p) => p.name === name) ?? null
    : null;
  let selected = null;
  if (name) {
    if (!selectedSpec) {
      selected = {
        found: false,
        reason: "no_producer_of_this_deployment_answers_to_that_name",
      };
    } else {
      const history = producerRunRows(selectedSpec);
      const row = producerRow(spec, selectedSpec);
      // The run query is capped at what the page needs; a full answer may be
      // hiding older runs, so the rows in hand are a floor, not the count.
      const truncated = history.length >= runLimit;
      row.run_count = truncated ? null : history.length;
      row.run_count_unavailable_reason = truncated
        ? `run_history_truncated_at_${runLimit}; ask for more runs to count them`
        : null;
      selected = {
        found: true,
        row,
        records_by_type: recordsByType(selectedSpec),
        // Not a figure any read can give: no rollup dimension pairs an edge
        // with the producer that emitted it. The live read says so by name and
        // so does this one.
        relationships_by_type: {
          status: "not_recorded",
          reason: "no_rollup_dimension_pairs_an_edge_with_its_producer",
        },
        run_history: history.slice(0, runLimit),
        run_offset: 0,
        run_limit: runLimit,
        run_history_fetched: history.length,
        run_history_truncated: truncated,
        more: truncated,
      };
    }
  }
  return {
    ok: true,
    generation_id: published ? published.generationId : null,
    payload: {
      schema: "okg.console.producers.v1",
      deployment: spec.name,
      generation_id: published ? published.generationId : null,
      producer_count: rows.length,
      producers: rows,
      read_authority: {
        generation_id: published ? published.generationId : null,
        sources: { status: "available" },
        enrichers: { status: "available" },
        views: { status: "available" },
      },
      omitted_data: [],
      selected_producer: name,
      selected,
    },
  };
}

// --- /viz/api/console/versions --------------------------------------------

export function versionsRead(spec) {
  const publishedAttempts = spec.attempts.filter(PUBLISHED).reverse();
  const published = latestPublished(spec);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      deployment: spec.name,
      versions: publishedAttempts.slice(0, 50).map((attempt) => ({
        generation_id: attempt.generationId,
        published_at: attempt.iso,
      })),
      latest: published ? published.generationId : null,
      truncated: publishedAttempts.length > 50,
    },
  };
}

// --- record reads ---------------------------------------------------------

export function recordTypesRead(spec) {
  const published = latestPublished(spec);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      types: spec.recordTypes,
      count_basis: "published_total_before_access_rules",
    },
  };
}

export function recordListRead(spec, type, offset, limit) {
  const published = latestPublished(spec);
  const rows = spec.records.filter((r) => r.type === type);
  const page = rows.slice(offset, offset + limit);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      type,
      records: page.map((r) => ({
        node_id: r.nodeId,
        type: r.type,
        label: r.label,
        source: r.source,
      })),
      next_offset: offset + limit < rows.length ? offset + limit : null,
      short_page: page.length < limit,
    },
  };
}

/**
 * `records/search?q=&type=&source=&offset=&limit=`.
 *
 * Text matches on the label and the text; an exact id match first.
 *
 * This answers in the shape `compose_record_search` answers in, measured
 * against a real published generation. It used to answer in a wider shape --
 * `updated`, `related_count`, `citation_url` and `has_artifact` per row, plus
 * `applied` and `facets` on the page, all listed under a `gap` key as asks the
 * backend had yet to serve. Drawing a view against those asks produced
 * controls that are permanently blank against any real deployment, so the
 * fixture no longer offers them: what it sends is what a live read sends.
 *
 * `sort` and `since` are gone for a stronger reason than absence. The live
 * read REFUSES them, with HTTP 422 and `filter_has_no_stored_basis`, because
 * the published graph stores no per-record date. A fixture that quietly sorted
 * by a date it invented would make a control look workable that fails in
 * production on the first click.
 */
export function recordSearchRead(spec, query, filters, offset, limit) {
  const { type = null, source = null } = filters ?? {};
  const published = latestPublished(spec);
  const needle = query.toLowerCase();
  const exact = spec.records.filter((r) => r.nodeId === query);
  const text = spec.records.filter(
    (r) => r.nodeId !== query
      && (r.label.toLowerCase().includes(needle) || (r.text ?? "").toLowerCase().includes(needle)),
  );
  const matched = [...exact, ...text]
    .filter((r) => (type ? r.type === type : true))
    .filter((r) => (source ? r.source === source : true));
  const page = matched.slice(offset, offset + limit);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      query,
      type: type ?? null,
      // The echo the live read sends on every answer, so the client can
      // compare what it asked for against what was honoured.
      filters: { type: type ?? null, source: source ?? null },
      results: page.map((r) => ({
        node_id: r.nodeId,
        type: r.type,
        label: r.label,
        source: r.source,
        match: r.nodeId === query ? "record_id" : "text",
        excerpt: r.nodeId === query ? null : (r.text ?? null),
      })),
      exact_match: exact.length > 0,
      exact_match_in_text: false,
      total_count: matched.length,
      // The rows the pages walk. The fixture pages its exact row with the
      // rest, so this is every match; the live read leaves exact rows out.
      paged_count: matched.length,
      truncated: false,
      next_offset: offset + limit < matched.length ? offset + limit : null,
      rank_capped: false,
      // The live read diagnoses a type-filtered miss. A fixture miss is a
      // plain one: the rows are here and none carries the words.
      zero_hit_reason: matched.length === 0 && type ? "no_match" : null,
      // The deployment search profile, as the live read reports it. It
      // applies only when no type is named, as the live default does.
      ...searchScope(spec, type),
    },
  };
}

function searchScope(spec, type) {
  const profile = type ? null : (spec.searchProfile ?? null);
  return {
    search_profile: profile ? profile.profile : null,
    searched_types: profile ? spec.recordTypes.map((row) => row.type) : null,
    skipped_types: profile ? [...profile.skipped] : [],
    skipped_types_reason: profile && profile.skipped.length ? "not_in_this_version_catalog" : null,
  };
}

export function recordDetailRead(spec, nodeId) {
  const published = latestPublished(spec);
  const record = spec.records.find((r) => r.nodeId === nodeId);
  if (!record) {
    return {
      generation_id: published ? published.generationId : "unpublished",
      payload: { found: false, node_id: nodeId },
    };
  }
  const cap = record.relatedCap ?? 100;
  const chunks = chunksOf(spec, record);
  const outgoing = (record.outgoing ?? []).slice(0, cap);
  const incoming = (record.incoming ?? []).slice(0, cap);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      found: true,
      node_id: record.nodeId,
      type: record.type,
      label: record.label,
      source: record.source,
      source_record_id: record.sourceRecordId ?? null,
      source_revision: record.sourceRevision ?? null,
      policy_redacted: (record.fields ?? []).some((f) => f.hidden),
      fields: (record.fields ?? []).map((f) => ({
        key: f.key,
        value: f.hidden ? null : f.value,
        hidden: Boolean(f.hidden),
        // No `hidden_policy`: the live composer sends none, so the harness
        // must not either. See pact/changes/operator-console-live-explore/ui/cuts.md.
        echoes_title: Boolean(f.echoesTitle),
      })),
      sections: (record.sections ?? []).map((s) => ({
        key: s.key,
        title: s.title,
        value: s.hidden ? null : s.value,
        hidden: Boolean(s.hidden),
      })),
      related: {
        outgoing: outgoing.map((r) => ({
          relationship: r.relationship,
          node_id: r.nodeId,
          type: r.type,
          label: r.label,
        })),
        incoming: incoming.map((r) => ({
          relationship: r.relationship,
          node_id: r.nodeId,
          type: r.type,
          label: r.label,
        })),
        cap,
        outgoing_capped: (record.outgoing ?? []).length > cap,
        incoming_capped: (record.incoming ?? []).length > cap,
        // Backend gap: the live composer states the cap but not how many rows
        // it omitted, so the Related tab cannot say "N more" today.
        outgoing_total: (record.outgoing ?? []).length,
        incoming_total: (record.incoming ?? []).length,
        by_relationship: tallyRelationships(record),
        outgoing_total_capped: false,
        incoming_total_capped: false,
        tally_ceiling: RELATED_TALLY_CEILING,
        gap: ["outgoing_total", "incoming_total", "by_relationship"],
      },
      masked_paths: [],
      masked_paths_rule: PATH_RULE_LIMIT,
      // No `provenance`: the live detail read carries no such key, so the
      // fact log, the last-write attempt and the enricher tally have no
      // source. They are cut from the view rather than faked here.
      // The record's chunks in order, with their text, so the Content tab
      // renders them without one read per chunk. Backend gap 4 as well.
      // Absent, not empty, when the record has none -- and the four cap
      // keys travel WITH the list, exactly as the composer sends them.
      ...(chunks === null ? {} : {
        chunks,
        chunk_cap: CHUNK_CAP,
        chunks_capped: chunks.length > CHUNK_CAP,
        // The recorded order this list is in, by the composer's own name for
        // it. Every fixture chunk carries an ordinal, so it is always the
        // first basis here; null would mean nothing was listed.
        chunk_order_basis: chunks.length ? "chunk_ordinal" : null,
        chunks_without_ordinal: 0,
        chunks_without_ordinal_capped: false,
      }),
      gap: ["chunks"],
    },
  };
}


// The server's own values, copied verbatim so a mock read has the SHAPE the
// live read has. A fixture that omits one of these lets a parser pass here
// and fail against the real deployment, which is what these reads exist to
// prevent. Sources: `PATH_RULE_LIMIT` and `RELATED_TALLY_CEILING` in
// src/okg/substrate/console_views/records.py, and the `pinning_basis` the
// retained-payload composer sets.
export const PATH_RULE_LIMIT = "any string value that begins with '/' -- or is a ~, drive-letter, UNC or file: path -- is treated as an absolute path and replaced by its last segment, so a root-relative URL path or a stored regex is shortened too, and every rewritten key is named in masked_paths; a path written inside free text is NOT detected and is served as stored";
export const RELATED_TALLY_CEILING = 500;
export const CHUNK_CAP = 200;
export const PINNING_BASIS = "bound_before_publish";

function chunksOf(spec, record) {
  const contained = (record.outgoing ?? [])
    .filter((r) => r.relationship === "contains" && r.type === "document_chunk")
    .map((r) => spec.records.find((candidate) => candidate.nodeId === r.nodeId))
    .filter((chunk) => chunk && chunk.chunk);
  if (!contained.length) return null;
  return contained
    .sort((a, b) => a.chunk.ordinal - b.chunk.ordinal)
    .map((chunk) => ({
      node_id: chunk.nodeId,
      ordinal: chunk.chunk.ordinal,
      // Where the chunk starts in its document. The live composer serves it
      // beside the ordinal and orders by it when NO chunk of the record has
      // a usable ordinal (issue #2178). These fixture chunks all have one,
      // so the offset rides along unset rather than being invented.
      char_offset: chunk.chunk.charOffset ?? null,
      page: chunk.chunk.page ?? null,
      heading_path: chunk.chunk.headingPath ?? null,
      text: chunk.text ?? "",
    }));
}

function tallyRelationships(record) {
  const tally = new Map();
  for (const [direction, rows] of [["outgoing", record.outgoing ?? []], ["incoming", record.incoming ?? []]]) {
    for (const row of rows) {
      const key = `${direction}:${row.relationship}`;
      tally.set(key, (tally.get(key) ?? 0) + 1);
    }
  }
  return [...tally.entries()]
    .sort((a, b) => (b[1] - a[1]) || a[0].localeCompare(b[0]))
    .map(([key, count]) => {
      const [direction, relationship] = key.split(":");
      return { direction, relationship, count };
    });
}

// --- the artifact read and the raw read (backend gap 4) -------------------

/** How many bytes of retained payload the raw read inlines. Past this it
 *  carries the first 200 lines and the file's URL. */
export const INLINE_PAYLOAD_LIMIT = 256 * 1024;
export const RAW_HEAD_LINES = 200;
export const RAW_EDGE_CAP = 200;

/** The retained payload of one record as a file: name, bytes. Null when the
 *  record retains none or policy refuses it (nothing is written for it). */
export function recordPayloadFile(record, artifacts) {
  if (record.payloadFile) {
    const file = artifacts[record.payloadFile];
    if (!file) throw new Error(`${record.nodeId} names a payload file that is not built: ${record.payloadFile}`);
    return { name: record.payloadFile, bytes: file.bytes, url: file.url, artifact: true };
  }
  if (record.payload === undefined) return null;
  const name = `${record.nodeId.replace(/[^a-z0-9]+/gi, "-")}.json`;
  return {
    name,
    bytes: Buffer.from(`${JSON.stringify(record.payload, null, 2)}\n`, "utf8"),
    url: `/design/fixtures/payloads/${name}`,
    artifact: false,
  };
}

/** `records/artifact?id=`: whether the original file exists, its media
 *  type, size and hash, and where the bytes are served. Governed by the same
 *  source policy as the retained payload. */
export function recordArtifactRead(spec, nodeId, artifacts) {
  const published = latestPublished(spec);
  const record = spec.records.find((r) => r.nodeId === nodeId);
  const envelope = (payload) => ({
    generation_id: published ? published.generationId : "unpublished",
    payload: { node_id: nodeId, ...payload, gap: true },
  });
  if (!record) return envelope({ exists: false, reason: "not_found", policy: null, detail: `no record ${nodeId}` });
  const spec_ = record.artifact;
  if (!spec_) {
    return envelope({
      exists: false, reason: "not_retained", policy: null,
      detail: `${record.source ?? "the source"} keeps no corpus root, so the original was not retained`,
    });
  }
  if (spec_.missing) {
    return envelope({
      exists: false,
      reason: spec_.missing,
      policy: spec_.policy ?? null,
      format: spec_.format ?? null,
      path: spec_.path ?? null,
      detail: spec_.detail,
    });
  }
  const file = artifacts[spec_.file];
  if (!file) throw new Error(`${nodeId} names an artifact that is not built: ${spec_.file}`);
  return envelope({
    exists: true,
    media_type: file.mediaType,
    format: spec_.format,
    path: spec_.path,
    byte_count: file.byteCount,
    sha256: file.sha256,
    url: file.url,
  });
}

/** The stored node row: every column, the hidden ones masked in place, so
 *  Raw and Fields agree on what exists (S15). */
function storedRow(record) {
  const row = { node_id: record.nodeId, type: record.type, generation: "current" };
  for (const field of record.fields ?? []) row[field.key] = field.hidden ? "[nomos:masked]" : field.value;
  return row;
}

function storedEdges(spec, record) {
  const at = latestPublished(spec)?.iso ?? null;
  const rows = [];
  for (const r of record.outgoing ?? []) {
    rows.push({ src: record.nodeId, dst: r.nodeId, relationship: r.relationship, generation: "current", source: record.source, observed_at: at });
  }
  for (const r of record.incoming ?? []) {
    rows.push({ src: r.nodeId, dst: record.nodeId, relationship: r.relationship, generation: "current", source: record.source, observed_at: at });
  }
  return rows;
}

/** `records/raw?id=`: the stored row, the stored edge rows (capped), and the
 *  retained payload inline when it fits, else its head and URL; or the
 *  refusal, with the row and edges still served. */
export function recordRawRead(spec, nodeId, artifacts) {
  const published = latestPublished(spec);
  const record = spec.records.find((r) => r.nodeId === nodeId);
  const envelope = (payload) => ({
    generation_id: published ? published.generationId : "unpublished",
    payload: { node_id: nodeId, ...payload, gap: true },
  });
  if (!record) return envelope({ found: false, row: null, edges: [], edge_total: 0, edge_cap: RAW_EDGE_CAP, payload: null, refusal: { reason: "not_found", policy: null, detail: `no record ${nodeId}` } });
  const edges = storedEdges(spec, record);
  let payload = null;
  let refusal = null;
  if (record.payloadRefused) {
    refusal = { reason: "policy_refused", policy: record.payloadRefused.policy, detail: record.payloadRefused.detail };
  } else {
    const file = recordPayloadFile(record, artifacts);
    if (!file) {
      refusal = {
        reason: "not_retained", policy: null,
        detail: `${record.source ?? "the source"} does not retain the canonical record for ${record.type} records`,
      };
    } else {
      const text = file.bytes.toString("utf8");
      const inline = file.bytes.length <= INLINE_PAYLOAD_LIMIT;
      payload = {
        sha256: sha256Hex(file.bytes),
        byte_count: file.bytes.length,
        inline: inline ? text : null,
        head: inline ? null : text.split("\n").slice(0, RAW_HEAD_LINES).join("\n"),
        line_count: text.replace(/\n$/, "").split("\n").length,
        pinning_basis: PINNING_BASIS,
        url: file.url,
      };
    }
  }
  return envelope({
    found: true,
    row: storedRow(record),
    edges: edges.slice(0, RAW_EDGE_CAP),
    edge_total: edges.length,
    edge_cap: RAW_EDGE_CAP,
    payload,
    refusal,
    masked_paths: [],
    masked_paths_rule: PATH_RULE_LIMIT,
  });
}

// --- ontology rollup ------------------------------------------------------

export function ontologyRollupRead(spec) {
  const published = latestPublished(spec);
  return {
    generation_id: published ? published.generationId : "unpublished",
    payload: {
      deployment: spec.name,
      node_types: spec.recordTypes,
      relationship_types: spec.relationshipTypes,
      node_type_total: spec.recordTypes.length,
      relationship_type_total: spec.relationshipTypes.length,
      count_basis: "published_total_before_access_rules",
    },
  };
}

// --- /viz/api/traces and /viz/api/workflows -------------------------------

export function tracesRead(spec) {
  return { items: spec.traces ?? [] };
}

export function workflowsRead(spec) {
  const items = [...spec.attempts].reverse().slice(0, 24).map((attempt, index) => ({
    trace_id: `trace-${spec.name}-${index}`,
    root_name: "update attempt",
    root_kind: "update",
    status: attempt.status === "published" ? "ok"
      : attempt.status === "running" ? "running" : "error",
    duration_ms: attempt.durationMs,
    started_at: attempt.iso,
    generation_id: attempt.generationId,
    workflow_id: `wf-${spec.name}-${index}`,
    span_count: 5 + (index % 7),
  }));
  return {
    meta: {
      count: items.length,
      workflow_tables_present: spec.runtime.workflowTablesPresent,
      root_kinds: ["update"],
    },
    items: spec.runtime.workflowTablesPresent ? items : [],
  };
}

export { QUEUE_LANES };

// --- one attempt's recorded detail ---------------------------------------
//
// The route `GET /viz/api/console/attempts/detail?id=<full generation id>`,
// composed live by src/okg/substrate/console_views/attempt_detail.py. The
// section shape here is copied from a payload captured verbatim against the
// published lane-d-home-render deployment on 2026-09-21, committed under
// pact/changes/operator-console-live-updates/evidence/. The NUMBERS are the
// mock deployment's own, derived from its spec; the KEYS, the markers and the
// cap are the live composer's, so a view drawn against this fixture meets the
// same shape the product serves.

//: The live composer's per-section row ceiling (attempt_detail.py line 113).
const ROLLUP_ROWS_PER_SECTION = 200;

//: Served by every count section that reads a checkpoint which never
//: published. Its counts are real facts about that checkpoint -- a refused
//: cycle still stages rows -- but they do not describe the graph.
/** The reason code the Python composer sets on a section read off a
 *  checkpoint that never published. Exported so a test can hold it against
 *  the composer's own constant: a generator and a parser that agree on a
 *  wrong code agree with each other forever. */
export const UNPUBLISHED_MARKER_REASON = "counts_describe_an_unpublished_version";

/** Split `total` across `weights` so the parts sum to `total` exactly.
 *  Largest-remainder, ties broken by name, so the result is deterministic. */
function apportion(total, weights) {
  const sum = weights.reduce((acc, w) => acc + w.weight, 0);
  if (sum <= 0 || total <= 0) return weights.map((w) => ({ name: w.name, count: 0 }));
  const raw = weights.map((w) => ({ name: w.name, exact: (total * w.weight) / sum }));
  const parts = raw.map((r) => ({ name: r.name, count: Math.floor(r.exact), rest: r.exact - Math.floor(r.exact) }));
  let left = total - parts.reduce((acc, p) => acc + p.count, 0);
  const order = [...parts].sort((a, b) => (b.rest - a.rest) || a.name.localeCompare(b.name));
  for (const part of order) {
    if (left <= 0) break;
    part.count += 1;
    left -= 1;
  }
  return parts.map((p) => ({ name: p.name, count: p.count }));
}

/** What each version in the history holds, newest first.
 *
 *  The newest version holds the spec's live totals; every older one holds
 *  that minus the deltas the publishes above it added. A blocked attempt
 *  staged nothing in these specs, so it holds what its predecessor holds --
 *  and its sections still carry the unpublished marker, because holding the
 *  same rows is not the same fact as having published them. */
function versionTotals(spec) {
  const ordered = [...spec.attempts].reverse();
  const totals = [];
  let nodes = spec.nodesLive ?? 0;
  let edges = spec.edgesLive ?? 0;
  for (const attempt of ordered) {
    totals.push({ attempt, nodes, edges });
    if (attempt.status === "published") {
      nodes -= attempt.nodesDelta ?? 0;
      edges -= attempt.edgesDelta ?? 0;
    }
  }
  return totals;
}

/** One by-dimension section: this version's counts beside its parent's. */
function countSection(total, parentTotal, weights, parentAvailable) {
  const mine = apportion(total, weights).filter((row) => row.count > 0);
  const parent = new Map(apportion(parentTotal, weights).map((row) => [row.name, row.count]));
  const ordered = [...mine].sort((a, b) => (b.count - a.count) || a.name.localeCompare(b.name));
  return {
    rows: ordered.slice(0, ROLLUP_ROWS_PER_SECTION).map((row) => ({
      name: row.name,
      count: row.count,
      // Null, never zero, when the parent side could not be read: zero is a
      // measured fact about the parent version, null is the absence of one.
      parent_count: parentAvailable ? (parent.get(row.name) ?? 0) : null,
      parent_available: parentAvailable,
    })),
    shown: Math.min(ordered.length, ROLLUP_ROWS_PER_SECTION),
    total: ordered.length,
    capped: ordered.length > ROLLUP_ROWS_PER_SECTION,
    limit: ROLLUP_ROWS_PER_SECTION,
    unavailable: null,
    unpublished_version: null,
  };
}

/** The publication checks that ran.
 *
 *  A deployment with no view producer configured records no
 *  `consistency_gates` key at all -- measured live on 2026-09-21 -- and that
 *  renders as a named marker with NULL counts, never as "0 checks ran, all
 *  passed". */
function checksSection(spec, attempt) {
  const views = (spec.producers ?? []).filter((p) => p.kind === "view");
  if (views.length === 0) {
    return { gates: [], total: null, passed: null, blocked: null,
      unavailable: { reason: "consistency_gates_not_recorded" } };
  }
  const blocked = attempt.status === "blocked";
  const gates = views.map((view) => ({
    blocks_publish: blocked,
    budget_ms: null,
    check_id: `transform.consistency.${view.name}.v4`,
    coverage_basis: "chronos_dirty_edges+incident_dirty_nodes",
    coverage_complete: !blocked,
    elapsed_ms: 173,
    gate_name: "transform_consistency_gate",
    is_probe: false,
    newly_opened_count: 0,
    open_observation_count: 0,
    reason: blocked ? (attempt.blockedReason ?? "publish_blocked") : "no_violations",
    resolved_count: 0,
    result: blocked ? "blocked" : "evaluated",
    run_id: null,
    target_generation_id: attempt.compatId,
    // The live gate's own `details` and `preflight` are withheld with their
    // byte size stated; the fixture carries the same shape so a view cannot
    // be written that reads a value the product never serves.
    withheld: [
      { key: "details", kind: "object", size: 36, withheld_reason: "exception_text" },
      { key: "preflight", kind: "object", size: 12, withheld_reason: "exception_text" },
    ],
  }));
  return {
    gates,
    total: gates.length,
    passed: blocked ? 0 : gates.length,
    blocked: blocked ? gates.length : 0,
    unavailable: null,
  };
}

export function attemptDetailRead(spec, attemptId) {
  const history = versionTotals(spec);
  const index = history.findIndex((row) => row.attempt.generationId === attemptId);
  if (index < 0) return null;
  const { attempt, nodes, edges } = history[index];
  const parent = history[index + 1] ?? null;
  const newer = history[index - 1] ?? null;
  const published = attempt.status === "published";
  const parentAvailable = parent !== null;
  const nodeWeights = (spec.recordTypes ?? []).map((t) => ({ name: t.type, weight: t.count }));
  const edgeWeights = (spec.relationshipTypes ?? []).map((t) => ({ name: t.type, weight: t.count }));
  const sourceWeights = (spec.producers ?? [])
    .filter((p) => p.kind === "source")
    .map((p) => ({ name: `${spec.name}.${p.name}`, weight: p.records ?? 0 }));
  const unpublished = published ? null : { reason: UNPUBLISHED_MARKER_REASON, attempt_status: attempt.status };
  // The composer's REAL key. This wrote the marker into `unavailable`, which
  // the composer never does -- so the generator and the parser agreed on a
  // shape the product does not serve, and the page's sentence was coming
  // from an inference rather than from the read. Checked against the Python
  // constant in attemptLive.test.tsx so the two cannot drift again.
  const withMarker = (section) => ({ ...section, unpublished_version: unpublished });
  const runs = attempt.viewRuns ?? [];
  return {
    generation_id: latestPublished(spec)?.generationId ?? null,
    payload: {
      deployment: spec.name,
      branch: "default",
      attempt_id: attempt.generationId,
      version_published: published,
      attempt: generationsRead(spec).payload.generations.find(
        (row) => row.full_generation_id === attempt.generationId,
      ) ?? null,
      holds: {
        node_count: nodes,
        edge_count: edges,
        // `updated` is the count the list's totals-subtraction delta cannot
        // see: a version whose totals did not move can still have rewritten
        // every row in it.
        nodes: { added: published ? (attempt.nodesDelta ?? 0) : 0, updated: 0, retracted: 0 },
        edges: { added: published ? (attempt.edgesDelta ?? 0) : 0, updated: 0, retracted: 0 },
        // The totals WERE recorded; the marker says which version they
        // describe, and it has its own key rather than sitting in the list
        // of parts that could not be read.
        unavailable: [],
        unpublished_version: unpublished,
      },
      checks: checksSection(spec, attempt),
      runs: { runs, total: runs.length, capped: false, unavailable: null },
      neighbours: {
        newer_attempt_id: newer ? newer.attempt.generationId : null,
        older_attempt_id: parent ? parent.attempt.generationId : null,
        position: index + 1,
        total: history.length,
        parent_generation_id: parent ? parent.attempt.generationId : null,
        parent_source: parent ? "recorded_predecessor" : "no_predecessor_recorded",
      },
      configured: {
        deployment_attestations: { recorded: false, names: [], withheld_count: 0 },
        nomos_policy_bundle_ids: { recorded: true, names: [], withheld_count: 0 },
        excluded_sources: { recorded: true, names: attempt.excludedSources ?? [] },
        enricher_governance_record_count: (spec.producers ?? []).filter((p) => p.kind === "enricher").length,
        catalog_version_id: 4,
      },
      by_record_type: withMarker(countSection(nodes, parent ? parent.nodes : 0, nodeWeights, parentAvailable)),
      by_relationship_type: withMarker(countSection(edges, parent ? parent.edges : 0, edgeWeights, parentAvailable)),
      by_source: withMarker(countSection(nodes, parent ? parent.nodes : 0, sourceWeights, parentAvailable)),
    },
  };
}
