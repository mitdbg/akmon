"""Source Diagnostics composer for the supervisor console.

This is the source-fleet diagnostic read model: one bounded row per source
plus secondary detail for the selected source. It composes existing read-only
surfaces and never runs source-sync poll, reconcile, recovery, or ingest work.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, TYPE_CHECKING

from okg.substrate.console_views.served_text import mask_served_text
from okg.substrate.console_views.source_detail import compose_source_detail
from okg.substrate.console_views.sources import compose_sources
from okg.substrate.console_views.console_readiness import (
    compose_console_readiness,
)
from okg.substrate.mcp.session import Session

if TYPE_CHECKING:  # pragma: no cover - typing only
    from okg.substrate.console_views.console_count_rollup_read import (
        ConsoleCountRollupCache,
    )
from okg.substrate.source_sync_status import source_sync_status_for_source


_LATEST_RUNS_SQL = """
WITH ranked AS (
  SELECT
    deployment,
    source_name,
    registry_source_name,
    run_id,
    status,
    run_mode,
    timing_status,
    timing_unavailable_reason,
    observed_at,
    started_at,
    finished_at,
    input_count,
    output_count,
    nodes_appended,
    edges_appended,
    node_retractions,
    edge_retractions,
    total_ms,
    source_runner_ms,
    adapter_run_ms,
    dedupe_persisted_ms,
    chronos_apply_ms,
    flush_chunks_ms,
    flush_final_ms,
    stream_walk_ms,
    record_set_chronos_verify_ms,
    slowest_phase,
    slowest_phase_ms,
    timings_ms,
    source_stats,
    sync_scope,
    row_number() OVER (
      PARTITION BY registry_source_name
      ORDER BY observed_at DESC NULLS LAST, run_id DESC
    ) AS rn
  FROM okg.v_source_run_diagnostics
  WHERE deployment = %(deployment)s
    AND (
      registry_source_name = ANY(%(source_names)s)
      OR source_name = ANY(%(source_keys)s)
    )
)
SELECT
  source_name,
  registry_source_name,
  run_id,
  status,
  run_mode,
  timing_status,
  timing_unavailable_reason,
  observed_at,
  started_at,
  finished_at,
  input_count,
  output_count,
  nodes_appended,
  edges_appended,
  node_retractions,
  edge_retractions,
    total_ms,
    source_runner_ms,
    adapter_run_ms,
    dedupe_persisted_ms,
    chronos_apply_ms,
    flush_chunks_ms,
    flush_final_ms,
    stream_walk_ms,
    record_set_chronos_verify_ms,
    slowest_phase,
    slowest_phase_ms,
    timings_ms,
    source_stats,
    sync_scope
FROM ranked
WHERE rn <= %(limit)s
ORDER BY registry_source_name, rn
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(v) for v in value]
    return value


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


_HOT_PHASES: tuple[tuple[str, str], ...] = (
    ("adapter_run", "adapter_run_ms"),
    ("dedupe_persisted", "dedupe_persisted_ms"),
    ("chronos_apply", "chronos_apply_ms"),
    ("stream_walk", "stream_walk_ms"),
    ("record_set_chronos_verify", "record_set_chronos_verify_ms"),
)


def _percentile(values: list[float], percentile: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def _run_aggregate(runs: list[dict[str, Any]]) -> dict[str, Any]:
    durations = [
        duration for run in runs
        if (duration := _as_float(run.get("total_ms"))) is not None
    ]
    observed = [
        str(value) for run in runs
        if (value := run.get("observed_at")) not in {None, ""}
    ]
    failed = sum(
        1 for run in runs
        if str(run.get("status") or "").lower() in {"failed", "error", "failed_run"}
    )
    return {
        "run_count": len(runs),
        "failed_run_count": failed,
        "total_nodes_appended": sum(
            _as_int(run.get("nodes_appended")) or 0 for run in runs
        ),
        "total_edges_appended": sum(
            _as_int(run.get("edges_appended")) or 0 for run in runs
        ),
        "p50_duration_ms": _percentile(durations, 0.50),
        "p95_duration_ms": _percentile(durations, 0.95),
        "window_started_at": min(observed) if observed else None,
        "window_ended_at": max(observed) if observed else None,
    }


def _phase_summary(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for phase, key in _HOT_PHASES:
        values = [
            value for run in runs
            if (value := _as_float(run.get(key))) is not None
        ]
        latest = _as_float(runs[0].get(key)) if runs else None
        rows.append({
            "phase": phase,
            "p50_ms": _percentile(values, 0.50),
            "p95_ms": _percentile(values, 0.95),
            "latest_ms": latest,
            "sample_count": len(values),
            "unavailable_reason": None if values else "phase_not_reported",
        })
    rows.sort(
        key=lambda row: (
            row.get("latest_ms") is None,
            -float(row.get("latest_ms") or row.get("p95_ms") or 0),
            str(row.get("phase")),
        )
    )
    return rows


def _selected_run(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    return runs[0] if runs else None


def _latest_runs_by_source(
    conn: Any,
    *,
    deployment_name: str,
    source_rows: list[dict[str, Any]],
    limit: int,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if not source_rows:
        return {}, {
            "status": "empty",
            "reason": "no_sources_to_query",
        }
    source_names = [str(row["name"]) for row in source_rows]
    source_keys = [str(row["source_key"]) for row in source_rows]
    runs: dict[str, list[dict[str, Any]]] = {name: [] for name in source_names}
    with conn.cursor() as cur:
        cur.execute(
            _LATEST_RUNS_SQL,
            {
                "deployment": deployment_name,
                "source_names": source_names,
                "source_keys": source_keys,
                "limit": int(limit),
            },
        )
        cols = [d.name for d in cur.description]
        for raw in cur.fetchall():
            row = _jsonable(dict(zip(cols, raw)))
            name = str(row.get("registry_source_name") or "")
            if not name:
                continue
            runs.setdefault(name, []).append(row)
    return runs, {
        "status": "available",
        # Deliberately no `view` key. This answer is served to a browser, and
        # the name of the database object behind an authority is the server's
        # business: a reader acts on the authority and its reason, never on
        # `okg.v_...`.
        "limit_per_source": int(limit),
    }


def _producer_summary(source_sync: Mapping[str, Any]) -> dict[str, Any]:
    runtime = _mapping(source_sync.get("runtime_liveness"))
    producers = [
        dict(row) for row in runtime.get("producers", [])
        if isinstance(row, Mapping)
    ]
    configured = len(producers)
    healthy = sum(1 for row in producers if row.get("status") == "healthy")
    # The runtime rows are copied from the liveness report, which may carry a
    # producer's settings or last error. They are masked by the one console
    # rule (#2130): a DSN, a credential-named setting, a server path or an
    # exception's text does not reach the browser.
    served, masked = mask_served_text(_jsonable(producers), at="producers")
    return {
        "status": runtime.get("status") or source_sync.get("status"),
        "healthy_count": healthy,
        "configured_count": configured,
        "producers": served,
        "masked_fields": sorted(masked),
    }


def _workflow_summary(source_sync: Mapping[str, Any]) -> dict[str, Any]:
    convergence = _mapping(source_sync.get("convergence"))
    active = _mapping(convergence.get("active_work"))
    status = "active" if active else (
        convergence.get("state") or convergence.get("status") or "not_observed"
    )
    return {
        "status": status,
        "state": convergence.get("state") or convergence.get("status"),
        "active": bool(active),
        "active_work": _jsonable(active),
        "stages": _jsonable(convergence.get("stages") or {}),
        # The code the convergence read recorded FOR THIS SOURCE, beside the
        # state it produced. `_source_convergence_row` decides the two
        # together -- `blocked` with `completed_scope_watermark_stale` is one
        # verdict, not a state and a separate note -- so a page that printed
        # the state and dropped the code said a source was blocked and could
        # not say by what. `blocking_stage` is the stage the same row named as
        # the one that has to clear.
        "reason": convergence.get("reason"),
        "blocking_stage": convergence.get("blocking_stage"),
        "recovery": _recovery_summary(convergence.get("recovery_action")),
    }


def _recovery_summary(action: Any) -> dict[str, Any]:
    """What the convergence read says it takes to bring this source back.

    Only what the read records: the command it names (a source that can be
    re-run by the runtime gets `okg ingest ... --include <source>`), or the
    read's own reason there is none -- `manual_required` records
    `credential_or_manual_source_cannot_be_auto_recovered` and the two
    constraints behind it. Nothing is composed here; a console that cannot
    start a run can still say who can, and with what.
    """
    recorded = _mapping(action)
    constraints = _mapping(recorded.get("constraints"))
    command = recorded.get("command")
    return {
        "command": str(command) if command else None,
        "command_unavailable_reason": (
            None if command
            else str(recorded.get("reason") or "")
            or ("no_recovery_action_recorded" if not recorded
                else "recovery_action_names_no_command")
        ),
        "needs_credential": (
            bool(constraints["credential_required"])
            if "credential_required" in constraints else None
        ),
        "needs_manual_run": (
            bool(constraints["manual_required"])
            if "manual_required" in constraints else None
        ),
    }


def _latest_run_summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        return {
            "status": "not_observed",
            "unavailable_reason": "no_source_run_diagnostics_rows",
        }
    latest = runs[0]
    timing_status = str(latest.get("timing_status") or "unavailable")
    status = "available" if timing_status == "available" else "unavailable"
    return {
        "status": status,
        # `status` above is this summary's OWN availability marker, derived
        # from `timing_status`. It is not the run's outcome. The run's
        # recorded `status` column is carried here under its own name,
        # because a caller that read `status` for the outcome got
        # "available" for a run that failed.
        "run_status": latest.get("status"),
        "run_id": latest.get("run_id"),
        "run_mode": latest.get("run_mode"),
        "observed_at": latest.get("observed_at"),
        "timing_status": timing_status,
        "unavailable_reason": latest.get("timing_unavailable_reason"),
        "input_count": _as_int(latest.get("input_count")),
        "output_count": _as_int(latest.get("output_count")),
        "nodes_appended": _as_int(latest.get("nodes_appended")),
        "edges_appended": _as_int(latest.get("edges_appended")),
        "node_retractions": _as_int(latest.get("node_retractions")),
        "edge_retractions": _as_int(latest.get("edge_retractions")),
        "total_ms": latest.get("total_ms"),
        "slowest_phase": latest.get("slowest_phase"),
        "slowest_phase_ms": latest.get("slowest_phase_ms"),
    }


#: The run outcomes the ingest service records (`ingest/service.py` writes
#: the run's `status` into its stage-timings row as `OK` or `FAIL`), lower
#: cased. Only these two are read as an outcome. Any other value is a recorded
#: word this rule does not classify, so it is neither health nor failure.
RUN_SUCCESS_OUTCOMES = frozenset({"ok"})
RUN_FAILURE_OUTCOMES = frozenset({"fail", "failed"})


def _run_outcome(latest_run: Mapping[str, Any]) -> str | None:
    """The latest run's RECORDED outcome, lower cased, or None.

    `run_status`, never the summary's `status`: that one says whether the
    run's timings were recorded, and a run whose timings were recorded can
    have failed.
    """
    value = latest_run.get("run_status")
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _has_recorded_health(
    *, freshness: Mapping[str, Any], latest_run: Mapping[str, Any],
) -> bool:
    """Whether the runtime recorded a POSITIVE signal for this source.

    Two things count, and each is something the runtime wrote down about this
    source: a watermark it judged `fresh` against the source's declared
    cadence, and a latest run whose recorded outcome is `OK`. Nothing else
    does. A progress row, a live watcher, records in the pinned version, or a
    convergence state of `fresh` given only because the source is not required
    for readiness each say the source exists or once ran -- not that it is
    healthy now (issue #2205).
    """
    if str(freshness.get("status") or "") == "fresh":
        return True
    return _run_outcome(latest_run) in RUN_SUCCESS_OUTCOMES


def _incidents(
    *,
    freshness: Mapping[str, Any],
    workflow: Mapping[str, Any],
    latest_run: Mapping[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    freshness_status = str(freshness.get("status") or "")
    if freshness_status in {"missing", "stale", "blocked"}:
        rows.append({
            "severity": "warn",
            "code": f"source_freshness_{freshness_status}",
            "label": freshness_status,
        })
    if workflow.get("active"):
        rows.append({
            "severity": "info",
            "code": "source_sync_workflow_active",
            "label": "workflow active",
        })
    if latest_run.get("status") in {"unavailable", "not_observed"}:
        rows.append({
            "severity": "info",
            "code": "latest_run_telemetry_unavailable",
            "label": str(latest_run.get("status")),
        })
    if _run_outcome(latest_run) in RUN_FAILURE_OUTCOMES:
        rows.append({
            "severity": "warn",
            "code": "latest_run_failed",
            "label": "latest run failed",
        })
    return rows


def _health_label(
    *,
    freshness: Mapping[str, Any],
    producers: Mapping[str, Any],
    workflow: Mapping[str, Any],
    incidents: list[dict[str, Any]],
    latest_run: Mapping[str, Any],
) -> str:
    """One source's health, from what the runtime recorded for it.

    Worst recorded signal first: a failed latest run, a stale or missing
    watermark, a producer the runtime marked down, any warning incident, work
    in flight. Then `healthy` only on a positive recorded signal
    (`_has_recorded_health`). With none of these the answer is `unknown`:
    the read found nothing wrong and nothing right, and says so.
    """
    freshness_status = str(freshness.get("status") or "")
    producer_status = str(producers.get("status") or "")
    if _run_outcome(latest_run) in RUN_FAILURE_OUTCOMES:
        return "failing"
    if freshness_status in {"missing", "stale", "blocked"}:
        return "stale / recovering" if workflow.get("active") else freshness_status
    if producer_status in {"blocked", "down", "missing", "stale", "unhealthy"}:
        return "degraded"
    if any(row.get("severity") == "warn" for row in incidents):
        return "degraded"
    if workflow.get("active"):
        return "recovering"
    if _has_recorded_health(freshness=freshness, latest_run=latest_run):
        return "healthy"
    return "unknown"


def _operator_health_label(
    *,
    runtime_available: bool,
    freshness: Mapping[str, Any],
    producers: Mapping[str, Any],
    workflow: Mapping[str, Any],
    incidents: list[dict[str, Any]],
    latest_run: Mapping[str, Any],
    contribution: Mapping[str, Any],
) -> str:
    """The health `console/source-diagnostics` and `console/producers` serve.

    Without the runtime read there is no watermark and no liveness, so only
    the latest run's recorded outcome can decide. Records in the pinned
    version (`contribution`) are deliberately NOT a signal: they say the
    source ran at some point, which is what a progress row says, and calling
    that healthy is the defect this rule replaces (issue #2205). The argument
    stays so a caller cannot mistake its absence for an oversight.
    """
    del contribution  # Records in the graph are not an outcome; see above.
    if runtime_available:
        return _health_label(
            freshness=freshness,
            producers=producers,
            workflow=workflow,
            incidents=incidents,
            latest_run=latest_run,
        )
    if _run_outcome(latest_run) in RUN_FAILURE_OUTCOMES:
        return "failing"
    if any(row.get("severity") == "warn" for row in incidents):
        return "degraded"
    if _run_outcome(latest_run) in RUN_SUCCESS_OUTCOMES:
        return "healthy"
    return "unknown"


def _selected_source_name(
    requested: str | None,
    sources: list[dict[str, Any]],
    *,
    deployment_name: str,
) -> str | None:
    if requested:
        candidates = [requested]
        prefix = f"{deployment_name}."
        if requested.startswith(prefix):
            candidates.append(requested[len(prefix):])
        if "." in requested:
            candidates.append(requested.rsplit(".", 1)[-1])
        for candidate in dict.fromkeys(candidates):
            for row in sources:
                if candidate in {row.get("name"), row.get("source_key")}:
                    return str(row.get("name"))
        return requested
    for row in sources:
        health = str(row.get("health", {}).get("status") or "")
        # `unknown` is not a problem to open first: it is the read saying it
        # has no outcome, which is the ordinary state of most sources.
        if health not in {"healthy", "unknown", ""}:
            return str(row.get("name"))
    return str(sources[0].get("name")) if sources else None


def compose_source_diagnostics(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path,
    dsn: str,
    session: Session,
    selected_source: str | None = None,
    source_limit: int = 80,
    run_limit: int = 5,
    include_runtime: bool = True,
    run_scope: str = "all",
    count_rollup: ConsoleCountRollupCache | None = None,
) -> dict[str, Any]:
    """Return the source-fleet-first diagnostics payload."""
    source_limit = max(1, min(int(source_limit), 200))
    run_limit = max(1, min(int(run_limit), 50))
    if run_scope not in {"all", "selected", "none"}:
        raise ValueError("run_scope must be one of: all, selected, none")
    omitted_data: list[dict[str, Any]] = []
    read_authority: dict[str, Any] = {
        "generation_id": session.generation_id,
        "source_contribution": {"status": "available"},
        "source_run_diagnostics": {"status": "not_requested"},
        "runtime_readiness": {"status": "not_requested"},
        "selected_progress": {"status": "not_requested"},
    }

    contributions = compose_sources(
        conn,
        deployment_name=deployment_name,
        session=session,
        count_rollup=count_rollup,
    )
    contribution_rows = list(contributions.get("sources") or [])[:source_limit]
    requested_selected_name = (
        _selected_source_name(
            selected_source,
            contribution_rows,
            deployment_name=deployment_name,
        )
        if selected_source is not None or run_scope != "none"
        else None
    )

    if include_runtime:
        try:
            # The console's OWN readiness, not `okg dogfood status`. This
            # page prints three things from readiness -- each source's sync
            # convergence, its freshness, and the published generation -- and
            # the full composition spent 21 seconds of an okg-workspace read
            # on checks none of them reaches (the Nomos source-lineage report
            # at 15.4 s, the dead-letter scan at 5.6 s). The narrower read
            # carries the same `details.readiness` keys, so nothing below
            # changes, and it NAMES every check it declined so the page can
            # say that this is not a release gate.
            runtime = compose_console_readiness(
                deployment_name=deployment_name,
                deployment_dir=deployment_dir,
                dsn=dsn,
                session=session,
            )
            read_authority["runtime_readiness"] = {
                "status": "available",
                "basis": runtime.get("basis"),
                "checks_run": runtime.get("checks_run"),
                "checks_not_run": runtime.get("checks_not_run"),
                "checks_not_run_count": runtime.get("checks_not_run_count"),
                "checks_not_run_command": runtime.get("checks_not_run_command"),
                # What each declined probe would have said, per probe, from
                # the console read's own authority block: a probe that failed
                # is named here rather than reading as a pass by omission.
                "probes": runtime.get("read_authority"),
            }
        except Exception as exc:  # noqa: BLE001 - surface as telemetry failure.
            runtime = {}
            read_authority["runtime_readiness"] = {
                "status": "unavailable",
                "reason": "console_readiness_composer_failed",
                # The class, never the message. This composer dials the
                # runtime from a DSN, so `str(exc)` is a driver error carrying
                # the host it reached for and, on the usual spelling, the DSN
                # with its password -- into a payload a browser renders. The
                # reason names the authority and the class says how it failed;
                # the message belongs in the server log.
                "error_type": type(exc).__name__,
            }
            omitted_data.append({
                "authority": "runtime_readiness",
                "reason": "console_readiness_composer_failed",
                # What the failure costs the rows, not which rows went
                # missing: every source is still listed. Without this a reader
                # cannot tell a runtime that records no watermark from a
                # runtime this read never reached -- both leave every row
                # `not_evaluated` with a null state, in the same words.
                "affects": "source_runtime_state",
            })
    else:
        runtime = {}

    readiness = _mapping(_mapping(runtime.get("details")).get("readiness"))
    source_sync_status = _mapping(readiness.get("source_sync_status"))

    run_rows = contribution_rows
    if run_scope == "selected":
        run_rows = [
            row for row in contribution_rows
            if row.get("name") == requested_selected_name
        ]
    if run_scope == "none":
        runs_by_source = {str(row["name"]): [] for row in contribution_rows}
    else:
        try:
            queried_runs, run_authority = _latest_runs_by_source(
                conn,
                deployment_name=deployment_name,
                source_rows=run_rows,
                limit=run_limit,
            )
            runs_by_source = {
                str(row["name"]): queried_runs.get(str(row["name"]), [])
                for row in contribution_rows
            }
            read_authority["source_run_diagnostics"] = {
                **run_authority,
                "scope": run_scope,
            }
        except Exception as exc:  # noqa: BLE001 - name the failed authority.
            runs_by_source = {str(row["name"]): [] for row in contribution_rows}
            read_authority["source_run_diagnostics"] = {
                "status": "unavailable",
                "scope": run_scope,
                "reason": "source_run_diagnostics_query_failed",
                # The class, never the message, for the same reason the
                # runtime branch above gives. And no `view` key: the database
                # object behind an authority is the server's business, not a
                # string a browser is handed.
                "error_type": type(exc).__name__,
            }
            omitted_data.append({
                "authority": "source_run_diagnostics",
                "reason": "source_run_diagnostics_query_failed",
                # As above: the rows are all still listed, but none of them
                # carries a last run, so "never run" on this page would be
                # this failed query rather than the producer's history.
                "affects": "source_latest_run",
            })

    sources: list[dict[str, Any]] = []
    active_workflow_count = 0
    stale_count = 0
    degraded_count = 0
    unknown_count = 0
    for contribution in contribution_rows:
        name = str(contribution.get("name") or "")
        runtime_available = bool(source_sync_status)
        sync = (
            source_sync_status_for_source(source_sync_status, name)
            if runtime_available
            else {
                "source_name": name,
                "status": "not_evaluated",
                "source_freshness": {"status": "not_evaluated"},
                "runtime_liveness": {},
                "convergence": {},
            }
        )
        freshness = _mapping(sync.get("source_freshness"))
        producers = _producer_summary(sync)
        workflow = _workflow_summary(sync)
        latest_runs = runs_by_source.get(name, [])
        latest_run = _latest_run_summary(latest_runs)
        row_incidents = _incidents(
            freshness=freshness,
            workflow=workflow,
            latest_run=latest_run,
        )
        health = _operator_health_label(
            runtime_available=runtime_available,
            freshness=freshness,
            producers=producers,
            workflow=workflow,
            incidents=row_incidents,
            latest_run=latest_run,
            contribution=contribution,
        )
        if workflow.get("active"):
            active_workflow_count += 1
        if str(freshness.get("status") or "") == "stale":
            stale_count += 1
        if health == "unknown":
            unknown_count += 1
        elif health != "healthy":
            degraded_count += 1
        sources.append({
            "name": name,
            "source_key": contribution.get("source_key"),
            "in_registry": bool(contribution.get("in_registry")),
            "health": {
                "status": health,
                "incident_count": len(row_incidents),
            },
            "freshness": _jsonable(freshness),
            "producers": producers,
            "workflow": workflow,
            "contribution": {
                "authority": "compose_sources",
                "horizon": "pinned_generation",
                "total_facts": contribution.get("total_facts"),
                "total_edge_facts": contribution.get("total_edge_facts"),
                "fact_counts_available": contribution.get("fact_counts_available"),
                "fact_counts_unavailable_reason": contribution.get(
                    "fact_counts_unavailable_reason"
                ),
                "records_in_gen": contribution.get("records_in_gen", 0),
                "edges_in_gen": contribution.get("edges_in_gen", 0),
                "last_sync_age": contribution.get("last_sync_age"),
            },
            "latest_run": latest_run,
            "incidents": row_incidents,
        })

    selected_name = requested_selected_name
    if selected_name is None and run_scope != "none":
        selected_name = _selected_source_name(
            selected_source,
            sources,
            deployment_name=deployment_name,
        )
    selected_row = next(
        (row for row in sources if row.get("name") == selected_name),
        None,
    )
    selected: dict[str, Any] | None = None
    if selected_name is not None:
        try:
            detail = compose_source_detail(
                conn,
                deployment_name=deployment_name,
                session=session,
                source_name=selected_name,
            )
            read_authority["selected_progress"] = {"status": "available"}
        except Exception as exc:  # noqa: BLE001
            detail = {
                "source_name": selected_name,
                # Unknown, not "no": the read that would say failed.
                "sync_progress_recorded": None,
                "poll": None,
                "poll_unreadable_reason": None,
                "progress": [],
                "unavailable_reason": "source_progress_read_failed",
                # The class, never the message; see the runtime branch above.
                "error_type": type(exc).__name__,
            }
            read_authority["selected_progress"] = {
                "status": "unavailable",
                "reason": "source_progress_read_failed",
                "error_type": type(exc).__name__,
            }
        if selected_row is None:
            omitted_data.append({
                "authority": "source_contribution",
                "reason": "selected_source_not_found",
                "source_name": selected_name,
            })
        selected = {
            "source_name": selected_name,
            "found": selected_row is not None,
            "row": selected_row,
            "source_progress": _jsonable(detail),
            "run_history": runs_by_source.get(selected_name, []),
            "run_summary": _run_aggregate(runs_by_source.get(selected_name, [])),
            "phase_summary": _phase_summary(runs_by_source.get(selected_name, [])),
            "selected_run": _selected_run(runs_by_source.get(selected_name, [])),
            "omitted_data": [
                row for row in omitted_data
                if row.get("source_name") in {None, selected_name}
            ],
        }

    return {
        "schema": "okg.console.source_diagnostics.v1",
        "deployment": deployment_name,
        "generation_id": session.generation_id,
        "selected_source": selected_name,
        "read_authority": read_authority,
        "summary": {
            "source_count": len(sources),
            "degraded_count": degraded_count,
            # Sources with no recorded outcome, counted apart from the
            # degraded ones: "the read does not know" is not a fault.
            "unknown_count": unknown_count,
            "stale_count": stale_count,
            "active_workflow_count": active_workflow_count,
            "omitted_data_count": len(omitted_data),
        },
        "sources": sources,
        "selected": selected,
        "omitted_data": omitted_data,
    }
