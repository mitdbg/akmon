"""Read-only native-view diagnostics for the operator Health workspace.

Serves no server path and no exception text (#2130): the registry is named by
its file name with ``registry_path_unavailable_reason``, and a run's recorded
error goes through the same filter the generations read uses, which keeps the
kind of failure and withholds its message and traceback.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.console_views.generations import _safe_view_run_error
from okg.substrate.console_views.served_text import SERVER_PATH_NOT_SERVED
from okg.substrate.console_views.today_workspace import _humanize_age
from okg.substrate.mcp.session import Session
from okg.substrate.views.registry import load_views_registry


_VIEW_RUNS_SQL = """
SELECT
  d.definition_id,
  d.logical_key,
  r.run_id,
  r.target_generation_id,
  r.status,
  r.trigger_kind,
  r.metrics,
  r.error,
  r.started_at,
  r.completed_at,
  (
    SELECT sum(s.row_count)
      FROM okg.derived_output_manifests m
      JOIN okg.derived_output_slots s ON s.manifest_id = m.manifest_id
     WHERE m.run_id = r.run_id
  ) AS row_count
FROM okg.derived_artifact_definitions d
JOIN LATERAL (
  SELECT run.*
    FROM okg.derived_artifact_runs run
   WHERE run.definition_id = d.definition_id
   ORDER BY run.completed_at DESC NULLS LAST, run.started_at DESC
   LIMIT %(run_limit)s
) r ON TRUE
WHERE d.deployment_name = %(deployment)s
  AND d.artifact_kind = 'view'
ORDER BY d.definition_id, r.completed_at DESC NULLS LAST, r.started_at DESC
"""


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def _elapsed_ms(row: Mapping[str, Any]) -> float | None:
    metrics = row.get("metrics")
    if isinstance(metrics, Mapping):
        for key in ("elapsed_ms", "scheduler_elapsed_ms", "total_ms"):
            value = metrics.get(key)
            if isinstance(value, int | float) and not isinstance(value, bool):
                return float(value)
    started = row.get("started_at")
    completed = row.get("completed_at")
    if isinstance(started, datetime) and isinstance(completed, datetime):
        return max(0.0, (completed - started).total_seconds() * 1000)
    return None


def _status(run: Mapping[str, Any] | None) -> str:
    value = str((run or {}).get("status") or "")
    if value in {"completed", "carried_forward"}:
        return "healthy"
    if value in {"failed", "blocked"}:
        return "failing"
    return "unknown"


def _run_payload(row: Mapping[str, Any]) -> dict[str, Any]:
    completed_at = row.get("completed_at")
    observed_at = completed_at or row.get("started_at")
    return {
        "run_id": str(row.get("run_id") or ""),
        "generation_id": row.get("target_generation_id"),
        "status": row.get("status"),
        "trigger_kind": row.get("trigger_kind"),
        "observed_at": _jsonable(observed_at),
        "age": _humanize_age(observed_at) if isinstance(observed_at, datetime) else None,
        "row_count": (
            int(row["row_count"])
            if row.get("row_count") is not None
            else None
        ),
        "total_ms": _elapsed_ms(row),
        "metrics": _jsonable(row.get("metrics") or {}),
        # The kind of failure only. `sql_gate.py` and `ir_executor.py` write
        # `str(exc)` and a traceback here, which name paths and the database.
        "error": _safe_view_run_error(_jsonable(row.get("error"))),
    }


def compose_view_diagnostics(
    conn: Any,
    *,
    deployment_name: str,
    deployment_dir: Path,
    session: Session,
    selected_view: str | None = None,
    run_limit: int = 20,
) -> dict[str, Any]:
    """Return registry-complete view inventory plus bounded run history."""
    run_limit = max(1, min(int(run_limit), 50))
    entries = load_views_registry(deployment_dir)
    histories: dict[str, list[dict[str, Any]]] = {}
    with conn.cursor() as cur:
        cur.execute(
            _VIEW_RUNS_SQL,
            {"deployment": deployment_name, "run_limit": run_limit},
        )
        columns = [description.name for description in cur.description]
        for raw in cur.fetchall():
            row = dict(zip(columns, raw))
            histories.setdefault(str(row["definition_id"]), []).append(row)

    views: list[dict[str, Any]] = []
    for entry in entries:
        runs = histories.get(entry.view_def_id, [])
        latest = runs[0] if runs else None
        status = _status(latest)
        latest_payload = _run_payload(latest) if latest else None
        views.append({
            "name": entry.view_def_id,
            "display_name": entry.view_name,
            "producer_key": f"view:{entry.view_def_id}",
            "status": status,
            "state": "materialized" if latest else "registered",
            "last_run_age": (latest_payload or {}).get("age"),
            "latest_issue": (
                "Latest materialization failed."
                if status == "failing"
                else None if status == "healthy"
                else "No materialization run is recorded."
            ),
            "next_check": (
                "Inspect the latest materialization error."
                if status == "failing"
                else "No action needed."
                if status == "healthy"
                else "Confirm this view has materialized at least once."
            ),
            "contribution": {
                "authority": "derived artifact manifest",
                "horizon": "pinned_generation",
                "records_in_gen": (latest_payload or {}).get("row_count"),
                "edges_in_gen": None,
                "total_facts": (latest_payload or {}).get("row_count"),
                "total_edge_facts": None,
            },
            "latest_run": latest_payload,
            "run_history": [_run_payload(run) for run in runs],
        })

    selected_name = selected_view
    selected_row = next(
        (row for row in views if row["name"] == selected_name or row["producer_key"] == selected_name),
        None,
    )
    selected = None
    if selected_name is not None:
        definition_id = str((selected_row or {}).get("name") or selected_name).removeprefix("view:")
        run_history = [_run_payload(row) for row in histories.get(definition_id, [])]
        selected = {
            "view_name": definition_id,
            "found": selected_row is not None,
            "row": selected_row,
            "selected_run": run_history[0] if run_history else None,
            "run_history": run_history,
            "omitted_data": [] if run_history else [{
                "authority": "derived artifact runs",
                "reason": "no_view_materialization_rows",
            }],
        }

    return {
        "generation_id": session.generation_id,
        "views": views,
        "selected": selected,
        "read_authority": {
            # The file name, not the path: the path names the server's
            # account. `okg` on the server resolves it from the deployment.
            "registry": "views_registry.yaml",
            "registry_path_unavailable_reason": SERVER_PATH_NOT_SERVED,
            "runs": "okg.derived_artifact_runs",
            "generation_id": session.generation_id,
        },
    }


__all__ = ["compose_view_diagnostics"]
