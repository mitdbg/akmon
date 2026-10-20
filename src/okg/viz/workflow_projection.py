"""Project rows of ``okg.telemetry_traces`` into the shape Home's activity
list consumes.

One row per recent workflow execution: its id, a readable root label, status,
duration, start time and span count. Home shows these as "Running now" /
recent pipeline activity alongside the MCP query traces from ``/traces``.

The per-trace span-timeline projection that used to live here (``project_span``,
``project_workflow_scene``, ``focus_workflow_scene``) served the Trace and
Performance views' Gantt/scene rendering and left with them in issue #1924, as
did ``trace_projection.py``.

Pure: parsed rows in, dicts out. Hydration / DB access stays in the endpoint.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional

# Trace root_kinds the workflow list surfaces. mcp_call is excluded here — those
# are the query traces ``/viz/api/traces`` already lists.
WORKFLOW_ROOT_KINDS = ("source_run", "publisher", "embed", "ingest", "job")


def _iso(value: Any) -> Optional[str]:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None


def _norm_status(code: Any) -> str:
    """Map the OTel-ish status_code to a small UI vocabulary."""
    s = str(code or "").lower()
    if s in ("error", "err", "failed", "failure"):
        return "error"
    if s in ("ok", "success", "succeeded"):
        return "ok"
    return "unset"


# A span name is ``source.<deployment>.<source>`` (e.g.
# ``source.okg-workspace.git-history``). The deployment segment is redundant in
# the UI — every node here belongs to the same deployment — so strip it for the
# node label / rail header. Non-source names (``publisher.cycle``, ``mcp.search``)
# don't match and pass through unchanged.
_SOURCE_PREFIX = re.compile(r"^source\.[^.]+\.")


def _short_label(name: Any) -> str:
    """``source.okg-workspace.git-history`` -> ``git-history``; pass other names
    through (so ``publisher.cycle`` stays ``publisher.cycle``)."""
    return _SOURCE_PREFIX.sub("", str(name or ""))


def project_workflow_list_item(row: dict[str, Any]) -> dict[str, Any]:
    """One row of the workflow LIST (recent telemetry_traces).

    ``row`` carries trace_id, root_name, root_kind, status_code, duration_ms,
    started_at, generation_id, workflow_id, and span_count (from the join).
    """
    return {
        "trace_id": row.get("trace_id"),
        "root_name": _short_label(row.get("root_name")),
        "root_kind": row.get("root_kind"),
        "status": _norm_status(row.get("status_code")),
        "duration_ms": row.get("duration_ms"),
        "started_at": _iso(row.get("started_at")),
        "generation_id": row.get("generation_id"),
        # Null today (the join gap); carried so the UI can show "linked to DBOS
        # workflow <id>" once collection-integrity stamps it.
        "workflow_id": row.get("workflow_id"),
        "span_count": int(row.get("span_count") or 0),
    }
