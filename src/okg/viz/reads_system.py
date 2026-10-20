"""System Overview reads — operator-facing aggregates.

One function per panel section. Each takes a ``Connection`` and the
deployment name; queries are deployment-scoped and NOT generation-pinned
(the system view asks "what is the latest", not "what was true in gen N").

Aggregates that the CLI dashboards already expose are duplicated here so
the viz `system` mode does not have to shell out. SQL is the source of
truth — keep these functions narrow translators, not business logic.
"""

from __future__ import annotations

import inspect
import json
import logging
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg import sql as psycopg_sql

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.runtime_queue_health import (
    queue_status_counts_for_viz,
    summarize_runtime_queue_health,
)

logger = logging.getLogger(__name__)


def _ts(v: Any) -> str | None:
    return v.isoformat() if v is not None else None


def _parse_ts(v: str | None) -> datetime | None:
    if not v:
        return None
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        return None


def _is_explainable(statement: str) -> bool:
    prefix = statement.lstrip().lower()
    if "pg_database_size(" in prefix:
        return False
    return prefix.startswith("select ") or prefix.startswith("with ")


def _extract_plan_doc(raw: Any) -> dict[str, Any]:
    if isinstance(raw, str):
        raw = json.loads(raw)
    if isinstance(raw, list) and raw:
        raw = raw[0]
    return raw if isinstance(raw, dict) else {}


def _buffer_summary(plan: dict[str, Any]) -> dict[str, int]:
    fields = (
        "Shared Hit Blocks",
        "Shared Read Blocks",
        "Shared Dirtied Blocks",
        "Shared Written Blocks",
        "Local Hit Blocks",
        "Local Read Blocks",
        "Temp Read Blocks",
        "Temp Written Blocks",
    )
    node = plan.get("Plan") if isinstance(plan.get("Plan"), dict) else {}
    return {f: int(node.get(f, 0) or 0) for f in fields}


def _caller_name() -> str:
    frame = inspect.currentframe()
    try:
        caller = frame.f_back.f_back if frame and frame.f_back else None
        current = caller
        while current is not None:
            name = current.f_code.co_name
            if name.startswith("fetch_") and name != "fetch_snapshot":
                return name
            current = current.f_back
        return caller.f_code.co_name if caller else "unknown"
    finally:
        del frame


class _ProfilingConnection:
    def __init__(self, conn: psycopg.Connection, max_execution_ms: float):
        self._conn = conn
        self.max_execution_ms = max_execution_ms
        self.profiles: list[dict[str, Any]] = []
        self._call_counts: dict[str, int] = {}

    def cursor(self, *args: Any, **kwargs: Any) -> "_ProfilingCursor":
        return _ProfilingCursor(self, self._conn.cursor(*args, **kwargs))

    @property
    def unprofiled_connection(self) -> psycopg.Connection:
        """The wrapped connection, for reads through the Chronos graph store
        (the source count rollup), which needs a real connection. Those reads
        are not profiled; the rollup read is an as-of stab of a small table."""
        return self._conn

    def _next_label(self, caller: str) -> str:
        n = self._call_counts.get(caller, 0) + 1
        self._call_counts[caller] = n
        return f"{caller}#{n}"

    def _record(self, caller: str, statement: str, plan: dict[str, Any]) -> None:
        execution_ms = float(plan.get("Execution Time", 0.0) or 0.0)
        planning_ms = float(plan.get("Planning Time", 0.0) or 0.0)
        budget_exempt = (
            "pg_database_size(" in statement
            or "pg_total_relation_size(" in statement
        )
        self.profiles.append({
            "label": self._next_label(caller),
            "caller": caller,
            "execution_ms": execution_ms,
            "planning_ms": planning_ms,
            "total_ms": execution_ms + planning_ms,
            "under_budget": (
                budget_exempt or execution_ms <= self.max_execution_ms
            ),
            "budget_exempt": budget_exempt,
            "buffers": _buffer_summary(plan),
            "statement": " ".join(statement.split())[:320],
        })


class _ProfilingCursor:
    def __init__(self, owner: _ProfilingConnection, cursor: Any):
        self._owner = owner
        self._cursor = cursor

    def __enter__(self) -> "_ProfilingCursor":
        self._cursor.__enter__()
        return self

    def __exit__(self, *exc_info: Any) -> Any:
        return self._cursor.__exit__(*exc_info)

    def execute(self, query: Any, params: Any = None) -> Any:
        # A query composed with psycopg.sql (the shared llm_usage_summary)
        # renders to its SQL text here; str() of it is only a repr.
        if isinstance(query, psycopg_sql.Composable):
            statement = query.as_string(self._cursor).strip()
        else:
            statement = str(query).strip()
        if _is_explainable(statement):
            self._cursor.execute(
                f"EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) {statement}",
                params,
            )
            row = self._cursor.fetchone()
            plan = _extract_plan_doc(row[0] if row else None)
            self._owner._record(_caller_name(), statement, plan)
        return self._cursor.execute(query, params)

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        return self._cursor.fetchall()


def profile_snapshot_queries(
    conn: psycopg.Connection,
    deployment: str,
    *,
    max_execution_ms: float = 50.0,
) -> list[dict[str, Any]]:
    """Run the system snapshot through EXPLAIN ANALYZE.

    The profiler is intended for CI/fixture checks. It executes each
    snapshot SELECT once under ``EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)``
    and then runs the real query unchanged so the existing read functions
    still assemble the normal payload.
    """
    profiled = _ProfilingConnection(conn, max_execution_ms)
    fetch_snapshot(profiled, deployment)  # type: ignore[arg-type]
    return profiled.profiles


def assert_snapshot_query_budget(
    conn: psycopg.Connection,
    deployment: str,
    *,
    max_execution_ms: float = 50.0,
) -> list[dict[str, Any]]:
    profiles = profile_snapshot_queries(
        conn,
        deployment,
        max_execution_ms=max_execution_ms,
    )
    slow = [p for p in profiles if not p["under_budget"]]
    if slow:
        detail = "\n".join(
            f"{p['label']}: {p['execution_ms']:.3f} ms :: {p['statement']}"
            for p in slow
        )
        raise AssertionError(
            "system snapshot query budget exceeded "
            f"({max_execution_ms:.1f} ms):\n{detail}"
        )
    return profiles


# ---------------------------------------------------------------------------
# Panel A — Database
# ---------------------------------------------------------------------------


def fetch_generation(conn: psycopg.Connection, deployment: str) -> dict[str, Any]:
    """Latest published + any pending + most recent blocked for the
    deployment. Mirrors `okg status`'s publishing snapshot."""
    out: dict[str, Any] = {"published": None, "pending": None, "blocked": None}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT generation_id, published_at, catalog_version_id,
                   correlation_id
              FROM okg.graph_generations
             WHERE deployment_name = %s AND status = 'published'
          ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment,),
        )
        row = cur.fetchone()
        if row:
            out["published"] = {
                "generation_id": int(row[0]),
                "published_at": _ts(row[1]),
                "catalog_version_id": int(row[2]),
                "correlation_id": row[3],
            }
        cur.execute(
            """
            SELECT generation_id, created_at, catalog_version_id,
                   correlation_id
              FROM okg.graph_generations
             WHERE deployment_name = %s AND status = 'pending'
          ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment,),
        )
        row = cur.fetchone()
        if row:
            out["pending"] = {
                "generation_id": int(row[0]),
                "created_at": _ts(row[1]),
                "catalog_version_id": int(row[2]),
                "correlation_id": row[3],
            }
        cur.execute(
            """
            SELECT generation_id, created_at, blocked_reason
              FROM okg.graph_generations
             WHERE deployment_name = %s AND status = 'blocked'
          ORDER BY generation_id DESC
             LIMIT 1
            """,
            (deployment,),
        )
        row = cur.fetchone()
        if row:
            out["blocked"] = {
                "generation_id": int(row[0]),
                "created_at": _ts(row[1]),
                "reason": row[2],
            }
    return out


def fetch_catalog(conn: psycopg.Connection) -> dict[str, Any]:
    """Latest applied catalog + any pending. Counts of subtypes /
    narrowings at the active version."""
    out: dict[str, Any] = {
        "applied": None, "pending": None,
        "subtypes": 0, "narrowings": 0,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT catalog_version_id, applied_at
              FROM okg.catalog_versions
             WHERE status = 'applied'
          ORDER BY catalog_version_id DESC
             LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            out["applied"] = {
                "catalog_version_id": int(row[0]),
                "applied_at": _ts(row[1]),
            }
            cv = int(row[0])
            cur.execute(
                "SELECT count(*) FROM okg.subtypes "
                "WHERE catalog_version_id = %s",
                (cv,),
            )
            out["subtypes"] = int(cur.fetchone()[0])
            cur.execute(
                "SELECT count(*) FROM okg.narrowings "
                "WHERE catalog_version_id = %s",
                (cv,),
            )
            out["narrowings"] = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT catalog_version_id
              FROM okg.catalog_versions
             WHERE status = 'pending'
          ORDER BY catalog_version_id DESC
             LIMIT 1
            """
        )
        row = cur.fetchone()
        if row:
            out["pending"] = {"catalog_version_id": int(row[0])}
    return out


def fetch_live_graph(
    conn: psycopg.Connection, generation_id: int | None,
) -> dict[str, Any]:
    """Total node and edge counts at the current published generation,
    plus the top-12 subtypes by count."""
    out: dict[str, Any] = {
        "node_count": 0, "edge_count": 0, "top_subtypes": [],
    }
    if generation_id is None:
        return out
    try:
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        out["node_count"] = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
            or 0
        )
        out["edge_count"] = int(
            graph.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
            or 0
        )
        out["top_subtypes"] = [
            {"subtype": r["subtype"], "count": int(r["n"])}
            for r in graph.query(
                """
                SELECT subtype, count(*)::int AS n
                  FROM okg.graph_nodes
              GROUP BY subtype
              ORDER BY n DESC
                 LIMIT 12
                """
            )
        ]
        out.update({
            "status": "available",
            "freshness": "fresh",
            "source": "chronos_generation",
            "scope": {"generation_id": generation_id},
        })
        return out
    except Exception:
        logger.warning(
            "Chronos live graph snapshot read failed; returning explicitly "
            "degraded current-table compatibility counts",
            exc_info=True,
        )
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.graph_nodes")
        out["node_count"] = int(cur.fetchone()[0])
        cur.execute("SELECT count(*) FROM okg.graph_edges")
        out["edge_count"] = int(cur.fetchone()[0])
        cur.execute(
            """
            SELECT subtype, count(*)
              FROM okg.graph_nodes
          GROUP BY subtype
          ORDER BY 2 DESC
             LIMIT 12
            """
        )
        out["top_subtypes"] = [
            {"subtype": r[0], "count": int(r[1])} for r in cur.fetchall()
        ]
    out.update({
        "status": "degraded",
        "cause": "chronos_generation_read_failed",
        "omitted_work": ["generation_pinned_live_graph_counts"],
        "scope": {"generation_id": generation_id},
        "retryable": True,
        "freshness": "unavailable",
        "source": "current_graph_tables",
    })
    return out


def fetch_sources(
    conn: psycopg.Connection, generation_id: int | str | None,
) -> dict[str, Any]:
    """Per-source watermark + row counts in the published generation.
    Mirrors `_format_status_dashboard`'s sources block. ``counts_error``
    names why the counts are None when the generation has no maintained
    count rollup."""
    from okg.substrate.source_live_counts import source_published_rows

    rows, counts_error = source_published_rows(
        conn,
        generation_id=generation_id,
        graph_conn=getattr(conn, "unprofiled_connection", None),
    )
    return {
        "rows": [
            {
                **row,
                "last_reconcile_at": _ts(row["last_reconcile_at"]),
                "projection_cursor": (
                    int(row["projection_cursor"])
                    if row["projection_cursor"] is not None else None
                ),
            }
            for row in rows
        ],
        "counts_error": counts_error,
    }


def fetch_storage(conn: psycopg.Connection) -> dict[str, Any]:
    """Total DB size + per-table size for the major operator-facing
    tables. Uses pg_total_relation_size for honest disk numbers."""
    tables = [
        "okg.graph_nodes", "okg.graph_edges",
        "okg.node_facts", "okg.edge_facts",
        "okg.llm_calls", "okg.mcp_trace_events",
    ]
    out: dict[str, Any] = {"db_size_bytes": 0, "tables": []}
    with conn.cursor() as cur:
        cur.execute("SELECT pg_database_size(current_database())")
        out["db_size_bytes"] = int(cur.fetchone()[0])
        for t in tables:
            try:
                cur.execute(f"SELECT pg_total_relation_size('{t}')")
                size = int(cur.fetchone()[0])
            except psycopg.errors.UndefinedTable:
                size = 0
            out["tables"].append({"table": t, "size_bytes": size})
    out["tables"].sort(key=lambda r: r["size_bytes"], reverse=True)
    return out


# ---------------------------------------------------------------------------
# Panel B — Inflight
# ---------------------------------------------------------------------------


def fetch_queue(conn: psycopg.Connection) -> dict[str, Any]:
    """Runtime queue depth by status, plus oldest active age."""
    with conn.cursor() as cur:
        summary = summarize_runtime_queue_health(cur)
    return {
        "by_status": queue_status_counts_for_viz(summary),
        "oldest_queued_age_s": summary["oldest_active_age_seconds"],
        "source": summary["source"],
        "ok": summary["ok"],
        "status": summary["status"],
        "runtime_queue_health": summary["runtime_queue_health"],
        "runtime_status_counts": summary["runtime_status_counts"],
        **(
            {"reason": summary["reason"]}
            if "reason" in summary else {}
        )
    }


def fetch_projection_backlog(conn: psycopg.Connection) -> dict[str, Any]:
    """Projection-pending row count + oldest age."""
    out: dict[str, Any] = {"pending": 0, "oldest_age_s": None}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),
                   extract(epoch FROM (now() - min(created_at)))
              FROM okg.projection_pending
            """
        )
        row = cur.fetchone()
        if row:
            out["pending"] = int(row[0])
            out["oldest_age_s"] = float(row[1]) if row[1] is not None else None
    return out


def fetch_enrichers(
    conn: psycopg.Connection, deployment: str,
) -> list[dict[str, Any]]:
    """Per-enricher progress for the deployment."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT enricher_name, fact_watermark, edge_watermark,
                   generation_id, last_run_at, last_status
              FROM okg.enricher_progress
             WHERE deployment_name = %s
          ORDER BY last_run_at DESC
            """,
            (deployment,),
        )
        rows = cur.fetchall()
    return [
        {
            "enricher_name": r[0],
            "fact_watermark": int(r[1]),
            "edge_watermark": int(r[2]),
            "generation_id": int(r[3]),
            "last_run_at": _ts(r[4]),
            "last_status": r[5],
        }
        for r in rows
    ]


# ---------------------------------------------------------------------------
# Panel C — Operations
# ---------------------------------------------------------------------------


def fetch_doctor(conn: psycopg.Connection) -> dict[str, Any]:
    """Same checks as `okg doctor` (default mode): required extensions
    and required roles. Pure read-only — no privilege escalation."""
    from okg.substrate.db.migrate import REQUIRED_EXTENSIONS

    out: dict[str, Any] = {
        "extensions": [], "roles": [], "ok": True,
    }
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extname, extversion FROM pg_extension "
            "WHERE extname = ANY(%s)",
            (list(REQUIRED_EXTENSIONS.keys()),),
        )
        installed = dict(cur.fetchall())
        for name in REQUIRED_EXTENSIONS:
            present = name in installed
            out["extensions"].append({
                "name": name,
                "installed": present,
                "version": installed.get(name),
            })
            if not present:
                out["ok"] = False
        for role in ("app_rw", "app_ro", "catalog_admin"):
            cur.execute(
                "SELECT 1 FROM pg_roles WHERE rolname = %s", (role,),
            )
            present = cur.fetchone() is not None
            out["roles"].append({"name": role, "present": present})
            if not present:
                out["ok"] = False
    return out


def fetch_llm_spend(
    conn: psycopg.Connection, deployment: str | None,
) -> dict[str, Any]:
    """LLM spend rollup for the last 24 h. Mirrors
    `okg metrics --llm --since 24h`.

    Reads through the shared ``llm_usage_summary`` (issue #2573 slice 2,
    closes #1739): a total, model or caller row with an unpriced call carries
    ``unpriced_calls`` and ``priced_cost_usd``, and its ``cost_usd`` is an
    ``Unknown`` that the viz read exit turns into ``null`` with the reason
    under ``unknown_reasons`` -- never a ``$0`` that reads as complete.
    """
    from okg.substrate.telemetry.llm_usage import (
        LlmUsageFilters,
        llm_usage_summary,
    )

    filters = LlmUsageFilters(deployment=deployment, since_seconds=24 * 3600)
    with conn.cursor() as cur:
        (total,) = llm_usage_summary(cur, filters=filters)
        models = llm_usage_summary(cur, filters=filters, group_by=("model",))
        callers = llm_usage_summary(cur, filters=filters, group_by=("caller",))

    def top(rows: list[Any], key: str) -> list[dict[str, Any]]:
        rows = sorted(rows, key=lambda r: (-r.calls, r.group[key]))[:5]
        return [
            {
                key: r.group[key],
                "calls": r.calls,
                "cost_usd": r.cost_usd,
                "priced_cost_usd": r.priced_cost_usd,
                "unpriced_calls": r.unpriced_calls,
            }
            for r in rows
        ]

    out = total.as_payload()
    out["top_models"] = top(models, "model")
    out["top_callers"] = top(callers, "caller")
    return out


def fetch_recent_errors(
    conn: psycopg.Connection, deployment: str | None,
    *, limit: int = 10,
) -> list[dict[str, Any]]:
    """Recent failures from current runtime-owned error ledgers."""
    items: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        where_dep = "AND deployment_name = %s" if deployment else ""
        args: tuple = (deployment,) if deployment else ()
        cur.execute(
            f"""
            SELECT ts, caller, model, coalesce(error_kind, 'error')
              FROM okg.llm_calls
             WHERE NOT success {where_dep}
          ORDER BY ts DESC
             LIMIT %s
            """,
            args + (limit,),
        )
        for r in cur.fetchall():
            items.append({
                "kind": "llm_error",
                "at": _ts(r[0]),
                "summary": f"{r[1]} · {r[2]} · {r[3]}",
            })
    items.sort(key=lambda i: i["at"] or "", reverse=True)
    return items[:limit]


# ---------------------------------------------------------------------------
# Snapshot — assemble all panels in one transaction
# ---------------------------------------------------------------------------


def fetch_snapshot(
    conn: psycopg.Connection, deployment: str,
) -> dict[str, Any]:
    """Single-round-trip aggregate across all three panels."""
    generation = fetch_generation(conn, deployment)
    catalog = fetch_catalog(conn)
    pinned = (
        generation.get("published", {}).get("generation_id")
        if generation.get("published") else None
    )
    return {
        "deployment": deployment,
        "generation": generation,
        "catalog": catalog,
        "live_graph": fetch_live_graph(conn, pinned),
        "sources": fetch_sources(conn, pinned),
        "storage": fetch_storage(conn),
        "queue": fetch_queue(conn),
        "projection": fetch_projection_backlog(conn),
        "enrichers": fetch_enrichers(conn, deployment),
        "doctor": fetch_doctor(conn),
        "llm_spend": fetch_llm_spend(conn, deployment),
        "recent_errors": fetch_recent_errors(conn, deployment),
        "profiling": fetch_profiling(deployment),
    }


def fetch_profiling(deployment: str) -> dict[str, Any]:
    """Read recent lightweight summaries and latest deep captures."""
    try:
        from okg.substrate.profiling.artifacts import (
            deployment_root,
            list_profiles,
            live_dir,
        )
    except Exception:
        return {"available": False, "reason": "profiling module unavailable"}

    now = datetime.now(timezone.utc)
    live: list[dict[str, Any]] = []
    ldir = live_dir(deployment)
    if ldir.is_dir():
        for f in sorted(ldir.glob("*.json")):
            try:
                row = json.loads(f.read_text())
            except Exception:
                logger.debug("skipping unreadable live profile %s", f, exc_info=True)
                continue
            updated = _parse_ts(row.get("updated_at"))
            stale = True
            if updated is not None:
                stale = (now - updated).total_seconds() > 60
            row["stale"] = stale
            live.append(row)
    profiles = list_profiles(deployment)[:5]
    latest = profiles[0] if profiles else None
    return {
        "available": True,
        "profile_root": str(deployment_root(deployment)),
        "live": live,
        "latest": latest,
        "recent": profiles,
    }
