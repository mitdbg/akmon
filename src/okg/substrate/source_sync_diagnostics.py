"""Deployment-neutral source-sync policy diagnostics."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

from okg.substrate.db.connection import connect as db_connect
from psycopg.rows import dict_row

from okg.substrate.source_sync import SourceSyncCapabilities
from okg.substrate.watch.watcher import Watcher


SOURCE_FRONTIER_REPORT_SCHEMA = "okg.source_sync.frontier_report.v1"

#: Read-visibility states that need nobody to act, so they do not count as
#: blocked (issue #3024): `attention_required` means a person must act.
#: `unpublished_writes` is applied work the next publish takes (an ingest
#: with publish off, an embedding write, a lineage repair), and
#: `frontier_advancing` is a run in flight; a stale run is already
#: `frontier_state = blocked`. Every other state counts as blocked: no
#: published generation, no watermark, a failed newest run, a published state
#: the view cannot judge, unpublished work behind a blocked publish
#: (`publish_blocked`), a required replay, a missing record-set blob.
_SETTLING_VISIBILITY_STATES = frozenset({
    "read_visible",
    "unpublished_writes",
    "frontier_advancing",
    None,
})


def source_frontier_report(
    dsn: str,
    *,
    deployment: str | None = None,
    source: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    """Return operator diagnostics from okg.v_source_frontier_state."""
    where: list[str] = []
    params: dict[str, Any] = {}
    row_limit = max(1, int(limit))
    if deployment:
        where.append("deployment = %(deployment)s")
        params["deployment"] = deployment
    if source:
        where.append(
            "(source_name = %(source)s OR registry_source_name = %(source)s)"
        )
        params["source"] = source
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    # One read of the view, which judges every source's published state: the
    # status counts and the returned rows come from the same snapshot. The
    # view holds one row per source, so reading all of them is cheap; the
    # limit only trims the rows returned.
    with db_connect(dsn, connect_timeout=5) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"""
                SELECT
                  deployment,
                  source_name,
                  registry_source_name,
                  branch_name,
                  latest_published_generation_id,
                  latest_published_at,
                  watermark_source,
                  last_reconcile_at,
                  last_reconcile_age_seconds,
                  projection_cursor,
                  progress_observed_at,
                  last_run_observed_at,
                  last_run_id,
                  last_run_status,
                  last_run_mode,
                  last_run_completed_scope,
                  last_error,
                  last_attempt_run_id,
                  last_attempt_status,
                  last_attempt_observed_at,
                  nodes_appended,
                  edges_appended,
                  record_set_state,
                  record_set_sha256,
                  record_set_record_count,
                  record_set_node_count,
                  record_set_edge_count,
                  cursor_blob_count,
                  cursor_blob_bytes,
                  active_workflow_id,
                  workflow_status,
                  workflow_run_mode,
                  active_work_stale,
                  active_work_age_seconds,
                  runtime_state,
                  read_visibility_state,
                  frontier_state
                FROM okg.v_source_frontier_state
                {where_sql}
                ORDER BY deployment NULLS LAST, registry_source_name, source_name
                """,
                params,
            )
            status_rows = [dict(row) for row in cur.fetchall()]
    rows = status_rows[:row_limit]

    frontier_counts: dict[str, int] = {}
    visibility_counts: dict[str, int] = {}
    runtime_counts: dict[str, int] = {}
    for row in status_rows:
        frontier = str(row.get("frontier_state") or "unknown")
        visibility = str(row.get("read_visibility_state") or "unknown")
        runtime = str(row.get("runtime_state") or "unknown")
        frontier_counts[frontier] = frontier_counts.get(frontier, 0) + 1
        visibility_counts[visibility] = visibility_counts.get(visibility, 0) + 1
        runtime_counts[runtime] = runtime_counts.get(runtime, 0) + 1

    blocked_count = sum(
        1 for row in status_rows
        if row.get("frontier_state") == "blocked"
        or row.get("read_visibility_state") not in _SETTLING_VISIBILITY_STATES
    )
    ok = bool(status_rows) and blocked_count == 0
    return {
        "schema": SOURCE_FRONTIER_REPORT_SCHEMA,
        "deployment": deployment,
        "source": source,
        "row_count": len(status_rows),
        "returned_row_count": len(rows),
        "ok": ok,
        "status": (
            "ready"
            if ok else (
                "no_frontier_rows" if not rows else "attention_required"
            )
        ),
        "blocked_count": blocked_count,
        "frontier_state_counts": frontier_counts,
        "read_visibility_state_counts": visibility_counts,
        "runtime_state_counts": runtime_counts,
        "rows": rows,
    }


def sync_policy_report(
    registry: Mapping[str, Any],
) -> dict[str, Any]:
    """Return static sync declarations for a source registry."""
    sources: dict[str, Any] = {}
    for name, entry in sorted(registry.items()):
        sync = getattr(entry, "sync", None)
        if sync is None:
            continue
        sources[name] = _sync_policy_dict(sync)
    return {
        "sources": sources,
        "source_count": len(sources),
    }


def explain_source_sync_event(
    registry: Mapping[str, Any],
    *,
    path: str,
    trigger_kind: str = "fs_event",
    source: str | None = None,
) -> dict[str, Any]:
    """Explain which source policies match a path/trigger."""
    matches: list[dict[str, Any]] = []
    for name, entry in sorted(registry.items()):
        if source is not None and name != source:
            continue
        sync = getattr(entry, "sync", None)
        if sync is None or trigger_kind not in set(sync.triggers):
            continue
        rel_path = _matching_rel_path(path, sync)
        if rel_path is None and trigger_kind in {"fs_event", "git_ref_change"}:
            continue
        is_reconcile = trigger_kind == "reconcile"
        run_mode = (
            sync.reconcile_mode or "scope_complete"
            if is_reconcile else sync.default_event_mode
        )
        fanout = (
            sync.dependents.on_reconcile
            if is_reconcile else sync.dependents.on_event
        )
        accepts_path_scope = bool(
            sync.scope is not None and "path" in set(sync.scope.accepts)
        )
        matches.append({
            "source": name,
            "trigger_kind": trigger_kind,
            "matched_scope_key": rel_path,
            "run_mode": run_mode,
            "dependent_fanout": fanout,
            "scoped_execution": (
                "path" if rel_path and accepts_path_scope else None
            ),
            "fallback_run_mode": (
                sync.scope.unsupported_fallback
                if sync.scope is not None else None
            ),
            "deletes_require_reconcile": sync.deletes_require_reconcile,
            "delete_fallback_run_mode": (
                sync.reconcile_mode or "scope_complete"
                if sync.deletes_require_reconcile else None
            ),
            "delete_fallback_dependent_fanout": (
                sync.dependents.on_reconcile
                if sync.deletes_require_reconcile else None
            ),
            "scoped_delete_run_mode": sync.scoped_delete_mode,
            "scoped_delete_dependent_fanout": (
                sync.scoped_delete_dependent_fanout
                or (
                    sync.dependents.on_event
                    if sync.scoped_delete_mode is not None else None
                )
            ),
            "compaction": (
                sync.compaction.to_dict() if sync.compaction else None
            ),
        })
    return {
        "path": path,
        "trigger_kind": trigger_kind,
        "matches": matches,
        "match_count": len(matches),
    }


def _sync_policy_dict(sync: SourceSyncCapabilities) -> dict[str, Any]:
    data = sync.to_dict()
    data["scope_support"] = (
        list(sync.scope.accepts) if sync.scope is not None else []
    )
    data["dependent_fanout"] = sync.dependents.to_dict()
    return data


def _matching_rel_path(path: str, sync: SourceSyncCapabilities) -> str | None:
    if sync.watch is None:
        return None
    p = Path(path).expanduser()
    for root_raw in sync.watch.roots:
        root = Path(root_raw).expanduser().resolve()
        rel: str | None = None
        if p.is_absolute():
            try:
                rel = str(p.resolve().relative_to(root))
            except ValueError:
                rel = None
        else:
            raw_rel = str(p).replace("\\", "/")
            if raw_rel.startswith(f"{root.name}/"):
                rel = raw_rel.removeprefix(f"{root.name}/")
            elif (root / raw_rel).exists():
                rel = raw_rel
            else:
                rel = None
        if rel is None:
            continue
        rel = rel.replace("\\", "/")
        if any(Watcher._glob_matches(rel, ex) for ex in sync.watch.exclude_globs):
            continue
        if any(Watcher._glob_matches(rel, inc) for inc in sync.watch.include_globs):
            return rel
    return None


__all__ = [
    "SOURCE_FRONTIER_REPORT_SCHEMA",
    "explain_source_sync_event",
    "source_frontier_report",
    "sync_policy_report",
]
