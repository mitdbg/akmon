"""QAGateMetrics — canonical per-deployment health snapshot.

The 18 fields below are exactly the ones the `deployment-qa`
capability spec pins. Every field derives from existing substrate
state:

  * Chronos generation checkpoints (status, invariant_summary,
    excluded_sources, deployment_name, node/edge counts)
  * `okg.v_runtime_queue_health` (DBOS-backed runtime queue authority)
  * `okg doctor` exit code (subprocess)

Per-deployment scoping is structural: generation resolution filters by
Chronos checkpoint metadata. Cross-deployment metrics are deliberately not
computed.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import psycopg

from okg.substrate.runtime_queue_health import summarize_runtime_queue_health
from okg.substrate.db.connection import connect as db_connect


@dataclass
class QAGateMetrics:
    """Per-deployment snapshot of substrate health for the QA gate.

    All fields are required. The serialization round-trip
    (`asdict` → `from_dict`) is byte-stable.
    """

    deployment_name: str
    generation_id: int | str

    nodes_live_delta: int
    edges_live_delta: int

    queue_depth: dict[str, int]
    dlq_count: int

    cross_source_invariants_total: int
    cross_source_invariants_failed: int
    cross_source_invariants_timed_out: int
    source_local_invariants_total: int
    source_local_invariants_failed: int

    substrate_integrity_checks: dict[str, dict[str, Any]]

    excluded_sources: list[str]

    total_invariant_duration_ms: int
    total_invariant_budget_exceeded: bool

    generation_blocked_count: int
    doctor_exit_code: int

    publish_wallclock_seconds: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QAGateMetrics:
        return cls(
            deployment_name=data["deployment_name"],
            generation_id=data["generation_id"],
            nodes_live_delta=int(data["nodes_live_delta"]),
            edges_live_delta=int(data["edges_live_delta"]),
            queue_depth=dict(data["queue_depth"]),
            dlq_count=int(data["dlq_count"]),
            cross_source_invariants_total=int(
                data["cross_source_invariants_total"]
            ),
            cross_source_invariants_failed=int(
                data["cross_source_invariants_failed"]
            ),
            cross_source_invariants_timed_out=int(
                data["cross_source_invariants_timed_out"]
            ),
            source_local_invariants_total=int(
                data["source_local_invariants_total"]
            ),
            source_local_invariants_failed=int(
                data["source_local_invariants_failed"]
            ),
            substrate_integrity_checks={
                str(k): dict(v)
                for k, v in dict(
                    data.get("substrate_integrity_checks", {})
                ).items()
            },
            excluded_sources=list(data["excluded_sources"]),
            total_invariant_duration_ms=int(
                data["total_invariant_duration_ms"]
            ),
            total_invariant_budget_exceeded=bool(
                data["total_invariant_budget_exceeded"]
            ),
            generation_blocked_count=int(data["generation_blocked_count"]),
            doctor_exit_code=int(data["doctor_exit_code"]),
            publish_wallclock_seconds=float(
                data["publish_wallclock_seconds"]
            ),
        )


# ---------------------------------------------------------------------------
# Snapshot computation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ChronosQATarget:
    target_generation_id: int
    checkpoint_id: str
    previous_checkpoint_id: str | None
    metadata: dict[str, Any]
    node_count: int
    edge_count: int
    previous_node_count: int | None
    previous_edge_count: int | None


def _checkpoint_counts(store: Any, checkpoint_id: str) -> tuple[int, int]:
    graph = store.checkout_generation(checkpoint_id)
    nodes = int(
        graph.query("SELECT COUNT(*) AS n FROM okg.graph_nodes")[0]["n"]
    )
    edges = int(
        graph.query("SELECT COUNT(*) AS n FROM okg.graph_edges")[0]["n"]
    )
    return nodes, edges


def _resolve_target_generation(
    conn: psycopg.Connection,
    deployment: str,
    generation_id: int | str | None,
) -> _ChronosQATarget:
    """Resolve the Chronos checkpoint and previous published checkpoint."""

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.generation_resolver import resolve_okg_generation

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    resolved = resolve_okg_generation(
        conn,
        deployment_name=deployment,
        requested=generation_id,
        status="published",
        include_counts=True,
    )
    if resolved is None:
        raise RuntimeError(
            f"deployment {deployment!r} has no published generation "
            "matching the requested target; nothing to snapshot"
        )
    if resolved.target_generation_id is None:
        raise RuntimeError(
            f"published generation {resolved.generation_id!r} has no "
            "target_generation_id metadata"
        )

    published = store.list_generations(
        deployment_name=deployment,
        branch=None,
        status="published",
    )
    previous = None
    for candidate in published:
        if candidate.checkpoint_id == resolved.generation_id:
            break
        previous = candidate

    if previous is None:
        previous_node_count = None
        previous_edge_count = None
    else:
        previous_node_count, previous_edge_count = _checkpoint_counts(
            store,
            previous.checkpoint_id,
        )
    return _ChronosQATarget(
        target_generation_id=resolved.target_generation_id,
        checkpoint_id=resolved.generation_id,
        previous_checkpoint_id=(
            previous.checkpoint_id if previous is not None else None
        ),
        metadata=resolved.metadata,
        node_count=int(resolved.node_count or 0),
        edge_count=int(resolved.edge_count or 0),
        previous_node_count=previous_node_count,
        previous_edge_count=previous_edge_count,
    )


def _delta_or_absolute(current: int, previous: int | None) -> int:
    if previous is None:
        return current
    return current - previous


def _queue_depth(
    cur: psycopg.Cursor,
    ignored_job_kinds: set[str] | None = None,
    ignored_workflow_names: set[str] | None = None,
) -> dict[str, int]:
    return summarize_runtime_queue_health(
        cur,
        ignored_job_kinds=ignored_job_kinds,
        ignored_workflow_names=ignored_workflow_names,
    )["queue_depth"]


def _dlq_count(
    cur: psycopg.Cursor,
    ignored_job_kinds: set[str] | None = None,
    ignored_workflow_names: set[str] | None = None,
) -> int:
    return int(
        summarize_runtime_queue_health(
            cur,
            ignored_job_kinds=ignored_job_kinds,
            ignored_workflow_names=ignored_workflow_names,
        )["dlq_count"]
    )


def _generation_blocked_count(
    conn: psycopg.Connection, deployment: str
) -> int:
    """Count blocked Chronos generations among the most recent 10."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    generations = store.list_generations(
        deployment_name=deployment,
        branch=None,
        status=None,
    )
    recent = list(reversed(generations))[:10]
    return sum(
        1 for checkpoint in recent
        if (checkpoint.metadata or {}).get("status") == "blocked"
    )


def _flatten_invariant_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Project the publisher's `_build_invariant_summary` JSONB shape
    onto QAGateMetrics fields."""
    cs = summary.get("cross_source") or {}
    sl = summary.get("source_local") or {}
    substrate_integrity = summary.get("substrate_integrity") or {}
    sl_total = sum(int(v.get("total", 0)) for v in sl.values())
    sl_failed = sum(int(v.get("failed", 0)) for v in sl.values())
    return {
        "cross_source_invariants_total": int(cs.get("total", 0)),
        "cross_source_invariants_failed": int(cs.get("failed", 0)),
        "cross_source_invariants_timed_out": int(cs.get("timed_out", 0)),
        "source_local_invariants_total": sl_total,
        "source_local_invariants_failed": sl_failed,
        "substrate_integrity_checks": dict(
            substrate_integrity.get("checks") or {}
        ),
        "total_invariant_duration_ms": int(
            summary.get("total_duration_ms", 0)
        ),
        "total_invariant_budget_exceeded": bool(
            summary.get("total_budget_exceeded", False)
        ),
    }


def _doctor_exit_code(dsn: str) -> int:
    """Run ``okg doctor --json --dsn <dsn>`` as a subprocess and
    return its exit code. We use the CLI rather than the in-process
    function because the CLI defines the exit-code semantics
    (0 = ok, 1 = problem) and the QA gate's contract checks rest
    on that semantic.

    We invoke through :func:`running_okg_argv` so the snapshot runs
    against the same source tree as the caller (not whichever ``okg``
    happens to be on PATH), with the caller's working directory kept off
    the child's ``sys.path`` (issue #2301).
    """
    import subprocess  # nosec B404

    from okg.substrate.cli.child_argv import running_okg_argv

    result = subprocess.run(  # nosec B603
        [
            *running_okg_argv(),
            "doctor",
            "--json",
            "--dsn",
            dsn,
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode


def snapshot(
    *,
    dsn: str,
    deployment: str,
    generation_id: int | str | None = None,
    skip_doctor: bool = False,
    ignored_job_kinds: set[str] | None = None,
    ignored_workflow_names: set[str] | None = None,
) -> QAGateMetrics:
    """Capture a `QAGateMetrics` snapshot.

    `generation_id` defaults to the latest `published` generation for
    `deployment`. `skip_doctor=True` bypasses the doctor subprocess
    (used by tests that already control the environment); production
    callers leave it False so the snapshot reflects extension/role
    health. ``ignored_workflow_names`` leaves named runtime workflows out of
    queue depth and the DLQ count (the caller decides which; default none).
    """
    with db_connect(dsn) as conn:
        target = _resolve_target_generation(conn, deployment, generation_id)
        with conn.cursor() as cur:
            inv = _flatten_invariant_summary(
                dict(target.metadata.get("invariant_summary") or {})
            )
            nodes_delta = _delta_or_absolute(
                target.node_count,
                target.previous_node_count,
            )
            edges_delta = _delta_or_absolute(
                target.edge_count,
                target.previous_edge_count,
            )
            depth = _queue_depth(
                cur, ignored_job_kinds, ignored_workflow_names
            )
            dlq = _dlq_count(cur, ignored_job_kinds, ignored_workflow_names)
            blocked = _generation_blocked_count(conn, deployment)

    wallclock = float(target.metadata.get("publish_wallclock_seconds") or 0.0)

    doctor_rc = 0 if skip_doctor else _doctor_exit_code(dsn)

    return QAGateMetrics(
        deployment_name=deployment,
        generation_id=target.checkpoint_id,
        nodes_live_delta=nodes_delta,
        edges_live_delta=edges_delta,
        queue_depth=depth,
        dlq_count=dlq,
        excluded_sources=[
            str(s) for s in target.metadata.get("excluded_sources") or []
        ],
        generation_blocked_count=blocked,
        doctor_exit_code=doctor_rc,
        publish_wallclock_seconds=wallclock,
        **inv,
    )
