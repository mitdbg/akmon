"""`fan_out_sealed_brief` substrate MCP primitive.

Per S-A7-R4:

Inputs:
  brief_id: str — Brief NodeFact id
  workers: list[WorkerDeclaration] — { worker_id, pre_join_tool?,
           param_overrides? } per worker
  scope_overrides: dict | None — global param overrides applied
                                 to every worker's bundle

Side effects (all inside one transaction):
  - 1 BriefDispatch NodeFact with deterministic id
  - N SubAgentSession placeholder NodeFacts
  - 1 BriefDispatch references Brief edge
  - N BriefDispatch references SubAgentSession edges

Return:
  { dispatch_id, per_worker: [...], generation_id }

Each per-worker `bundle` is built by calling the pre-join MCP
framework against the worker's resolved tool. Same call invoked
twice within the same wall-clock second returns the same
dispatch_id; the substrate's wire-level (dedupe_key, observed_at)
unique constraint collapses identical re-emissions.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

import psycopg

from okg.substrate.mcp.framework import (
    PreJoinToolError,
    ResolverRegistry,
    execute_pre_join_tool,
)
from okg.substrate.mcp.framework.schema import PreJoinToolSpec
from okg.substrate.mcp.session import (
    Session,
    assert_session_matches,
)


__all__ = [
    "FanOutResult",
    "WorkerDeclaration",
    "FanOutWorker",
    "fan_out_sealed_brief",
]


SOURCE = "substrate.fan_out_sealed_brief"


@dataclass(frozen=True)
class WorkerDeclaration:
    """One worker line item in a fan-out call.

    `pre_join_tool` is the name of the registered pre-join MCP
    tool to use for this worker's bundle; when None, the
    substrate falls back to the Brief's
    `pre_join_tool_for_per_worker_context` attr (S-A7-R4 task 4.2).
    """

    worker_id: str
    pre_join_tool: Optional[str] = None
    param_overrides: Optional[dict[str, Any]] = None


@dataclass(frozen=True)
class FanOutWorker:
    """One row in the response's `per_worker` list."""

    worker_id: str
    pre_join_tool: str
    scope: dict[str, Any]
    bundle: dict[str, Any]


@dataclass(frozen=True)
class FanOutResult:
    dispatch_id: str
    per_worker: list[FanOutWorker]
    generation_id: int


def fan_out_sealed_brief(
    conn: psycopg.Connection,
    session: Session,
    *,
    brief_id: str,
    workers: list[WorkerDeclaration],
    pre_join_tools: dict[str, PreJoinToolSpec],
    resolvers: ResolverRegistry,
    scope_overrides: Optional[dict[str, Any]] = None,
    dispatched_by: Optional[str] = None,
    observed_at: Optional[datetime] = None,
) -> FanOutResult:
    """Dispatch ``workers`` against ``brief_id``.

    `pre_join_tools` maps tool name -> spec (the substrate's
    registered pre-join tools at MCP server bootstrap). The
    framework executor is invoked per worker with the resolved
    tool spec; the per-worker bundle ships inline in the return
    envelope.

    `dispatched_by` is operator identity / agent session id when
    available. The substrate writes it to the BriefDispatch
    NodeFact's `dispatched_by` attr.

    `observed_at` is overridable for tests; the default uses now()
    rounded down to the second so retry collapses are predictable.
    """
    assert_session_matches(conn, session)

    if not workers:
        raise PreJoinToolError(
            "fan_out_sealed_brief: `workers` must be non-empty",
            data={"brief_id": brief_id},
        )

    now = (observed_at or datetime.now(timezone.utc)).replace(microsecond=0)
    dispatch_id = _compute_dispatch_id(
        brief_id=brief_id,
        worker_ids=[w.worker_id for w in workers],
        observed_at=now,
    )

    # Read the Brief node at the pin to discover its default
    # pre_join_tool_for_per_worker_context. The fallback path
    # in S-A7-R4 task 4.2 references this attr.
    brief_attrs = _fetch_brief_attrs(conn, session, brief_id)
    default_tool = (
        brief_attrs.get("pre_join_tool_for_per_worker_context")
        if isinstance(brief_attrs, dict) else None
    )

    per_worker: list[FanOutWorker] = []
    sub_agent_session_ids: list[str] = []

    for w in workers:
        tool_name = w.pre_join_tool or default_tool
        if not tool_name:
            raise PreJoinToolError(
                f"fan_out_sealed_brief: worker {w.worker_id!r} has no "
                "pre_join_tool and brief carries no "
                "pre_join_tool_for_per_worker_context",
                data={
                    "brief_id": brief_id,
                    "worker_id": w.worker_id,
                },
            )
        spec = pre_join_tools.get(tool_name)
        if spec is None:
            raise PreJoinToolError(
                f"fan_out_sealed_brief: worker {w.worker_id!r} references "
                f"unregistered pre-join tool {tool_name!r}",
                data={
                    "brief_id": brief_id,
                    "worker_id": w.worker_id,
                    "tool": tool_name,
                },
            )
        # Merge scope: global scope_overrides + per-worker
        # param_overrides; per-worker wins on key collision.
        params: dict[str, Any] = {}
        if scope_overrides:
            params.update(scope_overrides)
        if w.param_overrides:
            params.update(w.param_overrides)
        envelope = execute_pre_join_tool(
            spec,
            conn=conn,
            session=session,
            params=params,
            resolvers=resolvers,
        )
        sub_id = _sub_agent_session_id(
            dispatch_id=dispatch_id, worker_id=w.worker_id,
        )
        sub_agent_session_ids.append(sub_id)
        per_worker.append(
            FanOutWorker(
                worker_id=w.worker_id,
                pre_join_tool=tool_name,
                scope=params,
                bundle=envelope,
            )
        )

    # Emit BriefDispatch + N SubAgentSession placeholders + N+1
    # edges. All writes go into a single transaction so a partial
    # fan-out can't leave a Dispatch without its child placeholders.
    _emit_audit_nodes(
        conn=conn,
        session=session,
        dispatch_id=dispatch_id,
        brief_id=brief_id,
        per_worker=per_worker,
        sub_agent_session_ids=sub_agent_session_ids,
        observed_at=now,
        dispatched_by=dispatched_by,
        default_tool=default_tool,
    )

    return FanOutResult(
        dispatch_id=dispatch_id,
        per_worker=per_worker,
        generation_id=session.generation_id,
    )


# ---- helpers ----


def _compute_dispatch_id(
    *,
    brief_id: str,
    worker_ids: Iterable[str],
    observed_at: datetime,
) -> str:
    """Deterministic dispatch_id keyed by brief + sorted worker ids
    + observed-at-rounded-to-the-second. Two calls with identical
    args within the same second collapse via wire-level dedup.
    """
    payload = "|".join([
        brief_id,
        ",".join(sorted(worker_ids)),
        observed_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    ])
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]
    return f"dispatch:{digest}"


def _sub_agent_session_id(*, dispatch_id: str, worker_id: str) -> str:
    return f"sub_agent_session:{dispatch_id.split(':', 1)[1]}:{worker_id}"


def _fetch_brief_attrs(
    conn: psycopg.Connection, session: Session, brief_id: str,
) -> dict[str, Any]:
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn,
        ensure_schema=False,
    ).checkout_generation(
        session.generation_id,
        branch=session.branch_name,
    )
    rows = graph.query(
        """
        SELECT attrs FROM okg.graph_nodes
        WHERE node_id = :brief AND subtype = 'brief'
        LIMIT 1
        """,
        {"brief": brief_id},
    )
    if not rows:
        return {}
    attrs = rows[0]["attrs"]
    return attrs if isinstance(attrs, dict) else {}


def _emit_audit_nodes(
    *,
    conn: psycopg.Connection,
    session: Session,
    dispatch_id: str,
    brief_id: str,
    per_worker: list[FanOutWorker],
    sub_agent_session_ids: list[str],
    observed_at: datetime,
    dispatched_by: Optional[str],
    default_tool: Optional[str],
) -> None:
    run_id = uuid.uuid4()
    dispatch_attrs = {
        "dispatch_id": dispatch_id,
        "brief_node_ref": brief_id,
        "worker_count": len(per_worker),
        "pre_join_tool_used": default_tool or "",
        "created_at": observed_at.isoformat(),
    }
    if dispatched_by:
        dispatch_attrs["dispatched_by"] = dispatched_by
    sub_attrs_by_worker = [
        {
            "session_id": sub_id,
            "worker_id": worker.worker_id,
            "brief_node_ref": brief_id,
            "dispatch_node_ref": dispatch_id,
            "pre_join_tool": worker.pre_join_tool,
            "scope": worker.scope,
            "started_at": observed_at.isoformat(),
        }
        for worker, sub_id in zip(per_worker, sub_agent_session_ids)
    ]

    from okg.substrate.images.values import check_incoming
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    # Image values (#3046): `dispatched_by` and each worker's scope are
    # caller-supplied. Refuse a forged or malformed image value in any of
    # these nodes before the first one is written.
    for attrs in (dispatch_attrs, *sub_attrs_by_worker):
        check_incoming(attrs)

    graph_facts = [
        NodeFact(
            dispatch_id,
            "brief_dispatch",
            dispatch_attrs,
            {"dispatch_id": dispatch_id},
            {"observed_at": observed_at.isoformat()},
        )
    ]

    with conn.cursor() as cur:
        # BriefDispatch NodeFact.
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source,
               source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (%s, 'brief_dispatch', %s::jsonb, %s,
                    %s::jsonb, %s::jsonb,
                    %s, %s, %s, 'I')
            ON CONFLICT (dedupe_key, observed_at) DO NOTHING
            """,
            (
                dispatch_id,
                json.dumps(dispatch_attrs, sort_keys=True),
                SOURCE,
                json.dumps({"dispatch_id": dispatch_id}),
                json.dumps({"observed_at": observed_at.isoformat()}),
                observed_at,
                f"{SOURCE}|node|{dispatch_id}",
                str(run_id),
            ),
        )

        # SubAgentSession placeholders.
        for sub_id, sub_attrs in zip(sub_agent_session_ids, sub_attrs_by_worker):
            graph_facts.append(
                NodeFact(
                    sub_id,
                    "sub_agent_session",
                    sub_attrs,
                    {"sub_id": sub_id},
                    {"observed_at": observed_at.isoformat()},
                )
            )
            cur.execute(
                """
                INSERT INTO okg.node_facts
                  (node_id, subtype, attrs, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, 'sub_agent_session', %s::jsonb, %s,
                        %s::jsonb, %s::jsonb,
                        %s, %s, %s, 'I')
                ON CONFLICT (dedupe_key, observed_at) DO NOTHING
                """,
                (
                    sub_id,
                    json.dumps(sub_attrs, sort_keys=True, default=str),
                    SOURCE,
                    json.dumps({"sub_id": sub_id}),
                    json.dumps({"observed_at": observed_at.isoformat()}),
                    observed_at,
                    f"{SOURCE}|node|{sub_id}",
                    str(run_id),
                ),
            )

        # BriefDispatch references Brief.
        _insert_edge_fact(
            cur,
            src=dispatch_id,
            dst=brief_id,
            edge_type="references",
            observed_at=observed_at,
            run_id=run_id,
        )
        graph_facts.append(
            EdgeFact(
                dispatch_id,
                brief_id,
                "references",
                provenance="derived_deterministic",
                source_record_id={
                    "src": dispatch_id,
                    "dst": brief_id,
                    "edge_type": "references",
                },
                source_revision={"observed_at": observed_at.isoformat()},
            )
        )
        # BriefDispatch references SubAgentSession (N edges).
        for sub_id in sub_agent_session_ids:
            _insert_edge_fact(
                cur,
                src=dispatch_id,
                dst=sub_id,
                edge_type="references",
                observed_at=observed_at,
                run_id=run_id,
            )
            graph_facts.append(
                EdgeFact(
                    dispatch_id,
                    sub_id,
                    "references",
                    provenance="derived_deterministic",
                    source_record_id={
                        "src": dispatch_id,
                        "dst": sub_id,
                        "edge_type": "references",
                    },
                    source_revision={"observed_at": observed_at.isoformat()},
                )
            )

    from okg.substrate.graph_store import ChronosGraphStore

    ChronosGraphStore.from_connection(conn).apply_facts(
        graph_facts,
        branch=session.branch_name,
    )


def _insert_edge_fact(
    cur,
    *,
    src: str,
    dst: str,
    edge_type: str,
    observed_at: datetime,
    run_id: uuid.UUID,
) -> None:
    edge_id = _mint_edge_id(src=src, edge_type=edge_type, dst=dst)
    cur.execute(
        """
        INSERT INTO okg.edge_facts
          (edge_id, src, dst, edge_type, attrs, provenance,
           confidence, source, source_record_id,
           source_revision, observed_at, dedupe_key, run_id, op)
        VALUES (%s, %s, %s, %s, '{}'::jsonb, 'derived_deterministic',
                1.0, %s, %s::jsonb, %s::jsonb,
                %s, %s, %s, 'I')
        ON CONFLICT (dedupe_key, observed_at) DO NOTHING
        """,
        (
            edge_id, src, dst, edge_type,
            SOURCE,
            json.dumps({"src": src, "dst": dst, "edge_type": edge_type}),
            json.dumps({"observed_at": observed_at.isoformat()}),
            observed_at,
            f"{SOURCE}|edge|{src}|{edge_type}|{dst}",
            str(run_id),
        ),
    )


def _mint_edge_id(*, src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)
