"""Bounded final coverage of a private authority at a fresh graph snapshot.

This is a fixed internal consumer of the authority connection. It does not
enable ingestion, create a public reference, or authorize later acceptance.
"""
from __future__ import annotations

import json
from itertools import islice
from typing import Any

from psycopg import Error as DatabaseError
from psycopg.pq import TransactionStatus

from okg.substrate.canonicalize import canonicalize_id
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore, _edge_id
from okg.substrate.ingest.change_identity import PipelineIdentitySnapshot
from okg.substrate.ingest.record_authority import (
    AuthorityError,
    PrivateAuthorityCandidate,
    ProductionAuthorityStore,
    VerifiedPrivateIdentityReader,
)
from okg.substrate.library.sources.base import NODE_OWNERSHIP_MODES


READ_BATCH = 128
STAGE_BYTES = 1024 * 1024
SAMPLE_LIMIT = 10
_NODES_RAW = "_okg_private_coverage_nodes_raw"
_EDGES_RAW = "_okg_private_coverage_edges_raw"
_NODES = "_okg_private_coverage_nodes"
_EDGES = "_okg_private_coverage_edges"


def _intact(context: PipelineIdentitySnapshot) -> None:
    if not isinstance(context, PipelineIdentitySnapshot) or not context.is_intact():
        raise AuthorityError("private coverage requires an intact pipeline snapshot")


def _normalized_id(value: str, context: PipelineIdentitySnapshot) -> str:
    result = canonicalize_id(value, context.canonicalizers)
    if not isinstance(result, str) or not result or "\x00" in result:
        raise AuthorityError("private coverage canonicalizer returned an invalid identity")
    return result


def _copy_batch(conn, nodes: list[tuple], edges: list[tuple]) -> None:
    # All reader fetches finish before either COPY opens on this connection.
    with conn.cursor() as cursor:
        if nodes:
            with cursor.copy(
                "COPY _okg_private_coverage_nodes_raw (node_id, ownership_mode) FROM STDIN"
            ) as copy:
                for row in nodes:
                    copy.write_row(row)
        if edges:
            with cursor.copy(
                "COPY _okg_private_coverage_edges_raw (edge_id, src, edge_type, dst) FROM STDIN"
            ) as copy:
                for row in edges:
                    copy.write_row(row)


def _stage(conn, reader: VerifiedPrivateIdentityReader,
           context: PipelineIdentitySnapshot) -> None:
    conn.execute(f"CREATE TEMP TABLE {_NODES_RAW} ("
                 "node_id text NOT NULL, ownership_mode text NOT NULL) ON COMMIT DROP")
    conn.execute(f"CREATE TEMP TABLE {_EDGES_RAW} ("
                 "edge_id bigint NOT NULL, src text NOT NULL, edge_type text NOT NULL, "
                 "dst text NOT NULL) ON COMMIT DROP")
    identities = reader.iterate_identities()
    while batch := list(islice(identities, READ_BATCH)):
        nodes: list[tuple] = []
        edges: list[tuple] = []
        size = 0
        for kind, identity, ownership in batch:
            if kind == "node":
                if ownership not in NODE_OWNERSHIP_MODES:
                    raise AuthorityError("private coverage has unknown node ownership")
                row = (_normalized_id(identity, context), ownership)
                nodes.append(row)
            elif kind == "edge":
                raw = json.loads(identity)
                if (ownership or not isinstance(raw, list) or len(raw) != 3
                        or any(not isinstance(v, str) or not v or "\x00" in v for v in raw)):
                    raise AuthorityError("private coverage has an invalid edge identity")
                src = _normalized_id(raw[0], context)
                dst = _normalized_id(raw[2], context)
                row = (_edge_id(src, raw[1], dst), src, raw[1], dst)
                edges.append(row)
            else:
                raise AuthorityError("private coverage has an unknown identity kind")
            size += sum(len(v.encode("utf-8")) if isinstance(v, str) else 8 for v in row)
            if size >= STAGE_BYTES:
                _copy_batch(conn, nodes, edges)
                nodes, edges, size = [], [], 0
        _copy_batch(conn, nodes, edges)
    # SQL dedup spans all batches. No text-key index imposes a new identity
    # limit, and edge hashes never substitute for the complete edge tuple.
    conn.execute(f"CREATE TEMP TABLE {_NODES} ON COMMIT DROP AS "
                 f"SELECT DISTINCT node_id, ownership_mode FROM {_NODES_RAW}")
    conn.execute(f"CREATE TEMP TABLE {_EDGES} ON COMMIT DROP AS "
                 f"SELECT DISTINCT edge_id, src, edge_type, dst FROM {_EDGES_RAW}")
    conn.execute(f"ANALYZE {_NODES}")
    conn.execute(f"ANALYZE {_EDGES}")


def _count(session, sql: str, params: dict[str, Any]) -> int:
    rows = session.query(f"SELECT count(*) AS count FROM ({sql}) AS mismatch", params)
    if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
            or set(rows[0]) != {"count"} or type(rows[0]["count"]) is not int
            or rows[0]["count"] < 0):
        raise SubstrateError("private authority coverage returned an invalid count")
    return rows[0]["count"]


def _sample(session, sql: str, columns: tuple[str, ...],
            params: dict[str, Any]) -> list[dict[str, str]]:
    ordering = ", ".join(f'{column} COLLATE "C"' for column in columns)
    rows = session.query(f"SELECT * FROM ({sql}) AS mismatch "
                         f"ORDER BY {ordering} LIMIT {SAMPLE_LIMIT}", params)
    if (not isinstance(rows, list) or len(rows) > SAMPLE_LIMIT
            or any(not isinstance(row, dict) or set(row) != set(columns)
                   or any(not isinstance(row[column], str) for column in columns)
                   for row in rows)):
        raise SubstrateError("private authority coverage returned an invalid sample")
    return rows


def _compare(session, source: str) -> None:
    params = {"source": source}
    conflicts = (f"SELECT node_id FROM {_NODES} GROUP BY node_id "
                 "HAVING count(DISTINCT ownership_mode) > 1")
    conflict_count = _count(session, conflicts, params)
    if conflict_count:
        raise SubstrateError(
            "private authority declares conflicting normalized node ownership",
            data={"source_name": source, "ownership_conflict_count": conflict_count,
                  "nodes_sample": [row["node_id"] for row in
                                   _sample(session, conflicts, ("node_id",), params)]},
        )
    # NOT EXISTS gives set membership, so duplicated visible graph rows
    # cannot pay for a missing identity or inflate a distinct mismatch count.
    comparisons = {
        "missing_node": (
            f"SELECT e.node_id FROM {_NODES} e WHERE NOT EXISTS ("
            "SELECT 1 FROM okg.graph_nodes g WHERE g.node_id=e.node_id "
            "AND (e.ownership_mode='substitute' OR g.source=:source))",
            ("node_id",),
        ),
        "missing_edge": (
            f"SELECT e.src, e.edge_type, e.dst FROM {_EDGES} e WHERE NOT EXISTS ("
            "SELECT 1 FROM okg.graph_edges g WHERE g.edge_id=e.edge_id "
            "AND g.src=e.src AND g.edge_type=e.edge_type AND g.dst=e.dst)",
            ("src", "edge_type", "dst"),
        ),
        "stale_node": (
            "SELECT DISTINCT g.node_id FROM okg.graph_nodes g "
            "WHERE g.source=:source AND NOT EXISTS ("
            f"SELECT 1 FROM {_NODES} e WHERE e.node_id=g.node_id)",
            ("node_id",),
        ),
        "stale_edge": (
            "SELECT DISTINCT g.src, g.edge_type, g.dst FROM okg.graph_edges g "
            "WHERE g.source=:source AND NOT EXISTS ("
            f"SELECT 1 FROM {_EDGES} e WHERE e.edge_id=g.edge_id "
            "AND e.src=g.src AND e.edge_type=g.edge_type AND e.dst=g.dst)",
            ("src", "edge_type", "dst"),
        ),
    }
    counts = {key + "_count": _count(session, sql, params)
              for key, (sql, _columns) in comparisons.items()}
    if not any(counts.values()):
        return
    data: dict[str, Any] = {"source_name": source, "stale_counts_exact": True, **counts}
    for key, (sql, columns) in comparisons.items():
        rows = _sample(session, sql, columns, params) if counts[key + "_count"] else []
        data[key + "s_sample"] = ([row["node_id"] for row in rows]
                                  if columns == ("node_id",) else rows)
    raise SubstrateError(
        "private authority post-apply coverage check failed at the current snapshot",
        data=data,
        fix_hint="inspect the missing and extra identities; reconcile source scope before acceptance",
    )


def assert_private_post_apply_coverage(
    store: ProductionAuthorityStore,
    candidate: PrivateAuthorityCandidate,
    *,
    expected_source: str,
    expected_incarnation: str,
    expected_attempt: str,
    graph_branch: str,
    normalization_context: PipelineIdentitySnapshot,
) -> None:
    """Assert final coverage after all relevant graph writes have committed.

    The caller supplies independent run/branch bindings and the active run's
    captured pipeline snapshot. Snapshot integrity does not authenticate which
    source supplied that context. Callers serialize use of the borrowed handle.
    No caller transaction is committed or rolled back to manufacture freshness.
    Success refers only to this snapshot; later acceptance and seal need their
    own proof. No connection, table, reader or reusable proof escapes.

    Python staging retains a fixed raw batch plus STAGE_BYTES and at most one
    normalized identity, in addition to the caller's captured pipeline context.
    """
    if store.conn.info.transaction_status != TransactionStatus.IDLE:
        raise AuthorityError("private post-apply coverage requires an idle connection")
    if not isinstance(graph_branch, str) or not graph_branch or "\x00" in graph_branch:
        raise AuthorityError("private coverage requires an existing graph branch")
    _intact(normalization_context)
    try:
        with store.private_identity_reader(
            candidate, expected_source=expected_source,
            expected_incarnation=expected_incarnation, expected_attempt=expected_attempt,
        ) as reader:
            graph = ChronosGraphStore.from_connection(
                store.conn, ensure_schema=False, autocommit=False,
            )
            try:
                # Checkout refuses missing branches; it must never create one.
                session = graph.checkout(graph_branch)
                rows = session.query(
                    "SELECT created_at FROM _chronos_branch_interval_branches "
                    "WHERE branch_id=:branch", {"branch": graph_branch},
                )
                if (not isinstance(rows, list) or len(rows) != 1
                        or not isinstance(rows[0], dict)
                        or set(rows[0]) != {"created_at"}
                        or str(rows[0]["created_at"] or "") != expected_incarnation):
                    raise AuthorityError("private coverage graph incarnation mismatch")
                _stage(store.conn, reader, normalization_context)
                reader._check()
                _compare(session, expected_source)
                _intact(normalization_context)
                reader._check()
            finally:
                graph.close()  # Borrowed context: no implicit transaction control.
        # ON COMMIT DROP removes every staging table before exclusion releases.
    except DatabaseError as exc:
        raise SubstrateError("private authority coverage database operation failed") from exc
