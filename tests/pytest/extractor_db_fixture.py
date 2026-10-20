"""Real in-memory-style Chronos seed/read helpers for extractor + promote tests.

After the chronos cutover (commit b56a27ff4) the extractors and the
operator-promotion path read their source rows through
``ChronosGraphStore.from_connection(conn).checkout(...)`` /
``.checkout_generation(generation_id)`` — i.e. the real Chronos store, not a
hand-rolled cursor. These helpers seed the source graph through the SAME real
store (``apply_facts`` + ``checkpoint_generation``), so a unit test exercises
the production read path end-to-end instead of a fake that no longer matches it.

The store seeds ``okg.graph_nodes`` / ``okg.graph_edges`` (the Chronos read
side); the extractors then emit their typed nodes/edges as raw
``okg.node_facts`` / ``okg.edge_facts`` inserts (the write side, unchanged by
the cutover). Tests read those fact tables back with :func:`emitted_node_facts`
/ :func:`emitted_edge_facts` and assert on them — the same content the old fake's
``all_inserts()`` exposed, now observed against real rows.

Seeding uses the shared ``clean_runtime`` Postgres fixture (a migrated test DB
that is reset per-test, including the Chronos branch — see
``tests/pytest/chronos_reset.py``). ``strict_endpoints`` only requires that an
edge's endpoint nodes exist as seeded NodeFacts; node/edge *subtypes* are not
catalog-checked at the ``apply_facts`` layer, so no catalog seeding is needed
for these read-side fixtures.
"""
from __future__ import annotations

from typing import Any

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact


def seed_graph(
    conn,
    nodes: list[NodeFact],
    edges: list[EdgeFact] | None = None,
    *,
    generation_id: int | None = None,
    source: str = "_fixture",
) -> None:
    """Seed ``nodes`` (and optional ``edges``) into the real Chronos graph.

    When ``generation_id`` is given, a checkpoint is taken for that generation
    so extractors reading via ``checkout_generation(generation_id)`` see the
    seeded rows. The promote path reads the live default branch via
    ``checkout()`` and needs no checkpoint, so omit ``generation_id`` there.

    ``ensure_schema=False`` because ``clean_runtime`` already migrated and
    re-registered the Chronos graph tables; re-registering would raise a
    duplicate-key error on ``_chronos_branch_tables``.
    """
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    facts: list[NodeFact | EdgeFact] = list(nodes)
    if edges:
        facts.extend(edges)
    store.apply_facts(facts, source=source, strict_endpoints=True)
    if generation_id is not None:
        store.checkpoint_generation(generation_id)


def node(node_id: str, subtype: str, attrs: dict[str, Any]) -> NodeFact:
    """Build a seed NodeFact with deterministic record id / revision."""
    return NodeFact(node_id, subtype, attrs, {"id": node_id}, {"v": 1})


def edge(src: str, dst: str, edge_type: str, attrs: dict[str, Any] | None = None) -> EdgeFact:
    """Build a seed EdgeFact between two seeded nodes."""
    return EdgeFact(
        src,
        dst,
        edge_type,
        attrs=attrs or {},
        source_record_id={"id": f"{src}->{edge_type}->{dst}"},
        source_revision={"v": 1},
    )


def emitted_node_facts(conn, subtype: str | None = None) -> list[dict[str, Any]]:
    """Return ``okg.node_facts`` rows (the extractor write side).

    Mirrors the old fake's ``all_inserts()`` node view: one dict per inserted
    NodeFact with ``node_id``, ``subtype``, and the materialized ``attrs``.
    """
    sql = "SELECT node_id, subtype, attrs FROM okg.node_facts"
    params: tuple[Any, ...] = ()
    if subtype is not None:
        sql += " WHERE subtype = %s"
        params = (subtype,)
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return [
            {"node_id": r[0], "subtype": r[1], "attrs": r[2]}
            for r in cur.fetchall()
        ]


def emitted_edge_facts(conn) -> list[dict[str, Any]]:
    """Return ``okg.edge_facts`` rows (the extractor write side)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT src, dst, edge_type, confidence, attrs FROM okg.edge_facts"
        )
        return [
            {
                "src": r[0],
                "dst": r[1],
                "edge_type": r[2],
                "confidence": r[3],
                "attrs": r[4],
            }
            for r in cur.fetchall()
        ]
