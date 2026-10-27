"""The candidate set drives the MCP filter page.

PACT lane-h-mcp-filter-index-driven, checked against a real publish on a
disposable PostgreSQL. Parity: the index-driven page returns exactly what
the unindexed scan returns for the same predicate: same rows, same order,
same truncation. The reference is produced by the SAME production function
under ``OKG_ATTR_FILTER_INDEX=off``, which narrows nothing.

Since PACT lane-s-storage-cutover every page reads the graph rows ids first
(S2 design 2.3): the candidate set narrows the graph-id branch, and targets
with an overlay on a predicate key come from the overlay-id branch. The
graph page reads the subtype's predicate flat (``graph_read.
planning_predicate``), so a compound predicate narrows under a subtype
scope too. The materialized path's interval completeness cache is gone with
that path.

okg#3112 step 2 (PACT lane-h-3112-step2-reader): the file runs under every
accepted ``OKG_ATTR_FILTER_INDEX`` mode. The fixture builds the declared
expression indexes, which narrow node pages; ``off`` narrows nothing.

okg#3112 steps 3 and 4a (PACT lane-h-3112-step3, lane-h-3112-step4a): no
expression index is declared for edges (decision 287d), and since step 4a no
other candidate set exists, so an edge page is never narrowed: the default
and an explicit ``expression`` both answer it by the exact predicate and name
``kind_not_indexed``. No page statement reads ``okg.attr_index``.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate import declared_attr_indexes as dai
from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import attr_filter_source as afs
from okg.substrate.mcp import server as server_mod
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_PAGE_MARK = "page_limit"


@pytest.fixture(autouse=True)
def ids_first(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every page here is read ids first. The graph page reads a small
    scope merged instead, with no candidate set (``graph_read.small_scope``,
    a cost choice with the same rows); these tests are about where the
    declared indexes narrow, which only the ids-first form shows."""
    from okg.substrate.mcp import graph_read

    monkeypatch.setattr(graph_read, "small_scope", lambda *_a, **_k: None)


def _row_attrs(index: int) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        "path": f"file{index:02d}.py" if index % 5 else f"dir/file{index:02d}.txt",
        "owner": ["alice", "bob", "carol"][index % 3],
        "rank": index % 7,
        "active": index % 2 == 0,
        "seen_at": f"2026-08-{(index % 9) + 1:02d}T00:00:00Z",
    }
    # Stored values of the WRONG JSON type for their declaration: no declared
    # index admits them, so only the exact effective predicate can match.
    if index == 3:
        attrs["rank"] = "3"
    if index == 4:
        attrs["path"] = 42
    if index == 6:
        attrs["active"] = "true"
    # A target with no rank at all; an overlay adds one after publish.
    if index == 8:
        del attrs["rank"]
    # A declared ts that does not parse: no index key, exact text match only.
    if index == 9:
        attrs["seen_at"] = "not-a-timestamp"
    return attrs


def _edge_attrs(index: int) -> dict[str, Any]:
    attrs: dict[str, Any] = {
        "label": f"l{index % 4}",
        "rank": index % 5,
        "seen_at": f"2026-09-{(index % 6) + 1:02d}T12:00:00+00:00",
    }
    if index == 2:
        attrs["label"] = 7  # wrong JSON type for a text declaration
    return attrs


def _seed_catalog(conn: psycopg.Connection) -> None:
    seed_catalog(
        conn,
        subtypes=[
            SubtypeSpec(
                name="doc",
                metadata={
                    "filterable_attrs": {
                        "path": "text",
                        "rank": "num",
                        "active": "bool",
                        "seen_at": "ts",
                        # Declared on a node subtype; the declared registry is
                        # keyed by attribute name, so an edge filter on `label`
                        # is a declared-key filter too.
                        "label": "text",
                    }
                },
            )
        ],
        narrowings=[("doc", "links", "doc")],
    )


def _apply(conn: psycopg.Connection, facts: list[Any]) -> None:
    ChronosGraphStore.from_connection(conn).apply_facts(
        facts, source="default", strict_endpoints=True
    )


def _publish(dsn: str) -> str:
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")
    ).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


def _target(index: int, suffix: str) -> str:
    return f"doc:{index:02d}:{suffix}"


def _overlay(
    conn: psycopg.Connection, generation: str, target_id: str, key: str, value: Any
) -> None:
    record_attribute_observation(
        conn,
        target_kind="node",
        target_id=target_id,
        generation_id=generation,
        attr_key=key,
        value=value,
        producer="test:filter-index-driven",
    )


@pytest.fixture
def published(clean_runtime: str) -> Iterator[tuple[str, str, str]]:
    """30 docs and 20 edges, published, then post-publish overlays."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog(conn)
    # The declared expression indexes, built before the first write so the
    # writes maintain them (what OKG_ATTR_FILTER_INDEX=expression reads).
    report = dai.reconcile(clean_runtime, deployment="default", apply=True, enable=True)
    assert report["status"] == "converged", report
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _apply(
            conn,
            [
                NodeFact(
                    _target(i, suffix),
                    "doc",
                    _row_attrs(i),
                    {"id": _target(i, suffix)},
                    {"v": 1},
                )
                for i in range(30)
            ]
            + [
                EdgeFact(
                    src=_target(i, suffix),
                    dst=_target(i + 1, suffix),
                    edge_type="links",
                    attrs=_edge_attrs(i),
                    source_record_id={"id": f"link:{i}:{suffix}"},
                    source_revision={"v": 1},
                )
                for i in range(20)
            ],
        )
    generation = _publish(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Moves a target AWAY from its published (and indexed) value and
        # INTO a value the index has never seen.
        _overlay(conn, generation, _target(1, suffix), "path", "overlaid.py")
        # ADDS a key the published row did not have.
        _overlay(conn, generation, _target(8, suffix), "rank", 6)
        # Flips a declared bool against the index.
        _overlay(conn, generation, _target(11, suffix), "active", True)
        # Moves one edge's label into a value attr_index has never seen
        # (the first edge id in text order, read from the graph rows).
        rows = ChronosGraphStore.from_connection(
            conn, ensure_schema=False,
        ).checkout_generation(generation).query(
            "SELECT edge_id::text AS edge_id FROM okg.graph_edges "
            "ORDER BY edge_id::text LIMIT 1", {},
        )
        first = rows[0]
        edge_id = str(first["edge_id"] if isinstance(first, dict) else first[0])
        record_attribute_observation(
            conn,
            target_kind="edge",
            target_id=edge_id,
            generation_id=generation,
            attr_key="label",
            value="overlaid-edge",
            producer="test:filter-index-driven",
        )
        materialize_effective_attributes_for_generation(
            conn, generation_id=generation
        )
    try:
        yield clean_runtime, generation, suffix
    finally:
        close_filter_servers()


def _mode() -> str:
    return afs.filter_index_mode()


_SERVERS: dict[str, Any] = {}


def _server_for(dsn: str) -> Any:
    """One MCP server per test and database, built on first use.

    The index mode is read on every call (`afs.filter_index_mode`), so a test
    that flips `OKG_ATTR_FILTER_INDEX` between reads needs no new server.
    """
    if dsn not in _SERVERS:
        _SERVERS[dsn] = server_mod.build_server(dsn=dsn)
    return _SERVERS[dsn]


def close_filter_servers() -> None:
    """Close and forget every server `_run_filter` built.

    The `published` fixture calls this in its teardown, so every test that
    reads through `_run_filter` (this file and test_graph_parity_db.py) closes
    its servers; none outlives the database `clean_runtime` recreates.
    """
    servers = list(_SERVERS.values())
    _SERVERS.clear()
    for mcp in servers:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()


@pytest.fixture(autouse=True)
def _close_filter_servers() -> Iterator[None]:
    # Also covers a test here that reads without the `published` fixture.
    yield
    close_filter_servers()


def _run_filter(
    dsn: str,
    generation: str,
    predicate: dict[str, Any],
    *,
    monkeypatch: pytest.MonkeyPatch,
    scan_only: bool,
    limit: int = 50,
    offset: int = 0,
    order_by: str | None = None,
    scope: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[tuple[str, Any]]]:
    scope = scope or _DOCS
    id_field = "edge_id" if scope["kind"] == "edges" else "node_id"
    statements: list[tuple[str, Any]] = []
    real_execute = psycopg.Cursor.execute

    def capture(self, query, params=None, **kwargs):
        text = str(query)
        if _PAGE_MARK in text and "effective_page" in text:
            statements.append((text, dict(params or {})))
        return real_execute(self, query, params, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(psycopg.Cursor, "execute", capture)
        if scan_only:
            # The reference: production's own unnarrowed path (okg#3112 step
            # 4b; before it, the reference stubbed the attr_index planner and
            # the expression candidate planner to narrow nothing).
            patch.setenv(afs.SWITCH_ENV, "off")
        mcp = _server_for(dsn)
        result = mcp._tool_manager._tools["filter"].fn(
            input=scope,
            predicate=predicate,
            generation_id=generation,
            limit=limit,
            offset=offset,
            select=[id_field, "attrs"],
            order_by=order_by,
        )
    assert "error" not in result, result
    return result, statements


def _answer(result: dict[str, Any]) -> tuple[Any, ...]:
    return (
        [row.get("node_id", row.get("edge_id")) for row in result.get("items") or []],
        [row["attrs"] for row in result.get("items") or []],
        result.get("truncated"),
        result.get("next_offset"),
    )


def _narrowed(statements: list[tuple[str, Any]]) -> bool:
    """The graph-id branch of the page is narrowed by a candidate set: the
    ordered node page joins the candidate ids, any other tests ``IN``
    (``graph_read.id_stream_branches``). A two-phase page (an undeclared
    equality, S2 design 2.4) sends no single page statement; it is read with
    no candidate set."""
    assert len(statements) <= 1, statements
    return any(
        "candidate_ids(target_id)" in sql or "graph_rows.target_id IN (" in sql
        for sql, _params in statements
    )


def _expression_narrowed(statements: list[tuple[str, Any]]) -> bool:
    """Narrowed by the declared expression indexes (slice 1's probe)."""
    return _narrowed(statements) and "%(dai_bp)s" in statements[0][0]


def _expect_narrowed(label: str, pushed: bool, scope: dict[str, Any] | None = None) -> bool:
    """Whether the page is narrowed under the configured mode: the declared
    expression indexes narrow the node cases marked ``pushed``; no edge page
    is narrowed (no edge index, decision 287d, and no other candidate set
    since step 4a); off narrows none."""
    if _mode() == "off":
        return False
    if (scope or _DOCS)["kind"] == "edges":
        return False
    return pushed


def _expect_expression_narrowed(pushed: bool, scope: dict[str, Any]) -> bool:
    """Whether the declared expression indexes narrow the page (expression
    mode only): node cases only."""
    return pushed and scope["kind"] == "nodes"


_EQ = "eq"

# Scopes. The filter tool ANDs a subtype/edge_type scope onto the predicate,
# which nests a caller's own `and` one level down; the candidate planner
# takes only TOP-LEVEL conjuncts (mcp/attr_filter_source.py). The graph page
# plans with plain nested `and` clauses spliced to the top (S2,
# graph_read.planning_predicate), so a compound predicate narrows under a
# subtype scope too.
_DOCS: dict[str, Any] = {"kind": "nodes", "subtype": "doc"}
_NODES: dict[str, Any] = {"kind": "nodes"}
_EDGES: dict[str, Any] = {"kind": "edges"}
_LINKS: dict[str, Any] = {"kind": "edges", "edge_type": "links"}

_CONJ = {
    "op": "and",
    "clauses": [
        {"op": _EQ, "field": "attrs.active", "value": True},
        {"op": "gt", "field": "attrs.rank", "value": 2},
    ],
}
_AND_NOT = {
    "op": "and",
    "clauses": [
        {"op": _EQ, "field": "attrs.active", "value": True},
        {
            "op": "not",
            "clause": {"op": _EQ, "field": "attrs.path", "value": "file02.py"},
        },
    ],
}
_MIXED = {
    "op": "and",
    "clauses": [
        {"op": _EQ, "field": "attrs.owner", "value": "alice"},
        {"op": _EQ, "field": "attrs.active", "value": True},
    ],
}
_TS_RANGE = ["2026-08-02T00:00:00Z", "2026-08-05T00:00:00Z"]

# (label, scope, predicate, pushed?) -- `pushed` says whether the index narrows.
_PARITY_CASES: list[tuple[str, dict[str, Any], dict[str, Any], bool]] = [
    ("overlay-moved-away", _DOCS, {"op": _EQ, "field": "attrs.path", "value": "file01.py"}, True),
    ("overlay-moved-into", _DOCS, {"op": _EQ, "field": "attrs.path", "value": "overlaid.py"}, True),
    ("overlay-added-key", _DOCS, {"op": _EQ, "field": "attrs.rank", "value": 6}, True),
    ("overlay-bool-flip", _DOCS, {"op": _EQ, "field": "attrs.active", "value": True}, True),
    ("conjunction-under-scope", _DOCS, _CONJ, True),
    ("conjunction", _NODES, _CONJ, True),
    ("and-with-not-residual-under-scope", _DOCS, _AND_NOT, True),
    ("and-with-not-residual", _NODES, _AND_NOT, True),
    (
        "or-residual",
        _DOCS,
        {
            "op": "or",
            "clauses": [
                {"op": _EQ, "field": "attrs.path", "value": "file01.py"},
                {"op": _EQ, "field": "attrs.rank", "value": 6},
            ],
        },
        False,
    ),
    (
        "not-residual",
        _DOCS,
        {"op": "not", "clause": {"op": _EQ, "field": "attrs.active", "value": True}},
        False,
    ),
    ("number-for-text", _DOCS, {"op": _EQ, "field": "attrs.path", "value": 42}, False),
    ("string-for-num", _DOCS, {"op": _EQ, "field": "attrs.rank", "value": "3"}, False),
    ("string-for-bool", _DOCS, {"op": _EQ, "field": "attrs.active", "value": "true"}, False),
    ("num-skips-string-row", _DOCS, {"op": _EQ, "field": "attrs.rank", "value": 3}, True),
    ("zero-match", _DOCS, {"op": _EQ, "field": "attrs.path", "value": "does-not-exist"}, True),
    ("undeclared", _DOCS, {"op": _EQ, "field": "attrs.owner", "value": "alice"}, False),
    ("undeclared-and-declared-under-scope", _DOCS, _MIXED, True),
    ("undeclared-and-declared", _NODES, _MIXED, True),
    (
        "in-list",
        _DOCS,
        {"op": "in", "field": "attrs.path", "values": ["file02.py", "overlaid.py", "file01.py"]},
        True,
    ),
    ("between", _DOCS, {"op": "between", "field": "attrs.rank", "values": [2, 4]}, True),
    ("exists-not-pushed", _DOCS, {"op": "exists", "field": "attrs.rank"}, False),
    # Declared ts: an offset spelling and the Z spelling compare as instants.
    ("ts-eq", _DOCS, {"op": _EQ, "field": "attrs.seen_at", "value": "2026-08-03T00:00:00+00:00"}, True),
    ("ts-between", _DOCS, {"op": "between", "field": "attrs.seen_at", "values": _TS_RANGE}, True),
    ("ts-gt", _NODES, {"op": "gt", "field": "attrs.seen_at", "value": "2026-08-07T00:00:00Z"}, True),
    ("ts-unparseable-value", _DOCS, {"op": _EQ, "field": "attrs.seen_at", "value": "not-a-timestamp"}, False),
    # Edges: `pushed` says whether the clause is a narrowable shape; since
    # step 4a no candidate set narrows an edge page (no edge index).
    ("edge-eq", _EDGES, {"op": _EQ, "field": "attrs.label", "value": "l1"}, True),
    ("edge-overlay-moved-into", _EDGES, {"op": _EQ, "field": "attrs.label", "value": "overlaid-edge"}, True),
    ("edge-overlay-moved-away", _EDGES, {"op": _EQ, "field": "attrs.label", "value": "l0"}, True),
    ("edge-number-for-text", _EDGES, {"op": _EQ, "field": "attrs.label", "value": 7}, False),
    ("edge-num-between", _EDGES, {"op": "between", "field": "attrs.rank", "values": [1, 3]}, True),
    ("edge-ts-gt", _EDGES, {"op": "gt", "field": "attrs.seen_at", "value": "2026-09-03T00:00:00Z"}, True),
    ("edge-under-edge-type-scope", _LINKS, {"op": _EQ, "field": "attrs.label", "value": "l3"}, True),
    ("edge-zero-match", _EDGES, {"op": _EQ, "field": "attrs.label", "value": "nope"}, True),
]


@pytest.mark.parametrize(
    ("label", "scope", "predicate", "pushed"),
    _PARITY_CASES,
    ids=[case[0] for case in _PARITY_CASES],
)
def test_index_driven_page_matches_scan_path(
    published: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
    label: str,
    scope: dict[str, Any],
    predicate: dict[str, Any],
    pushed: bool,
) -> None:
    dsn, generation, _suffix = published
    indexed, indexed_sql = _run_filter(
        dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=False,
        scope=scope,
    )
    scanned, scanned_sql = _run_filter(
        dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=True,
        scope=scope,
    )
    assert _answer(indexed) == _answer(scanned), label
    assert _narrowed(indexed_sql) is _expect_narrowed(label, pushed, scope), label
    assert all("okg.attr_index" not in sql for sql, _p in indexed_sql), label
    if _mode() == "expression":
        assert _expression_narrowed(indexed_sql) is _expect_expression_narrowed(
            pushed, scope
        ), label
    assert _narrowed(scanned_sql) is False


_EDGE_CASES = [case for case in _PARITY_CASES if case[1]["kind"] == "edges"]


def test_edges_are_answered_by_the_exact_predicate_and_equal_off(
    published: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """okg#3112 decision 287d and step 4a: no expression index is declared for
    edges, and no other candidate set exists, whatever the mode the file runs
    under. Every edge case returns the page ``off`` returns in an explicit
    ``expression`` and in the default (switch unset), with identical notes:

    * never narrowed, and the page statement does not read okg.attr_index;
    * the response names ``kind_not_indexed`` for the edge attribute, with no
      fallback field;
    * an overlay-touched edge still comes back through the exact predicate;
    * no edge index exists on the edge table (the reconciler built none).

    Failure case: with the planner stubbed out the page is the same."""
    dsn, generation, _suffix = published
    assert len(_EDGE_CASES) >= 8
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.declared_attr_indexes WHERE target_kind = 'edge'"
        )
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT count(*) FROM pg_class WHERE relname LIKE 'okg\\_dattr\\_e\\_%'")
        assert cur.fetchone()[0] == 0, "an edge expression index was built"
    for label, scope, predicate, _pushed in _EDGE_CASES:
        attr = next(
            c["field"].split(".", 1)[1]
            for c in ([predicate] + list(predicate.get("clauses") or []))
            if isinstance(c.get("field"), str) and c["field"].startswith("attrs.")
        )
        monkeypatch.setenv(afs.SWITCH_ENV, "off")
        off, off_sql = _run_filter(
            dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=False,
            scope=scope,
        )
        assert not _narrowed(off_sql), label
        notes = {}
        for mode in ("expression", None):
            if mode is None:
                monkeypatch.delenv(afs.SWITCH_ENV)
            else:
                monkeypatch.setenv(afs.SWITCH_ENV, mode)
            got, got_sql = _run_filter(
                dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=False,
                scope=scope,
            )
            assert _answer(got) == _answer(off), (label, mode)
            assert not _narrowed(got_sql), (label, mode)
            assert all("okg.attr_index" not in sql for sql, _p in got_sql), (label, mode)
            assert got.get("notes") == [
                {"pushdown": "not_covered", "attrs": [{"attr": attr, "reason": "kind_not_indexed"}]}
            ], (label, mode, got.get("notes"))
            notes[mode] = got.get("notes")
        assert notes["expression"] == notes[None], label
        stubbed, stubbed_sql = _run_filter(
            dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=True,
            scope=scope,
        )
        assert _answer(stubbed) == _answer(off), label
        assert not _narrowed(stubbed_sql), label
    ids = _answer(
        _run_filter(
            dsn, generation, {"op": _EQ, "field": "attrs.label", "value": "overlaid-edge"},
            monkeypatch=monkeypatch, scan_only=False, scope=_EDGES,
        )[0]
    )[0]
    assert len(ids) == 1, "the overlaid edge is returned by the exact predicate"


def test_overlay_cases_have_the_expected_answers(
    published: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Parity alone would pass if both paths were wrong the same way."""
    dsn, generation, suffix = published

    def ids(predicate: dict[str, Any]) -> list[str]:
        result, _ = _run_filter(
            dsn, generation, predicate, monkeypatch=monkeypatch, scan_only=False
        )
        return [row["node_id"] for row in result["items"]]

    assert ids({"op": _EQ, "field": "attrs.path", "value": "file01.py"}) == []
    assert ids({"op": _EQ, "field": "attrs.path", "value": "overlaid.py"}) == [
        _target(1, suffix)
    ]
    assert _target(8, suffix) in ids({"op": _EQ, "field": "attrs.rank", "value": 6})
    assert _target(11, suffix) in ids(
        {"op": _EQ, "field": "attrs.active", "value": True}
    )
    assert ids({"op": _EQ, "field": "attrs.rank", "value": "3"}) == [
        _target(3, suffix)
    ]
    assert _target(3, suffix) not in ids(
        {"op": _EQ, "field": "attrs.rank", "value": 3}
    )
    assert ids({"op": _EQ, "field": "attrs.path", "value": "does-not-exist"}) == []


@pytest.mark.parametrize(
    ("order_by", "limit"),
    [
        (None, 4),
        ("attrs.path", 4),
        ("attrs.rank desc", 3),
        ("node_id desc", 5),
    ],
)
def test_index_driven_pagination_matches_scan_path(
    published: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
    order_by: str | None,
    limit: int,
) -> None:
    dsn, generation, _suffix = published
    predicate = {"op": _EQ, "field": "attrs.active", "value": True}
    offset = 0
    pages = 0
    while True:
        indexed, indexed_sql = _run_filter(
            dsn,
            generation,
            predicate,
            monkeypatch=monkeypatch,
            scan_only=False,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )
        scanned, _ = _run_filter(
            dsn,
            generation,
            predicate,
            monkeypatch=monkeypatch,
            scan_only=True,
            limit=limit,
            offset=offset,
            order_by=order_by,
        )
        assert _answer(indexed) == _answer(scanned), (order_by, offset)
        assert _narrowed(indexed_sql) is _expect_narrowed("pagination", True)
        pages += 1
        if not indexed["truncated"]:
            break
        offset = indexed["next_offset"]
    assert pages >= 3


def _plan_nodes(node: dict[str, Any]) -> Iterator[dict[str, Any]]:
    yield node
    for child in node.get("Plans") or []:
        yield from _plan_nodes(child)


def test_the_expression_index_drives_the_page_plan(
    published: tuple[str, str, str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EXPLAIN ANALYZE: the declared expression index narrows the page, the
    plan never reads okg.attr_index, and the overlay lateral runs only for
    candidates.

    The predicate matches nothing. The one target whose ``path`` was
    overlaid after publish comes from the overlay-id branch, which tests its
    overlaid value; the graph-id branch reads the empty declared candidate
    set. So the overlay merge over attribute_effective_attrs never runs.
    Without the narrowing the graph-id branch reads every pinned doc row
    (30 here; 527k on the graph that motivated this change).
    """
    dsn, generation, _suffix = published
    result, statements = _run_filter(
        dsn,
        generation,
        {"op": _EQ, "field": "attrs.path", "value": "does-not-exist"},
        monkeypatch=monkeypatch,
        scan_only=False,
    )
    assert result["items"] == []
    assert len(statements) == 1
    if not _expect_narrowed("plan", True):
        # off: no candidate set, nothing to read from an index.
        assert not _narrowed(statements)
        return
    assert _expression_narrowed(statements)
    sql, params = statements[0]
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("ANALYZE")
        cur.execute("EXPLAIN (ANALYZE, FORMAT JSON) " + sql, params)
        plan = cur.fetchone()[0][0]["Plan"]
    nodes = list(_plan_nodes(plan))
    dump = json.dumps(plan)
    assert not [n for n in nodes if n.get("Relation Name") == "attr_index"], dump
    # Ids first (S2 design 2.3): the overlay merge runs once per fetched id.
    # Here no id is fetched: the declared candidate set is empty, and the
    # overlay-id branch tests the one key's overlaid value ("overlaid.py")
    # itself. The overlay table's reads in the two id branches are aliased
    # `o` (EXPLAIN names the second `o_1`); every other read of it is the
    # merge, which never runs. (Without the narrowing the graph-id branch
    # would scan every doc row.)
    overlay_reads = [n for n in nodes
                     if n.get("Relation Name") == "attribute_effective_attrs"]

    def in_id_branch(node: dict[str, Any]) -> bool:
        alias = str(node.get("Alias"))
        return alias == "o" or alias.startswith("o_")

    merges = [n for n in overlay_reads if not in_id_branch(n)]
    assert [n for n in overlay_reads if in_id_branch(n)], dump
    assert merges, "the plan has no overlay merge: " + dump
    assert sum(int(n.get("Actual Loops") or 0) for n in merges) == 0, (
        "the overlay merge ran for non-candidates: " + dump)


def _filter_once(mcp: Any, generation: str) -> dict[str, Any]:
    result = mcp._tool_manager._tools["filter"].fn(
        input={"kind": "nodes", "subtype": "doc"},
        predicate={"op": _EQ, "field": "attrs.path", "value": "file02.py"},
        generation_id=generation,
        limit=5,
        select=["node_id"],
    )
    assert "error" not in result, result
    return result


def _close(mcp: Any) -> None:
    mcp._okg_pool.close()
    if mcp._okg_write_pool:
        mcp._okg_write_pool.close()


def test_a_pin_past_the_old_attribute_retention_window_is_served(
    published: tuple[str, str, str],
) -> None:
    """Issue #2621 and S2 design 5.3: each publish prunes closed attribute
    intervals of the materialized copy older than its retention window, so
    the materialized read refused a filter pinned to the first generation
    after two attribute changes. Every read now takes the graph rows at the
    pin plus that generation's overlays (PACT lane-s-storage-cutover), so
    that pin is served with its own values, as the newer pins are.

    Failure case: target 2's path moved in the second generation, so the
    same filter at the newer pins no longer returns it."""
    dsn, first, suffix = published
    target = _target(2, suffix)

    def change(path: str) -> str:
        with psycopg.connect(dsn, autocommit=True) as conn:
            _apply(conn, [NodeFact(
                target, "doc", {**_row_attrs(2), "path": path},
                {"id": target}, {"v": 1},
            )])
        return _publish(dsn)

    second = change("moved02.py")
    third = change("moved02b.py")
    mcp = server_mod.build_server(dsn=dsn)
    try:
        assert [row["node_id"] for row in _filter_once(mcp, first)["items"]] == [target]
        assert _filter_once(mcp, second)["items"] == []
        assert _filter_once(mcp, third)["items"] == []
    finally:
        _close(mcp)
