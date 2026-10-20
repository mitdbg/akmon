"""Calls edges follow their call site (issue #2836).

A calls edge is identified by (caller, calls, callee), so a call site that
moves keeps the edge's identity while its ``line`` and ``reference_id``
change. The linker used to skip every edge whose identity was already live,
so the graph kept the old line and disagreed with a rebuild. It also emitted
one calls edge per call site in read order, so a caller with two call sites
to the same callee got whichever came first in the database scan.

These tests run without a database: the graph reads are replaced with fixed
rows and the fact write is captured.
"""

from __future__ import annotations

from typing import Any

import pytest

import okg.substrate.library.linkers.code_reference as code_reference

CodeReferenceResolver = code_reference.CodeReferenceResolver
_Node = code_reference._Node

CALLER = "symbol:demo:tests/test_mw.py:test_caching_json_write"
CALLEE = "symbol:demo:tinydb/database.py:TinyDB"
LINEAGE = "nomos_source_lineage"


def _symbol(node_id: str, path: str, name: str) -> _Node:
    return _Node(node_id, {
        "repo": "demo",
        "path": path,
        "name": name,
        "qualified_name": name,
    })


def _call(reference_id: str, line: int) -> _Node:
    return _Node(reference_id, {
        "repo": "demo",
        "path": "tests/test_mw.py",
        "name": "TinyDB",
        "qualified_name": "TinyDB",
        "enclosing_symbol_id": CALLER,
        "start_line": line,
    })


SYMBOLS = [
    _symbol(CALLER, "tests/test_mw.py", "test_caching_json_write"),
    _symbol(CALLEE, "tinydb/database.py", "TinyDB"),
]


class _Cursor:
    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _Conn:
    def cursor(self) -> _Cursor:
        return _Cursor()


def _run(
    monkeypatch: pytest.MonkeyPatch,
    *,
    references: list[_Node],
    live_edges: list[dict[str, Any]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    """Run a full (unscoped) enrich; return the edges handed to the writer."""
    nodes = {
        "code_import": [],
        "code_reference": references,
        "code_symbol": SYMBOLS,
        "dependency_package": [],
    }
    monkeypatch.setattr(
        code_reference, "_fetch_nodes", lambda _cur, subtype: nodes[subtype],
    )

    def live(_cur: Any, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        assert "FROM okg.graph_edges" in sql
        wanted = set(params["edge_ids"])
        return [
            row for row in live_edges
            if code_reference._mint_edge_id(
                row["src"], row["edge_type"], row["dst"],
            ) in wanted
        ]

    monkeypatch.setattr(code_reference, "_chronos_query", live)
    # These edges are unranked (no declarer rows) and the linker owns no
    # earlier declarations to retract (item 92a).
    monkeypatch.setattr(
        code_reference, "_recorded_declarations", lambda _cur, _ids: {},
    )
    monkeypatch.setattr(
        code_reference, "_own_declarations", lambda _cur, *, srcs: set(),
    )
    written: list[tuple[str, str, str, dict[str, Any]]] = []

    def capture(_self: Any, _cur: Any, *, edges, generation_id):
        del generation_id
        written.extend(edges)
        return len(edges), 0

    monkeypatch.setattr(CodeReferenceResolver, "_emit_edges", capture)
    CodeReferenceResolver().enrich(_Conn(), generation_id=7)  # type: ignore[arg-type]
    return written


def _calls(edges: list[tuple[str, str, str, dict[str, Any]]]):
    return [edge for edge in edges if edge[1] == "calls"]


def _resolved_live_rows(
    monkeypatch: pytest.MonkeyPatch,
    references: list[_Node],
) -> list[dict[str, Any]]:
    """Live rows exactly as the linker would have written them earlier, plus
    the lineage attribute the publisher stamps on every derived edge."""
    nodes = {
        "code_import": [],
        "code_reference": references,
        "code_symbol": SYMBOLS,
        "dependency_package": [],
    }
    monkeypatch.setattr(
        code_reference, "_fetch_nodes", lambda _cur, subtype: nodes[subtype],
    )
    edges, _ = CodeReferenceResolver()._resolve_edges(None, skip_existing=False)
    return [
        {
            "src": src,
            "edge_type": edge_type,
            "dst": dst,
            "attrs": {**attrs, LINEAGE: {"status": "stamped by the publisher"}},
        }
        for src, edge_type, dst, attrs in edges
    ]


def test_moved_call_site_rewrites_line_and_reference_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    before = _resolved_live_rows(
        monkeypatch, [_call("reference:tinydb-call@106", 106)],
    )
    written = _run(
        monkeypatch,
        references=[_call("reference:tinydb-call@93", 93)],
        live_edges=before,
    )
    calls = _calls(written)
    assert [(src, dst) for src, _t, dst, _a in calls] == [(CALLER, CALLEE)]
    assert calls[0][3]["line"] == 93
    assert calls[0][3]["reference_id"] == "reference:tinydb-call@93"


def test_unchanged_edges_are_not_rewritten(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    references = [_call("reference:tinydb-call@93", 93)]
    live = _resolved_live_rows(monkeypatch, references)
    assert _calls([(r["src"], r["edge_type"], r["dst"], r["attrs"]) for r in live])
    written = _run(monkeypatch, references=references, live_edges=live)
    assert written == []


def test_edge_absent_from_graph_is_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    written = _run(
        monkeypatch,
        references=[_call("reference:tinydb-call@93", 93)],
        live_edges=[],
    )
    assert len(_calls(written)) == 1


@pytest.mark.parametrize("order", ["first_call_first", "second_call_first"])
def test_two_call_sites_yield_one_edge_at_the_first_line(
    monkeypatch: pytest.MonkeyPatch,
    order: str,
) -> None:
    first = _call("reference:tinydb-call@93", 93)
    second = _call("reference:tinydb-call@106", 106)
    references = [first, second] if order == "first_call_first" else [second, first]
    written = _run(monkeypatch, references=references, live_edges=[])
    calls = _calls(written)
    assert len(calls) == 1
    assert calls[0][3]["line"] == 93
    assert calls[0][3]["reference_id"] == "reference:tinydb-call@93"
    # Each call site still keeps its own references edge.
    assert {
        src for src, edge_type, _dst, _attrs in written
        if edge_type == "references"
    } == {first.node_id, second.node_id}


class _SiblingReadFailed(RuntimeError):
    pass


def _scoped_resolve(
    monkeypatch: pytest.MonkeyPatch,
    *,
    sibling_read_fails: bool,
) -> tuple[list[tuple[str, str, str, dict[str, Any]]], list[dict[str, Any]]]:
    """Resolve with only the call site at line 106 dirty.

    Graph reads are replaced at ``_chronos._query``, the one function both
    ``_chronos.query`` (a failed read yields no rows) and
    ``_chronos.query_strict`` (a failed read raises) call, so the test sees
    which of the two the linker used."""
    dirty = _call("reference:tinydb-call@106", 106)
    sibling = _call("reference:tinydb-call@93", 93)
    sibling_queries: list[dict[str, Any]] = []

    def read(
        _cur: Any,
        sql: str,
        params: dict[str, Any] | None,
        *,
        strict: bool,
    ) -> list[dict[str, Any]]:
        if "enclosing_symbol_id' = ANY(:callers)" in sql:
            sibling_queries.append(dict(params or {}))
            if sibling_read_fails:
                if strict:
                    raise _SiblingReadFailed("branch unreadable")
                return []
            return [
                {"node_id": node.node_id, "attrs": node.attrs}
                for node in (dirty, sibling)
            ]
        if "subtype = 'code_reference'" in sql:
            return [{"node_id": dirty.node_id, "attrs": dirty.attrs}]
        return []

    monkeypatch.setattr(code_reference._chronos, "_query", read)
    monkeypatch.setattr(
        code_reference,
        "_fetch_nodes",
        lambda _cur, subtype: SYMBOLS if subtype == "code_symbol" else [],
    )
    edges, _ = CodeReferenceResolver()._resolve_edges(
        None,
        skip_existing=False,
        scope=code_reference._ResolutionScope(
            reference_ids=frozenset({dirty.node_id}),
            paths=frozenset({"tests/test_mw.py"}),
        ),
    )
    return edges, sibling_queries


def test_scoped_run_reads_every_call_site_of_the_caller(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Only the later call site is dirty; the caller's earlier call site must
    still decide the edge's line, as it does in a rebuild."""
    edges, sibling_queries = _scoped_resolve(
        monkeypatch, sibling_read_fails=False,
    )
    assert sibling_queries == [{"callers": [CALLER]}]
    calls = _calls(edges)
    assert len(calls) == 1
    assert calls[0][3]["line"] == 93


def test_failed_sibling_read_stops_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If the caller's other call sites cannot be read, the run must fail
    rather than derive the edge from the dirty call site alone (line 106,
    where a rebuild says 93)."""
    with pytest.raises(_SiblingReadFailed):
        _scoped_resolve(monkeypatch, sibling_read_fails=True)


class _GucCursor(_Cursor):
    """Answers the session-setting reads a Chronos checkout makes first."""

    def __init__(self, connection: "_AutocommitConn") -> None:
        self.connection = connection

    def execute(self, *_args: object) -> None:
        return None

    def fetchone(self) -> tuple[None]:
        return (None,)


class _AutocommitConn:
    autocommit = True

    def cursor(self) -> _GucCursor:
        return _GucCursor(self)


class _GraphReadFailed(RuntimeError):
    pass


def test_failed_graph_read_fails_the_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2941: the linker's graph read turned any error into no rows, so
    a broken checkout read as "no code symbols" and the run succeeded with no
    edges. The error must reach the publisher, which records the run as
    failed and keeps the enricher's watermark."""
    from okg.substrate.graph_store import ChronosGraphStore

    def broken(*_args: object, **_kwargs: object) -> None:
        raise _GraphReadFailed("checkout unavailable")

    monkeypatch.setattr(ChronosGraphStore, "from_connection", broken)
    with pytest.raises(_GraphReadFailed):
        CodeReferenceResolver().enrich(
            _AutocommitConn(),  # type: ignore[arg-type]
            generation_id=0,
        )


def test_failed_scoped_graph_read_fails_the_incremental_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #2941 review: an incremental run reads only the dirty call
    sites. A failed read there returned no rows, so the run emitted nothing,
    reported success, and the publisher advanced the watermark past the
    edges it never wrote."""
    from okg.substrate.enrichers.base import IncrementalContext
    from okg.substrate.graph_store import ChronosGraphStore

    dirty = _call("reference:demo:tests/test_mw.py:TinyDB:106", 106)
    # The dirty-node read succeeds, so the run takes the scoped path.
    monkeypatch.setattr(
        code_reference,
        "_fetch_dirty_nodes",
        lambda _cur, _ids: [{
            "node_id": dirty.node_id,
            "subtype": "code_reference",
            "attrs": dirty.attrs,
        }],
    )

    def broken(*_args: object, **_kwargs: object) -> None:
        raise _GraphReadFailed("statement timeout")

    monkeypatch.setattr(ChronosGraphStore, "from_connection", broken)
    incremental = IncrementalContext(
        prev_fact_watermark=1,
        prev_edge_watermark=1,
        dirty_node_ids=frozenset({dirty.node_id}),
        dirty_edge_ids=frozenset(),
        is_first_run=False,
        last_status="success",
        prev_catalog_version_id=None,
        current_catalog_version_id=None,
    )
    with pytest.raises(_GraphReadFailed):
        CodeReferenceResolver().enrich(
            _AutocommitConn(),  # type: ignore[arg-type]
            generation_id=0,
            incremental=incremental,
        )
