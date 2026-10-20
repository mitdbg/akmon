from __future__ import annotations

from typing import Any

from okg.bench.views import file_symbols, symbol_calls
from okg.substrate.views.base import ViewDeltaScope
from okg.views.types import Anchor


class _FakeGraph:
    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.symbols = {
            "symbol:changed-callee": {
                "name": "changed_callee",
                "qualified_name": "pkg.changed_callee",
                "kind": "function",
            },
            "symbol:caller-from-callee": {
                "name": "caller_from_callee",
                "qualified_name": "pkg.caller_from_callee",
                "kind": "function",
                "path": "pkg.py",
            },
            "symbol:caller-from-edge": {
                "name": "caller_from_edge",
                "qualified_name": "pkg.caller_from_edge",
                "kind": "function",
                "path": "pkg.py",
            },
            "symbol:other-callee": {
                "name": "other_callee",
                "qualified_name": "pkg.other_callee",
                "kind": "function",
            },
        }
        self.calls = [
            {
                "edge_id": 7,
                "src": "symbol:caller-from-edge",
                "dst": "symbol:other-callee",
            },
            {
                "edge_id": 8,
                "src": "symbol:caller-from-callee",
                "dst": "symbol:changed-callee",
            },
        ]

    def query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.queries.append((sql, params))
        if "okg.graph_edges" in sql and "dst = ANY" in sql:
            ids = set(params["ids"])
            return [
                {"src": row["src"]}
                for row in self.calls
                if row["dst"] in ids
            ]
        if "okg.graph_edges" in sql and "edge_id = ANY" in sql:
            ids = {int(item) for item in params["ids"]}
            return [
                {"edge_id": row["edge_id"], "src": row["src"]}
                for row in self.calls
                if row["edge_id"] in ids
            ]
        if "okg.graph_edges" in sql and "src = ANY" in sql:
            ids = set(params["ids"])
            return [
                {"src": row["src"], "dst": row["dst"]}
                for row in self.calls
                if row["src"] in ids
            ]
        if "okg.graph_edges" in sql:
            return [
                {"src": row["src"], "dst": row["dst"]}
                for row in self.calls
            ]
        if "okg.graph_nodes" in sql and "node_id = ANY" in sql:
            ids = set(params["ids"])
            return [
                {"node_id": node_id, "attrs": attrs}
                for node_id, attrs in self.symbols.items()
                if node_id in ids
            ]
        if "okg.graph_nodes" in sql:
            return [
                {"node_id": node_id, "attrs": attrs}
                for node_id, attrs in self.symbols.items()
            ]
        raise AssertionError(f"unexpected query: {sql}")


class _FakeStore:
    def __init__(self, graph: _FakeGraph) -> None:
        self.graph = graph

    def checkout(self, *, branch: str):
        assert branch == "default"
        return self.graph


class _FileGraph:
    file_id = "file:tinydb:tinydb/table.py"
    semantic_symbol_id = "symbol:tinydb:semantic:new"

    def __init__(self) -> None:
        self.queries: list[tuple[str, dict[str, Any]]] = []
        self.files = {
            self.file_id: {"path": "tinydb/table.py"},
        }
        self.symbols = {
            self.semantic_symbol_id: {
                "name": "okg_delta",
                "kind": "function",
                "path": "tinydb/table.py",
                "qualified_name": "tinydb.table.okg_delta",
                "start_line": 12,
            },
        }
        self.contains = [
            {
                "edge_id": 11,
                "src": self.file_id,
                "dst": self.semantic_symbol_id,
            },
        ]

    def query(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        self.queries.append((sql, params))
        if "JOIN okg.graph_nodes s" in sql:
            ids = (
                set(params["ids"])
                if "src = ANY" in sql
                else {row["src"] for row in self.contains}
            )
            rows = []
            for edge in self.contains:
                if edge["src"] not in ids:
                    continue
                symbol_attrs = self.symbols.get(edge["dst"])
                if symbol_attrs is None:
                    continue
                file_attrs = self.files.get(edge["src"], {})
                rows.append(
                    {
                        "file_id": edge["src"],
                        "symbol_id": edge["dst"],
                        "file_path": (
                            file_attrs.get("path")
                            or symbol_attrs.get("path")
                            or edge["src"]
                        ),
                        "symbol_name": symbol_attrs.get("name"),
                        "symbol_kind": symbol_attrs.get("kind"),
                        "qualified_name": symbol_attrs.get("qualified_name"),
                        "start_line": symbol_attrs.get("start_line"),
                    }
                )
            return rows
        if "okg.graph_edges" in sql and "dst = ANY" in sql:
            ids = set(params["ids"])
            return [
                {"src": row["src"], "dst": row["dst"]}
                for row in self.contains
                if row["dst"] in ids
            ]
        if "okg.graph_edges" in sql and "src = ANY" in sql:
            ids = set(params["ids"])
            return [
                {"src": row["src"], "dst": row["dst"]}
                for row in self.contains
                if row["src"] in ids
            ]
        if "okg.graph_edges" in sql:
            return [
                {"src": row["src"], "dst": row["dst"]}
                for row in self.contains
            ]
        if "okg.graph_nodes" in sql and "subtype = :st" in sql:
            rows = self.files if params["st"] == "source_file" else self.symbols
            if "node_id = ANY" in sql:
                ids = set(params["ids"])
                return [
                    {"node_id": node_id, "attrs": attrs}
                    for node_id, attrs in rows.items()
                    if node_id in ids
                ]
            return [
                {"node_id": node_id, "attrs": attrs}
                for node_id, attrs in rows.items()
            ]
        raise AssertionError(f"unexpected query: {sql}")


class _FakeCursor:
    def __init__(self, conn: "_FakeConn") -> None:
        self.conn = conn
        self._select_prior = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql: str, params: dict[str, Any] | None = None) -> None:
        self.conn.executed.append((sql, params or {}))
        if sql.strip().upper().startswith("TRUNCATE "):
            self.conn.inserted_rows.clear()
        self._select_prior = "derived_output_manifests" in sql

    def executemany(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        self.conn.executed.append((sql, {}))
        self.conn.inserted_rows.extend(rows)

    def fetchone(self):
        if not self._select_prior:
            return None
        return (self.conn.prior_payload,)


class _FakeConn:
    def __init__(self, prior_payload: dict[str, Any] | None = None) -> None:
        self.prior_payload = prior_payload
        self.executed: list[tuple[str, dict[str, Any]]] = []
        self.inserted_rows: list[tuple[Any, ...]] = []

    def cursor(self):
        return _FakeCursor(self)


class _FakeIntervalDB:
    def __init__(self) -> None:
        self.sql: str | None = None
        self.params: list[Any] | None = None

    def execute(self, sql: str, params: list[Any]):
        self.sql = sql
        self.params = params
        return self

    def fetchall(self) -> list[dict[str, Any]]:
        return [
            {
                "file_id": "file:tinydb:tinydb/table.py",
                "symbol_id": "symbol:tinydb:semantic:abc",
                "file_path": "tinydb/table.py",
                "symbol_name": "insert",
                "symbol_kind": "function",
                "qualified_name": "tinydb.table.insert",
                "start_line": 12,
            }
        ]


class _FakeIntervalBackend:
    def __init__(self) -> None:
        self.db = _FakeIntervalDB()

    def _require_table(self, table_name: str):
        physical = {
            "okg.graph_edges": "_edge_interval",
            "okg.graph_nodes": "_node_interval",
        }[table_name]
        return type("Meta", (), {"physical_name": physical})()

    def _current_segment(self, branch: str):
        assert branch == "default"
        return type("Segment", (), {"branch_point": 123})()


class _FakeIntervalStore:
    def __init__(self) -> None:
        self.context = type(
            "Context",
            (),
            {"_backend": _FakeIntervalBackend()},
        )()


def _prior_payload(*anchors: Anchor) -> dict[str, Any]:
    return {
        "kind": "json",
        "value": {
            "view_manifest": {
                "fragments": [
                    {
                        "id": anchor.id,
                        "kind": "quote",
                        "sources": [anchor.id],
                        "source_hashes": {
                            anchor.id: anchor.content_hash,
                        },
                        "content": anchor.content,
                    }
                    for anchor in anchors
                ]
            }
        },
    }


def test_dirty_caller_scopes_falls_back_when_dirty_edge_is_unresolved() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(changed_edges={"calls": [7, 999]})

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) is None


def test_dirty_caller_scopes_uses_endpoint_metadata_for_deleted_edges() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(
        changed_edges={"calls": [7, 999]},
        changed_edge_endpoints={
            "calls": [
                {
                    "edge_id": 999,
                    "src": "symbol:deleted-edge-caller",
                    "dst": "symbol:deleted-edge-callee",
                }
            ]
        },
    )

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) == {
        "symbol:caller-from-edge",
        "symbol:deleted-edge-caller",
    }


def test_build_corpus_scopes_call_rows_and_preserves_clean_anchors(
    monkeypatch,
) -> None:
    graph = _FakeGraph()
    clean_anchor = Anchor.from_content(
        "symbol_calls:symbol:clean-caller",
        '{"calls": 1, "node_id": "symbol:clean-caller", "symbol": "pkg.clean"}',
    )
    conn = _FakeConn(prior_payload=_prior_payload(clean_anchor))

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda *_, **__: _FakeStore(graph),
    )

    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:changed-callee"]},
        changed_edges={"calls": [7]},
    )

    corpus = symbol_calls.build_corpus(
        conn,
        generation_id=42,
        delta_scope=delta_scope,
    )

    assert delta_scope.scoped is True
    assert delta_scope.dirty_scope_keys == {
        "symbol:caller-from-callee",
        "symbol:caller-from-edge",
    }
    assert set(corpus) == {
        "symbol_calls:symbol:clean-caller",
        "symbol_calls:symbol:caller-from-callee",
        "symbol_calls:symbol:caller-from-edge",
    }
    assert corpus[clean_anchor.id].content_hash == clean_anchor.content_hash
    assert sorted(conn.inserted_rows) == [
        (
            "symbol:caller-from-callee",
            "symbol:changed-callee",
            "pkg.caller_from_callee",
            "pkg.changed_callee",
            "function",
            "pkg.py",
        ),
        (
            "symbol:caller-from-edge",
            "symbol:other-callee",
            "pkg.caller_from_edge",
            "pkg.other_callee",
            "function",
            "pkg.py",
        ),
    ]
    assert not any(
        "okg.graph_nodes" in sql and "node_id = ANY" not in sql
        for sql, _ in graph.queries
    )
    assert not any(
        "okg.graph_edges" in sql
        and all(token not in sql for token in ("src = ANY", "dst = ANY", "edge_id = ANY"))
        for sql, _ in graph.queries
    )


def test_dirty_caller_scopes_ignores_symbol_without_call_rows() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:other-callee"]},
    )

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) == {
        "symbol:caller-from-edge",
    }


def test_dirty_caller_scopes_ignores_isolated_symbol() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:isolated"]},
    )

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) == set()


def test_dirty_caller_scopes_skips_lifecycle_symbol_without_call_edge_delta() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:isolated"]},
        changed_node_attr_fields={
            "code_symbol": {"symbol:isolated": ["__new__", "name"]}
        },
    )

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) == set()
    assert graph.queries == []


def test_dirty_caller_scopes_queries_stable_symbol_attr_change() -> None:
    graph = _FakeGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:other-callee"]},
        changed_node_attr_fields={
            "code_symbol": {"symbol:other-callee": ["name"]}
        },
    )

    assert symbol_calls._dirty_caller_scopes(graph, delta_scope) == {
        "symbol:caller-from-edge",
    }
    assert any("dst = ANY" in sql for sql, _params in graph.queries)


def test_build_corpus_empty_dirty_scope_skips_prior_anchor_load_and_rows(
    monkeypatch,
) -> None:
    graph = _FakeGraph()
    clean_anchor = Anchor.from_content(
        "symbol_calls:symbol:clean-caller",
        '{"calls": 1, "node_id": "symbol:clean-caller", "symbol": "pkg.clean"}',
    )
    conn = _FakeConn(prior_payload=_prior_payload(clean_anchor))

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda *_, **__: _FakeStore(graph),
    )

    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:isolated"]},
    )

    corpus = symbol_calls.build_corpus(
        conn,
        generation_id=42,
        delta_scope=delta_scope,
    )

    assert corpus == {}
    assert delta_scope.scoped is True
    assert delta_scope.dirty_scope_keys == set()
    assert conn.inserted_rows == []
    assert not any(
        "CREATE TEMP TABLE" in sql
        for sql, _params in conn.executed
    )


def test_build_corpus_empty_delta_carry_forward_avoids_graph_checkout(
    monkeypatch,
) -> None:
    clean_anchor = Anchor.from_content(
        "symbol_calls:symbol:clean-caller",
        '{"calls": 1, "node_id": "symbol:clean-caller", "symbol": "pkg.clean"}',
    )
    conn = _FakeConn(prior_payload=_prior_payload(clean_anchor))

    from okg.substrate.graph_store import ChronosGraphStore

    def _unexpected_graph_checkout(*_args, **_kwargs):
        raise AssertionError("empty caller delta should not open Chronos")

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        _unexpected_graph_checkout,
    )

    delta_scope = ViewDeltaScope(
        changed_nodes={"source_file": ["file:pkg.py"]},
    )

    corpus = symbol_calls.build_corpus(
        conn,
        generation_id=42,
        delta_scope=delta_scope,
    )

    assert corpus == {}
    assert delta_scope.scoped is True
    assert delta_scope.dirty_scope_keys == set()
    assert conn.inserted_rows == []
    assert not any(
        "CREATE TEMP TABLE" in sql
        for sql, _params in conn.executed
    )


def test_file_symbols_dirty_scopes_use_contains_endpoint_for_semantic_ids() -> None:
    graph = _FileGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": [graph.semantic_symbol_id]},
        changed_edge_endpoints={
            "contains": [
                {
                    "edge_id": 99,
                    "src": graph.file_id,
                    "dst": graph.semantic_symbol_id,
                }
            ]
        },
    )

    assert file_symbols._dirty_file_scopes(graph, delta_scope) == {graph.file_id}
    assert graph.queries == []


def test_file_symbols_dirty_scopes_fall_back_for_unowned_semantic_id() -> None:
    graph = _FileGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": ["symbol:tinydb:semantic:missing"]},
    )

    assert file_symbols._dirty_file_scopes(graph, delta_scope) is None
    assert any("dst = ANY" in sql for sql, _params in graph.queries)


def test_file_symbols_dirty_scopes_ignore_content_only_source_file_change() -> None:
    graph = _FileGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"source_file": [graph.file_id]},
        changed_node_attr_fields={
            "source_file": {graph.file_id: ["content_sha"]},
        },
    )

    assert file_symbols._dirty_file_scopes(delta_scope) == set()


def test_file_symbols_dirty_scopes_fall_back_without_source_file_attr_fields() -> None:
    graph = _FileGraph()
    delta_scope = ViewDeltaScope(
        changed_nodes={"source_file": [graph.file_id]},
    )

    assert file_symbols._dirty_file_scopes(delta_scope) is None


def test_file_symbols_interval_rows_materialize_scoped_edges_first() -> None:
    store = _FakeIntervalStore()

    rows = file_symbols._interval_file_symbol_rows(
        store,
        file_filter={"file:tinydb:tinydb/table.py"},
    )

    assert rows == [
        {
            "file_id": "file:tinydb:tinydb/table.py",
            "symbol_id": "symbol:tinydb:semantic:abc",
            "file_path": "tinydb/table.py",
            "symbol_name": "insert",
            "symbol_kind": "function",
            "qualified_name": "tinydb.table.insert",
            "start_line": 12,
        }
    ]
    db = store.context._backend.db
    assert db.sql is not None
    assert "WITH bp(branch_point) AS MATERIALIZED" in db.sql
    assert "scoped_edges AS MATERIALIZED" in db.sql
    assert "FROM unnest" not in db.sql
    assert db.params == [
        123,
        "contains",
        ["file:tinydb:tinydb/table.py"],
        "code_symbol",
    ]


def test_file_symbols_build_corpus_scopes_semantic_symbol_change(
    monkeypatch,
) -> None:
    graph = _FileGraph()
    module_symbol_id = "symbol:tinydb:tinydb/table.py:tinydb.table:1"
    graph.symbols[module_symbol_id] = {
        "name": "table",
        "kind": "module",
        "path": "tinydb/table.py",
        "qualified_name": "tinydb.table",
        "start_line": 1,
    }
    graph.contains.append({
        "edge_id": 12,
        "src": graph.file_id,
        "dst": module_symbol_id,
    })
    conn = _FakeConn()

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda *_, **__: _FakeStore(graph),
    )

    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": [graph.semantic_symbol_id]},
        changed_edge_endpoints={
            "contains": [
                {
                    "edge_id": 99,
                    "src": graph.file_id,
                    "dst": graph.semantic_symbol_id,
                }
            ]
        },
    )

    corpus = file_symbols.build_corpus(
        conn,
        generation_id=42,
        delta_scope=delta_scope,
    )

    assert delta_scope.scoped is True
    assert delta_scope.dirty_scope_keys == {graph.file_id}
    assert set(corpus) == {f"file_symbols:{graph.file_id}"}
    assert delta_scope.relation_rows[file_symbols.RELATION_ROWS_KEY] == [
        {
            "scope_key": graph.file_id,
            "row_key": graph.semantic_symbol_id,
            "file_path": "tinydb/table.py",
            "symbol_name": "okg_delta",
            "symbol_kind": "function",
            "qualified_name": "tinydb.table.okg_delta",
            "start_line": 12,
        }
    ]


def test_file_symbols_build_corpus_reuses_supplied_graph_store(monkeypatch) -> None:
    graph = _FileGraph()
    conn = _FakeConn()

    from okg.substrate.graph_store import ChronosGraphStore

    def fail_from_connection(*_: Any, **__: Any) -> None:
        raise AssertionError("build_corpus should reuse supplied graph_store")

    monkeypatch.setattr(ChronosGraphStore, "from_connection", fail_from_connection)
    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": [graph.semantic_symbol_id]},
        changed_edge_endpoints={
            "contains": [
                {
                    "src": graph.file_id,
                    "dst": graph.semantic_symbol_id,
                }
            ]
        },
    )

    corpus = file_symbols.build_corpus(
        conn,
        generation_id=1,
        delta_scope=delta_scope,
        graph_store=_FakeStore(graph),
    )

    assert set(corpus) == {f"file_symbols:{graph.file_id}"}
    assert delta_scope.scoped is True
    assert delta_scope.timings_ms["store_attach.reused"] == 1.0
    assert conn.inserted_rows == []
    assert not any(
        "okg.graph_nodes" in sql
        and "node_id = ANY" not in sql
        and "e.src = ANY" not in sql
        for sql, _params in graph.queries
    )
    assert not any(
        "okg.graph_edges" in sql
        and all(token not in sql for token in ("src = ANY", "dst = ANY"))
        for sql, _params in graph.queries
    )


def test_file_symbols_build_corpus_uses_contains_edge_without_source_file_node(
    monkeypatch,
) -> None:
    graph = _FileGraph()
    graph.files = {}
    conn = _FakeConn()

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda *_, **__: _FakeStore(graph),
    )

    delta_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": [graph.semantic_symbol_id]},
        changed_edge_endpoints={
            "contains": [
                {
                    "edge_id": 99,
                    "src": graph.file_id,
                    "dst": graph.semantic_symbol_id,
                }
            ]
        },
    )

    corpus = file_symbols.build_corpus(
        conn,
        generation_id=42,
        delta_scope=delta_scope,
    )

    assert delta_scope.scoped is True
    assert set(corpus) == {f"file_symbols:{graph.file_id}"}
    assert delta_scope.relation_rows[file_symbols.RELATION_ROWS_KEY] == [
        {
            "scope_key": graph.file_id,
            "row_key": graph.semantic_symbol_id,
            "file_path": "tinydb/table.py",
            "symbol_name": "okg_delta",
            "symbol_kind": "function",
            "qualified_name": "tinydb.table.okg_delta",
            "start_line": 12,
        }
    ]
    assert conn.inserted_rows == []


def test_file_symbols_direct_relation_rows_do_not_leak_stale_rows(
    monkeypatch,
) -> None:
    graph = _FileGraph()
    conn = _FakeConn()

    from okg.substrate.graph_store import ChronosGraphStore

    monkeypatch.setattr(
        ChronosGraphStore,
        "from_connection",
        lambda *_, **__: _FakeStore(graph),
    )

    first_scope = ViewDeltaScope(
        changed_nodes={"code_symbol": [graph.semantic_symbol_id]},
        changed_edge_endpoints={
            "contains": [
                {
                    "edge_id": 99,
                    "src": graph.file_id,
                    "dst": graph.semantic_symbol_id,
                }
            ]
        },
    )
    first = file_symbols.build_corpus(
        conn,
        generation_id=42,
        delta_scope=first_scope,
    )

    assert set(first) == {f"file_symbols:{graph.file_id}"}
    assert first_scope.relation_rows[file_symbols.RELATION_ROWS_KEY]

    graph.contains = []
    second_scope = ViewDeltaScope(
        changed_nodes={"source_file": [graph.file_id]},
        changed_node_attr_fields={
            "source_file": {graph.file_id: ["__deleted__"]},
        },
    )
    second = file_symbols.build_corpus(
        conn,
        generation_id=43,
        delta_scope=second_scope,
    )

    assert second_scope.scoped is True
    assert second_scope.dirty_scope_keys == {graph.file_id}
    assert second == {}
    assert second_scope.relation_rows[file_symbols.RELATION_ROWS_KEY] == []
    assert conn.inserted_rows == []
    assert sum(
        1
        for sql, _params in conn.executed
        if "CREATE TEMP TABLE IF NOT EXISTS" in sql
    ) == 0
    assert sum(
        1
        for sql, _params in conn.executed
        if sql.strip().upper().startswith("TRUNCATE ")
    ) == 0
