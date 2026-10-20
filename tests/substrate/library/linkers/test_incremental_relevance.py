from __future__ import annotations

from collections.abc import Callable
from typing import Any

from okg.substrate.enrichers.base import IncrementalContext


class _FakeCursor:
    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *_args: object) -> None:
        return None


class _FakeConn:
    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


def _ctx(
    *,
    dirty_node_ids: frozenset[str] = frozenset(),
    dirty_edge_ids: frozenset[int] = frozenset(),
    is_first_run: bool = False,
    last_status: str = "success",
    prev_catalog_version_id: int | None = None,
    current_catalog_version_id: int | None = None,
) -> IncrementalContext:
    return IncrementalContext(
        prev_fact_watermark=1,
        prev_edge_watermark=1,
        dirty_node_ids=dirty_node_ids,
        dirty_edge_ids=dirty_edge_ids,
        is_first_run=is_first_run,
        last_status=last_status,
        prev_catalog_version_id=prev_catalog_version_id,
        current_catalog_version_id=current_catalog_version_id,
    )


def _patch_query(monkeypatch, fn: Callable[[str, dict[str, Any]], list[dict[str, Any]]]) -> None:
    from okg.substrate.library.linkers import _incremental

    def query(_cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        return fn(sql, params or {})

    monkeypatch.setattr(_incremental._chronos, "query", query)


def test_incremental_relevance_skips_irrelevant_dirty_node(monkeypatch) -> None:
    from okg.substrate.library.linkers._incremental import incremental_touches

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {"node_id": "agent-event:1", "subtype": "agent_event"},
        ],
    )

    assert not incremental_touches(
        _FakeCursor(),
        _ctx(dirty_node_ids=frozenset({"agent-event:1"})),
        node_subtypes=("tool_use_event", "code_symbol"),
    )


def test_incremental_relevance_forces_run_after_prior_failure(monkeypatch) -> None:
    from okg.substrate.library.linkers._incremental import incremental_touches

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {"node_id": "agent-event:1", "subtype": "agent_event"},
        ],
    )

    assert incremental_touches(
        _FakeCursor(),
        _ctx(
            dirty_node_ids=frozenset({"agent-event:1"}),
            last_status="error",
        ),
        node_subtypes=("tool_use_event", "code_symbol"),
    )


def test_incremental_relevance_forces_run_after_catalog_change(monkeypatch) -> None:
    from okg.substrate.library.linkers._incremental import incremental_touches

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {"node_id": "agent-event:1", "subtype": "agent_event"},
        ],
    )

    assert incremental_touches(
        _FakeCursor(),
        _ctx(
            dirty_node_ids=frozenset({"agent-event:1"}),
            prev_catalog_version_id=1,
            current_catalog_version_id=2,
        ),
        node_subtypes=("tool_use_event", "code_symbol"),
    )


def test_incremental_relevance_forces_run_when_dirty_node_missing(monkeypatch) -> None:
    from okg.substrate.library.linkers._incremental import incremental_touches

    _patch_query(monkeypatch, lambda _sql, _params: [])

    assert incremental_touches(
        _FakeCursor(),
        _ctx(dirty_node_ids=frozenset({"tool-use:deleted"})),
        node_subtypes=("tool_use_event", "code_symbol"),
    )


def test_incremental_relevance_detects_relevant_dirty_edge_endpoint(monkeypatch) -> None:
    from okg.substrate.library.linkers._incremental import incremental_touches

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "src_subtype": "test_report",
                "dst_subtype": "test_case",
            },
        ],
    )

    assert incremental_touches(
        _FakeCursor(),
        _ctx(dirty_edge_ids=frozenset({42})),
        node_subtypes=("acceptance_criterion", "test_case"),
    )


def test_tool_use_symbol_skips_irrelevant_dirty_agent_node(monkeypatch) -> None:
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {"node_id": "agent-event:1", "subtype": "agent_event"},
        ],
    )

    def fail_match(*_args: object, **_kwargs: object) -> tuple[list, list]:
        raise AssertionError("irrelevant dirty node should not scan tool events")

    monkeypatch.setattr(ToolUseSymbolLinker, "_match_pairs", fail_match)

    result = ToolUseSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"agent-event:1"})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_tool_use_symbol_scopes_dirty_tool_event(monkeypatch) -> None:
    from okg.substrate.library.linkers import tool_use_symbol as module
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    calls: list[dict[str, Any]] = []

    def query(_cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        calls.append(dict(params))
        if "subtype = 'tool_use_event'" in sql:
            assert params == {"node_ids": ["tool-use:1"]}
            return [
                {
                    "node_id": "tool-use:1",
                    "subtype": "tool_use_event",
                    "attrs": {
                        "file_path": "src/app.py",
                        "line": 42,
                    },
                }
            ]
        if "SELECT node_id, subtype" in sql:
            return [{"node_id": "tool-use:1", "subtype": "tool_use_event"}]
        if "subtype = 'code_symbol'" in sql:
            assert params == {"paths": ["src/app.py"]}
            return [
                {
                    "node_id": "symbol:app:handle",
                    "attrs": {
                        "path": "src/app.py",
                        "kind": "function",
                        "start_line": 40,
                        "end_line": 50,
                    },
                }
            ]
        return []

    monkeypatch.setattr(module._chronos, "query", query)
    monkeypatch.setattr(
        ToolUseSymbolLinker,
        "_emit_edges",
        lambda _self, _cur, *, pairs, generation_id: (len(pairs), 0),
    )

    result = ToolUseSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"tool-use:1"})),
    )

    assert result.n_edges_emitted == 1
    assert {"node_ids": ["tool-use:1"]} in calls
    assert {"paths": ["src/app.py"]} in calls


def test_tool_use_symbol_scopes_dirty_code_symbol_by_path(monkeypatch) -> None:
    from okg.substrate.library.linkers import tool_use_symbol as module
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    calls: list[dict[str, Any]] = []

    def query(_cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        calls.append(dict(params))
        if "WHERE node_id = ANY(:node_ids)" in sql:
            assert params == {"node_ids": ["symbol:app:handle"]}
            return [
                {
                    "node_id": "symbol:app:handle",
                    "subtype": "code_symbol",
                    "attrs": {
                        "path": "src/app.py",
                        "start_line": 40,
                        "end_line": 50,
                    },
                }
            ]
        if "subtype = 'tool_use_event'" in sql:
            assert params == {"paths": ["src/app.py"]}
            return [
                {
                    "node_id": "tool-use:1",
                    "subtype": "tool_use_event",
                    "attrs": {
                        "file_path": "src/app.py",
                        "line": 42,
                    },
                }
            ]
        if "subtype = 'code_symbol'" in sql:
            assert params == {"paths": ["src/app.py"]}
            return [
                {
                    "node_id": "symbol:app:handle",
                    "attrs": {
                        "path": "src/app.py",
                        "kind": "function",
                        "start_line": 40,
                        "end_line": 50,
                    },
                }
            ]
        return []

    monkeypatch.setattr(module._chronos, "query", query)
    monkeypatch.setattr(
        ToolUseSymbolLinker,
        "_emit_edges",
        lambda _self, _cur, *, pairs, generation_id: (len(pairs), 0),
    )

    result = ToolUseSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"symbol:app:handle"})),
    )

    assert result.n_edges_emitted == 1
    assert {"paths": ["src/app.py"]} in calls


def test_tool_use_symbol_scopes_dirty_code_symbol_by_semantic_key(monkeypatch) -> None:
    from okg.substrate.library.linkers import tool_use_symbol as module
    from okg.substrate.library.linkers.tool_use_symbol import ToolUseSymbolLinker

    semantic_key = "code-symbol:demo:src.app.handle:function"
    calls: list[dict[str, Any]] = []

    def query(_cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        calls.append(dict(params))
        if "WHERE node_id = ANY(:node_ids)" in sql:
            return [
                {
                    "node_id": "symbol:app:handle",
                    "subtype": "code_symbol",
                    "attrs": {
                        "path": "src/app.py",
                        "semantic_key": semantic_key,
                        "start_line": 40,
                        "end_line": 50,
                    },
                }
            ]
        if "subtype = 'tool_use_event'" in sql:
            assert params == {
                "paths": ["src/app.py"],
                "semantic_keys": [semantic_key],
            }
            return [
                {
                    "node_id": "tool-use:semantic",
                    "subtype": "tool_use_event",
                    "attrs": {"symbol_semantic_key": semantic_key},
                }
            ]
        if "subtype = 'code_symbol'" in sql:
            assert params == {"semantic_keys": [semantic_key]}
            return [
                {
                    "node_id": "symbol:app:handle",
                    "attrs": {
                        "path": "src/app.py",
                        "kind": "function",
                        "semantic_key": semantic_key,
                        "start_line": 40,
                        "end_line": 50,
                    },
                }
            ]
        return []

    monkeypatch.setattr(module._chronos, "query", query)
    monkeypatch.setattr(
        ToolUseSymbolLinker,
        "_emit_edges",
        lambda _self, _cur, *, pairs, generation_id: (len(pairs), 0),
    )

    result = ToolUseSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"symbol:app:handle"})),
    )

    assert result.n_edges_emitted == 1
    assert {"semantic_keys": [semantic_key], "paths": ["src/app.py"]} in calls


def test_acceptance_criteria_skips_irrelevant_dirty_agent_edge(monkeypatch) -> None:
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "src_subtype": "agent_session",
                "dst_subtype": "agent_event",
            },
        ],
    )

    def fail_match(*_args: object, **_kwargs: object) -> set:
        raise AssertionError("irrelevant dirty edge should not scan tests")

    monkeypatch.setattr(AcceptanceCriteriaLinker, "_match_pairs", fail_match)

    result = AcceptanceCriteriaLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_edge_ids=frozenset({42})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_acceptance_criteria_skips_pact_contains_criterion_edge(monkeypatch) -> None:
    from okg.substrate.library.linkers.acceptance_criteria import (
        AcceptanceCriteriaLinker,
    )

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "edge_type": "contains",
                "src_subtype": "pact_change",
                "dst_subtype": "acceptance_criterion",
            },
        ],
    )

    def fail_match(*_args: object, **_kwargs: object) -> set:
        raise AssertionError("PACT structure edge should not scan tests")

    monkeypatch.setattr(AcceptanceCriteriaLinker, "_match_pairs", fail_match)

    result = AcceptanceCriteriaLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_edge_ids=frozenset({42})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_test_symbol_skips_irrelevant_dirty_agent_edge(monkeypatch) -> None:
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    assert "calls" in TestSymbolLinker.reads_edge_types

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "src_subtype": "agent_session",
                "dst_subtype": "agent_event",
            },
        ],
    )

    def fail_resolve(*_args: object, **_kwargs: object) -> list:
        raise AssertionError("irrelevant dirty edge should not scan test symbols")

    monkeypatch.setattr(TestSymbolLinker, "_resolve_edges", fail_resolve)

    result = TestSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_edge_ids=frozenset({42})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_test_symbol_runs_for_non_test_code_symbol_change(monkeypatch) -> None:
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "node_id": "symbol:app",
                "subtype": "code_symbol",
                "attrs": {"path": "src/app.py"},
            },
        ],
    )
    called = False

    def resolve(self, _cur, *, skip_existing: bool) -> list:
        nonlocal called
        called = True
        assert skip_existing is True
        return []

    monkeypatch.setattr(TestSymbolLinker, "_resolve_edges", resolve)

    result = TestSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"symbol:app"})),
    )

    assert called is True
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_test_symbol_runs_for_test_code_symbol_change(monkeypatch) -> None:
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "node_id": "symbol:test",
                "subtype": "code_symbol",
                "attrs": {"path": "tests/test_app.py"},
            },
        ],
    )
    called = False

    def resolve(self, _cur, *, skip_existing: bool) -> list:
        nonlocal called
        called = True
        assert skip_existing is True
        return []

    monkeypatch.setattr(TestSymbolLinker, "_resolve_edges", resolve)

    result = TestSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"symbol:test"})),
    )

    assert called is True
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_test_symbol_skips_non_test_code_structure_edge(monkeypatch) -> None:
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "edge_type": "contains",
                "src_subtype": "code_symbol",
                "dst_subtype": "code_symbol",
                "src_attrs": {"path": "src/app.py"},
                "dst_attrs": {"path": "src/app.py"},
            },
        ],
    )

    def fail_resolve(*_args: object, **_kwargs: object) -> list:
        raise AssertionError("non-test code structure edge should not scan tests")

    monkeypatch.setattr(TestSymbolLinker, "_resolve_edges", fail_resolve)

    result = TestSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_edge_ids=frozenset({42})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_test_symbol_runs_for_test_symbol_reference_edge(monkeypatch) -> None:
    from okg.substrate.library.linkers.test_symbol import TestSymbolLinker

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {
                "edge_id": 42,
                "edge_type": "references",
                "src_subtype": "code_symbol",
                "dst_subtype": "code_symbol",
                "src_attrs": {"path": "tests/test_app.py"},
                "dst_attrs": {"path": "src/app.py"},
            },
        ],
    )
    called = False

    def resolve(self, _cur, *, skip_existing: bool) -> list:
        nonlocal called
        called = True
        assert skip_existing is True
        return []

    monkeypatch.setattr(TestSymbolLinker, "_resolve_edges", resolve)

    result = TestSymbolLinker().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_edge_ids=frozenset({42})),
    )

    assert called is True
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_code_reference_skips_irrelevant_dirty_agent_node(monkeypatch) -> None:
    from okg.substrate.library.linkers.code_reference import CodeReferenceResolver

    _patch_query(
        monkeypatch,
        lambda _sql, _params: [
            {"node_id": "agent-event:1", "subtype": "agent_event", "attrs": {}},
        ],
    )

    def fail_resolve(*_args: object, **_kwargs: object) -> tuple[list, dict]:
        raise AssertionError("irrelevant dirty node should not scan code references")

    monkeypatch.setattr(CodeReferenceResolver, "_resolve_edges", fail_resolve)

    result = CodeReferenceResolver().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"agent-event:1"})),
    )

    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 0


def test_code_reference_scopes_dirty_code_symbol(monkeypatch) -> None:
    from okg.substrate.library.linkers import code_reference as module
    from okg.substrate.library.linkers.code_reference import (
        CodeReferenceResolver,
        _Node,
    )

    calls: list[tuple[str, dict[str, Any]]] = []
    dirty_semantic_key = "code-symbol:demo:python:app.handle:function"

    def query(_cur, sql: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        calls.append((sql, dict(params)))
        if "WHERE node_id = ANY(:node_ids)" in sql:
            return [
                {
                    "node_id": "symbol:handle",
                    "subtype": "code_symbol",
                    "attrs": {
                        "repo": "demo",
                        "path": "src/app.py",
                        "name": "handle",
                        "qualified_name": "handle",
                        "semantic_key": dirty_semantic_key,
                    },
                }
            ]
        if "enclosing_symbol_id' = ANY(:callers)" in sql:
            assert params["callers"] == ["symbol:caller", "symbol:handle"]
            return []
        if "subtype = 'code_reference'" in sql:
            assert params["paths"] == ["src/app.py"]
            assert params["semantic_keys"] == [dirty_semantic_key]
            assert "handle" in params["names"]
            return [
                {
                    "node_id": "reference:local",
                    "attrs": {
                        "repo": "demo",
                        "path": "src/app.py",
                        "name": "handle",
                        "qualified_name": "handle",
                        "semantic_target_key": dirty_semantic_key,
                        "enclosing_symbol_id": "symbol:handle",
                    },
                },
                {
                    "node_id": "reference:incoming",
                    "attrs": {
                        "repo": "demo",
                        "path": "src/other.py",
                        "name": "handle",
                        "qualified_name": "handle",
                        "enclosing_symbol_id": "symbol:caller",
                    },
                },
            ]
        if "subtype = 'code_import'" in sql:
            assert params["paths"] == ["src/app.py", "src/other.py"]
            return []
        return []

    def fetch_nodes(_cur: Any, subtype: str) -> list[_Node]:
        if subtype == "code_symbol":
            return [
                _Node(
                    "symbol:handle",
                    {
                        "repo": "demo",
                        "path": "src/app.py",
                        "name": "handle",
                        "qualified_name": "handle",
                        "semantic_key": dirty_semantic_key,
                    },
                ),
                _Node(
                    "symbol:caller",
                    {
                        "repo": "demo",
                        "path": "src/other.py",
                        "name": "caller",
                        "qualified_name": "caller",
                    },
                ),
            ]
        if subtype == "dependency_package":
            return []
        raise AssertionError(f"unexpected full {subtype} scan")

    monkeypatch.setattr(module._chronos, "query", query)
    monkeypatch.setattr(module._chronos, "query_strict", query)
    monkeypatch.setattr(module, "_fetch_nodes", fetch_nodes)
    monkeypatch.setattr(
        module, "_filter_unchanged_edges", lambda _cur, edges: edges,
    )
    monkeypatch.setattr(
        CodeReferenceResolver,
        "_emit_edges",
        lambda _self, _cur, *, edges, generation_id: (len(edges), 0),
    )

    result = CodeReferenceResolver().enrich(
        _FakeConn(),  # type: ignore[arg-type]
        generation_id=1,
        incremental=_ctx(dirty_node_ids=frozenset({"symbol:handle"})),
    )

    assert result.n_edges_emitted == 3
    assert any("subtype = 'code_reference'" in sql for sql, _params in calls)
    assert not any("unexpected full code_reference scan" in sql for sql, _params in calls)
