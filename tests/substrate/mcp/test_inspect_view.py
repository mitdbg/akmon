"""Schema introspection via inspect (operator-surface-list-work
req.listwork.introspection): view-column describe, the not-found path
listing available views, and the information_schema refusal naming the
allowed paths.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from okg.substrate.mcp.sql_safety import validate_run_sql
from okg.substrate.mcp.tools import inspect_view

_SESSION = SimpleNamespace(generation_id="gen-test")


class _FakeCursor:
    def __init__(self, results: list[list[tuple]]):
        self._results = results
        self._current: list[tuple] = []

    def execute(self, sql, params=None):
        self._current = self._results.pop(0)

    def fetchall(self):
        return self._current

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConn:
    def __init__(self, results: list[list[tuple]]):
        self._results = results

    def cursor(self):
        return _FakeCursor(self._results)


def test_inspect_view_returns_columns_and_strips_schema_prefix() -> None:
    conn = _FakeConn([[("src", "text"), ("dst", "text")]])
    out = inspect_view(conn, _SESSION, "okg.V_EDGES")
    assert out["view"] == "okg.v_edges"
    assert out["columns"] == [
        {"name": "src", "type": "text"},
        {"name": "dst", "type": "text"},
    ]
    assert out["pinned_generation_id"] == "gen-test"
    assert "subtype" in out["note"]  # points at the attr-key describe path


def test_inspect_view_payload_validates_through_the_public_result_model() -> None:
    """Issue #2793: `inspect` kind `view` failed for every view.

    `inspect_view` returns `{name, type}` column objects, and the published
    result model admitted only strings, so the public result builder raised
    and the call came back `operator_execution_failed`. The payload keys are
    unchanged; the model now admits what the operator emits.
    """
    import json

    from okg.substrate.mcp.operator_models import OperatorResult
    from okg.substrate.mcp.response_bounds import served_call_tool_result

    conn = _FakeConn([[("node_id", "text"), ("attrs", "jsonb")]])
    out = inspect_view(conn, _SESSION, "v_nodes")

    OperatorResult.model_validate(out)
    result = served_call_tool_result(out, tool="inspect", result_format="json")
    assert json.loads(result.content[0].text)["columns"] == [
        {"name": "node_id", "type": "text"},
        {"name": "attrs", "type": "jsonb"},
    ]


def test_result_model_still_accepts_plain_column_names_and_refuses_junk() -> None:
    import pydantic

    from okg.substrate.mcp.operator_models import OperatorResult

    OperatorResult.model_validate({"columns": ["a", "b"]})
    with pytest.raises(pydantic.ValidationError):
        OperatorResult.model_validate({"columns": [{"name": "a", "type": 3}]})
    with pytest.raises(pydantic.ValidationError):
        OperatorResult.model_validate({"columns": [7]})


def test_inspect_view_refuses_non_bare_names() -> None:
    for bad in ("", "v_nodes; drop", "pg_catalog.tables", "v nodes"):
        with pytest.raises(ValueError, match="bare okg-schema view name"):
            inspect_view(_FakeConn([]), _SESSION, bad)


def test_inspect_view_not_found_lists_available_views() -> None:
    conn = _FakeConn([[], [("v_edges",), ("v_nodes",)]])
    # The closest view first, then the rest that fit the message cap (#2809).
    with pytest.raises(ValueError, match="2 views; closest: v_nodes; others: v_edges"):
        inspect_view(conn, _SESSION, "v_nope")


def test_information_schema_refusal_names_introspection_paths() -> None:
    with pytest.raises(Exception) as excinfo:
        validate_run_sql(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'v_edges'"
        )
    assert "information_schema.columns" in str(excinfo.value)
    hint = str(getattr(excinfo.value, "fix_hint", ""))
    assert "inspect(target={'kind':'view','name':'v_nodes'})" in hint
    assert "inspect(target={'kind':'subtype','subtype':<name>})" in hint
