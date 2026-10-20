"""Envelope-contract test for MCP list-returning tools.

Per `improve-mcp-tool-surface`, every list-returning MCP tool
SHALL accept a `select: list[str] | None` parameter and SHALL
return the decision envelope `total_count`, `truncated`,
`pinned_generation_id`. This module enforces the contract by
introspecting tool signatures (no DB required) and by exercising
the `validate_select` helper that every tool uses.

The integration-level "tool actually returns the envelope"
assertion lives in the per-tool tests
(`test_mcp_tools.py`, `test_active_work.py`, etc.); here we
verify the static contract — signatures and error codes.
"""
from __future__ import annotations

import inspect

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import (
    aggregate_node,
    find_identifier_mentions,
    inspect_node,
    expand_neighbors,
    search,
    expand_path,
)
from okg.substrate.mcp.envelope import (
    SelectUnknownField,
    project_row,
    validate_select,
)


# Every list-returning tool that MUST accept a `select` parameter
# per the spec. `get_node` is included for symmetry — it returns
# one node, not a list, but the `select` projection still applies
# (per the spec's "select on get and search" wording).
#
# Per `retire-redundant-mcp-tools` (2026-04-27): aggregate_by_type,
# active_work, list_generations_since, changes_since, and
# search_by_attrs were retired to recipes; they are no longer in
# the contract test set.
_TOOLS_WITH_SELECT = (
    inspect_node,
    expand_neighbors,
    aggregate_node,
    expand_path,
    find_identifier_mentions,
    search,
)


@pytest.mark.parametrize("tool", _TOOLS_WITH_SELECT)
def test_tool_accepts_select_parameter(tool) -> None:
    """Every list-returning tool's signature SHALL include a
    keyword-only `select: list[str] | None = None` parameter."""
    sig = inspect.signature(tool)
    assert "select" in sig.parameters, (
        f"{tool.__name__} is missing the `select` parameter "
        f"required by improve-mcp-tool-surface"
    )
    param = sig.parameters["select"]
    # Must be keyword-only (each tool uses the `*,` separator
    # before `select`) and must default to None so callers can
    # omit it entirely.
    assert param.default is None, (
        f"{tool.__name__}.select must default to None; got "
        f"{param.default!r}"
    )
    assert param.kind in (
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ), (
        f"{tool.__name__}.select must be keyword-passable"
    )


def test_validate_select_passes_known_fields() -> None:
    """`validate_select` MUST accept a select list whose entries
    are all in `allowed`."""
    validate_select(
        ["node_id", "subtype"],
        ["node_id", "subtype", "attrs"],
        tool_name="test_tool",
    )


def test_validate_select_passes_none() -> None:
    """`select=None` is the default; MUST be accepted as a no-op."""
    validate_select(
        None,
        ["node_id", "subtype", "attrs"],
        tool_name="test_tool",
    )


def test_validate_select_rejects_unknown_field() -> None:
    """An unknown field in `select` SHALL raise
    `SelectUnknownField` with code `select_unknown_field` and
    list the valid field names per the spec."""
    with pytest.raises(SelectUnknownField) as excinfo:
        validate_select(
            ["node_id", "nonexistent"],
            ["node_id", "subtype", "attrs"],
            tool_name="test_tool",
        )
    err = excinfo.value
    assert err.code == "select_unknown_field"
    assert "nonexistent" in err.data["unknown_fields"]
    assert sorted(err.data["valid_fields"]) == [
        "attrs", "node_id", "subtype",
    ]
    assert err.data["tool"] == "test_tool"
    # SubstrateError-style payload — fix_hint helps the agent
    # correct without a separate introspection round-trip.
    assert isinstance(err, SubstrateError)
    assert err.fix_hint is not None


def test_project_row_default_returns_default_set() -> None:
    """When `select` is None, projection returns the union of
    `default` and `always`, intersected with the row's keys."""
    row = {"node_id": "n1", "subtype": "doc", "attrs": {}, "score": 0.5}
    out = project_row(
        row, None,
        default=("node_id", "subtype"),
        always=("node_id",),
    )
    assert out == {"node_id": "n1", "subtype": "doc"}


def test_project_row_select_projects_subset() -> None:
    """`select=[…]` returns the union of `select` and `always`,
    intersected with the row's keys."""
    row = {"node_id": "n1", "subtype": "doc", "attrs": {}, "score": 0.5}
    out = project_row(
        row, ["score"],
        default=("node_id", "subtype", "attrs", "score"),
        always=("node_id", "subtype"),
    )
    # always (node_id, subtype) + select (score) — attrs dropped.
    assert out == {"node_id": "n1", "subtype": "doc", "score": 0.5}


def test_project_row_always_fields_cannot_be_excluded() -> None:
    """Even if `select` omits an always-field, the projection
    SHALL include it. Per the spec's "always-included fields
    cannot be excluded" scenario."""
    row = {"node_id": "n1", "subtype": "doc", "attrs": {"k": "v"}}
    out = project_row(
        row, ["attrs"],  # agent omitted node_id and subtype
        default=("attrs",),
        always=("node_id", "subtype"),
    )
    assert out == {"node_id": "n1", "subtype": "doc", "attrs": {"k": "v"}}


def test_project_row_silently_drops_absent_fields() -> None:
    """Fields requested in `select` but not present in the row
    are silently dropped (the row may not carry every documented
    field — e.g. an unranked search result has no `score`)."""
    row = {"node_id": "n1", "subtype": "doc"}
    out = project_row(
        row, ["score"],  # row has no score
        default=("node_id", "subtype"),
        always=("node_id", "subtype"),
    )
    assert out == {"node_id": "n1", "subtype": "doc"}


# ---------- mcp_call_log helpers (task 3.4) ----------


def test_extract_result_count_prefers_total_count() -> None:
    """When the response carries the canonical envelope field,
    `_extract_result_count` returns it directly — independent of
    the items field name (`results`, `neighbors`, `counts`, ...).
    """
    from okg.substrate.mcp.tracing import _extract_result_count
    assert _extract_result_count(
        {"total_count": 42, "results": [1, 2, 3]}
    ) == 42
    assert _extract_result_count(
        {"total_count": 0, "neighbors": []}
    ) == 0


def test_extract_result_count_falls_back_to_list_field() -> None:
    """For tools that don't carry `total_count` (legacy / non-
    list responses), we still report the list length where
    possible so the observation log is meaningful."""
    from okg.substrate.mcp.tracing import _extract_result_count
    assert _extract_result_count({"items": [{"a": 1}]}) == 1
    assert _extract_result_count({"results": [1, 2, 3]}) == 3
    assert _extract_result_count({"neighbors": []}) == 0
    assert _extract_result_count({"counts": [{"a": 1}, {"b": 2}]}) == 2
    assert _extract_result_count({"suggestions": ["alias"]}) == 1


def test_extract_result_count_returns_none_for_non_list_response() -> None:
    """`get_node` returns one node (not a list); the column is
    nullable for these cases."""
    from okg.substrate.mcp.tracing import _extract_result_count
    assert _extract_result_count(None) is None
    assert _extract_result_count("not-a-dict") is None
    assert _extract_result_count(
        {"node_id": "n1", "subtype": "doc", "attrs": {}}
    ) is None


def test_extract_call_cost_models_latency_size_and_sql_plan() -> None:
    """The MCP call log records a cost vector, not just a count.

    This is the substrate evidence used to decide whether a recurring
    operator/query shape deserves promotion.
    """
    from okg.substrate.mcp.tracing import _extract_call_cost

    out = _extract_call_cost(
        args={"sql": "SELECT * FROM okg.v_nodes LIMIT 1"},
        response={
            "rows": [["n1", "doc"]],
            "total_count": 1,
            "truncated": False,
            "query_plan_summary": (
                "Limit (cost=1.25..9.50 rows=12)"
            ),
        },
        elapsed_ms=7,
        result_count=1,
        error_code=None,
    )

    assert out["model"] == "mcp_tool_cost_v1"
    assert out["elapsed_ms"] == 7
    assert out["result_count"] == 1
    assert out["row_count"] == 1
    assert out["truncated"] is False
    assert out["args_bytes"] > 0
    assert out["response_bytes"] > 0
    assert out["args_token_estimate"] > 0
    assert out["response_token_estimate"] > 0
    assert out["query_plan_startup_cost"] == 1.25
    assert out["query_plan_total_cost"] == 9.5
    assert out["query_plan_rows"] == 12


def test_extract_call_cost_records_error_code_without_response() -> None:
    from okg.substrate.mcp.tracing import _extract_call_cost

    out = _extract_call_cost(
        args={"node_id": "missing"},
        response=None,
        elapsed_ms=2,
        result_count=None,
        error_code="node_not_found",
    )

    assert out["error_code"] == "node_not_found"
    assert out["response_bytes"] == 0
    assert out["response_token_estimate"] == 0


def test_extract_error_code_uses_substrate_error_code() -> None:
    """SubstrateError subclasses carry `.code` — the canonical
    structured-error identifier. The observation log groups by
    code so failure-rate queries can target specific errors."""
    from okg.substrate.mcp.tracing import _extract_error_code
    assert _extract_error_code(SelectUnknownField(
        "x", data={"valid_fields": []},
    )) == "select_unknown_field"


def test_extract_error_code_falls_back_to_class_name() -> None:
    """Non-substrate exceptions log their class name as a poor-
    man's code rather than None — so a deployment with a wild
    exception still gets a groupable label."""
    from okg.substrate.mcp.tracing import _extract_error_code
    assert _extract_error_code(ValueError("bad input")) == "ValueError"
    assert _extract_error_code(RuntimeError("oops")) == "RuntimeError"


def test_extract_error_code_returns_none_on_success() -> None:
    """`error=None` means the call succeeded; `error_code` is NULL
    in the log row."""
    from okg.substrate.mcp.tracing import _extract_error_code
    assert _extract_error_code(None) is None


@pytest.mark.parametrize("tool", _TOOLS_WITH_SELECT)
def test_tool_module_imports_envelope_helpers(tool) -> None:
    """Defensive smoke check: the module hosting each tool MUST
    import `validate_select` and `project_row` so the contract
    is enforceable inside the tool body."""
    module = inspect.getmodule(tool)
    assert module is not None, f"{tool.__name__} has no module"
    src = inspect.getsource(module)
    # Either `validate_select` is called directly or the module
    # delegates to a per-method submodule (search() in
    # search.py dispatches to _search_lexical etc.). The module
    # must reference validate_select at least once OR have its
    # delegated implementations in the same file.
    assert "validate_select" in src, (
        f"{module.__name__} does not reference `validate_select`; "
        f"the `select` parameter is unenforced"
    )
