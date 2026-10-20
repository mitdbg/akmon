"""Decision 182a: a logical predicate must carry its sub-predicates where they are read.

`not` reads its one sub-predicate from `clause`; `and`/`or` read theirs from
`clauses`. Before this change the `Predicate` model accepted a logical op that
put its operand anywhere else. `{"op": "not", "value": {...}}` lowered to
`NOT (TRUE)` in `operator_sql.predicate_sql` and to `{'not': {'lit': True}}`
in `transform.operators._predicate_to_expr`, so `filter` answered an empty
page with `isError: false`. `{"op": "and", "values": [...]}` was caught only
later, as `unsupported_operator_form` with the message "and predicate requires
non-empty clauses", a "Use query" hint and `fallback_operator: query`.

The model now refuses those shapes, so every tool that takes a predicate
(`filter.predicate`, the `filter`/`map`/`aggregate` scope `filter`, and
`expand.filter`) refuses them before the tool body runs, through the existing
`invalid_operator_arguments` path. No database: every refusal here happens
before the server opens a connection.
"""

from __future__ import annotations

from typing import Any

import anyio
import pydantic
import pytest

from okg.substrate.mcp.operator_models import Predicate

#: The reviewer's shape at 35693aa03c: `not` with its operand in `value`.
NOT_WITH_VALUE = {
    "op": "not",
    "value": {"op": "contains", "field": "node_id", "value": "docker"},
}
AND_WITH_VALUES = {
    "op": "and",
    "values": [
        {"op": "eq", "field": "subtype", "value": "document"},
        {"op": "exists", "field": "attrs.path"},
    ],
}
VALID_NESTED = {
    "op": "and",
    "clauses": [
        {"op": "eq", "field": "subtype", "value": "document"},
        {
            "op": "or",
            "clauses": [
                {"op": "suffix", "field": "attrs.path", "value": "a.py"},
                {"op": "not", "clause": {"op": "in", "field": "subtype",
                                         "values": ["x", "y"]}},
            ],
        },
    ],
}

#: (predicate, the error kind, a phrase the message must hold)
REFUSED = [
    pytest.param(NOT_WITH_VALUE, "not_takes_clause", "`value`", id="not-with-value"),
    pytest.param({"op": "not"}, "not_takes_clause", "`clause`", id="not-with-nothing"),
    pytest.param(
        {"op": "not", "clauses": [{"op": "exists", "field": "attrs.path"}]},
        "not_takes_clause",
        "`clauses`",
        id="not-with-clauses",
    ),
    pytest.param(
        {"op": "not", "field": "node_id", "clause": {"op": "exists", "field": "attrs.x"}},
        "not_takes_clause",
        "`field`",
        id="not-with-clause-and-field",
    ),
    pytest.param(AND_WITH_VALUES, "and_takes_clauses", "`values`", id="and-with-values"),
    pytest.param({"op": "or"}, "or_takes_clauses", "`clauses`", id="or-with-no-clauses"),
    pytest.param(
        {"op": "or", "clause": {"op": "exists", "field": "attrs.path"}},
        "or_takes_clauses",
        "`clause`",
        id="or-with-clause",
    ),
    pytest.param(
        {"op": "eq", "field": "subtype", "value": "x",
         "clauses": [{"op": "exists", "field": "attrs.path"}]},
        "clause_only_for_and_or_not",
        "`clauses`",
        id="eq-with-clauses",
    ),
    pytest.param(
        {"op": "exists", "field": "attrs.path",
         "clause": {"op": "exists", "field": "attrs.x"}},
        "clause_only_for_and_or_not",
        "`clause`",
        id="exists-with-clause",
    ),
    pytest.param(
        {"op": "not", "clause": {}},
        "sub_predicate_needs_op",
        "`clause`",
        id="not-of-empty",
    ),
    pytest.param(
        {"op": "and", "clauses": [{"op": "exists", "field": "attrs.x"}, {}]},
        "sub_predicate_needs_op",
        "`clauses`",
        id="and-with-empty-clause",
    ),
]

VALID = [
    pytest.param({}, id="empty"),
    pytest.param({"op": "eq", "field": "subtype", "value": "document"}, id="eq"),
    pytest.param(
        {"op": "not", "clause": {"op": "contains", "field": "attrs", "value": {"k": "v"}}},
        id="not",
    ),
    pytest.param(VALID_NESTED, id="nested"),
    pytest.param(
        # An operand that happens to hold logical keys is data, not a predicate.
        {"op": "eq", "field": "attrs.x", "value": {"op": "not", "clauses": []}},
        id="operand-shaped-like-a-predicate",
    ),
]


@pytest.mark.parametrize(("predicate", "kind", "phrase"), REFUSED)
def test_model_refuses_a_misplaced_sub_predicate(
    predicate: dict[str, Any], kind: str, phrase: str
) -> None:
    with pytest.raises(pydantic.ValidationError) as caught:
        Predicate.model_validate(predicate)
    errors = caught.value.errors(include_url=False)
    assert [row["type"] for row in errors] == [kind]
    assert phrase in errors[0]["msg"]


def test_model_refusal_names_the_nested_location() -> None:
    nested = {
        "op": "and",
        "clauses": [{"op": "eq", "field": "subtype", "value": "x"}, NOT_WITH_VALUE],
    }
    with pytest.raises(pydantic.ValidationError) as caught:
        Predicate.model_validate(nested)
    errors = caught.value.errors(include_url=False)
    assert [(row["loc"], row["type"]) for row in errors] == [
        (("clauses", 1), "not_takes_clause")
    ]


@pytest.mark.parametrize("predicate", VALID)
def test_model_accepts_well_formed_predicates(predicate: dict[str, Any]) -> None:
    Predicate.model_validate(predicate)


# ── through the MCP server ────────────────────────────────────────────

pytest.importorskip("mcp")

from okg.substrate.mcp.server import build_server  # noqa: E402
from tests.substrate.mcp._result_payload import (  # noqa: E402
    call_json,
    payload,
    result_text,
)

DEAD_DSN = "postgresql://postgres:okg@localhost:1/okg_probe"


@pytest.fixture
def mcp_server(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    server = build_server(dsn=DEAD_DSN)
    try:
        yield server
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()


def _call(server: Any, name: str, arguments: dict[str, Any]) -> Any:
    async def run() -> Any:
        return await server.call_tool(name, arguments)

    return anyio.run(run)


def _call_json(server: Any, name: str, arguments: dict[str, Any]) -> Any:
    async def run() -> Any:
        return await call_json(server, name, arguments)

    return anyio.run(run)


@pytest.mark.parametrize(
    ("predicate", "kind"),
    [
        pytest.param(NOT_WITH_VALUE, "not_takes_clause", id="not-with-value"),
        pytest.param(AND_WITH_VALUES, "and_takes_clauses", id="and-with-values"),
        pytest.param({"op": "or"}, "or_takes_clauses", id="or-with-no-clauses"),
    ],
)
def test_filter_refuses_the_shape_as_an_error(
    mcp_server: Any, predicate: dict[str, Any], kind: str
) -> None:
    arguments = {"input": {"kind": "nodes"}, "predicate": predicate}

    # The model's text card (#3123): an error naming the code, the field and
    # the kind that says which key the op reads.
    result = _call(mcp_server, "filter", arguments)
    assert result.isError is True
    text = result_text(result)
    assert "filter refused invalid_operator_arguments" in text
    assert "filter input validation failed at 'predicate'." in text
    assert kind in text
    # The old late refusal of and/or: wrong message, a hint naming a tool a
    # default reader cannot use, and a fallback to it.
    assert "requires non-empty clauses" not in text
    assert "Use query" not in text
    assert "fallback_operator" not in text

    # The same refusal in the JSON format a program reads.
    as_json = _call_json(mcp_server, "filter", arguments)
    assert as_json.isError is True
    body = payload(as_json)
    assert body["error"]["code"] == "invalid_operator_arguments"
    assert body["error"]["data"]["invalid_fields"] == ["predicate"]
    assert body["error"]["data"]["validation_kinds"] == [kind]
    assert "fallback_operator" not in body["error"]


@pytest.mark.parametrize(
    ("tool", "arguments"),
    [
        pytest.param(
            "filter",
            {"input": {"kind": "nodes", "filter": NOT_WITH_VALUE}, "predicate": {}},
            id="filter-scope",
        ),
        pytest.param(
            "map",
            {"input": {"kind": "nodes", "filter": NOT_WITH_VALUE}},
            id="map-scope",
        ),
        pytest.param(
            "aggregate",
            {
                "input": {"kind": "nodes", "filter": NOT_WITH_VALUE},
                "by": ["subtype"],
                "metrics": ["count"],
            },
            id="aggregate-scope",
        ),
        pytest.param(
            "expand",
            {"input": "node:x", "filter": NOT_WITH_VALUE},
            id="expand-filter",
        ),
    ],
)
def test_every_predicate_caller_gets_the_same_refusal(
    mcp_server: Any, tool: str, arguments: dict[str, Any]
) -> None:
    as_json = _call_json(mcp_server, tool, arguments)
    assert as_json.isError is True
    body = payload(as_json)
    assert body["error"]["code"] == "invalid_operator_arguments", body["error"]
    assert "not_takes_clause" in body["error"]["data"]["validation_kinds"]
    assert any(
        path.endswith("filter") for path in body["error"]["data"]["invalid_fields"]
    ), body["error"]["data"]["invalid_fields"]


def test_filter_admits_a_valid_nested_predicate(mcp_server: Any) -> None:
    """A well-formed tree passes validation and reaches the tool body.

    With no database behind the server the body then fails to connect; what
    matters here is that validation did not refuse it.
    """
    arguments = {"input": {"kind": "nodes"}, "predicate": VALID_NESTED}
    as_json = _call_json(mcp_server, "filter", arguments)
    assert payload(as_json)["error"]["code"] != "invalid_operator_arguments"
