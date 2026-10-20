"""Issue #3067: every `contains` no read serves is refused honestly.

`contains` is JSON containment on attrs, or (issue #3666) text inside one
attribute (`attrs.<key>` and a string; test_contains_text.py). Every other
`contains` is refused with `isError: true` and a hint that names only tools
a default reader can use. No database: every refusal here happens before
the server opens a connection.

Since #3059 a result is one text block (`structuredContent` is None), so
the model-side assertions read that block through `result_text`, and the
program-side ones read the JSON format through `call_json` and `payload`.
"""

from __future__ import annotations

from typing import Any

import anyio
import pytest

pytest.importorskip("mcp")

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.mcp.server import build_server  # noqa: E402
from tests.substrate.mcp._result_payload import (  # noqa: E402
    call_json,
    payload,
    result_text,
)

DEAD_DSN = "postgresql://postgres:okg@localhost:1/okg_probe"
#: The exact sentence, spelled out so a reworded refusal fails here. Since
#: loose-match-3666 it also names `like` for a substring that ignores
#: accents and case.
CONTAINS_FORMS_MESSAGE = (
    "contains takes text on one attribute (field 'attrs.<key>': the stored "
    "text holds it, ASCII case ignored) or an object on field 'attrs' (JSON "
    "containment, {key: exact value}); like on one field matches a "
    "substring, ignoring accents and case. Use search(query=...) to find "
    "words."
)
ATTRS_CONTAINS_HINT = "{op:'contains', field:'attrs', value:{<key>: <exact value>}}"


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


def _assert_honest_refusal(result: Any, tool: str) -> None:
    """The model's text card: an error naming the code, the sentence and the hint."""
    # Before #3067 the stable-column shape came back `isError: false` with
    # `fallback_operator: "query"`, a tool a default reader cannot use.
    assert result.isError is True
    text = result_text(result)
    assert f"{tool} refused unsupported_operator_form" in text
    assert CONTAINS_FORMS_MESSAGE in text
    assert "{op:'suffix', field:'attrs.<key>'" in text
    assert "{op:'contains', field:'attrs.<key>', value:'<text>'}" in text
    assert ATTRS_CONTAINS_HINT in text
    # The old fix_hint: "Use query for this shape until the filter operator
    # supports it directly."
    assert "Use query" not in text
    assert "fallback_operator" not in text


SUBSTRING_SHAPES = [
    # Text on `attrs.<key>` is served since #3666 (test_contains_text.py).
    pytest.param(
        {"op": "contains", "field": "attrs.path", "value": {"k": "docker"}},
        id="attrs-key-object",
    ),
    pytest.param(
        {"op": "contains", "field": "attrs.path", "value": 5},
        id="attrs-key-number",
    ),
    pytest.param(
        {"op": "contains", "field": "attrs", "value": "neighbor"},
        id="attrs-string",
    ),
    pytest.param(
        {"op": "contains", "field": "node_id", "value": "docker"},
        id="stable-column-string",
    ),
    pytest.param(
        {"op": "contains", "field": "node_id", "value": {"path": "x"}},
        id="stable-column-object",
    ),
    pytest.param(
        {
            "op": "and",
            "clauses": [
                {"op": "eq", "field": "subtype", "value": "document"},
                {
                    "op": "not",
                    "clause": {"op": "contains", "field": "attrs", "value": "x"},
                },
            ],
        },
        id="nested",
    ),
]


@pytest.mark.parametrize("predicate", SUBSTRING_SHAPES)
def test_filter_refuses_substring_contains_as_an_error(
    mcp_server: Any, predicate: dict[str, Any]
) -> None:
    arguments = {"input": {"kind": "nodes"}, "predicate": predicate}
    _assert_honest_refusal(_call(mcp_server, "filter", arguments), "filter")

    # The same refusal in the JSON format a program reads.
    as_json = _call_json(mcp_server, "filter", arguments)
    assert as_json.isError is True
    error = payload(as_json)["error"]
    assert error["code"] == "unsupported_operator_form"
    assert error["message"] == CONTAINS_FORMS_MESSAGE
    assert "fallback_operator" not in error


def test_filter_refuses_substring_contains_in_the_input_scope(
    mcp_server: Any,
) -> None:
    result = _call(
        mcp_server,
        "filter",
        {
            "input": {
                "kind": "nodes",
                "filter": {"op": "contains", "field": "node_id", "value": "x"},
            },
            "predicate": {},
        },
    )
    _assert_honest_refusal(result, "filter")


def test_map_refuses_substring_contains_in_the_input_scope(
    mcp_server: Any,
) -> None:
    result = _call(
        mcp_server,
        "map",
        {
            "input": {
                "kind": "nodes",
                "filter": {"op": "contains", "field": "node_id", "value": "x"},
            },
            "projection": ["node_id"],
        },
    )
    _assert_honest_refusal(result, "map")


def test_attrs_containment_is_not_refused_by_the_substring_guard() -> None:
    from okg.substrate.mcp.operator_models import refuse_substring_contains

    exact = {"op": "contains", "field": "attrs", "value": {"path": "a/b.py"}}
    refuse_substring_contains("filter", exact, {"kind": "nodes", "filter": exact})
    refuse_substring_contains(
        "filter", {"op": "and", "clauses": [exact, {"op": "eq", "field": "subtype",
                                                    "value": "document"}]},
    )


def test_an_operand_that_looks_like_a_predicate_is_not_refused() -> None:
    """`value` and `values` are data: the walk must not read them as predicates."""
    from okg.substrate.mcp.operator_models import refuse_substring_contains

    lookalike = {"op": "contains", "field": "node_id", "value": "x"}
    refuse_substring_contains(
        "filter", {"op": "eq", "field": "attrs.rule", "value": lookalike}
    )
    refuse_substring_contains(
        "filter", {"op": "in", "field": "attrs.rule", "values": [lookalike]}
    )
    refuse_substring_contains(
        "filter",
        {"kind": "nodes", "filter": {"op": "eq", "field": "attrs.rule",
                                     "value": lookalike}},
    )
    refuse_substring_contains(
        "filter",
        {"op": "contains", "field": "attrs", "value": {"rule": lookalike}},
    )


def test_filter_serves_an_exact_match_on_a_predicate_shaped_value(
    mcp_server: Any,
) -> None:
    """Through the server: the guard passes it on to the read, unrefused."""
    result = _call(
        mcp_server,
        "filter",
        {
            "input": {"kind": "nodes"},
            "predicate": {
                "op": "eq",
                "field": "attrs.rule",
                "value": {"op": "contains", "field": "node_id", "value": "x"},
            },
        },
    )
    # With no database the read itself fails; what matters is that the
    # substring guard did not refuse it first.
    assert CONTAINS_FORMS_MESSAGE not in result_text(result)


def test_every_lowering_site_says_the_same_sentence() -> None:
    """The program lowering raises the sentence, and the operator compiler
    (#3201: the one SQL lowering left for filter, map, aggregate and expand)
    never lowers the shape, so the guard that raises it runs first."""
    from okg.substrate.mcp.operator_models import refuse_substring_contains
    from okg.substrate.mcp.operator_sql import (
        compile_read_predicate,
        effective_attr_lowerer,
    )
    from okg.substrate.transform import operators

    assert operators.CONTAINS_FORMS_MESSAGE == CONTAINS_FORMS_MESSAGE
    _predicate_to_expr = operators._predicate_to_expr

    for shape in (
        {"op": "contains", "field": "node_id", "value": {"a": 1}},
        {"op": "contains", "field": "attrs", "value": "a"},
    ):
        with pytest.raises(ValueError) as lowered:
            _predicate_to_expr("nodes", shape)
        assert str(lowered.value) == CONTAINS_FORMS_MESSAGE
        compiled = compile_read_predicate(
            shape,
            leaf_lowerer=effective_attr_lowerer("nodes"),
            field_map_identity="contains-honest",
        )
        assert compiled.qualification == "superset"
        for tool in ("filter", "expand"):
            with pytest.raises(SubstrateError) as refused:
                refuse_substring_contains(tool, shape)
            assert str(refused.value) == CONTAINS_FORMS_MESSAGE


def test_published_text_names_both_contains_forms(
    mcp_server: Any,
) -> None:
    from okg.substrate.mcp.operator_models import SEARCH_FILTER_FIX_HINT

    tools = {tool.name: tool for tool in anyio.run(mcp_server.list_tools)}
    filter_text = tools["filter"].description
    search_text = tools["search"].description

    assert "JSON containment on attrs" in filter_text
    assert "text inside attrs.<key>" in filter_text
    # loose-match-3666: filter's `like` is the substring test that ignores
    # accents and case; #3666 adds `contains` with text beside it.
    assert "`like` matches a substring of a field" in filter_text
    # #3666: the texts said no tool matches substrings; both now name the
    # text form.
    assert "no substring operator" not in filter_text
    assert "matches substrings" not in search_text
    assert "`contains` finds text in attrs.<key>" in search_text
    # search used to send substring conditions to filter: "Conditions
    # (contains, suffix, ranges, and/or) belong to the `filter` tool".
    assert "Conditions (contains" not in search_text
    assert "Neither tool matches substrings" not in SEARCH_FILTER_FIX_HINT
    assert "contains on attrs.<key>" in SEARCH_FILTER_FIX_HINT
    assert "filter with like" in SEARCH_FILTER_FIX_HINT


def test_filter_schema_hint_shows_contains_with_an_object_value(
    mcp_server: Any,
) -> None:
    refused = mcp_server._tool_manager._tools["filter"].fn(
        input={"kind": "nodes"},
        predicate={"op": "regex", "field": "node_id", "value": "n:.*"},
    )
    hint = refused["error"]["schema_hint"]
    # The old hint listed 'contains' among ops taking <stable field> and a
    # <scalar> value; both halves were false.
    assert '"predicate"' not in hint  # examples are calls, not JSON objects
    assert 'predicate={"op":"contains","field":"attrs","value":{"status":"open"}}' in hint
    assert "'in'|'contains'" not in hint


def test_filter_schema_hint_teaches_the_current_condition_grammar(
    mcp_server: Any,
) -> None:
    """#3666: the hint taught `value:<scalar>` for in/between/and/or/not, which
    the predicate model refuses, and never named order_by."""
    refused = mcp_server._tool_manager._tools["filter"].fn(
        input={"kind": "nodes"},
        predicate={"op": "regex", "field": "node_id", "value": "n:.*"},
    )
    hint = refused["error"]["schema_hint"]
    assert "value:<scalar>" not in hint
    assert "'between'|'and'|'or'|'not'" not in hint
    assert "in and between take field and values" in hint
    assert "and/or take clauses; not takes clause" in hint
    assert '"values":["open","closed"]' in hint
    assert '"clauses":[' in hint and '"clause":{' in hint
    assert 'order_by="node_id desc"' in hint
