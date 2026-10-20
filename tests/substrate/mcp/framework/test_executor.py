"""Tests for the pre-join MCP framework executor.

Two layers:

(1) Pure-Python tests that exercise the summary-template formatter
    and the resolver-reference resolution path with stubbed
    primitives. Hermetic; no postgres needed.

(2) An end-to-end fixture (`test_executor_pg.py`) runs a real
    pre-join tool against a hermetic Publisher and confirms the
    response envelope has the expected shape. That file is
    `pytest.mark.postgres`.
"""
from __future__ import annotations


import pytest

from okg.substrate.mcp.framework import (
    PreJoinBundleStep,
    PreJoinParam,
    PreJoinToolSpec,
    PreJoinToolError,
    ResolverRegistry,
    execute_pre_join_tool,
    format_summary_template,
)
from okg.substrate.mcp.framework.executor import (
    _first_node_id,
    _DotAccessFormatter,
)


# ---- summary template ----


def test_summary_template_resolves_simple_param() -> None:
    s = format_summary_template(
        template="topic={topic}",
        params={"topic": "transformers"},
        bundles={},
    )
    assert s == "topic=transformers"


def test_summary_template_resolves_emit_count() -> None:
    s = format_summary_template(
        template="papers={papers.count}; cards={method_cards.count}",
        params={},
        bundles={
            "papers": [{"node_id": "paper:a"}, {"node_id": "paper:b"}],
            "method_cards": [{"node_id": "card:x"}],
        },
    )
    assert s == "papers=2; cards=1"


def test_summary_template_resolves_indexed_attr() -> None:
    s = format_summary_template(
        template="first={papers.0.node_id}",
        params={},
        bundles={
            "papers": [
                {"node_id": "paper:a", "attrs": {"title": "Hi"}},
                {"node_id": "paper:b"},
            ],
        },
    )
    assert s == "first=paper:a"


def test_summary_template_indirects_through_attrs_key() -> None:
    """The substrate's node-shape returns rows as
    `{node_id, subtype, attrs: {...}}`; templates use the
    `<row>.<attr_key>` shorthand to dig through `attrs`.
    """
    s = format_summary_template(
        template="title={papers.0.title}",
        params={},
        bundles={
            "papers": [
                {"node_id": "paper:a", "attrs": {"title": "Hi"}},
            ],
        },
    )
    assert s == "title=Hi"


def test_summary_template_error_returns_literal_template() -> None:
    """A template referencing an unknown emit falls back to the
    literal template + logged warning, per S-A7-R1 task 3.4."""
    template = "missing={ghost.0.node_id}"
    s = format_summary_template(
        template=template, params={}, bundles={"papers": []},
    )
    assert s == template


def test_summary_template_empty_returns_empty() -> None:
    assert format_summary_template(template=None, params={}, bundles={}) == ""
    assert format_summary_template(template="", params={}, bundles={}) == ""


def test_dot_access_formatter_count_for_dict_is_one() -> None:
    """A dict emit (single-row primitive like get_node) counts as 1."""
    formatter = _DotAccessFormatter()
    out = formatter.vformat(
        "{row.count}", (),
        {"row": {"node_id": "n:1", "attrs": {"k": 1}}},
    )
    assert out == "1"


# ---- _first_node_id helper ----


@pytest.mark.parametrize(
    ("emit", "expected"),
    [
        ({"node_id": "n:1"}, "n:1"),
        ({"attrs": {}}, None),
        ([{"node_id": "n:1"}, {"node_id": "n:2"}], "n:1"),
        ([], None),
        (None, None),
        ("plain-string", None),
    ],
)
def test_first_node_id_supports_node_and_list_emits(emit, expected) -> None:
    assert _first_node_id(emit) == expected


# ---- execute_pre_join_tool missing required param ----


def test_execute_pre_join_tool_rejects_missing_required_param() -> None:
    spec = PreJoinToolSpec(
        name="t",
        description="needs a param",
        params=(PreJoinParam(name="topic", kind="text", required=True),),
        bundle=(
            PreJoinBundleStep(
                call="get_node",
                args={"node_id": {"resolve": "alias_table", "input": "topic"}},
                emit_as="row",
            ),
        ),
    )
    with pytest.raises(PreJoinToolError, match="topic"):
        execute_pre_join_tool(
            spec,
            conn=None,  # type: ignore[arg-type]
            session=None,  # type: ignore[arg-type]
            params={},
            resolvers=ResolverRegistry.with_substrate_builtins(),
        )
