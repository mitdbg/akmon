"""bound-inspect-graph-select: the block-narrowing contract, without a database.

The defect this change fixes is that `inspect(target={"kind":"graph"})` accepted
a `select` list and ran every aggregate anyway. The core of the fix -- "an
unselected block issues NO query" -- is a property of `_graph_summary_sql` and
the block vocabulary around it, and needs no Postgres to check: a stub graph
records the statements it is asked to run.

Why this file exists SEPARATELY from tests/substrate/mcp/test_inspect_graph_select.py:
that file is `pytest.mark.postgres`, and the per-PR Postgres lane (`substrate-db`)
has no budget headroom to take it -- 181s enforced, dev measured at 129.79-178.61s,
and the budget guard only ratchets DOWN. So the Postgres file runs nightly. This
file carries NO postgres marker, so the per-PR `fast` lane (which sweeps
`tests/substrate` under `not postgres`) guards the narrowing behaviour on every
PR. Both are needed: this one proves which queries are issued, the Postgres one
proves the payload and the rollup against a real graph.
"""

from __future__ import annotations

import pytest

from okg.substrate.mcp import tools as tools_mod
from okg.substrate.mcp.envelope import SelectUnknownField, validate_select


class _RecordingGraph:
    """Stub Chronos checkout that records every statement it is handed.

    `_graph_summary_sql` only calls `.query(sql, params)` and reads the rows, so
    the stub returns shapes each block expects and nothing else.
    """

    def __init__(self) -> None:
        self.statements: list[str] = []

    def query(self, sql, params=None, *a, **kw):
        self.statements.append(str(sql))
        text = str(sql)
        if "GROUP BY subtype" in text:
            return [{"subtype": "source_file", "c": 3}]
        if "GROUP BY edge_type" in text:
            return [{"edge_type": "mentions", "c": 2}]
        if "embedding IS NULL" in text:
            return [{"c": 7}]
        if "entity_mention" in text:
            return [{"entity_type": "person", "total": 4, "resolved": 1}]
        raise AssertionError(f"unexpected statement: {text}")


def _issued(blocks) -> tuple[dict, list[str]]:
    graph = _RecordingGraph()
    out = tools_mod._graph_summary_sql(graph, frozenset(blocks))
    return out, graph.statements


# --------------------------------------------------------------------------- #
# the narrowing itself
# --------------------------------------------------------------------------- #


def test_the_reported_two_field_request_issues_only_two_statements() -> None:
    """`{"select": ["subtypes","edge_types"]}` -- the request that timed out.

    Measured on live okg-workspace, the two blocks it does NOT ask for cost
    2435 ms and 104 ms. Neither statement may be issued.
    """
    out, statements = _issued({"subtypes", "edge_types"})
    assert len(statements) == 2, statements
    joined = "\n".join(statements)
    assert "embedding IS NULL" not in joined, joined
    assert "entity_mention" not in joined, joined
    assert set(out) == {"subtype_counts", "edge_type_counts"}, sorted(out)


@pytest.mark.parametrize(
    "block,key,forbidden",
    [
        ("subtypes", "subtype_counts", ("GROUP BY edge_type", "embedding IS NULL")),
        ("edge_types", "edge_type_counts", ("GROUP BY subtype", "entity_mention")),
        ("embed_lag", "embed_lag", ("GROUP BY subtype", "GROUP BY edge_type")),
        (
            "unresolved_mentions",
            "unresolved_mentions",
            ("GROUP BY subtype", "embedding IS NULL"),
        ),
    ],
)
def test_each_block_alone_issues_exactly_one_statement(
    block: str, key: str, forbidden: tuple[str, ...]
) -> None:
    """Every block is independently skippable, not just the two cheap ones."""
    out, statements = _issued({block})
    assert len(statements) == 1, statements
    assert set(out) == {key}, sorted(out)
    for fragment in forbidden:
        assert fragment not in statements[0], (fragment, statements[0])


def test_the_empty_selection_issues_nothing() -> None:
    """Selecting no block runs no query at all -- the floor of the contract."""
    out, statements = _issued(set())
    assert statements == []
    assert out == {}


def test_every_block_together_issues_every_statement() -> None:
    """The compatibility case: the full set still computes the full payload.

    Without this, a bug that skipped everything would pass every test above.
    """
    out, statements = _issued(tools_mod.INSPECT_GRAPH_BLOCKS)
    assert len(statements) == 4, statements
    assert set(out) == {
        "subtype_counts",
        "edge_type_counts",
        "embed_lag",
        "unresolved_mentions",
    }, sorted(out)


# --------------------------------------------------------------------------- #
# the block vocabulary
# --------------------------------------------------------------------------- #


def test_the_block_names_are_exactly_the_four_documented_ones() -> None:
    assert tools_mod.INSPECT_GRAPH_BLOCKS == frozenset(
        {"subtypes", "edge_types", "embed_lag", "unresolved_mentions"}
    )


def test_every_block_maps_to_a_summary_key() -> None:
    """A block with no key would KeyError at payload assembly, not at selection."""
    assert set(tools_mod._BLOCK_KEYS) == set(tools_mod.INSPECT_GRAPH_BLOCKS)
    assert len(set(tools_mod._BLOCK_KEYS.values())) == len(tools_mod._BLOCK_KEYS)


def test_the_schema_digest_needs_both_count_blocks() -> None:
    """The digest is built from BOTH count maps and coerces a missing one to {}.

    Built from one, it would report `edge_type_count: 0` for a caller that
    selected only `subtypes` -- telling it the graph has no edge types at all.
    """
    assert tools_mod._SCHEMA_BLOCKS == frozenset({"subtypes", "edge_types"})
    for partial in ({"subtypes"}, {"edge_types"}, set()):
        assert not tools_mod._SCHEMA_BLOCKS <= frozenset(partial), partial
    assert tools_mod._SCHEMA_BLOCKS <= tools_mod.INSPECT_GRAPH_BLOCKS


def test_the_rollup_serves_only_the_two_count_blocks() -> None:
    """The rollup has no embedding-nullity or mention dimension.

    If this ever widened without the read widening too, the extra block would be
    silently dropped from the payload rather than computed.
    """
    assert tools_mod._ROLLUP_SERVED_BLOCKS == frozenset(
        {"subtypes", "edge_types"}
    )
    assert tools_mod._ROLLUP_KINDS_FOR_BLOCKS == ("subtype", "edge_type")


# --------------------------------------------------------------------------- #
# the refusal
# --------------------------------------------------------------------------- #


def test_an_unknown_block_is_refused_with_the_valid_names() -> None:
    """A misspelled block must not widen back to the full (slow) answer."""
    with pytest.raises(SelectUnknownField) as excinfo:
        validate_select(
            ["subtypes", "subtpyes"],
            tools_mod.INSPECT_GRAPH_BLOCKS,
            tool_name="inspect(graph)",
        )
    assert excinfo.value.data["unknown_fields"] == ["subtpyes"]
    assert set(excinfo.value.data["valid_fields"]) == set(
        tools_mod.INSPECT_GRAPH_BLOCKS
    )


def test_omitting_select_is_not_a_refusal() -> None:
    """`select=None` means "everything", the compatibility path."""
    validate_select(None, tools_mod.INSPECT_GRAPH_BLOCKS, tool_name="inspect(graph)")
