"""The documented candidate-card fields must equal the implementation's.

This is the guard whose absence let the two drift: `docs/use/search/index.md`
documented a `source_id` result field the implementation has never emitted, and
`docs/reference/mcp.md` described the row shape loosely enough that four landed
changes could add fields the docs never learned. Comparing identifier SETS —
not sentences — is the approach `test_documented_surface_agreement.py` uses,
and each parser here is paired with a negative test so it cannot pass
vacuously.

Database-free: it imports the vocabularies and parses the Markdown.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[2]
MCP_DOC = "docs/reference/mcp.md"
SEARCH_DOC = "docs/use/search/index.md"

_BEGIN = "<!-- card-fields:begin -->"
_END = "<!-- card-fields:end -->"
#: The first cell may name more than one field (`score` / `distance`), so every
#: backticked identifier in it counts.
_FIELD_RE = re.compile(r"`([a-z][a-z0-9_]*)`")


def _text(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def _documented_card_fields(text: str) -> set[str]:
    """Field names in the pinned card-fields table.

    Keyed on explicit markers rather than the `| Field |` header, because this
    page carries a SECOND table whose first column is also `Field` (the record
    reference fields). A header-keyed parser would silently read the wrong one.
    """
    assert _BEGIN in text and _END in text, (
        f"{MCP_DOC} must carry the pinned card-fields block "
        f"({_BEGIN} ... {_END})"
    )
    block = text.split(_BEGIN, 1)[1].split(_END, 1)[0]
    fields: set[str] = set()
    for line in block.splitlines():
        line = line.strip()
        if not line.startswith("|") or line.startswith("|---"):
            continue
        first_cell = line.split("|")[1]
        fields.update(_FIELD_RE.findall(first_cell))
    return fields


def _implementation_card_fields() -> set[str]:
    """Every field a DEFAULT-projected lexical or semantic row can carry.

    Semantic has no separate default tuple — `_SEMANTIC_FIELDS` is what its
    projection defaults to — so the two are unioned directly.
    """
    from okg.substrate.mcp.search import _LEXICAL_DEFAULT, _SEMANTIC_FIELDS

    return set(_LEXICAL_DEFAULT) | set(_SEMANTIC_FIELDS)


def test_documented_card_fields_match_the_search_vocabularies() -> None:
    documented = _documented_card_fields(_text(MCP_DOC))
    implemented = _implementation_card_fields()
    assert documented == implemented, (
        "the card-fields table in docs/reference/mcp.md and the search field "
        "vocabularies disagree; "
        f"documented-only={sorted(documented - implemented)}, "
        f"implemented-only={sorted(implemented - documented)}"
    )


def test_every_documented_field_is_actually_selectable() -> None:
    """A documented field a caller cannot name in `select` is a broken promise."""
    from okg.substrate.mcp.search import _LEXICAL_FIELDS, _SEMANTIC_FIELDS

    selectable = set(_LEXICAL_FIELDS) | set(_SEMANTIC_FIELDS)
    documented = _documented_card_fields(_text(MCP_DOC))
    assert documented <= selectable, sorted(documented - selectable)


def test_the_card_parser_notices_a_removed_row() -> None:
    """Non-vacuity: the parser must fail when the doc drops a documented row."""
    text = _text(MCP_DOC)
    mutated = text.replace(
        "| `source_record_id` | Producer record; null when unattributed. |\n",
        "",
    )
    assert mutated != text, "the mutation target row moved; update this test"
    assert "source_record_id" not in _documented_card_fields(mutated)
    assert _documented_card_fields(mutated) != _implementation_card_fields()


def test_the_card_parser_reads_the_card_table_not_the_reference_table() -> None:
    """Non-vacuity: the record reference table must not leak into the parse."""
    documented = _documented_card_fields(_text(MCP_DOC))
    # `project_ref` and `study_ref` are first-column entries of the OTHER
    # `| Field |` table on this page.
    assert "project_ref" not in documented
    assert "study_ref" not in documented


@pytest.mark.parametrize("relative", [MCP_DOC, SEARCH_DOC])
def test_no_page_documents_a_source_id_result_field(relative: str) -> None:
    """`source_id` is not, and never was, a search result field.

    The row names its source through `provenance.source`, and the producer
    record through `source_record_id`. A reader who filtered on the documented
    `source_id` got nothing back and had no way to tell a wrong field name from
    an empty graph.
    """
    text = _text(relative)
    assert '"source_id"' not in text, (
        f"{relative} documents a `source_id` result field, which search does "
        "not emit; the source name is `provenance.source`"
    )


def test_the_coverage_inspect_target_is_documented() -> None:
    """A read surface an agent cannot discover is a surface it will not use."""
    from okg.substrate.mcp.source_coverage import (
        COVERAGE_BLOCKS,
        STATE_DECLARED_GAP,
        STATE_INCLUDED,
        STATE_NOT_INGESTED,
        STATE_UNREGISTERED,
    )

    text = _text(MCP_DOC)
    assert '{"kind": "coverage"}' in text
    for state in (
        STATE_INCLUDED,
        STATE_NOT_INGESTED,
        STATE_UNREGISTERED,
        STATE_DECLARED_GAP,
    ):
        assert f"`{state}`" in text, f"coverage state {state!r} is undocumented"
    for block in sorted(COVERAGE_BLOCKS):
        assert f"`{block}`" in text, f"coverage block {block!r} is undocumented"


def test_the_parent_states_are_documented() -> None:
    from okg.substrate.mcp.passage_rollup import (
        PARENT_STATE_AMBIGUOUS_DECLARATION,
        PARENT_STATE_BOUNDED_UNKNOWN,
        PARENT_STATE_CANDIDATE,
        PARENT_STATE_MULTIPLE_PARENTS,
        PARENT_STATE_RESOLVED,
        PARENT_STATE_UNLINKED,
    )

    text = _text(MCP_DOC)
    for state in (
        PARENT_STATE_RESOLVED,
        PARENT_STATE_UNLINKED,
        PARENT_STATE_MULTIPLE_PARENTS,
        PARENT_STATE_BOUNDED_UNKNOWN,
        # `candidate` is the one an agent could most easily mistake for a
        # citable parent, so its documentation is pinned like the rest.
        PARENT_STATE_CANDIDATE,
    ):
        assert f"`{state}`" in text, f"parent state {state!r} is undocumented"
    # Issue #2753 retired `ambiguous_declaration`. The reference may name it
    # only to say it is no longer emitted, never as a live state.
    retired = f"`{PARENT_STATE_AMBIGUOUS_DECLARATION}`"
    for line in text.splitlines():
        if retired in line:
            tail = line.split(retired, 1)[1]
            assert tail.startswith(" is no longer emitted"), line
    # Several declarations for one child are disclosed as a list.
    assert '`{"declarations": [...]}`' in text
