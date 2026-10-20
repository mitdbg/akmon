"""Unit tests for the review_as response parser + helpers.

The DB-backed end-to-end review_as test lives in
tests/substrate/mcp/test_review_as_pg.py and runs against a real
Publisher.
"""
from __future__ import annotations

import uuid

import pytest

from okg.substrate.mcp.review_as import (
    PersonaUnknown,
    _compute_critique_id,
    parse_persona_response,
)
from okg.substrate.mcp.review_as import _resolve_persona


def test_parse_persona_response_well_formed() -> None:
    parsed, err = parse_persona_response({
        "verdict": "approve",
        "severity": "info",
        "text": "Looks good.",
        "confidence": 0.85,
    })
    assert err is None
    assert parsed == {
        "verdict": "approve",
        "severity": "info",
        "text": "Looks good.",
        "confidence": 0.85,
    }


def test_parse_persona_response_rejects_bad_verdict_into_conditional() -> None:
    """Per S-A7-R5 task 5.6: parse failure yields a Critique with
    verdict=conditional, severity=info, text=raw + parse note."""
    parsed, err = parse_persona_response({
        "verdict": "thumbs-up",
        "severity": "info",
        "text": "Sure.",
    })
    assert err is not None
    assert "verdict" in err
    assert parsed["verdict"] == "conditional"
    assert parsed["severity"] == "info"
    assert "thumbs-up" in parsed["text"]
    assert "response_parse_error" in parsed["text"]


def test_parse_persona_response_non_dict_input() -> None:
    parsed, err = parse_persona_response("just a string")
    assert err is not None
    assert parsed["verdict"] == "conditional"
    assert parsed["severity"] == "info"
    assert "just a string" in parsed["text"]


def test_parse_persona_response_missing_text() -> None:
    parsed, err = parse_persona_response({
        "verdict": "reject",
        "severity": "blocker",
    })
    assert err is not None
    assert "text" in err
    assert parsed["verdict"] == "conditional"  # collapsed on parse fail
    assert parsed["severity"] == "info"


def test_critique_id_includes_run_id_short() -> None:
    """Per D8: distinct call_run_ids produce distinct critique_ids
    so re-reviews don't collapse into one row. The id slices the
    leading 8 hex chars of run_id.hex; random UUIDs almost always
    differ at that prefix length."""
    run_a = uuid.UUID("a1b2c3d4-0000-0000-0000-000000000001")
    run_b = uuid.UUID("dcba1234-0000-0000-0000-000000000001")
    a = _compute_critique_id(
        persona_slug="vonneumann",
        target_node_id="paper:foo",
        run_id=run_a,
    )
    b = _compute_critique_id(
        persona_slug="vonneumann",
        target_node_id="paper:foo",
        run_id=run_b,
    )
    assert a != b
    assert a.startswith("critique:vonneumann:")
    assert b.startswith("critique:vonneumann:")


def test_critique_id_collapses_when_same_run_id() -> None:
    """Same call_run_id -> same critique_id -> wire-level dedup
    collapses the second emission."""
    run = uuid.UUID("a1b2c3d4-0000-0000-0000-000000000001")
    a = _compute_critique_id(
        persona_slug="vonneumann",
        target_node_id="paper:foo",
        run_id=run,
    )
    b = _compute_critique_id(
        persona_slug="vonneumann",
        target_node_id="paper:foo",
        run_id=run,
    )
    assert a == b


def test_resolve_persona_canonical_slug() -> None:
    """The substrate's canonical personas resolve by slug or
    by persona:<slug> alias form."""
    seed = _resolve_persona("vonneumann")
    assert seed.slug == "vonneumann"
    seed2 = _resolve_persona("persona:dijkstra")
    assert seed2.slug == "dijkstra"


def test_resolve_persona_unknown_raises_typed_error() -> None:
    """Per S-A7-R5: unknown persona raises PersonaUnknown with
    a typed wire code (matches the MCP error envelope)."""
    with pytest.raises(PersonaUnknown, match="obscure") as exc:
        _resolve_persona("obscure")
    assert exc.value.code == "persona_unknown"
