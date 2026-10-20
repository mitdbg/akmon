"""A path hop named without a direction (lane RD: 14 agent calls refused)."""
from __future__ import annotations

import pytest

from okg.substrate.mcp import parse_path_hops
from okg.substrate.mcp.path_diagnostics import InvalidPathGrammar


def test_a_hop_without_a_direction_takes_the_calls_direction_and_says_so() -> None:
    """`'*'` alone (the description's "'*' for any link") or a bare edge type
    is read with the expand call's own direction, and the reply's adjusted
    note says how; without a default direction it is still refused."""
    from okg.substrate.mcp.response_bounds import adjusted_scope, call_adjusted

    with adjusted_scope(()):
        assert parse_path_hops(["*"], default_direction="out") == [(None, "out")]
        assert parse_path_hops(["first", "*:in"], default_direction="in") == [
            ("first", "in"), (None, "in"),
        ]
        notes = call_adjusted()
    assert {"rule": "path_hop_direction", "sent": "path hop '*'",
            "served": "'*:out'"} in notes
    assert {"rule": "path_hop_direction", "sent": "path hop 'first'",
            "served": "'first:in'"} in notes
    with pytest.raises(InvalidPathGrammar):
        parse_path_hops(["*"])
    with pytest.raises(InvalidPathGrammar):
        parse_path_hops([""], default_direction="out")


def test_direction_both_gives_a_bare_hop_no_direction_and_the_refusal_says_why() -> None:
    """`both` is a one-hop direction: a bare hop is never built as '*:both'."""
    with pytest.raises(InvalidPathGrammar) as refused:
        parse_path_hops(["*"], default_direction="both")
    assert refused.value.data["requested_hop"] == "*"
    assert "direction='both' does not give it one" in refused.value.fix_hint
    assert "add ':out' or ':in'" in refused.value.fix_hint
    # A hop that names its own direction is read as written under `both`.
    assert parse_path_hops(["*:in"], default_direction="both") == [(None, "in")]


def test_a_call_refused_at_a_later_hop_carries_no_note_for_an_earlier_one() -> None:
    from okg.substrate.mcp.response_bounds import adjusted_scope, call_adjusted

    with adjusted_scope(()):
        with pytest.raises(InvalidPathGrammar):
            parse_path_hops(["*", ":out"], default_direction="out")
        with pytest.raises(InvalidPathGrammar):
            parse_path_hops(["first", 7], default_direction="in")
        notes = call_adjusted()
    assert all(record.get("rule") != "path_hop_direction" for record in notes), notes
