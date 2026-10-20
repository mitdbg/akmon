"""Database-free rules of the shared-edge declarer table (item 92a).

The PostgreSQL publish proof is tests/substrate/test_edge_declarers_db.py.
"""

from __future__ import annotations

from typing import Any

import pytest

from okg.substrate.graph_store_internal.edge_declarers import (
    EdgePrecedenceUnranked,
    parse_edge_precedence,
    reconcile_edge_declarers,
    withdraw_declarations,
)

EDGE = 7
RANKS = {"calls": {"_enricher:code_reference": 0, "lsp": 1, "structure": 2}}


def _row(source: str, via: str, **extra: Any) -> dict[str, Any]:
    return {
        "edge_id": EDGE,
        "src": "symbol:a",
        "dst": "symbol:b",
        "edge_type": "calls",
        "attrs": {"via": via},
        "provenance": "explicit",
        "confidence": 1.0,
        "source": source,
        "source_record_id": {},
        "source_revision": {},
        "run_id": None,
        "observed_at": None,
        **extra,
    }


def _reconcile(
    *,
    source: str | None,
    current: dict[str, Any] | None,
    recorded: list[dict[str, Any]],
    upserts: list[dict[str, Any]] = (),  # type: ignore[assignment]
    withdrawn: tuple[int, ...] = (),
    purged: tuple[int, ...] = (),
):
    return reconcile_edge_declarers(
        source=source,
        ranks=RANKS,
        upserts=list(upserts),
        withdrawn_edge_ids=withdrawn,
        purged_edge_ids=purged,
        current_edges={} if current is None else {EDGE: current},
        declarations={EDGE: recorded} if recorded else {},
    )


def test_lower_rank_rewrite_records_declaration_and_keeps_row() -> None:
    winner = _row("_enricher:code_reference", "enricher")
    outcome = _reconcile(
        source="structure",
        current=winner,
        recorded=[winner, _row("structure", "v1")],
        upserts=[_row("structure", "v2")],
    )
    assert outcome.edge_upserts == []
    assert outcome.edge_deletes == []
    assert [d["attrs"]["via"] for d in outcome.declaration_upserts] == ["v2"]


def test_higher_rank_write_takes_the_row() -> None:
    lower = _row("structure", "v1")
    outcome = _reconcile(
        source="lsp",
        current=lower,
        recorded=[lower],
        upserts=[_row("lsp", "typed")],
    )
    assert [row["source"] for row in outcome.edge_upserts] == ["lsp"]
    assert outcome.handoffs == [EDGE]


def test_lower_rank_retraction_is_inert() -> None:
    winner = _row("_enricher:code_reference", "enricher")
    outcome = _reconcile(
        source="structure",
        current=winner,
        recorded=[winner, _row("structure", "v1")],
        withdrawn=(EDGE,),
    )
    assert outcome.edge_upserts == []
    assert outcome.edge_deletes == []
    assert outcome.declaration_deletes == [{"edge_id": EDGE, "source": "structure"}]


def test_winner_withdrawal_hands_row_to_next_rank() -> None:
    winner = _row("_enricher:code_reference", "enricher")
    lsp = _row("lsp", "typed")
    outcome = _reconcile(
        source="_enricher:code_reference",
        current=winner,
        recorded=[winner, lsp, _row("structure", "v1")],
        withdrawn=(EDGE,),
    )
    assert [row["attrs"]["via"] for row in outcome.edge_upserts] == ["typed"]
    assert outcome.edge_deletes == []


def test_last_declarer_withdrawal_deletes_row() -> None:
    only = _row("structure", "v1")
    outcome = _reconcile(
        source="structure", current=only, recorded=[only], withdrawn=(EDGE,),
    )
    assert outcome.edge_deletes == [{"edge_id": EDGE}]
    assert outcome.edge_upserts == []


def test_purge_removes_every_declaration_and_the_row() -> None:
    winner = _row("_enricher:code_reference", "enricher")
    outcome = _reconcile(
        source="structure",
        current=winner,
        recorded=[winner, _row("structure", "v1")],
        purged=(EDGE,),
    )
    assert outcome.edge_deletes == [{"edge_id": EDGE}]
    assert sorted(d["source"] for d in outcome.declaration_deletes) == [
        "_enricher:code_reference",
        "structure",
    ]


def test_unranked_second_declarer_is_refused_naming_both() -> None:
    lone = _row("structure", "v1")
    with pytest.raises(EdgePrecedenceUnranked) as refused:
        _reconcile(
            source="stranger",
            current=lone,
            recorded=[lone],
            upserts=[_row("stranger", "x")],
        )
    assert refused.value.data["unranked_sources"] == ["stranger"]
    assert refused.value.data["declaring_sources"] == ["stranger", "structure"]


def test_lone_unranked_declarer_wins() -> None:
    outcome = _reconcile(
        source="stranger", current=None, recorded=[], upserts=[_row("stranger", "x")],
    )
    assert [row["source"] for row in outcome.edge_upserts] == ["stranger"]


def test_legacy_row_is_adopted_before_a_lower_rank_write() -> None:
    """A stored row with no declaration (written before the table existed)
    speaks for its source: a lower-ranked write does not replace it."""
    legacy = _row("lsp", "typed")
    outcome = _reconcile(
        source="structure",
        current=legacy,
        recorded=[],
        upserts=[_row("structure", "v1")],
    )
    assert outcome.edge_upserts == []
    assert outcome.adopted == [EDGE]
    assert sorted(d["source"] for d in outcome.declaration_upserts) == [
        "lsp",
        "structure",
    ]


def test_unchanged_redeclaration_writes_nothing() -> None:
    own = _row("structure", "v1")
    outcome = _reconcile(
        source="structure", current=own, recorded=[own], upserts=[dict(own)],
    )
    assert outcome.declaration_upserts == []
    assert outcome.edge_upserts == []


def test_withdrawing_a_retired_source_hands_its_rows_on() -> None:
    winner = _row("lsp", "typed")
    outcome = withdraw_declarations(
        sources=["lsp"],
        ranks=RANKS,
        current_edges={EDGE: winner},
        declarations={EDGE: [winner, _row("structure", "v1")]},
        withdrawn=[(EDGE, "lsp")],
    )
    assert [row["source"] for row in outcome.edge_upserts] == ["structure"]
    assert outcome.declaration_deletes == [{"edge_id": EDGE, "source": "lsp"}]


@pytest.mark.parametrize(
    "value, message",
    [
        ([], "must map an edge type"),
        ({"calls": []}, "non-empty list"),
        ({"calls": "a"}, "non-empty list"),
        ({"calls": ["a", "a"]}, "twice"),
        ({"calls": ["a", ""]}, "empty or non-string producer"),
        ({"": ["a"]}, "empty or non-string edge type"),
    ],
)
def test_malformed_ranking_is_refused(value: Any, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        parse_edge_precedence(value)


def test_ranking_parses_in_order() -> None:
    assert parse_edge_precedence({"calls": ["e", "l", "s"]}) == {
        "calls": ("e", "l", "s"),
    }
    assert parse_edge_precedence(None) == {}
