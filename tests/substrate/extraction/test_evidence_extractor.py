"""Unit tests for EvidenceExtractor (fake LLM + real Chronos source rows).

After the chronos cutover (commit b56a27ff4) ``EvidenceExtractor`` reads
its claims (joined to the containing paragraph via the ``contains`` edge)
and the adjacent citables through the real Chronos store
(``okg.graph_nodes`` / ``okg.graph_edges`` via
``ChronosGraphStore.from_connection(conn).checkout_generation(...)``), so
these tests seed that source graph through the same real store and assert
against the real ``okg.node_facts`` / ``okg.edge_facts`` write side. See
``tests/pytest/extractor_db_fixture.py``.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.library.enrichers.extraction.evidence_extractor import (
    EvidenceExtractor,
    parse_llm_evidence_response,
)

from tests.pytest.extractor_db_fixture import (
    edge,
    emitted_edge_facts,
    emitted_node_facts,
    node,
    seed_graph,
)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def test_parses_supports_link() -> None:
    raw = (
        '[{"evidence_ref": "citation:c1",'
        ' "support_polarity": "supports", "confidence": 0.8}]'
    )
    out = parse_llm_evidence_response(raw)
    assert len(out) == 1
    assert out[0]["evidence_ref"] == "citation:c1"
    assert out[0]["support_polarity"] == "supports"


def test_parses_contradicts_link() -> None:
    raw = (
        '[{"evidence_ref": "theorem:t1",'
        ' "support_polarity": "contradicts", "confidence": 0.7}]'
    )
    out = parse_llm_evidence_response(raw)
    assert out[0]["support_polarity"] == "contradicts"


def test_drops_unknown_polarity() -> None:
    raw = '[{"evidence_ref": "c", "support_polarity": "neutral", "confidence": 0.7}]'
    assert parse_llm_evidence_response(raw) == []


def test_dedups_repeated_evidence_ref() -> None:
    raw = (
        '[{"evidence_ref": "c1", "support_polarity": "supports", "confidence": 0.8},'
        ' {"evidence_ref": "c1", "support_polarity": "supports", "confidence": 0.7}]'
    )
    out = parse_llm_evidence_response(raw)
    assert len(out) == 1


def test_handles_fenced_and_empty() -> None:
    fenced = (
        "```json\n"
        '[{"evidence_ref": "c", "support_polarity": "supports", "confidence": 0.6}]\n'
        "```"
    )
    assert len(parse_llm_evidence_response(fenced)) == 1
    assert parse_llm_evidence_response("") == []
    assert parse_llm_evidence_response("not json") == []
    assert parse_llm_evidence_response("[]") == []


# ---------------------------------------------------------------------------
# Fake LLM + real source-graph seeding
# ---------------------------------------------------------------------------


class _FakeLLM:
    model = "fake"

    def __init__(self, response: str) -> None:
        self.response = response
        self.calls = 0

    def complete(self, system: str, user: str, **_: Any) -> str:
        self.calls += 1
        return self.response


# The evidence extractor reads each Claim joined to its containing
# Paragraph (via `Paragraph contains Claim`) plus the adjacent citables
# scoped to the same paper (attrs.paper_ref). These helpers seed that
# shape into the real Chronos graph.

CLAIM_ID = "claim:c1"
PARAGRAPH_ID = "paragraph:p1"
PAPER_REF = "paper:doi/test"
AUTHOR_REF = "author:test-author"


def _claim_with_paragraph() -> list[Any]:
    return [
        node(
            PARAGRAPH_ID,
            "paragraph",
            {
                "text": "We show X improves throughput",
                "paper_ref": PAPER_REF,
                "author_ref": AUTHOR_REF,
            },
        ),
        node(CLAIM_ID, "claim", {"text": "X improves throughput"}),
        edge(PARAGRAPH_ID, CLAIM_ID, "contains"),
    ]


def _citation_node() -> Any:
    return node(
        "citation:cite1",
        "citation",
        {
            "label": "Smith 2024",
            "paper_ref": PAPER_REF,
        },
    )


# ---------------------------------------------------------------------------
# End-to-end (real Chronos source rows + fake LLM)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def test_emits_evidence_node_supported_by_claim_and_cites_citation(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"evidence_ref": "citation:cite1",'
        ' "support_polarity": "supports", "confidence": 0.75}]'
    )
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            _claim_with_paragraph() + [_citation_node()],
            generation_id=10,
        )
        result = e.enrich(conn, generation_id=10)
        assert llm.calls == 1
        assert result.n_edges_emitted == 2  # supported_by + cites

        # Evidence NodeFact carries hypothesis_ref + observation_ref + paper attrs
        ev_nodes = emitted_node_facts(conn, "evidence")
        assert len(ev_nodes) == 1
        attrs = ev_nodes[0]["attrs"]
        assert attrs["hypothesis_ref"] == CLAIM_ID
        assert attrs["observation_ref"] == PARAGRAPH_ID
        assert attrs["support_polarity"] == "supports"
        assert attrs["extracted_from_paper_ref"] == PAPER_REF
        assert attrs["author_ref"] == AUTHOR_REF
        assert attrs["provenance"] == "extracted_model"
        assert attrs["extractor_floor_at_emit"] == 0.6

        # Edges: claim supported_by evidence + evidence cites citation
        edge_set = {
            (edge_row["src"], edge_row["edge_type"], edge_row["dst"])
            for edge_row in emitted_edge_facts(conn)
        }
        assert (CLAIM_ID, "supported_by", attrs["evidence_id"]) in edge_set
        assert (attrs["evidence_id"], "cites", "citation:cite1") in edge_set


def test_emits_contradicts_polarity(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"evidence_ref": "theorem:t1",'
        ' "support_polarity": "contradicts", "confidence": 0.7}]'
    )
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            _claim_with_paragraph()
            + [
                node(
                    "theorem:t1",
                    "theorem",
                    {
                        "statement": "Lemma 3",
                        "paper_ref": PAPER_REF,
                    },
                ),
            ],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)
        attrs = emitted_node_facts(conn, "evidence")[0]["attrs"]
        assert attrs["support_polarity"] == "contradicts"


def test_below_floor_evidence_becomes_extraction_dropped(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"evidence_ref": "citation:cite1",'
        ' "support_polarity": "supports", "confidence": 0.4}]'
    )
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            _claim_with_paragraph() + [_citation_node()],
            generation_id=1,
        )
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0
        assert result.n_edges_skipped == 1
        assert emitted_node_facts(conn, "evidence") == []
        assert len(emitted_node_facts(conn, "extraction_dropped")) == 1


def test_unknown_evidence_ref_marks_snapshot_incomplete(clean_runtime: str) -> None:
    """If the LLM hallucinates an evidence_ref not in the candidate
    pool, the extractor SHALL preserve the prior snapshot by marking
    this pass incomplete rather than treating it as an authoritative
    empty answer or emitting a dangling edge."""
    llm = _FakeLLM(
        '[{"evidence_ref": "citation:does-not-exist",'
        ' "support_polarity": "supports", "confidence": 0.8}]'
    )
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            _claim_with_paragraph() + [_citation_node()],  # only cite1 is real
            generation_id=1,
        )
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0
        assert result.n_edges_skipped == 0
        assert result.incomplete is True
        assert result.n_input_failures == 1


def test_skips_claim_with_no_adjacent_citables(clean_runtime: str) -> None:
    llm = _FakeLLM("[]")
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Claim + paragraph but no citable nodes scoped to the paper.
        seed_graph(conn, _claim_with_paragraph(), generation_id=1)
        e.enrich(conn, generation_id=1)
        # No LLM call when no candidate evidence items exist
        assert llm.calls == 0


def test_skips_when_no_claims(clean_runtime: str) -> None:
    llm = _FakeLLM("[]")
    e = EvidenceExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, [node("person:x", "person", {})], generation_id=1)
        e.enrich(conn, generation_id=1)
        assert llm.calls == 0


def test_skips_when_llm_unconfigured(clean_runtime: str) -> None:
    e = EvidenceExtractor(confidence_floor=0.6, llm=None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _claim_with_paragraph(), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0


def test_extractor_protocol_metadata() -> None:
    e = EvidenceExtractor(confidence_floor=0.6, llm=None)
    assert "claim" in e.reads_subtypes
    assert "paragraph" in e.reads_subtypes
    assert ("claim", "supported_by", "evidence") in e.requires_narrowings
    assert ("evidence", "cites", "citation") in e.requires_narrowings


def test_extractor_required_floor() -> None:
    with pytest.raises(ValueError):
        EvidenceExtractor(confidence_floor=None, llm=None)  # type: ignore[arg-type]
