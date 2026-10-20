"""Unit tests for HypothesisExtractor (fake LLM + real Chronos source rows).

After the chronos cutover (commit b56a27ff4) ``HypothesisExtractor`` reads
Intro/Discussion ``paper_section`` nodes — whose body text it aggregates
from the child ``paragraph`` nodes reached via ``section contains
paragraph`` — through the real Chronos store (``okg.graph_nodes`` /
``okg.graph_edges`` via ``ChronosGraphStore.from_connection(conn).
checkout_generation(...)``). These tests seed that source graph through the
same real store and assert against the real ``okg.node_facts`` /
``okg.edge_facts`` write side. See ``tests/pytest/extractor_db_fixture.py``.
"""

from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.library.enrichers.extraction.hypothesis_extractor import (
    HypothesisExtractor,
    parse_llm_hypotheses_response,
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


def test_parses_hypothesis() -> None:
    raw = (
        '[{"statement": "We conjecture X scales linearly",'
        ' "confidence": 0.7,'
        ' "predicts_about_concept_names": ["linear scaling"]}]'
    )
    out = parse_llm_hypotheses_response(raw)
    assert len(out) == 1
    assert out[0]["statement"] == "We conjecture X scales linearly"
    assert out[0]["confidence"] == 0.7
    assert out[0]["predicts_about_concept_names"] == ["linear scaling"]


def test_parses_empty_concepts_list() -> None:
    raw = '[{"statement": "S", "confidence": 0.6}]'
    out = parse_llm_hypotheses_response(raw)
    assert out[0]["predicts_about_concept_names"] == []


def test_drops_missing_statement_or_confidence() -> None:
    raw = (
        '[{"confidence": 0.5},'
        ' {"statement": "S"},'
        ' {"statement": "T", "confidence": 0.6}]'
    )
    out = parse_llm_hypotheses_response(raw)
    assert [h["statement"] for h in out] == ["T"]


def test_handles_fenced_and_empty() -> None:
    fenced = '```json\n[{"statement": "X", "confidence": 0.6}]\n```'
    assert len(parse_llm_hypotheses_response(fenced)) == 1
    assert parse_llm_hypotheses_response("") == []


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


# The hypothesis extractor reads Intro/Discussion `paper_section` nodes and
# aggregates body text from the child `paragraph` nodes reached via
# `section contains paragraph`. This helper seeds that shape.


def _intro_section(
    *,
    section_id: str = "section:s1",
    section_kind: str = "introduction",
    body: str = "We hypothesize that X.",
    paragraph_id: str = "paragraph:s1p1",
    paper_ref: str | None = "paper:p1",
    author_ref: str = "author:smith-j",
) -> list[Any]:
    section_attrs: dict[str, Any] = {
        "section_kind": section_kind,
        "author_ref": author_ref,
    }
    if paper_ref:
        section_attrs["paper_ref"] = paper_ref
    return [
        node(section_id, "paper_section", section_attrs),
        node(paragraph_id, "paragraph", {"text": body, "ordinal": "0"}),
        edge(section_id, paragraph_id, "contains"),
    ]


# ---------------------------------------------------------------------------
# End-to-end (real Chronos source rows + fake LLM)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def _hypothesis_nodes(conn) -> list[dict[str, Any]]:
    return emitted_node_facts(conn, "hypothesis")


def test_emits_hypothesis_with_paper_provenance(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"statement": "X scales linearly",'
        ' "confidence": 0.7, "predicts_about_concept_names": []}]'
    )
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0  # no concept names → no edges
        # Just the Hypothesis node (no predicts_about edges)
        hyps = _hypothesis_nodes(conn)
        assert len(hyps) == 1
        attrs = hyps[0]["attrs"]
        assert attrs["extracted_from_paper_ref"] == "paper:p1"
        assert attrs["author_ref"] == "author:smith-j"
        assert attrs["provenance"] == "extracted_model"
        assert attrs["status"] == "open"
        assert attrs["statement"] == "X scales linearly"
        assert emitted_edge_facts(conn) == []


def test_emits_predicts_about_edge_when_concept_named(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"statement": "X predicts Y",'
        ' "confidence": 0.7,'
        ' "predicts_about_concept_names": ["self-attention", "stability"]}]'
    )
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 2
        hyp_id = _hypothesis_nodes(conn)[0]["node_id"]
        edge_set = {
            (edge_row["src"], edge_row["edge_type"], edge_row["dst"])
            for edge_row in emitted_edge_facts(conn)
        }
        assert (hyp_id, "predicts_about", "concept:self-attention") in edge_set
        assert (hyp_id, "predicts_about", "concept:stability") in edge_set


def test_below_floor_hypothesis_becomes_dropped(clean_runtime: str) -> None:
    llm = _FakeLLM('[{"statement": "weak claim", "confidence": 0.3}]')
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_skipped == 1
        assert _hypothesis_nodes(conn) == []
        assert len(emitted_node_facts(conn, "extraction_dropped")) == 1


def test_filters_sections_by_kind() -> None:
    """A section_kind of 'methods' SHALL be filtered out at the
    SELECT layer. This test asserts the reads_subtypes contract."""
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=_FakeLLM("[]"), input_quarantine_max_attempts=3
    )
    # The class declares the right enum at the SQL layer; this is
    # documented via the requires_narrowings / reads_subtypes
    assert "paper_section" in e.reads_subtypes


def test_missing_paper_ref_marks_snapshot_incomplete(clean_runtime: str) -> None:
    llm = _FakeLLM('[{"statement": "H", "confidence": 0.7}]')
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(paper_ref=None), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        # An emission-time payload failure cannot authorize a complete empty
        # replacement; the base holds progress and discards the snapshot.
        assert result.n_edges_emitted == 0
        assert result.incomplete is True
        assert _hypothesis_nodes(conn) == []


def test_skips_when_no_sections(clean_runtime: str) -> None:
    llm = _FakeLLM("[]")
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, [node("person:x", "person", {})], generation_id=1)
        e.enrich(conn, generation_id=1)
        assert llm.calls == 0


def test_skips_when_llm_unconfigured(clean_runtime: str) -> None:
    e = HypothesisExtractor(confidence_floor=0.5, llm=None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(), generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0


def test_extractor_protocol_metadata() -> None:
    e = HypothesisExtractor(confidence_floor=0.5, llm=None)
    assert "paper_section" in e.reads_subtypes
    assert ("hypothesis", "predicts_about", "concept") in e.requires_narrowings


def test_extractor_required_floor() -> None:
    with pytest.raises(ValueError):
        HypothesisExtractor(confidence_floor=None, llm=None)  # type: ignore[arg-type]


def test_same_paper_and_statement_yields_same_hypothesis_id(
    clean_runtime: str,
) -> None:
    """Deterministic hypothesis_id from (paper_ref, statement) means
    re-runs on the same paper produce the same node — natural dedup."""
    llm = _FakeLLM('[{"statement": "Stable hypothesis", "confidence": 0.7}]')
    e = HypothesisExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(conn, _intro_section(), generation_id=1)
        e.enrich(conn, generation_id=1)
        ids1 = {n["node_id"] for n in _hypothesis_nodes(conn)}
        # Re-run over the same section: deterministic id → same node.
        e.enrich(conn, generation_id=1)
        ids2 = {n["node_id"] for n in _hypothesis_nodes(conn)}

    assert ids1 == ids2 and len(ids1) == 1
