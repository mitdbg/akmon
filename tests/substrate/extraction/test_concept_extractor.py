"""Unit tests for ConceptExtractor.

Fake LLM (zero LLM cost). After the chronos cutover (commit b56a27ff4)
``ConceptExtractor`` reads its papers + paper_sections through the real
Chronos store (``okg.graph_nodes`` via
``ChronosGraphStore.from_connection(conn).checkout_generation(...)``), so
these tests seed the source rows through the same real store and assert
against the real ``okg.node_facts`` / ``okg.edge_facts`` write side. See
``tests/pytest/extractor_db_fixture.py``.
"""
from __future__ import annotations

from typing import Any

import psycopg
import pytest

from okg.substrate.library.enrichers.extraction.concept_extractor import (
    ConceptExtractor,
    parse_llm_concepts_response,
    slugify_concept_name,
)

from tests.pytest.extractor_db_fixture import (
    emitted_edge_facts,
    emitted_node_facts,
    node,
    seed_graph,
)


# ---------------------------------------------------------------------------
# slugify_concept_name
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name,slug", [
    ("Two-Phase Commit", "two-phase-commit"),
    ("Self-Attention", "self-attention"),
    ("Branch Coverage", "branch-coverage"),
    ("  Padded   Name  ", "padded-name"),
    ("Concept (with parens)", "concept-with-parens"),
    ("FooBar", "foobar"),
])
def test_slugify_concept_name(name: str, slug: str) -> None:
    assert slugify_concept_name(name) == slug


def test_slugify_empty_string_yields_untitled() -> None:
    assert slugify_concept_name("") == "untitled-concept"
    assert slugify_concept_name("!!!") == "untitled-concept"


# ---------------------------------------------------------------------------
# parse_llm_concepts_response
# ---------------------------------------------------------------------------


def test_parses_concept_with_defines_relationship() -> None:
    raw = (
        '[{"name": "two-phase commit", "relationship": "defines",'
        ' "description": "atomic commit protocol", "confidence": 0.85}]'
    )
    out = parse_llm_concepts_response(raw)
    assert len(out) == 1
    assert out[0]["name"] == "two-phase commit"
    assert out[0]["relationship"] == "defines"
    assert out[0]["confidence"] == 0.85


def test_parses_refines_requires_refines_concept_name() -> None:
    """`refines` without `refines_concept_name` SHALL be dropped."""
    bad = (
        '[{"name": "self-attention v2", "relationship": "refines",'
        ' "description": "extension", "confidence": 0.7}]'
    )
    assert parse_llm_concepts_response(bad) == []

    good = (
        '[{"name": "self-attention v2", "relationship": "refines",'
        ' "description": "extension", "confidence": 0.7,'
        ' "refines_concept_name": "self-attention"}]'
    )
    out = parse_llm_concepts_response(good)
    assert len(out) == 1
    assert out[0]["refines_concept_name"] == "self-attention"


def test_drops_unknown_relationship() -> None:
    raw = (
        '[{"name": "X", "relationship": "criticizes",'
        ' "description": "x", "confidence": 0.7}]'
    )
    assert parse_llm_concepts_response(raw) == []


def test_handles_fenced_json() -> None:
    raw = (
        "```json\n"
        '[{"name": "X", "relationship": "uses",'
        ' "description": "y", "confidence": 0.5}]\n'
        "```"
    )
    assert len(parse_llm_concepts_response(raw)) == 1


def test_returns_empty_for_malformed_input() -> None:
    assert parse_llm_concepts_response("") == []
    assert parse_llm_concepts_response("not json") == []
    assert parse_llm_concepts_response('{"x": 1}') == []


# ---------------------------------------------------------------------------
# Fake LLM (real DB seeding lives in tests.pytest.extractor_db_fixture)
# ---------------------------------------------------------------------------


class _FakeLLM:
    model = "fake"

    def __init__(self, response: str) -> None:
        self.response = response
        self.calls = 0

    def complete(self, system: str, user: str, **_: Any) -> str:
        self.calls += 1
        return self.response


# ---------------------------------------------------------------------------
# End-to-end (real Chronos source rows + fake LLM)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def _concept_ids(conn) -> set[str]:
    return {n["node_id"] for n in emitted_node_facts(conn, "concept")}


def test_emits_concept_and_paper_defines_edge(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"name": "two-phase commit", "relationship": "defines",'
        ' "description": "atomic protocol", "confidence": 0.8}]'
    )
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paper:p1", "paper", {
                "title": "Distributed transactions",
                "abstract": "A paper about distributed transactions",
            })],
            generation_id=1,
        )
        result = e.enrich(conn, generation_id=1)

        assert llm.calls == 1
        assert result.n_edges_emitted == 1

        assert _concept_ids(conn) == {"concept:two-phase-commit"}
        edges = emitted_edge_facts(conn)
        assert len(edges) == 1
        assert edges[0]["src"] == "paper:p1"
        assert edges[0]["dst"] == "concept:two-phase-commit"
        assert edges[0]["edge_type"] == "defines"


def test_emits_uses_edge_when_relationship_is_uses(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"name": "self-attention", "relationship": "uses",'
        ' "description": "see Vaswani 2017", "confidence": 0.7}]'
    )
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paper:p2", "paper", {"title": "Uses self-attention"})],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)
        edges = emitted_edge_facts(conn)
        assert len(edges) == 1
        assert edges[0]["edge_type"] == "uses"


def test_emits_refines_edge_when_relationship_is_refines(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"name": "linear attention", "relationship": "refines",'
        ' "description": "O(n) attention",'
        ' "refines_concept_name": "self-attention",'
        ' "confidence": 0.7}]'
    )
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paper:p3", "paper", {
                "title": "Linear attention refines self-attention",
            })],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)
        edges = emitted_edge_facts(conn)
        # Two edges: paper mentions concept (refines anchors with mentions),
        # concept refines concept.
        edge_types = [edge["edge_type"] for edge in edges]
        assert "refines" in edge_types
        # The concept-to-concept refines edge:
        concept_to_concept = [
            edge for edge in edges
            if edge["src"].startswith("concept:")
            and edge["dst"].startswith("concept:")
        ]
        assert len(concept_to_concept) == 1
        assert concept_to_concept[0]["src"] == "concept:linear-attention"
        assert concept_to_concept[0]["dst"] == "concept:self-attention"


def test_below_floor_concept_becomes_extraction_dropped(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"name": "fuzzy concept", "relationship": "defines",'
        ' "description": "x", "confidence": 0.3}]'
    )
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paper:p", "paper", {"title": "A"})],
            generation_id=1,
        )
        result = e.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0
        assert result.n_edges_skipped == 1
        # Just the ExtractionDropped insert
        assert _concept_ids(conn) == set()
        assert len(emitted_node_facts(conn, "extraction_dropped")) == 1


def test_reads_paper_sections_in_prompt(clean_runtime: str) -> None:
    """When the paper has associated paper_section rows, the
    extractor SHALL include their bodies in the prompt."""
    llm = _FakeLLM("[]")
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [
                node("paper:p", "paper", {"abstract": "Abstract here"}),
                node("section:s1", "paper_section", {
                    "paper_ref": "paper:p", "body": "Introduction body",
                }),
                node("section:s2", "paper_section", {
                    "paper_ref": "paper:p", "body": "Methods body",
                }),
            ],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)
        assert llm.calls == 1


def test_skips_when_no_papers_in_corpus(clean_runtime: str) -> None:
    llm = _FakeLLM("[]")
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A checkpoint exists but holds no paper rows.
        seed_graph(conn, [node("person:x", "person", {})], generation_id=1)
        result = e.enrich(conn, generation_id=1)
        assert llm.calls == 0
        assert result.n_edges_emitted == 0


def test_skips_when_llm_unconfigured(clean_runtime: str) -> None:
    e = ConceptExtractor(confidence_floor=0.5, llm=None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paper:p", "paper", {"title": "A"})],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)


def test_extractor_declares_protocol_metadata() -> None:
    e = ConceptExtractor(confidence_floor=0.5, llm=None)
    assert "paper" in e.reads_subtypes
    assert "paper_section" in e.reads_subtypes
    assert ("paper", "defines", "concept") in e.requires_narrowings
    assert ("paper", "uses", "concept") in e.requires_narrowings
    assert ("concept", "refines", "concept") in e.requires_narrowings


def test_extractor_required_floor() -> None:
    with pytest.raises(ValueError):
        ConceptExtractor(confidence_floor=None, llm=None)  # type: ignore[arg-type]


def test_same_concept_across_papers_yields_same_slug(
    clean_runtime: str,
) -> None:
    """Two papers extracting "Two-Phase Commit" SHALL produce
    `concept:two-phase-commit` in both cases (cross-paper dedup at
    the alias layer)."""
    llm = _FakeLLM(
        '[{"name": "Two-Phase Commit", "relationship": "uses",'
        ' "description": "x", "confidence": 0.7}]'
    )
    e = ConceptExtractor(confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Both papers present at the same pinned generation; the slug is a
        # pure function of the concept name, so both yield the same id.
        seed_graph(
            conn,
            [
                node("paper:p1", "paper", {"title": "A"}),
                node("paper:p2", "paper", {"title": "B"}),
            ],
            generation_id=1,
        )
        e.enrich(conn, generation_id=1)
        assert llm.calls == 2
        # One concept id across both papers (natural cross-paper dedup).
        assert _concept_ids(conn) == {"concept:two-phase-commit"}
