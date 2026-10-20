"""Unit tests for ClaimExtractor.

Uses a FakeLLMClient (returns canned JSON) — zero LLM cost. The
extractor's source reads go through the real Chronos store after the
cutover (commit b56a27ff4: ``ClaimExtractor._fetch_source_rows`` reads
``okg.graph_nodes`` via ``ChronosGraphStore.from_connection(conn).
checkout_generation(...)``), so these tests seed the paragraph / theorem
/ table source rows through the same real store (``apply_facts`` +
``checkpoint_generation``) and assert against the real ``okg.node_facts``
/ ``okg.edge_facts`` write side. See ``tests/pytest/extractor_db_fixture.py``.

Coverage:
- LLM response parser handles bare JSON, fenced JSON, malformed input.
- Above-floor candidates emit Claim NodeFact + Paragraph contains Claim edge.
- Below-floor candidates land as ExtractionDropped (via base class).
- Source body lookup: paragraph → text, theorem → statement, table → caption.
- Per-source edge type: paragraph → contains, theorem → states, table → states.
- Missing LLM → zero candidates (no crash).
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.library.enrichers.extraction.claim_extractor import (
    ClaimExtractor,
    parse_llm_claims_response,
)

from tests.pytest.extractor_db_fixture import (
    emitted_edge_facts,
    emitted_node_facts,
    node,
    seed_graph,
)


# ---------------------------------------------------------------------------
# parse_llm_claims_response
# ---------------------------------------------------------------------------


def test_parses_bare_json_array() -> None:
    raw = '[{"text": "X is Y", "kind": "definitional", "confidence": 0.8}]'
    out = parse_llm_claims_response(raw)
    assert len(out) == 1
    assert out[0]["text"] == "X is Y"
    assert out[0]["kind"] == "definitional"
    assert out[0]["confidence"] == 0.8


def test_parses_fenced_json() -> None:
    raw = (
        "```json\n"
        '[{"text": "Z", "kind": "empirical", "confidence": 0.7}]\n'
        "```"
    )
    out = parse_llm_claims_response(raw)
    assert len(out) == 1
    assert out[0]["kind"] == "empirical"


def test_returns_empty_for_malformed_json() -> None:
    assert parse_llm_claims_response("not json") == []


def test_returns_empty_for_non_array() -> None:
    assert parse_llm_claims_response('{"text": "X"}') == []


def test_skips_unknown_kind() -> None:
    raw = (
        '[{"text": "X", "kind": "metaphysical", "confidence": 0.9},'
        ' {"text": "Y", "kind": "empirical", "confidence": 0.7}]'
    )
    out = parse_llm_claims_response(raw)
    assert len(out) == 1
    assert out[0]["text"] == "Y"


def test_skips_out_of_range_confidence() -> None:
    raw = (
        '[{"text": "X", "kind": "empirical", "confidence": 1.5},'
        ' {"text": "Y", "kind": "empirical", "confidence": -0.1},'
        ' {"text": "Z", "kind": "empirical", "confidence": 0.5}]'
    )
    out = parse_llm_claims_response(raw)
    assert [c["text"] for c in out] == ["Z"]


def test_skips_missing_text() -> None:
    raw = '[{"kind": "empirical", "confidence": 0.7}]'
    assert parse_llm_claims_response(raw) == []


def test_empty_input_returns_empty() -> None:
    assert parse_llm_claims_response("") == []
    assert parse_llm_claims_response("   ") == []


def test_returns_empty_array_when_llm_says_no_claims() -> None:
    assert parse_llm_claims_response("[]") == []


# ---------------------------------------------------------------------------
# Fake LLM (real DB seeding lives in tests.pytest.extractor_db_fixture)
# ---------------------------------------------------------------------------


class _FakeLLM:
    """Returns canned JSON regardless of prompt; records call count."""
    model = "fake"

    def __init__(self, response: str) -> None:
        self.response = response
        self.calls = 0

    def complete(
        self, system: str, user: str, *,
        fragment_id: str = "", caller: str = "",
        generation_id: int | None = None,
        outcome_key_digest: str | None = None,
    ) -> str:
        self.calls += 1
        return self.response


# ---------------------------------------------------------------------------
# Extractor end-to-end (real Chronos source rows + fake LLM)
# ---------------------------------------------------------------------------


pytestmark = pytest.mark.postgres


def test_emits_claim_node_and_contains_edge_for_paragraph_input(
    clean_runtime: str,
) -> None:
    """Above-floor paragraph candidate → 1 NodeFact + 1 'contains' edge."""
    llm = _FakeLLM(
        '[{"text": "The model achieves SOTA",'
        ' "kind": "empirical", "confidence": 0.8}]'
    )
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paragraph:p1", "paragraph", {"text": "Body text"})],
            generation_id=10,
        )
        result = extractor.enrich(conn, generation_id=10)

        assert llm.calls == 1
        assert result.n_edges_emitted == 1
        assert result.n_edges_skipped == 0

        # The Claim node
        node_inserts = emitted_node_facts(conn, "claim")
        assert len(node_inserts) == 1
        attrs = node_inserts[0]["attrs"]
        assert attrs["text"] == "The model achieves SOTA"
        # The assertion is also kept in `text_blob` (the schema's un-rolled
        # "richer body"). The projector rebuilds `text` as a searchable rollup
        # of the OTHER searchable fields (kind/tags/text_blob); populating
        # text_blob keeps the assertion present + searchable in the live graph.
        assert attrs["text_blob"] == "The model achieves SOTA"
        assert attrs["kind"] == "empirical"
        assert attrs["provenance"] == "extracted_model"
        assert attrs["extractor_floor_at_emit"] == 0.6
        assert attrs["extractor"] == "claim_extractor"
        assert attrs["confidence"] == 0.8

        # The Paragraph contains Claim edge
        edges = emitted_edge_facts(conn)
        assert len(edges) == 1
        assert edges[0]["src"] == "paragraph:p1"
        assert edges[0]["dst"].startswith("claim:")
        assert edges[0]["edge_type"] == "contains"
        assert edges[0]["confidence"] == pytest.approx(0.8)


def test_emits_states_edge_for_theorem_input(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"text": "Theorem X holds",'
        ' "kind": "theoretical", "confidence": 0.9}]'
    )
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # theorem rows carry the body in the `statement` attr, not `text`.
        seed_graph(
            conn,
            [node("theorem:t1", "theorem", {"statement": "If A then B"})],
            generation_id=10,
        )
        extractor.enrich(conn, generation_id=10)

        edges = emitted_edge_facts(conn)
        assert len(edges) == 1
        assert edges[0]["src"] == "theorem:t1"
        assert edges[0]["edge_type"] == "states"


def test_emits_states_edge_for_table_input(clean_runtime: str) -> None:
    llm = _FakeLLM(
        '[{"text": "Table shows latency",'
        ' "kind": "empirical", "confidence": 0.7}]'
    )
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("table:t1", "table", {"caption": "Latency by model"})],
            generation_id=10,
        )
        extractor.enrich(conn, generation_id=10)

        edges = emitted_edge_facts(conn)
        assert len(edges) == 1
        assert edges[0]["src"] == "table:t1"
        assert edges[0]["edge_type"] == "states"


def test_below_floor_candidate_lands_as_extraction_dropped(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM(
        '[{"text": "uncertain claim",'
        ' "kind": "conjectural", "confidence": 0.3}]'
    )
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paragraph:p1", "paragraph", {"text": "Body"})],
            generation_id=10,
        )
        result = extractor.enrich(conn, generation_id=10)

        assert result.n_edges_emitted == 0
        assert result.n_edges_skipped == 1

        # Just the ExtractionDropped NodeFact, no Claim, no edge.
        assert emitted_node_facts(conn, "claim") == []
        assert emitted_edge_facts(conn) == []
        dropped = emitted_node_facts(conn, "extraction_dropped")
        assert len(dropped) == 1
        attrs = dropped[0]["attrs"]
        assert attrs["candidate_text"] == "uncertain claim"
        assert attrs["extractor_confidence"] == 0.3
        assert attrs["extractor_floor_at_emit"] == 0.6


def test_extractor_skips_when_llm_unconfigured(clean_runtime: str) -> None:
    """When the deployment hasn't passed an LLMClient, the extractor
    SHALL emit zero candidates (with a warning) rather than crash."""
    extractor = ClaimExtractor(confidence_floor=0.6, llm=None)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paragraph:p1", "paragraph", {"text": "Body"})],
            generation_id=1,
        )
        result = extractor.enrich(conn, generation_id=1)
        assert result.n_edges_emitted == 0
        assert result.n_edges_skipped == 0


def test_extractor_skips_source_rows_with_empty_body(
    clean_runtime: str,
) -> None:
    llm = _FakeLLM('[]')
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [
                node("paragraph:empty", "paragraph", {"text": ""}),
                node("paragraph:none", "paragraph", {}),
            ],
            generation_id=1,
        )
        extractor.enrich(conn, generation_id=1)
        # Empty body never reaches the LLM
        assert llm.calls == 0


def test_zero_source_rows_emits_nothing(clean_runtime: str) -> None:
    llm = _FakeLLM('[]')
    extractor = ClaimExtractor(
        confidence_floor=0.6, llm=llm, input_quarantine_max_attempts=3
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Seed an unrelated node so a generation checkpoint exists but no
        # paragraph/theorem/table source rows are present.
        seed_graph(
            conn,
            [node("person:p", "person", {})],
            generation_id=1,
        )
        result = extractor.enrich(conn, generation_id=1)
        assert llm.calls == 0
        assert result.n_edges_emitted == 0


def test_extractor_declares_protocol_metadata() -> None:
    """Per spec.md §Claim Extractor Scenario:
    'reads_subtypes = (paragraph, theorem, table)',
    'emits_edge_types = (contains, states)',
    'requires_narrowings listing (paragraph, contains, claim) and
     (theorem, states, claim)'."""
    e = ClaimExtractor(confidence_floor=0.6, llm=None)
    assert e.reads_subtypes == ("paragraph", "theorem", "table")
    assert e.emits_edge_types == ("contains", "states")
    assert ("paragraph", "contains", "claim") in e.requires_narrowings
    assert ("theorem", "states", "claim") in e.requires_narrowings


def test_extractor_required_confidence_floor() -> None:
    """The extractor inherits the floor validation from LLMExtractor."""
    with pytest.raises(ValueError, match="confidence_floor is required"):
        ClaimExtractor(confidence_floor=None, llm=None)  # type: ignore[arg-type]


def test_extractor_floor_at_emit_per_deployment(clean_runtime: str) -> None:
    """Two deployments with different floors stamp different
    extractor_floor_at_emit on the same above-floor candidate."""
    llm = _FakeLLM(
        '[{"text": "Y", "kind": "empirical", "confidence": 0.75}]'
    )
    e_strict = ClaimExtractor(
        confidence_floor=0.7, llm=llm, input_quarantine_max_attempts=3
    )
    e_loose = ClaimExtractor(
        confidence_floor=0.5, llm=llm, input_quarantine_max_attempts=3
    )

    # Both extractors read the same seeded paragraph at the same pinned
    # generation; the conf-0.75 candidate clears both floors. Each emits a
    # claim NodeFact carrying its own floor — two facts for the same
    # claim_id (op='I'), differing only by extractor_floor_at_emit.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_graph(
            conn,
            [node("paragraph:p", "paragraph", {"text": "Body"})],
            generation_id=1,
        )
        e_strict.enrich(conn, generation_id=1)
        e_loose.enrich(conn, generation_id=1)

        floors = {
            n["attrs"]["extractor_floor_at_emit"]
            for n in emitted_node_facts(conn, "claim")
        }

    assert floors == {0.5, 0.7}
