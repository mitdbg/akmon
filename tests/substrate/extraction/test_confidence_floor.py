"""Unit tests for the confidence-floor convention (Phase 2 of impl-A2).

Per `extend-substrate-extraction-with-claim-evidence-extractors`
Requirement "Confidence-Floor Convention" + design.md D3:

- ``LLMExtractor.__init__`` SHALL raise ``ValueError`` for missing /
  out-of-range floor (mirrors ``SemanticConceptLinker.min_similarity``).
- The floor predicate SHALL route candidates ``>= floor`` to the
  typed-node emit path and ``< floor`` to ``ExtractionDropped``.
- ``dropped_id`` SHALL be deterministic from
  ``(extractor, source_node_ref, sha256(candidate_text))``.
- ``ExtractionDropped`` SHALL NOT be emitted when the typed node
  is; the two paths are mutually exclusive per candidate.
- Above-floor count SHALL flow to ``EnrichResult.n_edges_emitted``;
  below-floor count SHALL flow to ``n_edges_skipped``.

Uses a mock psycopg connection (no real DB) and a fake LLMClient
(no LLM calls). Zero cost.
"""
from __future__ import annotations

from typing import Any, Iterable

import pytest

from okg.substrate.library.enrichers.extraction.base import (
    ExtractionCandidate,
    LLMExtractor,
    make_dedupe_key,
    make_dropped_id,
)


# ---------------------------------------------------------------------------
# Mock psycopg primitives (no real DB; record SQL + params)
# ---------------------------------------------------------------------------


class _Cursor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []
        self.rowcount = 0

    def execute(self, sql: str, params: Any = None) -> None:
        self.calls.append((sql, params))
        self.rowcount = 1

    def fetchall(self) -> list[tuple[Any, ...]]:
        return []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_: Any) -> None:
        pass


class _Conn:
    def __init__(self) -> None:
        self.cursors: list[_Cursor] = []

    def cursor(self) -> _Cursor:
        c = _Cursor()
        self.cursors.append(c)
        return c

    def all_inserts(self) -> list[tuple[str, Any]]:
        out: list[tuple[str, Any]] = []
        for c in self.cursors:
            for sql, params in c.calls:
                if sql.strip().upper().startswith("INSERT"):
                    out.append((sql, params))
        return out


# ---------------------------------------------------------------------------
# Test subclass — exercises the base without touching real LLMs / DB
# ---------------------------------------------------------------------------


class _StubExtractor(LLMExtractor):
    name = "stub_extractor"
    emits_typed_subtype = "claim"
    # Node provenance rides the base class: LLMExtractor._emit_dropped passes
    # derived_from via derivation_refs, and this fixture's emit_typed_node
    # records to a list rather than writing facts.
    declares_node_provenance = True

    def __init__(self, *, confidence_floor: float, candidates: list[ExtractionCandidate]) -> None:
        super().__init__(confidence_floor=confidence_floor)
        self._candidates = candidates
        self.emitted_typed: list[ExtractionCandidate] = []

    def extract_candidates(
        self, conn, generation_id: int,
    ) -> Iterable[ExtractionCandidate]:
        yield from self._candidates

    def emit_typed_node(
        self, conn, candidate, *, generation_id, run_id, observed_at,
    ) -> int:
        self.emitted_typed.append(candidate)
        # The real subclasses INSERT to okg.node_facts / okg.edge_facts
        # and return the edge count; the stub just records the call.
        return 1  # pretend one edge emitted


# ---------------------------------------------------------------------------
# Constructor floor validation
# ---------------------------------------------------------------------------


def test_missing_floor_raises() -> None:
    with pytest.raises(ValueError, match="confidence_floor is required"):
        _StubExtractor(confidence_floor=None, candidates=[])


def test_negative_floor_raises() -> None:
    with pytest.raises(ValueError, match="out of range"):
        _StubExtractor(confidence_floor=-0.1, candidates=[])


def test_floor_above_one_raises() -> None:
    with pytest.raises(ValueError, match="out of range"):
        _StubExtractor(confidence_floor=1.5, candidates=[])


def test_non_numeric_floor_raises() -> None:
    with pytest.raises(ValueError, match="must be a number"):
        _StubExtractor(confidence_floor="0.5", candidates=[])  # type: ignore[arg-type]


def test_floor_zero_accepted() -> None:
    e = _StubExtractor(confidence_floor=0.0, candidates=[])
    assert e.confidence_floor == 0.0


def test_floor_one_accepted() -> None:
    e = _StubExtractor(confidence_floor=1.0, candidates=[])
    assert e.confidence_floor == 1.0


def test_floor_int_coerced_to_float() -> None:
    e = _StubExtractor(confidence_floor=1, candidates=[])
    assert e.confidence_floor == 1.0


# ---------------------------------------------------------------------------
# Floor predicate
# ---------------------------------------------------------------------------


def test_above_floor_predicate_strict_at_floor_value() -> None:
    """Per spec: `confidence >= floor` admits; equality lands as
    typed node, not dropped."""
    e = _StubExtractor(confidence_floor=0.6, candidates=[])
    assert e.above_floor(0.6) is True
    assert e.above_floor(0.7) is True
    assert e.above_floor(0.59) is False


# ---------------------------------------------------------------------------
# enrich() routing
# ---------------------------------------------------------------------------


def test_enrich_routes_above_floor_to_emit_typed() -> None:
    cands = [
        ExtractionCandidate(
            candidate_text="model achieves SOTA",
            confidence=0.75,
            source_node_ref="paragraph:p1",
        ),
    ]
    e = _StubExtractor(confidence_floor=0.6, candidates=cands)
    conn = _Conn()
    result = e.enrich(conn, generation_id=42)
    # Subclass got the candidate via emit_typed_node hook
    assert len(e.emitted_typed) == 1
    # No ExtractionDropped insert (zero INSERT calls in this path)
    inserts = conn.all_inserts()
    assert len(inserts) == 0, (
        f"above-floor candidate SHALL NOT emit ExtractionDropped; "
        f"got inserts: {inserts}"
    )
    assert result.n_edges_emitted == 1
    assert result.n_edges_skipped == 0


def test_enrich_routes_below_floor_to_emit_dropped() -> None:
    cands = [
        ExtractionCandidate(
            candidate_text="vague candidate",
            confidence=0.4,
            source_node_ref="paragraph:p2",
        ),
    ]
    e = _StubExtractor(confidence_floor=0.6, candidates=cands)
    conn = _Conn()
    result = e.enrich(conn, generation_id=42)
    # Subclass did NOT see this candidate via emit_typed_node
    assert len(e.emitted_typed) == 0
    # One INSERT into okg.node_facts for the ExtractionDropped
    inserts = conn.all_inserts()
    assert len(inserts) == 1
    sql, params = inserts[0]
    assert "okg.node_facts" in sql
    assert params["subtype"] == "extraction_dropped"
    # Attrs payload carries the candidate text + confidence
    attrs = params["attrs"].obj if hasattr(params["attrs"], "obj") else params["attrs"]
    assert attrs["candidate_text"] == "vague candidate"
    assert attrs["extractor_confidence"] == 0.4
    assert attrs["extractor_floor_at_emit"] == 0.6
    assert attrs["operator_disposition"] == "pending"
    assert "below 0.6 floor" in attrs["reason"].lower() or attrs["reason"]
    # EnrichResult accounting
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 1


def test_below_floor_entity_resolution_retry_counts_candidate_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A diagnostic retry must not double-count its below-floor candidate."""
    from okg.substrate.entity_resolution import EntityResolutionRejected

    candidate = ExtractionCandidate(
        candidate_text="vague candidate",
        confidence=0.4,
        source_node_ref="paragraph:p2",
    )
    extractor = _StubExtractor(confidence_floor=0.6, candidates=[candidate])
    emit_calls = 0

    def retrying_emit_dropped(*_args: Any, **_kwargs: Any) -> str:
        nonlocal emit_calls
        emit_calls += 1
        if emit_calls == 1:
            raise EntityResolutionRejected("reject typed identity")
        return "extraction_dropped:retry"

    monkeypatch.setattr(extractor, "emit_dropped", retrying_emit_dropped)
    result = extractor.enrich(_Conn(), generation_id=42)

    assert emit_calls == 2
    assert result.n_edges_skipped == 1


def test_above_floor_entity_resolution_fallback_counts_candidate_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An above-floor identity rejection becomes one dropped candidate."""
    from okg.substrate.entity_resolution import EntityResolutionRejected

    candidate = ExtractionCandidate(
        candidate_text="typed candidate",
        confidence=0.8,
        source_node_ref="paragraph:p1",
    )
    extractor = _StubExtractor(confidence_floor=0.6, candidates=[candidate])
    monkeypatch.setattr(
        extractor,
        "emit_typed_node",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            EntityResolutionRejected("reject typed identity")
        ),
    )
    emit_calls = 0

    def emit_dropped(*_args: Any, **_kwargs: Any) -> str:
        nonlocal emit_calls
        emit_calls += 1
        return "extraction_dropped:fallback"

    monkeypatch.setattr(extractor, "emit_dropped", emit_dropped)
    result = extractor.enrich(_Conn(), generation_id=42)

    assert emit_calls == 1
    assert result.n_edges_emitted == 0
    assert result.n_edges_skipped == 1


def test_enrich_mixed_batch() -> None:
    cands = [
        ExtractionCandidate("above", 0.8, "paragraph:1"),
        ExtractionCandidate("below", 0.3, "paragraph:2"),
        ExtractionCandidate("above too", 0.65, "paragraph:3"),
        ExtractionCandidate("right at floor", 0.6, "paragraph:4"),
    ]
    e = _StubExtractor(confidence_floor=0.6, candidates=cands)
    conn = _Conn()
    result = e.enrich(conn, generation_id=99)
    # 3 above floor (0.8, 0.65, 0.6); 1 below (0.3)
    assert len(e.emitted_typed) == 3
    inserts = conn.all_inserts()
    assert len(inserts) == 1
    assert result.n_edges_emitted == 3
    assert result.n_edges_skipped == 1


# ---------------------------------------------------------------------------
# Deterministic dropped_id (spec.md ExtractionDropped Scenario)
# ---------------------------------------------------------------------------


def test_dropped_id_is_deterministic_across_calls() -> None:
    a = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="the same text",
    )
    b = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="the same text",
    )
    assert a == b


def test_dropped_id_changes_with_extractor() -> None:
    a = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="text",
    )
    b = make_dropped_id(
        extractor="concept_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="text",
    )
    assert a != b


def test_dropped_id_changes_with_source_node() -> None:
    a = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="text",
    )
    b = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p2",
        candidate_text="text",
    )
    assert a != b


def test_dropped_id_changes_with_candidate_text() -> None:
    a = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="A",
    )
    b = make_dropped_id(
        extractor="claim_extractor",
        source_node_ref="paragraph:p1",
        candidate_text="B",
    )
    assert a != b


def test_dropped_id_prefix_is_dropped() -> None:
    d = make_dropped_id(
        extractor="x", source_node_ref="y", candidate_text="z",
    )
    assert d.startswith("dropped:")
    # 16 hex chars after the prefix
    assert len(d) == len("dropped:") + 16


def test_make_dedupe_key_is_stable() -> None:
    a = make_dedupe_key("a", "b", "c")
    b = make_dedupe_key("a", "b", "c")
    assert a == b
    assert make_dedupe_key("a", "b") != make_dedupe_key("a", "b", "c")


# ---------------------------------------------------------------------------
# Exception-tolerance in enrich loop
# ---------------------------------------------------------------------------


class _RaisingExtractor(LLMExtractor):
    name = "raising_extractor"

    def __init__(self) -> None:
        super().__init__(confidence_floor=0.5)

    def extract_candidates(self, conn, generation_id):
        yield ExtractionCandidate("good", 0.7, "src:1")
        yield ExtractionCandidate("bad", 0.8, "src:2")
        yield ExtractionCandidate("good again", 0.75, "src:3")

    def emit_typed_node(self, conn, candidate, *, generation_id, run_id, observed_at):
        if "bad" in candidate.candidate_text:
            raise RuntimeError("emit failure simulated")
        return 1


def test_enrich_continues_past_per_candidate_failures() -> None:
    """One candidate raising in emit_typed_node SHALL NOT abort the
    whole enricher; the surviving candidates SHALL emit and the
    failure SHALL surface in EnrichResult.error."""
    conn = _Conn()
    e = _RaisingExtractor()
    result = e.enrich(conn, generation_id=1)
    assert result.n_edges_emitted == 2  # two surviving emit calls
    assert result.error is not None
    assert "failed emission" in result.error


# ---------------------------------------------------------------------------
# NUL admission on the below-floor twin (PACT
# sanitize-fact-text-at-admission, adversarial-review follow-up):
# emit_dropped routes through the shared emitter, so raw
# candidate_text scrubs and a NUL identity rejects loudly.
# ---------------------------------------------------------------------------


_NUL = "\x00"
_RC = "�"


def test_emit_dropped_scrubs_nul_candidate_text() -> None:
    """Below-floor candidate whose text carries NUL (the #638 shape)
    lands as an ExtractionDropped with the value scrubbed — pre-fix
    this INSERT carried raw NUL and aborted the publish with
    UntranslatableCharacter."""
    cands = [
        ExtractionCandidate(
            candidate_text=f"vague{_NUL}candidate",
            confidence=0.4,
            source_node_ref="paragraph:p2",
        ),
    ]
    e = _StubExtractor(confidence_floor=0.6, candidates=cands)
    conn = _Conn()
    result = e.enrich(conn, generation_id=42)
    inserts = conn.all_inserts()
    assert len(inserts) == 1
    _sql, params = inserts[0]
    attrs = params["attrs"].obj if hasattr(params["attrs"], "obj") else params["attrs"]
    assert attrs["candidate_text"] == f"vague{_RC}candidate"
    assert _NUL not in str(attrs)
    assert result.n_edges_skipped == 1


def test_emit_dropped_rejects_nul_identity_from_source_node_ref(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A NUL in source_node_ref is an identity NUL — it lands in the
    record's source_record_id (dropped_id itself hashes its inputs
    and stays hex-clean): the record is rejected loudly (no insert)
    while the enricher run continues."""
    import logging

    cands = [
        ExtractionCandidate(
            candidate_text="fine text",
            confidence=0.4,
            source_node_ref=f"paragraph:{_NUL}p2",
        ),
    ]
    e = _StubExtractor(confidence_floor=0.6, candidates=cands)
    conn = _Conn()
    logger_name = "okg.substrate.library.enrichers._fact_emitters"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        result = e.enrich(conn, generation_id=42)
    assert conn.all_inserts() == []
    # The candidate still counted as routed-below-floor; the loud
    # signal for the missing record is the emitter warning.
    assert result.n_edges_skipped == 1
    warning_text = " ".join(r.getMessage() for r in caplog.records)
    assert "NUL (U+0000) in identity field(s)" in warning_text
    assert "stub_extractor" in warning_text
