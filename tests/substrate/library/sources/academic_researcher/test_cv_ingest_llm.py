"""Emission test for `CvIngestLlmSource` — proves the heading-based
CV ingester projects TalkEngagement / Committee / Course /
RecommendationLetter candidates from a fixture .tex CV, with
`extracted_model` provenance, without an LLM or the network.

(The class name anticipates LLM enrichment; the shipping fast path
is the heading + itemize parser exercised here.)

Part of `academic-pi-source-tail` task.aps.implement: "the other
named sources discover + project".
"""
from __future__ import annotations

from pathlib import Path

from okg.substrate.library.sources.academic_researcher.cv_ingest_llm import (
    CvIngestLlmSource,
)
from okg.substrate.library.sources.base import NodeFact

_CV = str(Path(__file__).parent / "fixtures" / "sample_cv.tex")


def _nodes_by_subtype() -> dict[str, list[NodeFact]]:
    src = CvIngestLlmSource(cv_path=_CV)
    facts = list(src.run(run_id="r").facts)
    out: dict[str, list[NodeFact]] = {}
    for f in facts:
        if isinstance(f, NodeFact):
            out.setdefault(f.subtype, []).append(f)
    return out


def test_emits_each_cv_section_subtype() -> None:
    by_subtype = _nodes_by_subtype()
    # 2 invited talks, 2 service committees, 1 course, 1 letter.
    assert len(by_subtype.get("talk_engagement", [])) == 2
    assert len(by_subtype.get("committee", [])) == 2
    assert len(by_subtype.get("course", [])) == 1
    assert len(by_subtype.get("recommendation_letter", [])) == 1


def test_candidates_are_extracted_model_provenance() -> None:
    by_subtype = _nodes_by_subtype()
    every = [n for nodes in by_subtype.values() for n in nodes]
    assert every, "expected CV candidates"
    assert all(n.attrs.get("_provenance") == "extracted_model" for n in every)
    # A talk title round-trips from the fixture.
    talks = by_subtype["talk_engagement"]
    assert any("Scaling Graph Learning" in t.attrs["title"] for t in talks)


def test_run_skips_when_cv_missing(tmp_path: Path) -> None:
    src = CvIngestLlmSource(cv_path=str(tmp_path / "nope.tex"))
    assert list(src.run(run_id="r").facts) == []
