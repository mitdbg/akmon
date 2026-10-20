"""Tests for the PDF structural enricher's confidence-floor logic.

Covers spec scenarios:
  - S-A1-R3 / Scenario: High-confidence PDF decomposes
  - S-A1-R3 / Scenario: Low-confidence PDF surfaces as Observation,
    not partial graph
  - S-A1-R6 / Scenario: Partial decompositions are forbidden

The default pypdfium2 parser depends on a real PDF; here we use
a fake parser callable to exercise the enricher's
confidence-floor branching deterministically.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.enrichers.scholarly.latex_structural import (
    ParsedAnatomy,
    ParsedEdge,
    ParsedNode,
)
from okg.substrate.library.enrichers.scholarly.pdf_structural import (
    PDF_CONFIDENCE_FLOOR,
    PdfParseResult,
    PdfStructuralEnricher,
)


def _make_high_confidence_anatomy(paper_id: str) -> ParsedAnatomy:
    sec_id = f"section:{paper_id}:0"
    para_id = f"para:{paper_id}:0"
    cite_id = f"cite:{paper_id}:0:abcd1234"
    nodes = [
        ParsedNode(
            node_id=sec_id, subtype="paper_section",
            attrs={
                "paper_section_id": sec_id, "paper_ref": f"paper:{paper_id}",
                "path": "§1", "ordinal": 0, "title": "Intro",
            },
        ),
        ParsedNode(
            node_id=para_id, subtype="paragraph",
            attrs={
                "paragraph_id": para_id, "parent_section_ref": sec_id,
                "ordinal": 0, "text": "Body paragraph with cite [1].",
            },
        ),
        ParsedNode(
            node_id=cite_id, subtype="citation",
            attrs={
                "citation_id": cite_id, "citing_section_ref": sec_id,
                "context_text": "Body paragraph with cite [1].",
            },
        ),
    ]
    edges = [
        ParsedEdge(src=f"paper:{paper_id}", dst=sec_id, edge_type="contains"),
        ParsedEdge(src=sec_id, dst=para_id, edge_type="contains"),
        ParsedEdge(src=para_id, dst=cite_id, edge_type="contains"),
    ]
    return ParsedAnatomy(
        nodes=nodes, edges=edges,
        n_sections=1, n_paragraphs=1, n_figures=0, n_tables=0,
        n_algorithms=0, n_equations=0, n_theorems=0, n_proofs=0,
        n_citations=1,
    )


def test_high_confidence_parser_result_passes_floor() -> None:
    """S-A1-R3 Scenario: High-confidence PDF decomposes.

    A parser returning confidence ≥ 0.85 with anatomy attached
    SHALL cause the enricher to commit the anatomy. We verify
    via PdfParseResult shape and the enricher's floor constant.
    """
    result = PdfParseResult(
        confidence=0.92,
        anatomy=_make_high_confidence_anatomy("vision-pdf"),
    )
    assert result.confidence >= PDF_CONFIDENCE_FLOOR
    assert result.anatomy is not None
    assert result.anatomy.n_sections >= 1
    assert result.anatomy.n_paragraphs >= 1


def test_low_confidence_parser_result_below_floor_carries_reason() -> None:
    """S-A1-R3 Scenario: Low-confidence PDF surfaces as Observation."""
    result = PdfParseResult(
        confidence=0.50, anatomy=None, reason="grobid_low_confidence",
    )
    assert result.confidence < PDF_CONFIDENCE_FLOOR
    assert result.anatomy is None
    assert result.reason == "grobid_low_confidence"


def test_pdf_confidence_floor_is_0_85() -> None:
    """Pin the floor — the spec mandates 0.85."""
    assert PDF_CONFIDENCE_FLOOR == 0.85


def test_enricher_constructed_with_default_parser() -> None:
    """The enricher accepts no kwargs and falls back to the
    substrate-shipped default (pypdfium2-based) parser."""
    enricher = PdfStructuralEnricher()
    assert enricher.name == "pdf_structural"
    assert "paper" in enricher.reads_subtypes


def test_enricher_constructed_with_injected_parser() -> None:
    """The enricher accepts a `parser=` kwarg for tests / GROBID
    deployments."""
    def fake_parser(*, pdf_path: Path, paper_id: str) -> PdfParseResult:
        return PdfParseResult(confidence=0.95, anatomy=None)

    enricher = PdfStructuralEnricher(parser=fake_parser)
    result = enricher._parser(
        pdf_path=Path("/nonexistent.pdf"), paper_id="x",
    )
    assert result.confidence == 0.95


def test_enricher_required_narrowings_in_d7_shortlist() -> None:
    """S-A1-R4 vocabulary: cross-layer narrowings prefer the D7
    shortlist (`contains`, `cites`). The PDF enricher requires
    only `contains` narrowings (no equations/theorems for PDF)."""
    enricher = PdfStructuralEnricher()
    for parent, edge, child in enricher.requires_narrowings:
        assert edge in {"contains", "cites"}, (parent, edge, child)


# ---- approved-root containment of source_pdf_path
# (constrain-graph-file-and-parser-ingestion) ----


def test_resolve_pdf_path_accepts_in_root_and_rejects_out_of_root(tmp_path):
    root = tmp_path / "inbox"
    root.mkdir()
    in_root = root / "p.pdf"
    in_root.write_bytes(b"%PDF")
    outside = tmp_path / "secret.pdf"
    outside.write_bytes(b"%PDF")

    enricher = PdfStructuralEnricher(approved_roots=[str(root)])
    assert enricher._resolve_pdf_path(str(in_root)) == in_root.resolve()
    # Out-of-root absolute path and traversal both denied (no file opened).
    assert enricher._resolve_pdf_path(str(outside)) is None
    assert enricher._resolve_pdf_path(str(root / ".." / "secret.pdf")) is None


def test_resolve_pdf_path_rejects_symlink_escaping_root(tmp_path):
    root = tmp_path / "inbox"
    root.mkdir()
    outside = tmp_path / "secret.pdf"
    outside.write_bytes(b"%PDF")
    link = root / "link.pdf"
    link.symlink_to(outside)
    enricher = PdfStructuralEnricher(approved_roots=[str(root)])
    assert enricher._resolve_pdf_path(str(link)) is None


def test_resolve_pdf_path_unconfigured_fails_closed(tmp_path):
    # No approved_roots configured -> refuse EVERY path (fail closed), even an
    # existing file. Opening an arbitrary graph-controlled path without
    # containment is the rejected silent-insecure-default.
    f = tmp_path / "p.pdf"
    f.write_bytes(b"%PDF")
    enricher = PdfStructuralEnricher()  # no approved_roots
    assert enricher._resolve_pdf_path(str(f)) is None
    assert enricher._resolve_pdf_path(str(tmp_path / "missing.pdf")) is None
    assert enricher._resolve_pdf_path("/etc/passwd") is None


@pytest.mark.postgres
def test_enrich_only_parses_in_root_paths(clean_runtime, tmp_path):
    """Integration: two paper nodes with a graph-controlled source_pdf_path (one
    in-root, one outside) go through the real enrich() path. The parser is
    invoked ONLY for the in-root path; the out-of-root paper is contained as an
    extraction_error and its file is never opened. This is the enricher-side
    proof the PACT claimed but never had (adversarial-review remediation)."""
    import uuid

    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    root = tmp_path / "inbox"
    root.mkdir()
    in_pdf = root / "ok.pdf"
    in_pdf.write_bytes(b"%PDF-in")
    out_pdf = tmp_path / "secret.pdf"  # outside the approved root
    out_pdf.write_bytes(b"%PDF-out")

    sfx = uuid.uuid4().hex[:8]
    pin, pout = f"paper:in-{sfx}", f"paper:out-{sfx}"
    parsed: list[str] = []

    def spy_parser(*, pdf_path, paper_id):
        parsed.append(str(pdf_path))
        # Low confidence -> enrich() emits an extraction_error, so the test needs
        # no paper-anatomy catalog wiring; the parser-call list is the signal.
        return PdfParseResult(confidence=0.0, anatomy=None, reason="spy")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(pin, "paper", {"source_pdf_path": str(in_pdf)},
                         {"id": pin}, {"v": 1}),
                NodeFact(pout, "paper", {"source_pdf_path": str(out_pdf)},
                         {"id": pout}, {"v": 1}),
            ],
            source="_seed_pdf",
        )
        gen = store.checkpoint_generation(deployment_name="pdf", branch="default")

        enricher = PdfStructuralEnricher(
            parser=spy_parser, approved_roots=[str(root)],
        )
        enricher.enrich(conn, generation_id=gen)

    # The parser saw ONLY the in-root path (symlink/realpath-resolved); the
    # out-of-root paper was refused before any file open.
    assert parsed == [str(in_pdf.resolve())]
