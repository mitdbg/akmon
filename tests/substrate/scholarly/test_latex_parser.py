"""Tests for the deterministic LaTeX paper-anatomy parser.

Covers spec scenarios:

  - S-A1-R2 / Scenario: LaTeX paper decomposes on successful parse
  - S-A1-R2 / Scenario: Catastrophic LaTeX failure surfaces as error
  - S-A1-R6 / Scenario: All emitted nodes have anatomy attrs (the
    enricher then stamps provenance + extractor_version)

The parser is pure-function — no DB required. The enricher's
DB-side commit path uses the same parser output via
`_commit_anatomy` and is exercised by catalog-load smoke
verification (catalog_version_id ticks, narrowings register).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.enrichers.scholarly.latex_structural import (
    LatexParseError,
    parse_latex,
)


FIXTURES = Path(__file__).parent / "fixtures"


def test_sample_paper_decomposes_with_expected_anatomy() -> None:
    """S-A1-R2 happy path: 4 sections, ≥4 citations, theorem+proof,
    figure, table, algorithm, equation all emit.

    The Citation count is asserted as ≥ N per the spec scenario
    ("the enricher SHALL emit ≥ M Citation nodes"); citations inside
    Proof bodies are not enumerated because the spec's
    `*_contains_citation` narrowings cover paragraph / theorem /
    figure / table only (Proof is not listed)."""
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)

    # 4 \section + 1 \subsection = 5.
    assert anatomy.n_sections == 5, anatomy.n_sections
    # ≥ 5 citations across paragraphs/theorem (proof bodies aren't
    # mined per the spec's narrowing inventory).
    assert anatomy.n_citations >= 5, anatomy.n_citations
    assert anatomy.n_figures == 1
    assert anatomy.n_tables == 1
    assert anatomy.n_algorithms == 1
    assert anatomy.n_equations == 1
    assert anatomy.n_theorems == 1
    assert anatomy.n_proofs == 1
    assert anatomy.n_paragraphs >= 4


def test_sample_paper_node_attrs_minimal_viable() -> None:
    """S-A1-R1 minimal-viable required-attr coverage."""
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)
    by_subtype: dict[str, list] = {}
    for n in anatomy.nodes:
        by_subtype.setdefault(n.subtype, []).append(n)

    for section in by_subtype["paper_section"]:
        assert set(section.attrs) >= {
            "paper_section_id", "paper_ref", "path", "ordinal",
        }
    for para in by_subtype["paragraph"]:
        assert set(para.attrs) >= {
            "paragraph_id", "parent_section_ref", "ordinal", "text",
        }
    for fig in by_subtype["figure"]:
        assert set(fig.attrs) >= {
            "figure_id", "parent_section_ref", "label", "ordinal",
        }
    for tbl in by_subtype["table"]:
        assert set(tbl.attrs) >= {
            "table_id", "parent_section_ref", "label", "ordinal",
        }
    for alg in by_subtype["algorithm"]:
        assert set(alg.attrs) >= {
            "algorithm_id", "parent_section_ref", "label", "ordinal",
        }
    for eqn in by_subtype["equation"]:
        assert set(eqn.attrs) >= {
            "equation_id", "parent_section_ref", "label", "ordinal",
        }
    for thm in by_subtype["theorem"]:
        assert set(thm.attrs) >= {
            "theorem_id", "parent_section_ref", "label", "kind", "statement",
        }
    for prf in by_subtype["proof"]:
        assert set(prf.attrs) >= {
            "proof_id", "proves_theorem_ref", "text",
        }
    for cit in by_subtype["citation"]:
        assert set(cit.attrs) >= {
            "citation_id", "citing_section_ref", "context_text",
        }


def test_section_containment_edges_link_paper_then_recurse() -> None:
    """S-A1-R4 nested-section round-trip: top-level sections attach
    to Paper; subsections attach to their parent section."""
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)
    paper_section_edges = [
        e for e in anatomy.edges
        if e.src == "paper:vision-test" and e.edge_type == "contains"
    ]
    # 4 \section commands → 4 top-level Paper→Section contains edges.
    section_dst_subtypes = {
        e.dst for e in paper_section_edges
        if any(
            n.node_id == e.dst and n.subtype == "paper_section"
            for n in anatomy.nodes
        )
    }
    assert len(section_dst_subtypes) == 4, section_dst_subtypes

    # The \subsection inside Background should attach to Background's
    # section node, not to Paper.
    subsection_node = next(
        (n for n in anatomy.nodes
         if n.subtype == "paper_section"
         and n.attrs.get("title") == "Bitemporal Storage"),
        None,
    )
    assert subsection_node is not None
    assert subsection_node.attrs.get("parent_section_ref")
    # The subsection's parent_section_ref points at a Background
    # section. That parent should exist among the sections.
    parent_ref = subsection_node.attrs["parent_section_ref"]
    parent_node_id = f"section:{parent_ref}"
    parent_node = next(
        (n for n in anatomy.nodes if n.node_id == parent_node_id),
        None,
    )
    assert parent_node is not None
    assert parent_node.attrs["title"] == "Background"


def test_theorem_and_proof_are_paired_via_proves_theorem_ref() -> None:
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)
    thm = next(n for n in anatomy.nodes if n.subtype == "theorem")
    prf = next(n for n in anatomy.nodes if n.subtype == "proof")
    assert prf.attrs["proves_theorem_ref"] == thm.node_id.split(":", 1)[-1]
    # The theorem_proved_by_proof narrowing uses `contains` archetype
    # (Theorem→Proof) per the finding in this child's findings dir.
    pairing_edges = [
        e for e in anatomy.edges
        if e.src == thm.node_id and e.dst == prf.node_id
        and e.edge_type == "contains"
    ]
    assert len(pairing_edges) == 1, pairing_edges


def test_citations_attach_to_their_containing_paragraph_or_theorem() -> None:
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)
    by_id = {n.node_id: n for n in anatomy.nodes}
    cite_containers = {
        e.src for e in anatomy.edges
        if e.edge_type == "contains"
        and by_id.get(e.dst, None) is not None
        and by_id[e.dst].subtype == "citation"
    }
    # Citations show up inside paragraphs (intro), inside the theorem,
    # and inside a paragraph following the proof.
    container_subtypes = {by_id[c].subtype for c in cite_containers}
    assert container_subtypes & {"paragraph", "theorem"}


def test_malformed_latex_raises_latex_parse_error() -> None:
    """S-A1-R2 catastrophic-failure scenario."""
    src = (FIXTURES / "malformed.tex").read_text("utf-8")
    with pytest.raises(LatexParseError) as exc:
        parse_latex(paper_id="malformed-test", source=src)
    assert "unbalanced_environments" in str(exc.value)


def test_citation_cited_paper_ref_uses_paper_alias_format() -> None:
    """The cited_paper_ref attr on Citation nodes uses `paper:<key>`
    so the cross-link rule (matcher: exact, dst.field: paper_id)
    can resolve to the canonical Paper node when it exists."""
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    anatomy = parse_latex(paper_id="vision-test", source=src)
    citations = [n for n in anatomy.nodes if n.subtype == "citation"]
    assert citations, "fixture should have citations"
    for cit in citations:
        ref = cit.attrs.get("cited_paper_ref")
        assert ref is not None and ref.startswith("paper:"), ref


def test_parser_is_deterministic_idempotent() -> None:
    """Same source → same anatomy. Important for content-hash-stable
    identity in the source layer (S-A1-R5)."""
    src = (FIXTURES / "sample_paper.tex").read_text("utf-8")
    a1 = parse_latex(paper_id="vision-test", source=src)
    a2 = parse_latex(paper_id="vision-test", source=src)
    assert [n.node_id for n in a1.nodes] == [n.node_id for n in a2.nodes]
    assert [(e.src, e.dst, e.edge_type) for e in a1.edges] == [
        (e.src, e.dst, e.edge_type) for e in a2.edges
    ]
