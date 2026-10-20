from __future__ import annotations

from pathlib import Path

import yaml


def test_latex_project_cross_module_narrowings_are_declared() -> None:
    root = Path(__file__).parents[4]
    schema = yaml.safe_load(
        (root / "src/okg/substrate/library/ontologies/scholarly/schema.yaml").read_text()
    )
    attributes = schema["classes"]["EdgeNarrowings"]["attributes"]
    expected = {
        "paper_derived_from_document": ("derived_from", "Paper", "Document"),
        "paper_references_document": ("references", "Paper", "Document"),
        "paper_references_visual_artifact": (
            "references", "Paper", "VisualArtifact",
        ),
        "paper_section_contains_document_chunk": (
            "contains", "PaperSection", "DocumentChunk",
        ),
        "paper_section_contains_citation": (
            "contains", "PaperSection", "Citation",
        ),
        "document_contains_citation": ("contains", "Document", "Citation"),
        "citation_references_document_chunk": (
            "references", "Citation", "DocumentChunk",
        ),
    }
    for name, triple in expected.items():
        annotations = attributes[name]["annotations"]
        assert (
            annotations["edge_archetype"],
            annotations["src_subtypes"],
            annotations["dst_subtypes"],
        ) == triple
