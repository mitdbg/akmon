"""Stride A — extraction ontology module compose test.

When a deployment loads `extraction` in its modules list, the
catalog composer should pick up the three subtypes
(`document`, `document_chunk`, `entity_mention`), their
`okg_searchable_text_fields` annotations, and the three
intra-module narrowings (contains / member_of / member_of).
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.module_manifest import load_manifest


EXTRACTION_MODULE = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/extraction/module.yaml"
)


@pytest.fixture
def extraction_only_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-deploy"
    dep.mkdir()
    # Per A2 (extend-substrate-extraction-with-claim-evidence-extractors),
    # the extraction module declares cross-module narrowings into the
    # reasoning module (`claim supported_by evidence`,
    # `hypothesis derived_from extraction_dropped`). Loading extraction
    # standalone now fails subtype resolution; load reasoning + its
    # transitive deps (git_graph for source_file, openspec for
    # acceptance_criterion, agent_memory for agent_observation) so
    # the composer resolves all narrowings.
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-deploy",
        "description": "extraction smoke deployment (composed with reasoning)",
        "modules": [
            "git_graph",
            "openspec",
            "agent_memory",
            "extraction",
            "reasoning",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_extraction_subtypes(
    extraction_only_deployment: Path,
) -> None:
    composed = compose_catalog(extraction_only_deployment)
    assert "document" in composed.subtypes
    assert "document_chunk" in composed.subtypes
    assert "entity_mention" in composed.subtypes


def test_extraction_module_declares_caption_of_edge_type() -> None:
    manifest = load_manifest(EXTRACTION_MODULE)
    assert "caption_of" in manifest.provides.edge_types


def test_compose_propagates_searchable_text_fields(
    extraction_only_deployment: Path,
) -> None:
    composed = compose_catalog(extraction_only_deployment)
    md = composed.subtype_metadata
    # DocumentChunk's body is the principal search target.
    assert md["document_chunk"]["searchable_text_fields"] == ["text"]
    # EntityMention exposes entity_value so substring search hits.
    assert md["entity_mention"]["searchable_text_fields"] == ["entity_value"]
    # Document indexes both its title and its path. Per
    # `index-document-source-file-search` (issue 900): `title` alone was
    # the whole declaration, and doc_corpus seeds it with the
    # repo-relative path, which the BM25 index's `english` config lexes as
    # ONE token — so no word in a document's name could ever match. Both
    # are also declared as path fields, which appends their component
    # words to the rollup.
    assert md["document"]["searchable_text_fields"] == ["title", "path"]
    assert md["document"]["searchable_text_path_fields"] == ["title", "path"]
    # `git_graph` is in this deployment's module list, so assert
    # SourceFile's declaration through the same real compose step.
    # Reading its schema.yaml directly would validate the annotation
    # against the very file the assertion was written from; only compose
    # proves the key is where the catalog looks for it. Before issue 900
    # SourceFile declared nothing here at all, so every source_file node
    # was absent from the BM25 partial index.
    assert md["source_file"]["searchable_text_fields"] == ["path"]
    assert md["source_file"]["searchable_text_path_fields"] == ["path"]


def test_compose_emits_intra_module_narrowings(
    extraction_only_deployment: Path,
) -> None:
    composed = compose_catalog(extraction_only_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("document", "contains", "document_chunk"),
        ("document_chunk", "member_of", "document"),
        ("document_chunk", "caption_of", "visual_artifact"),
        ("entity_mention", "member_of", "document_chunk"),
    }
    missing = expected - triples
    assert not missing, missing
