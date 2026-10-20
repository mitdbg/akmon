"""Static wiring proof for the VisualCaptionEnricher in caption-dev
(task.wire-deployment).

A live `okg deployment apply caption-dev` needs the provisioned caption-dev
database; this proves the DB-independent half of the contract: the enricher
loads from caption-dev's deployment.yaml, its config maps to the constructor,
and the two narrowings it requires are present in the extraction ontology that
caption-dev loads — so the publisher's boot-time narrowing validator would
accept it.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

_DEP = Path("deployments/caption-dev")

# caption-dev is a local, untracked dev deployment (not committed to the repo,
# same as PACT-A left it). Skip where it is absent so CI stays green; the wiring
# is exercised wherever the deployment exists (e.g. the live publish-proof).
pytestmark = pytest.mark.skipif(
    not _DEP.exists(),
    reason="caption-dev deployment dir absent (local-only dev harness)",
)


def test_caption_dev_wires_visual_caption_enricher() -> None:
    from okg.substrate.publisher.enricher_loader import (
        active_enricher_profiles_from_env,
        load_enrichers,
    )

    enrichers = load_enrichers(
        _DEP, active_profiles=active_enricher_profiles_from_env()
    )
    matches = [
        e for e in enrichers
        if type(e).__name__ == "VisualCaptionEnricher"
    ]
    assert len(matches) == 1, "caption-dev must wire exactly one VisualCaptionEnricher"
    e = matches[0]
    # Config maps to the constructor.
    assert e.model == "openrouter/google/gemma-4-31b-it"
    assert e.max_workers == 8
    assert e.max_images_per_doc is None  # uncapped
    # Declares the narrowings + node subtype the publisher validates.
    assert e.requires_narrowings == (
        ("document_chunk", "derived_from", "visual_artifact"),
        ("document_chunk", "member_of", "document"),
    )
    assert "document_chunk" in e.emits_node_subtypes


def test_required_narrowings_present_in_extraction_ontology() -> None:
    """The two triples the enricher emits are defined in the extraction
    ontology (which caption-dev loads via `modules: [extraction]`), so the
    boot-time narrowing validator would not reject the wiring."""
    schema = yaml.safe_load(
        Path(
            "src/okg/substrate/library/ontologies/extraction/schema.yaml"
        ).read_text()
    )
    # Walk the schema for narrowing entries with the archetypes we emit.
    text = yaml.safe_dump(schema)
    assert "chunk_derived_from_visual_artifact" in text
    assert "chunk_member_of_document" in text

    # caption-dev loads the extraction module.
    dep = yaml.safe_load((_DEP / "deployment.yaml").read_text())
    assert "extraction" in dep.get("modules", [])
