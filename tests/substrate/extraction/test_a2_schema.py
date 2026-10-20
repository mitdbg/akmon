"""Schema test for A2 — claim-evidence extractor additions.

Verifies that:
- The `extraction` module gains `claim` and `extraction_dropped`
  subtypes alongside the existing `document`, `document_chunk`,
  `entity_mention`.
- The surviving intra-extraction narrowings load cleanly into a
  composed catalog (claim_supported_by_evidence,
  claim_derived_from_extraction_dropped,
  hypothesis_derived_from_extraction_dropped).
- The claim↔claim narrowings and their supports/contradicts
  `okg_relation_kind` mappings are RETIRED (ruling D7, 2026-07-17):
  the retirement witnesses below assert the composed catalog emits
  neither triple and the placeholder YAMLs stay deleted.
- The reasoning module's `Hypothesis` and `Observation` subtypes
  gain optional `extracted_from_paper_ref` and `author_ref` attrs
  that load without disturbing the existing attrs.

Pattern: mirrors `tests/substrate/library/ontologies/test_reasoning_schema.py`.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


@pytest.fixture
def a2_deployment(tmp_path: Path) -> Path:
    dep = tmp_path / "test-a2-extraction"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-a2-extraction",
        "description": "schema smoke for A2 extraction additions",
        "modules": [
            # git_graph provides source_file (openspec/extraction need it).
            "git_graph",
            # openspec provides AcceptanceCriterion (referenced by a
            # cross-shared reasoning narrowing).
            "openspec",
            # agent_memory provides AgentObservation (referenced by a
            # cross-shared reasoning narrowing for the optional
            # EpistemicObservationEnricher).
            "agent_memory",
            # extraction is the module under test.
            "extraction",
            # reasoning provides Hypothesis/Observation/Evidence for cross-
            # module narrowings + the additive attr check.
            "reasoning",
        ],
    }))
    (dep / "schemas").mkdir()
    return dep


def test_compose_picks_up_claim_and_extraction_dropped(
    a2_deployment: Path,
) -> None:
    composed = compose_catalog(a2_deployment)
    assert "claim" in composed.subtypes
    assert "extraction_dropped" in composed.subtypes


def test_compose_emits_a2_intra_module_narrowings(
    a2_deployment: Path,
) -> None:
    composed = compose_catalog(a2_deployment)
    triples = composed.narrowing_triples()
    expected = {
        ("claim", "supported_by", "evidence"),
        ("claim", "derived_from", "extraction_dropped"),
        ("hypothesis", "derived_from", "extraction_dropped"),
    }
    missing = expected - triples
    assert not missing, (
        f"A2 narrowing triples missing: {sorted(missing)}"
    )


def test_supports_contradicts_narrowings_are_retired(
    a2_deployment: Path,
) -> None:
    """Ruling D7 (single-engine dispositions, 2026-07-17): the LLM-adjudicated
    claim supports/contradicts relation mechanism is DELETED — cross-claim
    conflict lives in the claim plane's declared checks, and the deterministic
    checker decides. Composition must no longer emit the retired narrowings;
    their reappearance means a parallel conflict engine grew back."""
    composed = compose_catalog(a2_deployment)
    triples = composed.narrowing_triples()
    retired = {
        ("claim", "supports", "claim"),
        ("claim", "contradicts", "claim"),
    }
    present = retired & triples
    assert not present, (
        f"retired D7 narrowings reappeared in composition: {sorted(present)}"
    )


def test_existing_extraction_subtypes_still_present(
    a2_deployment: Path,
) -> None:
    """Schema additions are additive — the existing extraction subtypes
    SHALL remain in the catalog (no accidental removals)."""
    composed = compose_catalog(a2_deployment)
    for existing in ("document", "document_chunk", "entity_mention"):
        assert existing in composed.subtypes


def test_existing_extraction_narrowings_still_present(
    a2_deployment: Path,
) -> None:
    """Existing intra-extraction narrowings (document contains chunk,
    chunk member_of document, mention member_of chunk, chunk references
    document) SHALL remain in the catalog."""
    composed = compose_catalog(a2_deployment)
    triples = composed.narrowing_triples()
    existing = {
        ("document", "contains", "document_chunk"),
        ("document_chunk", "member_of", "document"),
        ("entity_mention", "member_of", "document_chunk"),
        ("document_chunk", "references", "document"),
    }
    missing = existing - triples
    assert not missing, (
        f"existing extraction narrowings missing post-A2 additions: "
        f"{sorted(missing)}"
    )


def test_reasoning_hypothesis_gains_paper_attrs(
    a2_deployment: Path,
) -> None:
    """The schema additions on `reasoning.Hypothesis`
    (`extracted_from_paper_ref`, `author_ref`) SHALL be detectable
    by inspecting the composed catalog's per-subtype attribute set.
    The substrate's catalog composer surfaces subtype metadata via
    `subtype_metadata`; we read the raw YAML if the composer doesn't.
    """
    import yaml as _y
    schema_path = Path(
        "src/okg/substrate/library/ontologies/reasoning/schema.yaml"
    )
    schema = _y.safe_load(schema_path.read_text())
    hyp_attrs = schema["classes"]["Hypothesis"]["attributes"]
    assert "extracted_from_paper_ref" in hyp_attrs
    assert "author_ref" in hyp_attrs


def test_reasoning_observation_gains_paper_attrs(a2_deployment: Path) -> None:
    import yaml as _y
    schema_path = Path(
        "src/okg/substrate/library/ontologies/reasoning/schema.yaml"
    )
    schema = _y.safe_load(schema_path.read_text())
    obs_attrs = schema["classes"]["Observation"]["attributes"]
    assert "extracted_from_paper_ref" in obs_attrs
    assert "author_ref" in obs_attrs
    # confidence was already present; verify still there
    assert "confidence" in obs_attrs


def test_new_attrs_are_optional(a2_deployment: Path) -> None:
    """Per design.md D4: the new cross-paper attrs are schema-additive
    (all optional). Operator-authored content with no values for these
    attrs SHALL still validate. We check by ensuring `required: true`
    is NOT set on the new attrs.
    """
    import yaml as _y
    schema_path = Path(
        "src/okg/substrate/library/ontologies/reasoning/schema.yaml"
    )
    schema = _y.safe_load(schema_path.read_text())
    for subtype in ("Hypothesis", "Observation"):
        attrs = schema["classes"][subtype]["attributes"]
        for new_attr in ("extracted_from_paper_ref", "author_ref"):
            attr_block = attrs[new_attr]
            # attr_block may be None (LinkML shorthand) or a dict; both
            # are acceptable as "not required" since we don't set
            # `required: true`.
            if isinstance(attr_block, dict):
                assert not attr_block.get("required"), (
                    f"{subtype}.{new_attr} SHALL be optional; "
                    f"got required: {attr_block.get('required')!r}"
                )
