"""The consistency ontology module composes into a deployment catalog.

task.core.ontology-module (consistency-claim-model-core): the (C, Σ, ≻)
tier-0 node types — ConsistencyClaim (refines extraction.Claim),
ClaimCorrection (with the review-F1 observed-content snapshot),
ClaimPrecedent, PredicatePolicy (Σ as data, review-F6 row shapes), and
ConflictFinding — materialize as subtypes with their narrowings when a
deployment composes the module against the REAL substrate library.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import compose_catalog


def _write_deployment(tmp_path: Path, modules: list[str]) -> Path:
    d = tmp_path / "scratch"
    d.mkdir()
    (d / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "scratch", "schema_dir": "schemas", "modules": modules})
    )
    (d / "schemas").mkdir()
    return d


def test_consistency_module_composes_with_extraction(tmp_path) -> None:
    deployment = _write_deployment(tmp_path, ["extraction", "consistency"])
    composed = compose_catalog(deployment)

    # The five subtypes materialize (ConsistencyClaim refines extraction's
    # abstract Claim, like academic_researcher's Advisee refines person.Person).
    for st in (
        "consistency_claim",
        "claim_correction",
        "claim_precedent",
        "predicate_policy",
        "conflict_finding",
    ):
        assert st in composed.subtypes, (st, sorted(composed.subtypes))
        assert composed.subtype_origin[st] == "consistency"

    # Refinement closure: consistency_claim's parent IS the extraction
    # module's claim shape (review L4: pin the value, not mere presence).
    assert composed.subtype_parent.get("consistency_claim") == "claim"

    # The three narrowing triples, with per-row origin attribution.
    triples = composed.narrowing_triples()
    assert ("claim_correction", "corrects", "consistency_claim") in triples
    assert ("conflict_finding", "involves_claim", "consistency_claim") in triples
    assert ("conflict_finding", "constrained_by", "predicate_policy") in triples
    origins = {r.origin for r in composed.narrowings if r.parent_subtype == "conflict_finding"}
    assert origins == {"consistency"}


def test_consistency_module_requires_extraction(tmp_path) -> None:
    # extraction is a HARD dependency (review M1): compose harvests the
    # is_a parent unconditionally — only extends cross-link rules silently
    # skip. Composing alone therefore still emits the dangling parent
    # 'claim', which apply_composed_catalog rejects with a
    # subtypes_parent_fk violation. Pin the compose-level fact here (the
    # FK rejection is apply-layer behavior, exercised in the apply suite).
    deployment = _write_deployment(tmp_path, ["consistency"])
    composed = compose_catalog(deployment)
    for st in ("claim_correction", "claim_precedent", "predicate_policy", "conflict_finding"):
        assert st in composed.subtypes
    assert composed.subtype_parent.get("consistency_claim") == "claim"
    assert "claim" not in composed.subtypes  # dangling without extraction


def test_consistency_schema_declares_required_attrs() -> None:
    """Schema-content test (the methodology-module convention): the LinkML
    ``required: true`` flags are DECLARED on the load-bearing attributes.

    This is where required-ness lives today — compose_catalog does not
    harvest per-attribute requiredness into ComposedCatalog, so no
    projection/publish layer enforces it mechanically (see
    test_catalog_consistency_module_publish.py's module docstring for the
    enforcement-reality citations). Enum vocabularies (scope_state, action,
    policy_kind, outcome, locality) are description prose per the library
    convention, deliberately NOT LinkML enums."""
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "src" / "okg" / "substrate" / "library"
        / "ontologies" / "consistency" / "schema.yaml"
    )
    classes = yaml.safe_load(schema_path.read_text())["classes"]
    required = {
        "ConsistencyClaim": {
            "predicate", "normalized_key", "value",
            "scope_state", "claim_provenance", "source_id",
        },
        "ClaimCorrection": {
            "correction_id", "target_claim_id", "action", "sequence", "decided_by",
        },
        "ClaimPrecedent": {"precedent_id", "prefer_source", "sequence", "decided_by"},
        "PredicatePolicy": {"policy_id", "policy_kind", "predicate", "enabled"},
        "ConflictFinding": {
            "finding_id", "constraint_id", "predicate",
            "bucket_key", "outcome", "typed_category", "actionable_conflict",
            "locality", "finding_status",
        },
    }
    enum_prose_attrs = {
        "ConsistencyClaim": {"scope_state", "claim_provenance"},
        "ClaimCorrection": {"action"},
        "PredicatePolicy": {"policy_kind", "enabled"},
        "ConflictFinding": {"outcome", "locality", "finding_status"},
    }
    for cls, expected in required.items():
        attrs = classes[cls]["attributes"]
        declared = {
            name for name, body in attrs.items()
            if isinstance(body, dict) and body.get("required") is True
        }
        assert declared == expected, (cls, declared, expected)
    # Review L1: pin the enum-prose convention itself — vocabularies live in
    # description text, deliberately NOT as LinkML ranges/permissible_values.
    for cls, attr_names in enum_prose_attrs.items():
        for name in attr_names:
            body = classes[cls]["attributes"][name]
            assert "Enum {" in body.get("description", ""), (cls, name)
            assert "range" not in body, (cls, name)
            assert "permissible_values" not in body, (cls, name)
