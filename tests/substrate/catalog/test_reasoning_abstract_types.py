"""Coverage for `extend-reasoning-with-abstract-types` (Layer 1C).

Verifies that the new concrete refinements ship with correct
`is_a:` parent declarations and compose cleanly through the
Layer 1A subtype-closure machinery. Real per-deployment
migration of legacy `claim` / `evidence` nodes is operator-
driven and deferred to a follow-on (closure-join queries
already cover both old and new subtype names).
"""
from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.catalog.modules_apply import (
    _detect_refinement_cycles,
)


REASONING_SCHEMA = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/ontologies/reasoning/schema.yaml"
)
EXTRACTION_SCHEMA = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/ontologies/extraction/schema.yaml"
)
ENGRAM_SCHEMA = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/ontologies/engram_runs/schema.yaml"
)
ENGRAM_MODULE = (
    Path(__file__).resolve().parents[3]
    / "src/okg/substrate/library/ontologies/engram_runs/module.yaml"
)


def _load_classes(path: Path) -> dict:
    with path.open() as f:
        return (yaml.safe_load(f) or {}).get("classes") or {}


# ---------- schema-shape checks ----------


def test_reasoning_has_generic_evidence_refining_evidence() -> None:
    classes = _load_classes(REASONING_SCHEMA)
    assert "GenericEvidence" in classes, (
        "reasoning module SHALL ship GenericEvidence as the "
        "concrete refinement of Evidence (Layer 1C)"
    )
    assert classes["GenericEvidence"].get("is_a") == "Evidence", (
        "GenericEvidence.is_a SHALL be Evidence so the refinement "
        "closure links the concrete child to the abstract parent"
    )


def test_extraction_has_generic_claim_refining_claim() -> None:
    classes = _load_classes(EXTRACTION_SCHEMA)
    assert "GenericClaim" in classes, (
        "extraction module SHALL ship GenericClaim as the "
        "concrete refinement of Claim (Layer 1C)"
    )
    assert classes["GenericClaim"].get("is_a") == "Claim", (
        "GenericClaim.is_a SHALL be Claim so the refinement "
        "closure links the concrete child to the abstract parent"
    )


def test_reasoning_evidence_remains_concrete_class() -> None:
    """Evidence stays a real subtype with archetype + class_uri.

    The 'abstract parent' framing is documentation, not a runtime
    gate — the substrate doesn't refuse direct Evidence emissions.
    This test guards against accidental schema corruption (e.g.,
    removing class_uri while reshaping the file).
    """
    classes = _load_classes(REASONING_SCHEMA)
    ev = classes.get("Evidence") or {}
    assert ev.get("class_uri") == "reasoning:Evidence"
    annotations = ev.get("annotations") or {}
    assert annotations.get("archetype") == "artifact"


def test_extraction_claim_remains_concrete_class() -> None:
    classes = _load_classes(EXTRACTION_SCHEMA)
    cl = classes.get("Claim") or {}
    assert cl.get("class_uri") == "extr:Claim"
    annotations = cl.get("annotations") or {}
    assert annotations.get("archetype") == "claim"


# ---------- composition shape (no DB) ----------


def test_generic_refinements_have_no_cycle() -> None:
    """The new Generic* subtypes must form an acyclic refinement
    forest with their abstract parents.
    """
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {
        "claim", "generic_claim", "evidence", "generic_evidence",
    }
    composed.subtype_parent = {
        "generic_claim": "claim",
        "generic_evidence": "evidence",
    }
    _detect_refinement_cycles(composed)  # no raise


def test_generic_refinements_pair_with_domain_refinements() -> None:
    """A future deployment refining Evidence with a domain subtype
    (e.g., engram_runs.ExperimentResult) coexists peacefully with
    GenericEvidence. Two refinements of the same abstract parent
    are fine; cycle detection doesn't trip.
    """
    composed = ComposedCatalog(deployment_name="test")
    composed.subtypes = {
        "evidence", "generic_evidence", "experiment_result",
        "paper_citation_evidence",
    }
    composed.subtype_parent = {
        "generic_evidence": "evidence",
        "experiment_result": "evidence",
        "paper_citation_evidence": "evidence",
    }
    _detect_refinement_cycles(composed)  # no raise


# ---------- single canonical Claim home (reasoning-abstract-types) ----------


def _load_module(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f) or {}


def test_reasoning_has_no_claim_class() -> None:
    """The Claim half of extend-reasoning-with-abstract-types is resolved by
    declaring extraction.Claim canonical (reasoning-abstract-types). The
    reasoning ring is closed at 8 subtypes — its claim-archetypes are
    Hypothesis / Principle / Refutation — so a reasoning.Claim would be a
    duplicate/ambiguous home and MUST NOT exist."""
    classes = _load_classes(REASONING_SCHEMA)
    assert "Claim" not in classes, (
        "reasoning MUST NOT define a Claim class; the canonical home is "
        "extraction.Claim"
    )
    assert "GenericClaim" not in classes


def test_extraction_is_the_single_canonical_claim_home() -> None:
    """Exactly one Claim home: extraction.Claim (parent) + extraction.
    GenericClaim (concrete default). reasoning has none."""
    extr = _load_classes(EXTRACTION_SCHEMA)
    reasoning = _load_classes(REASONING_SCHEMA)
    assert extr.get("Claim", {}).get("class_uri") == "extr:Claim"
    assert extr.get("GenericClaim", {}).get("is_a") == "Claim"
    assert "Claim" not in reasoning


def test_engram_empirical_claim_refines_canonical_extraction_claim() -> None:
    """engram_runs.EmpiricalClaim `is_a: Claim` must resolve to the canonical
    extraction.Claim — so engram_runs MUST declare `extends: extraction` (the
    parent used to be mis-declared under reasoning, which has no Claim, leaving
    a dangling refinement)."""
    classes = _load_classes(ENGRAM_SCHEMA)
    assert classes.get("EmpiricalClaim", {}).get("is_a") == "Claim"

    extends = _load_module(ENGRAM_MODULE).get("extends") or {}
    assert "extraction" in extends, (
        "engram_runs MUST extend `extraction` so EmpiricalClaim's `is_a: Claim` "
        "resolves to the canonical extraction.Claim"
    )
    # the stale reasoning.Claim reference is gone from the reasoning extends
    reasoning_desc = (extends.get("reasoning") or {}).get("description", "")
    assert "EmpiricalClaim" not in reasoning_desc, (
        "engram_runs extends.reasoning MUST NOT claim to provide the Claim "
        "parent for EmpiricalClaim"
    )
