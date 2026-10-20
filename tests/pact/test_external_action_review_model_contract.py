"""Contract checks for the production external-action formal surface."""

from __future__ import annotations

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = ROOT / "formal" / "external_action_review"
PACT_CHANGE_ID = "external-automation-review-lifecycle"


def _pact_design_dir() -> Path:
    candidates = [
        ROOT / "pact" / tree / PACT_CHANGE_ID for tree in ("changes", "archive")
    ]
    existing = [path for path in candidates if path.is_dir()]
    assert len(existing) == 1
    return existing[0]


def test_model_and_witness_inventory_is_complete() -> None:
    source = (MODEL_DIR / "ExternalActionReview.tla").read_text(encoding="utf-8")
    assert "ExternalActionReviewSafety" in source
    assert "ExternalActionExecutionSafety" in source
    assert "providerCalls = 0" in source
    assert {path.name for path in MODEL_DIR.glob("*.cfg")} == {
        "ExternalActionReview.cfg",
        "ExternalActionReviewStaleDecisionWitness.cfg",
        "ExternalActionReviewExpiredClaimWitness.cfg",
        "ExternalActionReviewStaleCancelWitness.cfg",
        "ExternalActionReviewReplayOwnerWitness.cfg",
    }


def test_production_model_matches_the_approved_design_model() -> None:
    design = _pact_design_dir() / "formal/ExternalActionReview.tla"
    assert (MODEL_DIR / "ExternalActionReview.tla").read_bytes() == design.read_bytes()


def test_formal_refinement_map_covers_all_model_actions() -> None:
    refinement = yaml.safe_load(
        (_pact_design_dir() / "formal/refinement-map.yaml").read_text(encoding="utf-8")
    )
    assert set(refinement["mappings"]) == {
        "Submit",
        "Edit",
        "Defer",
        "PrepareApproval",
        "ApplyPreparedApproval",
        "Reject",
        "PrepareCancel",
        "ApplyPreparedCancel",
        "AdvanceTime",
        "MaterializeExpiry",
        "Claim",
        "Report",
        "RevokeClaimOwner",
        "RecordOwnerUnavailabilityAttestation",
        "ReconcileIndeterminate",
        "ReplayOutcome",
    }


def test_checker_binds_runtime_refinement_and_emits_formal_report() -> None:
    checker = (ROOT / "scripts/check_formal_external_action_review.py").read_text(
        encoding="utf-8"
    )
    for required in (
        "--binding-only",
        "test_formal_refinement_map_covers_all_model_actions",
        "tests/substrate/external_actions/test_service.py",
        "tests/substrate/external_actions/test_store_postgres.py",
        "tests/e2e/test_external_action_review.py",
        "formal-report.yaml",
        "provider_calls",
    ):
        assert required in checker
