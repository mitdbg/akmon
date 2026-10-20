from __future__ import annotations

import importlib.util
from pathlib import Path

from _v5_formal import carried_bindings


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGE_ID = "formalize-pact-v4-decision-ledger-safety"
MODEL_PATH = REPO_ROOT / "formal" / "pact" / "PactV4DecisionLedger.tla"
CONFIG_PATH = REPO_ROOT / "formal" / "pact" / "PactV4DecisionLedger.cfg"
CHECKER_PATH = REPO_ROOT / "scripts" / "check_formal_pact_v4_decision_ledger.py"
REFINEMENT_TESTS = {
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_without_any_decision",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_passes_with_matching_approval",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_pre_merge_gate_accepts_legacy_archived_approval_without_v4_lifecycle_fields",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_when_intent_changed_after_approval",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_matching_approval_after_reopen",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_matching_approval_when_manifest_is_active",
    "tests/pact/test_review.py::TestPreMergeDecisionGate::"
    "test_refuses_when_ledger_unreachable",
}


def _manifest_path(change_id: str) -> Path:
    for base in ["changes", "archive"]:
        candidate = REPO_ROOT / "pact" / base / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise AssertionError(f"PACT manifest not found: {change_id}")


def _checker_properties() -> set[str]:
    spec = importlib.util.spec_from_file_location(
        "check_formal_pact_v4_decision_ledger",
        CHECKER_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {property.property_id for property in module.PROPERTIES}


def test_pact_v4_decision_ledger_formal_binding_covers_contract() -> None:
    properties = _checker_properties()
    model_text = MODEL_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_PATH.read_text(encoding="utf-8")

    assert "PactV4DecisionLedgerContract" in properties
    assert "PactV4DecisionLedgerContract ==" in model_text
    assert "PactV4DecisionLedgerContract" in config_text

    # v5 has no `formal[]`: `pact migrate` carried each binding into a
    # `formal_check` evidence row naming the property, the module and the
    # checker command. `config` and `refinement_tests` were not carried, so
    # they are asserted against the files themselves below instead of being
    # restated by the manifest.
    formal = {
        property_id: binding
        for property_id, binding in carried_bindings(CHANGE_ID).items()
        if binding.module == "formal/pact/PactV4DecisionLedger.tla"
    }
    assert set(formal) >= {"PactV4DecisionLedgerContract"}
    binding = formal["PactV4DecisionLedgerContract"]
    assert binding.requirement == (
        "req.formalize-pact-v4-decision-ledger-safety.merge-boundary"
    )
    assert binding.checker_command == (
        "uv run python scripts/check_formal_pact_v4_decision_ledger.py "
        "--out .scratch/formal/pact-v4-decision-ledger"
    )
    for test in REFINEMENT_TESTS:
        assert (REPO_ROOT / test.partition("::")[0]).is_file(), test

    for property_id in properties:
        assert property_id in model_text
        assert property_id in config_text
