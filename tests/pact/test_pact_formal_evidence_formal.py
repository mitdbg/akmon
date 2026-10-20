from __future__ import annotations

import importlib.util
from pathlib import Path

from _v5_formal import carried_bindings


REPO_ROOT = Path(__file__).resolve().parents[2]
CHANGE_ID = "add-pact-formal-evidence-checker"
MODEL_PATH = REPO_ROOT / "formal" / "pact" / "PactFormalEvidence.tla"
CONFIG_PATH = REPO_ROOT / "formal" / "pact" / "PactFormalEvidence.cfg"
CHECKER_PATH = REPO_ROOT / "scripts" / "check_formal_pact_formal_evidence.py"
REFINEMENT_TESTS = {
    "tests/pact/test_pact_formal_evidence_formal.py::"
    "test_pact_formal_evidence_formal_binding_covers_contract",
    "tests/pact/test_v3_formal_flow.py::"
    "test_formal_binding_requires_checker_refinement_and_freshness",
    "tests/pact/test_v3_formal_flow.py::"
    "test_stale_formal_evidence_no_longer_satisfies_requirements_tasks_or_gates",
    "tests/pact/test_v3_formal_flow.py::"
    "test_formal_report_rejects_unknown_and_omitted_bound_properties",
    "tests/pact/test_formal_binding.py::"
    "test_formal_binding_validates_artifacts_when_repo_root_is_available",
    "tests/pact/test_evidence_ingest.py::"
    "test_formal_report_attaches_property_evidence",
}


def _manifest_path(change_id: str) -> Path:
    for base in ["changes", "archive"]:
        candidate = REPO_ROOT / "pact" / base / change_id / "pact.yaml"
        if candidate.is_file():
            return candidate
    raise AssertionError(f"PACT manifest not found: {change_id}")


def _checker_properties() -> set[str]:
    spec = importlib.util.spec_from_file_location(
        "check_formal_pact_formal_evidence",
        CHECKER_PATH,
    )
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return {property.property_id for property in module.PROPERTIES}


def test_pact_formal_evidence_formal_binding_covers_contract() -> None:
    properties = _checker_properties()
    model_text = MODEL_PATH.read_text(encoding="utf-8")
    config_text = CONFIG_PATH.read_text(encoding="utf-8")

    assert properties == {
        "TypeOK",
        "VerificationRequiresImplementationOrBypass",
        "VerificationEnteredFresh",
        "NoSilentStaleAfterVerification",
        "VerificationRequiresSemanticAcceptance",
        "VerificationRequiresCompletedTask",
        "PactFormalEvidenceFresh",
    }
    assert "PactFormalEvidenceFresh ==" in model_text
    assert "PactFormalEvidenceFresh" in config_text

    # v5 has no `formal[]`: `pact migrate` carried each binding into a
    # `formal_check` evidence row naming the property, the module and the
    # checker command. `config` and `refinement_tests` were not carried, so
    # they are asserted against the files themselves below instead of being
    # restated by the manifest.
    formal = {
        property_id: binding
        for property_id, binding in carried_bindings(CHANGE_ID).items()
        if binding.module == "formal/pact/PactFormalEvidence.tla"
    }
    assert set(formal) >= {"PactFormalEvidenceFresh"}
    binding = formal["PactFormalEvidenceFresh"]
    assert binding.requirement == "req.pact-formal-evidence.strict-checker"
    assert binding.checker_command == (
        "uv run --extra dev python "
        "scripts/check_formal_pact_formal_evidence.py "
        "--out .scratch/formal/pact-formal-evidence"
    )
    for test in REFINEMENT_TESTS:
        assert (REPO_ROOT / test.partition("::")[0]).is_file(), test

    for property_id in properties:
        assert property_id in model_text
        assert property_id in config_text
