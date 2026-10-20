from __future__ import annotations

from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.validation import validate_manifest


def test_pact_core_tla_is_design_target_not_v1_gate() -> None:
    manifest = PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "PACT Core may be modeled in normal TLA+.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "formal is metadata",
                        "when": ["validation runs"],
                        "then": ["TLA+ remains standalone"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "formal metadata",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_formal.py::test_pact_core_tla_is_design_target_not_v1_gate",
            }
        ],
        formal=[
            {
                "id": "formal.pact-core",
                "requirement": "req.demo",
                "property_id": "PactCoreInvariant",
                "module": "formal/pact/PactCore.tla",
                "config": "formal/pact/PactCore.cfg",
                "checker_command": "tlc2.TLC formal/pact/PactCore.tla",
                "refinement_tests": [
                    "tests/pact/test_v1_formal.py::test_pact_core_tla_is_design_target_not_v1_gate"
                ],
            }
        ],
    )

    proposal = validate_manifest(manifest, lifecycle_gate="proposal")
    verification = validate_manifest(manifest, lifecycle_gate="verification")

    assert proposal.ok
    assert not verification.ok
    assert any(
        issue.code == "verification_gate_missing_formal_evidence"
        for issue in verification.issues
    )
