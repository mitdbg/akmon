from __future__ import annotations

from okg.substrate.pact.models import Change, Evidence, PactManifest
from okg.substrate.pact.validation import validate_manifest


def _manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must validate.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "validates",
                        "when": ["validation runs"],
                        "then": ["validation passes"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "planned test exists",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_gates.py::test_planned_test_is_traceable_not_satisfied",
            }
        ],
    )


def test_planned_test_is_traceable_not_satisfied() -> None:
    report = validate_manifest(_manifest())
    state = report.alignment["requirements"]["req.demo"]

    assert report.ok
    assert state["traceability_status"] == "traceable"
    assert state["satisfaction_status"] == "unsatisfied"
    assert state["satisfaction_missing"] == ["executed_test"]


def test_proposal_implementation_and_verification_gates_differ() -> None:
    manifest = _manifest()

    proposal = validate_manifest(manifest, lifecycle_gate="proposal")
    verification = validate_manifest(manifest, lifecycle_gate="verification")

    assert proposal.ok
    assert not verification.ok
    assert any(
        issue.code == "completion_gate_unsatisfied" for issue in verification.issues
    )

    manifest.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="pass",
            refs=["req.demo"],
            data={
                "junit_test_ids": [
                    "tests.pact.test_v1_gates::test_planned_test_is_traceable_not_satisfied"
                ]
            },
        )
    )
    verified = validate_manifest(manifest, lifecycle_gate="verification")
    assert verified.ok
