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
                "statement": "Demo must be traceable.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "traceable",
                        "when": ["validation runs"],
                        "then": ["acceptance proof is checked"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "acceptance proof exists",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_validation_invariants.py::test_acceptance_criteria_require_traceable_proof",
            }
        ],
    )


def test_loop_records_require_recipe_fields() -> None:
    manifest = _manifest()
    manifest.evidence.append(
        Evidence(
            id="ev.loop.demo",
            kind="loop_record",
            status="pass",
            refs=["demo"],
            data={
                "observation": "Trace existed.",
                "failure_mode": "agent_ux_friction",
                "friction_ledger": [],
                "ranked_improvement": "Improve the status surface.",
                "decision": "Use existing status output.",
                "next_observation": "Check the next trace.",
            },
        )
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(issue.code == "incomplete_loop_record" for issue in report.issues)

    manifest.evidence[0].data["friction_ledger"] = [
        {
            "observation": "Status did not explain next=[].",
            "failure_mode": "agent_ux_friction",
            "severity": "p1",
            "frequency": "likely common",
            "candidate_improvement": "Add agent guidance to status.",
            "verification": "pytest",
        }
    ]
    manifest.evidence[0].data["verification"] = "pytest tests/pact/test_validation_invariants.py -q"

    fixed = validate_manifest(manifest)

    assert fixed.ok


def test_acceptance_criteria_require_traceable_proof() -> None:
    manifest = _manifest()
    manifest.requirements[0].acceptance[0].evidence = []

    missing = validate_manifest(manifest)

    assert not missing.ok
    assert any(issue.code == "missing_acceptance_evidence" for issue in missing.issues)

    manifest.requirements[0].acceptance[0].evidence = ["ev.unknown"]
    unknown = validate_manifest(manifest)

    assert not unknown.ok
    assert any(issue.code == "unknown_acceptance_evidence" for issue in unknown.issues)

    manifest.requirements[0].acceptance[0].evidence = ["planned_test"]
    valid = validate_manifest(manifest)

    assert valid.ok

    # `manual` is a first-class evidence kind (evidence_policy.required: [manual]),
    # so it MUST also be a valid acceptance proof token. Regression: it was
    # previously absent from ACCEPTANCE_PROOF_TOKENS, which stranded every
    # manual / doc-only requirement as unknown_acceptance_evidence.
    manifest.requirements[0].acceptance[0].evidence = ["manual"]
    manual = validate_manifest(manifest)

    assert manual.ok
    assert not any(
        issue.code == "unknown_acceptance_evidence" for issue in manual.issues
    )


def test_default_requirement_boilerplate_warns_before_review() -> None:
    manifest = _manifest()
    manifest.requirements[0].title = "Demo"
    manifest.requirements[0].scenarios[0].id = "scn.req-demo.default"
    manifest.requirements[0].scenarios[0].title = "Demo validates"
    manifest.requirements[0].scenarios[0].when = ["okg pact validate runs"]
    manifest.requirements[0].scenarios[0].then = ["requirement req.demo is checked"]
    manifest.requirements[0].acceptance[0].id = "acc.req-demo"
    manifest.requirements[0].acceptance[0].description = manifest.requirements[0].statement

    report = validate_manifest(manifest, lifecycle_gate="proposal")

    assert report.ok
    assert any(
        issue.code == "default_requirement_boilerplate"
        and issue.severity == "warning"
        for issue in report.issues
    )
