from __future__ import annotations

from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.validation import validate_manifest


def test_fast_path_upgrade_policy_flags_complex_work() -> None:
    manifest = PactManifest(
        target_pact_version=3,
        change=Change(
            id="demo",
            title="Demo",
            affected_areas=["qa-gates"],
            affected_code=[
                "a.py",
                "b.py",
                "c.py",
                "d.py",
            ],
        ),
        prose={"pact_shape": "fast_path"},
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must be small.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "small",
                        "when": ["validation runs"],
                        "then": ["policy is checked"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "policy is checked",
                        "evidence": ["executed_test"],
                    }
                ],
            },
            {
                "id": "req.second",
                "statement": "Second requirement makes this complex.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [
                    {
                        "id": "scn.second",
                        "title": "second",
                        "when": ["validation runs"],
                        "then": ["policy is checked"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.second",
                        "description": "policy is checked",
                        "evidence": ["executed_test"],
                    }
                ],
            },
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "verification": "pytest tests/pact/test_v3_fast_path_policy.py -q",
                "requirements": ["req.demo"],
            }
        ],
    )
    report = validate_manifest(manifest)

    assert report.ok
    assert any(
        issue.code == "fast_path_graduation_recommended"
        and issue.severity == "warning"
        for issue in report.issues
    )
