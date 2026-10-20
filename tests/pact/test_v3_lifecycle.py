from __future__ import annotations

from okg.substrate.pact.models import Change, Evidence, PactManifest, Waiver
from okg.substrate.pact.validation import validate_manifest


def _verified_v3_manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=3,
        change=Change(id="demo", title="Demo", status="implementation"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must be verified.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "verified",
                        "when": ["verification runs"],
                        "then": ["proof is present"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "executed proof exists",
                        "evidence": ["executed_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "status": "done",
                "verification": "pytest tests/pact/test_v3_lifecycle.py -q",
                "requirements": ["req.demo"],
                "evidence": ["ev.pytest.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v3_lifecycle.py::test_verification_requires_implementation_or_bypass",
            }
        ],
        evidence=[
            Evidence(
                id="ev.pytest.demo",
                kind="pytest",
                status="pass",
                refs=["req.demo", "task.demo", "test.demo"],
                artifact="junit.xml",
                data={
                    "junit_test_ids": [
                        "tests.pact.test_v3_lifecycle::test_verification_requires_implementation_or_bypass"
                    ]
                },
            )
        ],
    )


def test_verification_requires_implementation_or_bypass() -> None:
    manifest = _verified_v3_manifest()
    blocked = validate_manifest(manifest, lifecycle_gate="verification")

    assert not blocked.ok
    assert any(
        issue.code == "verification_gate_missing_implementation_gate"
        for issue in blocked.issues
    )

    manifest.waivers.append(
        Waiver(
            id="waiver.lifecycle",
            scope="lifecycle.bypass",
            reason="fast path explicitly bypasses implementation approval",
            owner="owner@example.com",
        )
    )
    bypassed = validate_manifest(manifest, lifecycle_gate="verification")

    assert bypassed.ok
