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
                "statement": "Demo must be verified.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "verified",
                        "when": ["validation runs"],
                        "then": ["evidence is resolved"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "pytest evidence",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "status": "done",
                "verification": "pytest tests/pact/test_v1_evidence_resolution.py -q",
                "requirements": ["req.demo"],
                "evidence": ["ev.missing"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_evidence_resolution.py::test_task_completion_rejects_unresolved_or_failing_evidence",
            }
        ],
    )


def test_task_completion_rejects_unresolved_or_failing_evidence() -> None:
    manifest = _manifest()

    unresolved = validate_manifest(manifest)

    assert not unresolved.ok
    assert any(issue.code == "unresolved_task_evidence" for issue in unresolved.issues)
    task_state = unresolved.alignment["tasks"]["task.demo"]
    assert "unresolved_evidence:ev.missing" in task_state["evidence_issues"]

    manifest.tasks[0].evidence = ["ev.pytest.req.demo"]
    manifest.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="fail",
            refs=["req.demo"],
        )
    )
    failing = validate_manifest(manifest)

    assert not failing.ok
    assert any(issue.code == "task_evidence_not_passing" for issue in failing.issues)
    assert (
        "failing_evidence:ev.pytest.req.demo:fail"
        in failing.alignment["tasks"]["task.demo"]["evidence_issues"]
    )


def test_task_completion_accepts_passing_scoped_evidence() -> None:
    manifest = _manifest()
    manifest.tasks[0].evidence = ["ev.pytest.req.demo"]
    manifest.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="pass",
            refs=["req.demo"],
            data={
                "junit_test_ids": [
                    "tests.pact.test_v1_evidence_resolution::test_task_completion_rejects_unresolved_or_failing_evidence"
                ]
            },
        )
    )

    report = validate_manifest(manifest, lifecycle_gate="verification")

    assert report.ok
    assert report.alignment["tasks"]["task.demo"]["status"] == "complete"
