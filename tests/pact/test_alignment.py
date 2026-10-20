from __future__ import annotations

from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.validation import validate_manifest


def test_missing_evidence_status() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs executed test.",
                "evidence_policy": {"required": ["pytest"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "missing",
                        "when": ["status runs"],
                        "then": ["missing evidence is reported"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "pytest evidence",
                        "evidence": ["pytest"],
                    }
                ],
            }
        ],
    )

    report = validate_manifest(manifest)

    assert report.ok
    assert report.alignment["requirements"]["req.demo"]["status"] == "missing_evidence"


def test_done_task_missing_evidence_fails() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs pytest.",
                "evidence_policy": {"required": ["pytest"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "missing",
                        "when": ["validation runs"],
                        "then": ["task fails"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "pytest evidence",
                        "evidence": ["pytest"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "status": "done",
                "verification": "pytest tests/pact/test_alignment.py -q",
                "requirements": ["req.demo"],
            }
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(issue.code == "task_done_missing_evidence" for issue in report.issues)


def test_blocked_completion_satisfied_fails() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "status": "blocked",
                "verification": "pytest tests/pact/test_alignment.py -q",
                "completion_satisfied": True,
            }
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(
        issue.code == "blocked_task_completion_satisfied"
        for issue in report.issues
    )


def test_alignment_derives_waived_and_stale() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs pytest.",
                "evidence_policy": {"required": ["pytest"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "missing",
                        "when": ["validation runs"],
                        "then": ["task fails"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "pytest evidence",
                        "evidence": ["pytest"],
                    }
                ],
            }
        ],
        test_plan=[
            {
                "id": "test.stale",
                "requirement": "req.deleted",
                "test_id": "tests/pact/test_alignment.py::test_deleted",
            }
        ],
        waivers=[
            {
                "id": "waiver.req.demo",
                "scope": "req.demo",
                "reason": "accepted bootstrap risk",
                "owner": "owner@example.com",
            }
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert report.alignment["requirements"]["req.demo"]["status"] == "waived"
    assert report.alignment["test_plan"]["test.stale"]["status"] == "stale"


def test_waiver_requires_owner_approval_record() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        waivers=[
            {
                "id": "waiver.demo",
                "scope": "req.demo",
                "reason": "bad waiver",
                "requires_owner_approval": False,
            }
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(
        issue.code == "invalid_waiver_owner_approval"
        for issue in report.issues
    )
