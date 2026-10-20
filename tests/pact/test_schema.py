from __future__ import annotations

import pytest
from pydantic import ValidationError

from okg.substrate.pact.models import (
    AcceptanceCriterion,
    Change,
    PactManifest,
    Requirement,
)
from okg.substrate.pact.store import render_manifest
from okg.substrate.pact.validation import validate_manifest


def test_valid_change_fixture_passes() -> None:
    manifest = PactManifest(
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
                "test_id": "tests/pact/test_schema.py::test_valid_change_fixture_passes",
            }
        ],
    )

    report = validate_manifest(manifest)

    assert report.ok
    assert report.alignment["requirements"]["req.demo"]["status"] == "complete"


def test_missing_stable_id_fails() -> None:
    with pytest.raises(ValidationError):
        Requirement.model_validate({
            "statement": "No id.",
            "evidence_policy": {"required": ["planned_test"]},
            "scenarios": [
                {
                    "id": "scn.no-id",
                    "title": "invalid",
                    "when": ["validation runs"],
                    "then": ["validation fails"],
                }
            ],
            "acceptance": [
                {"id": "acc.no-id", "description": "criterion"}
            ],
        })


def test_missing_stable_acceptance_id_fails() -> None:
    with pytest.raises(ValidationError):
        AcceptanceCriterion.model_validate({"description": "No id."})


def test_unknown_normative_key_fails() -> None:
    with pytest.raises(ValidationError):
        PactManifest.model_validate({
            "pact_version": 0,
            "change": {"id": "demo", "title": "Demo"},
            "requirements": [],
            "tasks": [],
            "dependencies": [],
            "test_plan": [],
            "evidence": [],
            "edits": [],
            "surprise": True,
        })


def test_render_manifest_omits_legacy_completion_satisfied() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        tasks=[
            {
                "id": "task.demo",
                "title": "Demo",
                "verification": "pytest tests/pact/test_schema.py -q",
                "completion_satisfied": True,
            }
        ],
    )

    assert manifest.tasks[0].completion_satisfied is True
    assert "completion_satisfied" not in render_manifest(manifest)


def test_missing_scenarios_warn_only_without_evidence_policy() -> None:
    """Per `simplify-pact-surface` task 4 scenarios are advisory; per
    `polish-pact-first-run-dx` req.pactdx.warning-contradiction (issue
    #643) the documented lean shape — evidence_policy declared, no
    scenarios/acceptance — draws NO completeness warnings (the tool must
    not scold authors for following pact/AGENTS.md). The warnings remain
    for a requirement with no evidence policy, where scenarios/acceptance
    are the only completeness signal.
    """
    lean = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must validate.",
                "evidence_policy": {"required": ["planned_test"]},
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "criterion",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
    )

    report = validate_manifest(lean)

    assert report.ok, "slim PACT (no scenarios) must validate"
    assert not any(i.code == "missing_scenario" for i in report.issues), (
        "declared evidence_policy is the documented lean shape; "
        "missing_scenario must not warn on it"
    )

    no_policy = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo must validate.",
            }
        ],
    )

    report = validate_manifest(no_policy)

    warnings = [i for i in report.issues if i.severity == "warning"]
    assert any(i.code == "missing_scenario" for i in warnings), (
        "a requirement with NO evidence policy still warns missing_scenario"
    )
    assert any(i.code == "missing_acceptance" for i in warnings), (
        "a requirement with NO evidence policy still warns missing_acceptance"
    )


def test_dependency_cycle_without_waiver_fails() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        tasks=[
            {
                "id": "task.a",
                "title": "A",
                "verification": "pytest tests/pact/test_schema.py -q",
            },
            {
                "id": "task.b",
                "title": "B",
                "verification": "pytest tests/pact/test_schema.py -q",
            },
        ],
        dependencies=[
            {"from": "task.a", "to": "task.b"},
            {"from": "task.b", "to": "task.a"},
        ],
    )

    report = validate_manifest(manifest)

    assert not report.ok
    assert any(issue.code == "dependency_cycle" for issue in report.issues)


def test_missing_planned_test_fails_completion_gate() -> None:
    manifest = PactManifest(
        change=Change(id="demo", title="Demo"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Needs a planned test.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "missing",
                        "when": ["validation runs"],
                        "then": ["gate fails"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "planned test",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
    )

    report = validate_manifest(manifest, completion_gate=True)

    assert not report.ok
    assert any(
        issue.code == "completion_gate_unsatisfied"
        for issue in report.issues
    )
