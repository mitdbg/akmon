"""Gates refuse on ABSENCE of the subject they quantify over.

pact-vacuous-gate-close req.vacuous.presence. Every check here has the same
shape: delete what a check quantifies over and the check used to pass
vacuously. The benchmark scores these as a rate; these tests pin the
individual predicates and their messages, so a refusal cannot quietly become a
refusal for a different reason.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import (
    _graph_evidence_requirements,
    task_completion_gate,
)
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import task_proof_obligation, validate_manifest


def _document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": "demo",
            "title": "Demo",
            "status": "implementation",
            "tier": 1,
        },
        "requirements": [
            {
                "id": "req.demo",
                "title": "Demo",
                "statement": "The demo behaviour holds.",
                "evidence_policy": {"required": ["executed_test"]},
            }
        ],
        "tasks": [
            {
                "id": "task.demo",
                "title": "Prove the demo",
                "status": "todo",
                "verification": "uv run pytest tests/pact -q",
                "requirements": ["req.demo"],
            }
        ],
        "evidence": [],
    }
    document.update(overrides)
    return document


def _errors(document: dict[str, Any]) -> list[tuple[str, str, str]]:
    manifest = PactManifest.model_validate(document)
    return [
        (issue.code, issue.path, issue.message)
        for issue in validate_manifest(manifest).issues
        if issue.severity == "error"
    ]


def _codes(document: dict[str, Any]) -> set[str]:
    return {code for code, _path, _message in _errors(document)}


def test_the_baseline_document_is_clean() -> None:
    """Otherwise every assertion below could pass for an unrelated reason."""
    assert _errors(_document()) == []


def test_a_task_linked_to_no_requirement_is_refused() -> None:
    document = _document()
    document["tasks"][0]["requirements"] = []
    errors = _errors(document)
    assert "task_requirements_link" in {code for code, _p, _m in errors}
    # The refusal names the offending task, so the repair is locatable.
    assert any("task.demo" in message for _c, _p, message in errors)


def test_a_done_and_unlinked_task_is_refused_by_name() -> None:
    """The compound case: the 35-task incident's exact mechanism.

    `task_done_missing_evidence` quantifies over the task's linked
    requirements, so deleting the links deleted the subject and the check
    passed on a task that proved nothing.
    """
    document = _document()
    document["tasks"][0]["status"] = "done"
    document["tasks"][0]["requirements"] = []
    errors = _errors(document)
    assert {code for code, _p, _m in errors} >= {
        "task_requirements_link",
        "task_done_missing_evidence",
    }
    vacuity = [
        message for code, _p, message in errors if code == "task_done_missing_evidence"
    ]
    assert vacuity and "task.demo" in vacuity[0]
    assert "no proof obligation" in vacuity[0]


def test_gate_task_done_refuses_the_vacuous_task_by_name(tmp_path: Path) -> None:
    """`missing_proof: []` beside a pass is what this replaces."""
    document = _document()
    document["tasks"][0]["requirements"] = []
    (tmp_path / "pact").mkdir(parents=True, exist_ok=True)
    save_manifest(tmp_path, PactManifest.model_validate(document))

    result = task_completion_gate(tmp_path, "demo", "task.demo")
    assert result["ok"] is False
    assert result["verdict"] == "refused"
    assert "task_proof_obligation_vacuous:task.demo" in result["blocked_by"]
    assert "task_proof_obligation_vacuous:task.demo" in result["blockers"]


def test_a_change_with_tasks_but_no_requirements_is_refused() -> None:
    document = _document(requirements=[])
    document["tasks"][0]["requirements"] = []
    codes = _codes(document)
    assert "missing_evidence_policy" in codes


def test_a_program_umbrella_with_no_tasks_stays_clean() -> None:
    """Delegation is not vacuity.

    A program umbrella carries its contract in `hierarchy`, and its children
    carry the requirements. Refusing it would be the specificity failure the
    catch rate is paired against.
    """
    document = _document(requirements=[], tasks=[], evidence=[])
    document["hierarchy"] = {"role": "program", "children": ["child-one"]}
    assert _errors(document) == []


def test_evidence_with_no_refs_is_refused() -> None:
    document = _document()
    document["evidence"] = [
        {
            "id": "ev.executed-test.refless",
            "kind": "executed_test",
            "status": "pass",
            "refs": [],
            "artifact": "tests/pact/test_vacuous_gate_close.py",
        }
    ]
    errors = _errors(document)
    assert "unknown_evidence_ref" in {code for code, _p, _m in errors}
    assert any("(empty refs)" in message for _c, _p, message in errors)


def test_an_unregistered_kind_is_refused_in_a_row_and_in_a_policy() -> None:
    row = _document()
    row["evidence"] = [
        {
            "id": "ev.rogue.demo",
            "kind": "kind_the_registry_does_not_know",
            "status": "pass",
            "refs": ["req.demo"],
        }
    ]
    assert "unknown_evidence_kind" in _codes(row)

    policy = _document()
    policy["requirements"][0]["evidence_policy"] = {
        "required": ["kind_the_registry_does_not_know"]
    }
    assert "unknown_evidence_kind" in _codes(policy)


def test_proof_obligation_reads_the_policy_not_what_is_missing() -> None:
    """ "Has an obligation" and "meets it" are different questions.

    Deriving vacuity from the MISSING set would call a fully proven task
    vacuous — the check would fire exactly when it should not.
    """
    manifest = PactManifest.model_validate(_document())
    assert task_proof_obligation(manifest, manifest.tasks[0]) == [
        "req.demo:executed_test"
    ]
    document = _document()
    document["evidence"] = [
        {
            "id": "ev.executed-test.req-demo",
            "kind": "executed_test",
            "status": "pass",
            "refs": ["req.demo"],
            "artifact": "tests/pact/test_vacuous_gate_close.py",
        }
    ]
    proven = PactManifest.model_validate(document)
    assert task_proof_obligation(proven, proven.tasks[0]) == ["req.demo:executed_test"]
    # A link to a requirement that does not exist is no obligation at all.
    dangling = _document()
    dangling["tasks"][0]["requirements"] = ["req.does-not-exist"]
    manifest = PactManifest.model_validate(dangling)
    assert task_proof_obligation(manifest, manifest.tasks[0]) == []


@pytest.mark.parametrize("policy_kind", ["graph_report", "graph"])
def test_graph_authority_is_demanded_without_a_generation_pin(
    policy_kind: str,
) -> None:
    """Deleting the pin must not delete the graph check.

    `graph_required` read only `task.coordination_generation_id`, so a change
    whose requirement DEMANDS graph proof silently needed no graph authority
    the moment nobody pinned a generation.
    """
    document = _document()
    document["requirements"][0]["evidence_policy"] = {"required": [policy_kind]}
    manifest = PactManifest.model_validate(document)
    assert manifest.tasks[0].coordination_generation_id is None
    assert _graph_evidence_requirements(manifest, manifest.tasks[0]) == ["req.demo"]


def test_a_non_graph_task_demands_no_graph_authority() -> None:
    manifest = PactManifest.model_validate(_document())
    assert _graph_evidence_requirements(manifest, manifest.tasks[0]) == []


def test_published_generation_policy_does_not_expand_shared_projection_authority() -> (
    None
):
    document = _document()
    document["requirements"][0]["evidence_policy"] = {
        "required": ["published_generation"]
    }
    manifest = PactManifest.model_validate(document)
    assert _graph_evidence_requirements(manifest, manifest.tasks[0]) == []


# --- regression rows: the checks that already refused must keep refusing ----


def test_empty_evidence_policy_still_refuses() -> None:
    document = _document()
    document["requirements"][0]["evidence_policy"] = {"required": []}
    assert "missing_evidence_policy" in _codes(document)


def test_dangling_evidence_ref_still_refuses() -> None:
    document = _document()
    document["evidence"] = [
        {
            "id": "ev.executed-test.dangling",
            "kind": "executed_test",
            "status": "pass",
            "refs": ["req.does-not-exist"],
        }
    ]
    assert "unknown_evidence_ref" in _codes(document)


def test_done_task_with_linked_but_unproven_requirement_still_refuses() -> None:
    document = _document()
    document["tasks"][0]["status"] = "done"
    errors = _errors(document)
    messages = [
        message for code, _p, message in errors if code == "task_done_missing_evidence"
    ]
    assert messages
    # The genuine-unmet-obligation arm, NOT the vacuity one.
    assert "missing required evidence" in messages[0]
