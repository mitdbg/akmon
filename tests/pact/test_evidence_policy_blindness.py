"""A requirement must be able to fail on a measurement its policy never named.

`pact-evidence-policy-blindness`. Requirement satisfaction quantified over
`evidence_policy.required`, so a recorded `fail` row of any other kind was
never a candidate and its status was never read. The gate closed with
`missing_proof=0` and `pact view` reported `gaps: 0`, while the failing row sat
in the same manifest.

Every arm here runs against the real gate (`task_completion_gate`) or the real
alignment, never against a re-implementation of the rule. The BEFORE half is
proved by exercising the policy-filtered predicate directly (`_has_evidence`,
the computation that was the whole of the answer before), so a reader can see
that the defect was the one described rather than only that the new behaviour
holds.
"""
from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import task_completion_gate
from okg.substrate.pact.repair import parse_proof_slot, proof_slot_commands
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import (
    _has_evidence,
    _refuting_requirement_evidence,
    validate_manifest,
)


PLANNED_TEST_ID = "tests/pact/test_evidence_policy_blindness.py::test_demo"
PASSING_ROW: dict[str, Any] = {
    "id": "ev.executed-test.req-demo",
    "kind": "executed_test",
    "status": "pass",
    "refs": ["req.demo"],
    "artifact": "pytest:tests/pact/test_evidence_policy_blindness.py",
    "summary": "The hermetic half only; the structural half is measured live.",
    # A passing test row must attest the concrete tests its plan names, or it
    # does not count as proof at all (`_executed_test_evidence_reasons`). The
    # fixture satisfies that rule so every arm below turns on the refutation
    # question and not on an unattested row.
    "data": {"junit_test_ids": [PLANNED_TEST_ID]},
}
# The doc-loader shape: the measurement the requirement's own statement calls
# blocking cannot be a pytest result, so it lands in a kind the policy never
# named.
REFUTING_ROW: dict[str, Any] = {
    "id": "ev.operator-report.req-demo",
    "kind": "operator_report",
    "status": "fail",
    "refs": ["req.demo"],
    "artifact": "live:demo-zero-vector-rows",
    "summary": "MEASURED FAILURE: the batch path wrote 2 rows with the bypass on.",
}


def _document(**overrides: Any) -> dict[str, Any]:
    document: dict[str, Any] = {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": "demo", "title": "Demo", "status": "implementation", "tier": 1},
        "requirements": [
            {
                "id": "req.demo",
                "title": "Demo",
                "statement": (
                    "The demo behaviour holds. A non-zero live count is a MEASURED "
                    "FAILURE that BLOCKS this requirement."
                ),
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
        "test_plan": [
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": PLANNED_TEST_ID,
                "evidence": ["pytest"],
            }
        ],
        "evidence": [copy.deepcopy(PASSING_ROW)],
    }
    document.update(overrides)
    return document


def _saved(tmp_path: Path, document: dict[str, Any]) -> Path:
    (tmp_path / "pact").mkdir(parents=True, exist_ok=True)
    save_manifest(tmp_path, PactManifest.model_validate(document))
    return tmp_path


def _requirement_missing(document: dict[str, Any]) -> list[str]:
    manifest = PactManifest.model_validate(document)
    alignment = validate_manifest(manifest).alignment
    return list(alignment["requirements"]["req.demo"]["satisfaction_missing"])


def _refutations(missing: list[str]) -> list[str]:
    """Requirement-level tokens are BARE and task-level ones carry a
    `{req_id}:` prefix (validation.py:2819-2827); both are matched here."""
    return [
        token
        for token in missing
        if token.startswith("refuted_by:") or ":refuted_by:" in token
    ]


# --------------------------------------------------------------------------
# The defect, shown before the fix is shown to hold.
# --------------------------------------------------------------------------


def test_the_policy_filtered_predicate_still_cannot_see_the_failing_row() -> None:
    """BEFORE, in code: the whole of the old answer, run on the same manifest.

    `_has_evidence` filtered on `evidence.kind in kinds` (validation.py:4373-4384)
    and the policy expanded only to the executed_test family, so the
    `operator_report` row was never a candidate and its `status: fail` was
    never read. That predicate is UNCHANGED by this fix -- it is the right
    answer to "is the declared proof present" -- which is why a second
    quantifier was needed rather than a wider filter.
    """
    document = _document()
    document["evidence"].append(copy.deepcopy(REFUTING_ROW))
    manifest = PactManifest.model_validate(document)

    policy_kinds = {"executed_test", "pytest", "junit", "test"}
    assert _has_evidence(manifest, "req.demo", policy_kinds) is True
    # ... and the failing row is invisible to it, whatever its status.
    assert _has_evidence(manifest, "req.demo", {"operator_report"}) is False


def test_the_refuting_row_is_read_by_the_new_quantifier() -> None:
    """AFTER: the same row, read with no policy filter at all."""
    document = _document()
    document["evidence"].append(copy.deepcopy(REFUTING_ROW))
    manifest = PactManifest.model_validate(document)

    assert _refuting_requirement_evidence(manifest, "req.demo") == [
        "refuted_by:ev.operator-report.req-demo:operator_report:fail"
    ]


# --------------------------------------------------------------------------
# Arm 1: the gate refuses, and the refusal names the row.
# --------------------------------------------------------------------------


def test_failing_row_of_unnamed_kind_refuses_task_done(tmp_path: Path) -> None:
    document = _document()
    document["evidence"].append(copy.deepcopy(REFUTING_ROW))
    root = _saved(tmp_path, document)

    result = task_completion_gate(root, "demo", "task.demo")

    assert result["ok"] is False
    assert result["verdict"] == "refused"
    token = (
        "req.demo:refuted_by:ev.operator-report.req-demo:operator_report:fail"
    )
    assert token in result["missing_proof"]
    assert f"missing_proof:{token}" in result["blocked_by"]


def test_the_refusal_names_the_rows_id_kind_and_status(tmp_path: Path) -> None:
    """An operator reading the refusal must learn WHICH measurement blocked."""
    document = _document()
    document["evidence"].append(copy.deepcopy(REFUTING_ROW))
    root = _saved(tmp_path, document)

    result = task_completion_gate(root, "demo", "task.demo")

    why = result["why"]
    assert "ev.operator-report.req-demo" in why
    assert "operator_report" in why
    assert "fail" in why
    assert "REFUTE" in why


def test_no_attach_command_is_offered_for_a_refutation(tmp_path: Path) -> None:
    """Attaching another row leaves the failing row exactly where it is.

    A repair command that runs cleanly and moves nothing is the class
    `pact-self-repairing-refusals` closed; a refutation must not reopen it.
    """
    document = _document()
    document["evidence"].append(copy.deepcopy(REFUTING_ROW))
    root = _saved(tmp_path, document)

    result = task_completion_gate(root, "demo", "task.demo")

    assert all(
        "operator-report" not in command
        for command in result["repair"]["commands"]
    )
    manifest = PactManifest.model_validate(document)
    token = "req.demo:refuted_by:ev.operator-report.req-demo:operator_report:fail"
    assert parse_proof_slot(token).note == "refuted_by"
    assert (
        proof_slot_commands(
            manifest,
            "demo",
            [token],
            subject_requirements=["req.demo"],
        )
        == []
    )


# --------------------------------------------------------------------------
# Arm 2: the view surface reports the same quantity.
# --------------------------------------------------------------------------


def test_view_reports_gap_for_unnamed_kind_failure() -> None:
    """`pact view`'s `gaps` is the same computation the gate reads.

    Fixing only the gate would leave `gaps: 0` on a PACT the gate refuses --
    a second, quieter copy of the same defect.
    """
    clean = _document()
    assert _requirement_missing(clean) == []

    refuted = _document()
    refuted["evidence"].append(copy.deepcopy(REFUTING_ROW))
    missing = _requirement_missing(refuted)
    assert missing == [
        "refuted_by:ev.operator-report.req-demo:operator_report:fail"
    ]

    manifest = PactManifest.model_validate(refuted)
    alignment = validate_manifest(manifest).alignment
    assert alignment["requirements"]["req.demo"]["satisfaction_status"] == (
        "unsatisfied"
    )


# --------------------------------------------------------------------------
# Arm 3: the negative controls. Without these a blanket refusal passes.
# --------------------------------------------------------------------------


def _proof_is_clean(result: dict[str, Any]) -> None:
    """No proof gap, and nothing left that this change could have caused.

    The gate's `ok` is deliberately NOT asserted: in a bare tmp repo it is
    False for `ledger_not_fresh` / `pact_coordination_disagreement`, which are
    runtime-authority blockers this change does not touch. Asserting `ok` would
    make the control fail for the wrong reason today, and — worse — asserting
    it loosely would let a genuine proof refusal hide behind a ledger one. So
    the control names both halves: the proof quantity is empty, and every
    remaining blocker is an authority blocker.
    """
    assert result["missing_proof"] == []
    assert _refutations(result["missing_proof"]) == []
    assert result["blockers"] == []
    assert set(result["blocked_by"]) <= {
        "ledger_not_fresh",
        "pact_coordination_disagreement",
    }, result["blocked_by"]


def test_clean_requirement_still_closes(tmp_path: Path) -> None:
    """A passing required row and no failing rows closes exactly as before.

    Without this arm a blanket refusal would satisfy every other test here.
    """
    root = _saved(tmp_path, _document())

    _proof_is_clean(task_completion_gate(root, "demo", "task.demo"))


def test_a_failing_row_of_a_REQUIRED_kind_still_refuses(tmp_path: Path) -> None:
    """The pre-existing A/B arm, unchanged.

    Flipping the REQUIRED executed_test row to `fail` refused before this
    change and must still refuse, by the OLD path: the policy slot goes
    unfilled. It must not now be double-counted.
    """
    document = _document()
    document["evidence"][0]["status"] = "fail"
    root = _saved(tmp_path, document)

    result = task_completion_gate(root, "demo", "task.demo")

    assert result["ok"] is False
    assert "req.demo:executed_test" in result["missing_proof"]
    # Counted once: the unfilled slot AND the refutation are the same row, so
    # the refutation arm reports it too -- but as one token, not a duplicate
    # of the slot token.
    assert len(_refutations(result["missing_proof"])) == 1


def test_a_planned_row_of_an_unnamed_kind_does_not_refute(tmp_path: Path) -> None:
    """The decided boundary of the refuting status set.

    `planned` records an INTENTION, not a measurement. 10 of the 17 exposed
    pairs in the corpus sweep are future-tense design_review scaffolds;
    blocking on them would refuse a requirement for having declared a plan,
    and would pressure authors into DELETING scaffold rows to close a gate.
    """
    document = _document()
    document["evidence"].append(
        {
            "id": "ev.planned.design.demo",
            "kind": "design_review",
            "status": "planned",
            "refs": ["req.demo"],
            "summary": "Planned design review will define the finding taxonomy.",
        }
    )
    root = _saved(tmp_path, document)

    _proof_is_clean(task_completion_gate(root, "demo", "task.demo"))


def test_partial_and_missing_of_an_unnamed_kind_do_refute() -> None:
    """The other two members of the refuting set, each on its own.

    `partial` is a run that executed some of what was asked and could not
    execute the rest, and `missing` is an absent artifact. Both record an
    OUTCOME, which is the line the decision drew.
    """
    for status in ("partial", "missing"):
        document = _document()
        row = copy.deepcopy(REFUTING_ROW)
        row["status"] = status
        document["evidence"].append(row)
        assert _requirement_missing(document) == [
            f"refuted_by:ev.operator-report.req-demo:operator_report:{status}"
        ], status


def test_a_waiver_clears_the_view_gap_but_not_the_gate(tmp_path: Path) -> None:
    """The escape hatch, as MEASURED rather than as assumed.

    A requirement-scoped waiver takes a refutation out of the requirement
    alignment exactly as it takes out any other gap (validation.py:2771-2776),
    so `pact view` stops reporting it. It does NOT clear `gate task-done`: the
    task rollup calls `_missing_requirement_evidence` directly
    (validation.py:2819-2827) and never consults `_waiver_for`.

    That asymmetry is PRE-EXISTING and not introduced here -- the control
    below shows an ordinary `executed_test` gap behaving identically -- but it
    is recorded because it changes the honest answer to "how do I record a
    failure without blocking": at the gate, the working answer is to correct
    the row's status, not to waive it.
    """
    waiver = {
        "id": "waiver.demo",
        "scope": "req.demo",
        "owner": "riley",
        "reason": "The residual leak is accepted and bounded; see the row.",
    }

    refuted = _document()
    refuted["evidence"].append(copy.deepcopy(REFUTING_ROW))
    refuted["waivers"] = [copy.deepcopy(waiver)]
    manifest = PactManifest.model_validate(refuted)
    requirement_state = validate_manifest(manifest).alignment["requirements"][
        "req.demo"
    ]
    assert requirement_state["satisfaction_status"] == "waived"
    assert requirement_state["satisfaction_missing"] == []

    root = _saved(tmp_path, refuted)
    result = task_completion_gate(root, "demo", "task.demo")
    assert _refutations(result["missing_proof"]) == [
        "req.demo:refuted_by:ev.operator-report.req-demo:operator_report:fail"
    ]

    # The control: an ORDINARY gap, waived, reaches the gate the same way. So
    # the line above measures pre-existing waiver scope, not a refutation the
    # waiver failed to reach.
    plain = _document(evidence=[])
    plain["waivers"] = [copy.deepcopy(waiver)]
    plain_root = tmp_path / "plain"
    plain_root.mkdir()
    _saved(plain_root, plain)
    plain_result = task_completion_gate(plain_root, "demo", "task.demo")
    assert plain_result["missing_proof"] == ["req.demo:executed_test"]


def test_correcting_the_rows_status_is_the_repair_the_refusal_names(
    tmp_path: Path,
) -> None:
    """The route that does clear the gate, exercised rather than asserted."""
    document = _document()
    corrected = copy.deepcopy(REFUTING_ROW)
    corrected["status"] = "pass"
    corrected["summary"] = "Re-run after the batch route was closed: 0 rows."
    document["evidence"].append(corrected)
    root = _saved(tmp_path, document)

    _proof_is_clean(task_completion_gate(root, "demo", "task.demo"))


def test_a_refuting_row_referencing_an_acceptance_criterion_still_blocks() -> None:
    """A failure recorded against a criterion refutes the requirement that
    owns it, rather than sitting one indirection away from the gate.

    Two of the corpus hits (audit-docs-agent-surface-consistency) reference
    an acceptance id rather than the requirement id.
    """
    document = _document()
    document["requirements"][0]["acceptance"] = [
        {
            "id": "acc.demo",
            "description": "A live run measuring zero rows.",
            "evidence": ["executed_test"],
        }
    ]
    document["evidence"][0]["refs"] = ["req.demo", "acc.demo"]
    row = copy.deepcopy(REFUTING_ROW)
    row["refs"] = ["acc.demo"]
    document["evidence"].append(row)

    assert _refutations(_requirement_missing(document)) == [
        "refuted_by:ev.operator-report.req-demo:operator_report:fail"
    ]


def test_a_row_referencing_another_requirement_does_not_refute_this_one() -> None:
    """Scope control: refutation is per requirement, not per manifest."""
    document = _document()
    document["requirements"].append(
        {
            "id": "req.other",
            "title": "Other",
            "statement": "Something else holds.",
            "evidence_policy": {"required": ["executed_test"]},
        }
    )
    row = copy.deepcopy(REFUTING_ROW)
    row["refs"] = ["req.other"]
    document["evidence"].append(row)

    assert _refutations(_requirement_missing(document)) == []
