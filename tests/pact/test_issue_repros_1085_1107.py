"""The #1085 and #1107 repro cases, as fixtures.

pact-vacuous-gate-close req.vacuous.presence closes both issues, and its
acceptance clause says they close "with their repro cases as fixtures". Both
are the same disease as the rest of the change -- a gate reading absence as
agreement -- so they are pinned here in the shape the issues describe, not
paraphrased:

  #1085  an acceptance is satisfied by the KIND of evidence attached to it and
         never by what the acceptance says, so a criterion demanding a live
         transcript closes on a hermetic pytest run whose own summary says the
         live arm never ran.

  #1107  an acceptance criterion declaring NO proof tokens computes
         `satisfied = True`: the per-token loop never runs, `satisfaction_missing`
         stays empty, and the criterion closes on nothing at all.

SCOPE. #1107 is closed by this change. #1085 is HALF closed, and the halves are
kept apart here on purpose:

  BINDING-ENFORCEMENT half (closed, and pre-existing behavior): an acceptance
  BOUND to an `ev.*` id refuses when that id is absent. Pinned below as a
  regression row -- `pact-vacuous-gate-close` did not add this refusal, it
  relies on it, so the tests exist to stop it weakening.

  PROSE half (OPEN): nothing compares the criterion's DESCRIPTION to the row
  that satisfies it, so a criterion may still be closed by a kind token. That
  is asserted below in its defective present tense rather than left out, so the
  gap is a failing-when-fixed row in the suite instead of an absence. It is
  re-scoped out of this change (req.vacuous.presence acceptance) to a follow-on
  PACT; #1085 stays open.

Each test asserts the refusal a reader can act on -- the code and the fact that
the task will not close -- rather than an internal shape, so a later refactor
of the alignment dict cannot silently reopen either issue.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import task_completion_gate
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import validate_manifest

# The hermetic row both issues turn on: a real, passing pytest run whose own
# summary states the arm the acceptance demands was NOT covered. The authors in
# #1085 were scrupulous enough to write the limitation down; the gate could not
# read it.
HERMETIC_ROW: dict[str, Any] = {
    "id": "ev.executed-test.hermetic",
    "kind": "executed_test",
    "status": "pass",
    "refs": ["req.live"],
    "artifact": "tests/pact/test_issue_repros_1085_1107.py",
    "command": "uv run pytest tests/pact -q",
    "summary": "PYTEST ARM ONLY -- the live-transcript arm is NOT covered.",
}


def _document(*, acceptance_proofs: list[str]) -> dict[str, Any]:
    """A change whose acceptance demands a live arm a hermetic run cannot give."""
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {"id": "repro", "title": "Repro", "status": "implementation"},
        "requirements": [
            {
                "id": "req.live",
                "title": "Live arm",
                "statement": "The server refuses an unauthenticated caller.",
                "evidence_policy": {"required": ["executed_test"]},
                "acceptance": [
                    {
                        "id": "acc.live-transcript",
                        "description": (
                            "A live transcript against an authenticated server "
                            "shows the refusal."
                        ),
                        "evidence": list(acceptance_proofs),
                    }
                ],
            }
        ],
        "tasks": [
            {
                "id": "task.live",
                "title": "Prove the live arm",
                "status": "todo",
                "verification": "uv run pytest tests/pact -q",
                "requirements": ["req.live"],
            }
        ],
        "evidence": [HERMETIC_ROW],
    }


def _errors(document: dict[str, Any]) -> list[tuple[str, str]]:
    manifest = PactManifest.model_validate(document)
    return [
        (issue.code, issue.message)
        for issue in validate_manifest(manifest).issues
        if issue.severity == "error"
    ]


def _gate(tmp_path: Path, document: dict[str, Any], task_id: str) -> dict[str, Any]:
    (tmp_path / "pact").mkdir(parents=True, exist_ok=True)
    save_manifest(tmp_path, PactManifest.model_validate(document))
    return task_completion_gate(tmp_path, "repro", task_id)


# --- #1107: a criterion declaring no proof tokens ---------------------------


def test_issue_1107_a_criterion_with_no_proof_tokens_is_refused() -> None:
    """`evidence: []` used to compute satisfied on nothing whatsoever."""
    codes = {code for code, _message in _errors(_document(acceptance_proofs=[]))}
    assert "missing_acceptance_evidence" in codes


def test_issue_1107_the_task_cannot_close_over_a_tokenless_criterion(
    tmp_path: Path,
) -> None:
    """The refusal reaches the gate, not just the linter.

    An error visible only in `validate` would leave the closing path -- the one
    that actually marks work done -- as vacuous as before.
    """
    document = _document(acceptance_proofs=[])
    result = _gate(tmp_path, document, "task.live")
    assert result["ok"] is False
    assert result["verdict"] == "refused"
    assert "missing_acceptance_evidence" in {
        issue["code"] for issue in result["validation_errors"]
    }
    # `missing_proof` is still empty -- the issue's exact reading, "nothing is
    # owed" -- so the refusal has to be carried by `blocked_by`, or a reader of
    # the owed list alone would correctly conclude the criterion was met.
    assert result["missing_proof"] == []
    assert "missing_acceptance_evidence" in result["blocked_by"]


# --- #1085 BINDING-ENFORCEMENT half: regression rows, not new behavior ------
#
# Both tests below pin behavior that already refused before this change. They
# are here so the half of #1085 this change RELIES on cannot weaken while the
# other half stays open.


def test_issue_1085_binding_half_a_bound_live_arm_is_not_closed_by_a_hermetic_row(
    tmp_path: Path,
) -> None:
    """Regression pin: bind the arm to an `ev.*` id, and the kind stops standing in.

    The acceptance names a live transcript and binds it to `ev.live-transcript`.
    A passing `executed_test` row of the SAME kind the requirement's policy
    demands is present -- the exact configuration that closed the criterion
    before -- and must not satisfy it. This is the ENFORCEABLE half of #1085,
    and it worked before `pact-vacuous-gate-close`; the assertion guards it,
    it does not claim it.
    """
    document = _document(acceptance_proofs=["ev.live-transcript"])
    result = _gate(tmp_path, document, "task.live")
    assert result["ok"] is False
    assert result["verdict"] == "refused"
    # The bound row does not exist yet, so the criterion names it as unknown
    # rather than quietly resolving to the hermetic row's kind.
    assert "unknown_acceptance_evidence" in {
        issue["code"] for issue in result["validation_errors"]
    }
    assert "unknown_acceptance_evidence" in result["blocked_by"]


def test_issue_1085_binding_half_the_bound_live_row_is_what_satisfies_the_criterion(
    tmp_path: Path,
) -> None:
    """The paired positive: with the live row present, the criterion is met.

    Without this the test above would pass just as happily against a gate that
    refuses everything -- the specificity failure the catch rate is paired
    against.
    """
    document = _document(acceptance_proofs=["ev.live-transcript"])
    document["evidence"] = [
        HERMETIC_ROW,
        {
            "id": "ev.live-transcript",
            "kind": "executed_test",
            "status": "pass",
            "refs": ["req.live", "acc.live-transcript"],
            "artifact": "tests/pact/test_issue_repros_1085_1107.py",
            "command": "uv run pytest tests/pact -q",
            "summary": "Live transcript against the authenticated server.",
        },
    ]
    codes = {code for code, _message in _errors(document)}
    assert "unknown_acceptance_evidence" not in codes
    assert "missing_acceptance_evidence" not in codes

    result = _gate(tmp_path, document, "task.live")
    assert "unknown_acceptance_evidence" not in {
        issue["code"] for issue in result["validation_errors"]
    }
    # The acceptance criterion itself is no longer owed.
    assert not [
        entry for entry in result.get("missing_proof", [])
        if "acc.live-transcript" in str(entry)
    ]


# --- #1085 PROSE half: still open, asserted in its defective present tense ---


def test_issue_1085_prose_half_is_still_open_a_kind_token_closes_a_live_criterion(
    tmp_path: Path,
) -> None:
    """OPEN GAP, pinned as behavior: the gate cannot read the description.

    The criterion says "a live transcript against an authenticated server", and
    its only proof token is the KIND `executed_test`. The one row in the
    manifest is the hermetic run whose own summary states the live arm was NOT
    covered. Nothing refuses: a kind token is a valid proof token
    (validation.py `_acceptance_proof_issues`, ACCEPTANCE_PROOF_TOKENS /
    `kind_registry.is_known`), and no clause compares the criterion's prose to
    the row that satisfies it.

    This asserts what the gate DOES today, not what it should do, so the gap is
    a load-bearing row: the follow-on PACT that teaches the gate to bind prose
    demands to rows will red this test, which is the point. Do not "fix" it by
    deleting it -- rewrite it when #1085's prose half lands.
    """
    document = _document(acceptance_proofs=["executed_test"])
    codes = {code for code, _message in _errors(document)}
    assert "unknown_acceptance_evidence" not in codes
    assert "missing_acceptance_evidence" not in codes

    result = _gate(tmp_path, document, "task.live")
    # Nothing is owed for the criterion, and no validation error names it --
    # the live arm the description demands is unproven and unremarked.
    assert not [
        entry for entry in result.get("missing_proof", [])
        if "acc.live-transcript" in str(entry)
    ]
    assert not [
        issue for issue in result["validation_errors"]
        if "acc.live-transcript" in issue.get("message", "")
    ]
    # And the manifest's only evidence row says so in plain text, which is
    # exactly the material the gate still cannot use.
    assert "NOT covered" in HERMETIC_ROW["summary"]
