"""Which post-approval edits keep an approval, and which legitimately drop it.

Issue #1476. The working belief during the sprint was "any edit to pact.yaml
after approval resets the state to `proposed`", so agents stopped attaching
evidence until re-approval and the CI lane alone spent fourteen approve turns.
The truth is narrower and worth pinning as a contract: an approval binds the
INTENT digest, and proof attachment is not intent.

The allowed set - edits that keep an approval alive:

- `change.status`
- any task's `status`, `evidence` and `blocked_reason`
- the top-level `evidence` list
- `proof_receipts`
- the `edits` journal

Everything else is intent: requirements, scope, tier, task verification, the
test plan. Changing any of them legitimately drops the approval, because the
thing that was approved is no longer the thing in the file.
"""

from __future__ import annotations

from copy import deepcopy

import pytest

from okg.substrate.pact.fsm import DecisionEvent, derive_state
from okg.substrate.pact.models import Evidence, PactManifest
from okg.substrate.pact.store import intent_digest


def _manifest() -> PactManifest:
    return PactManifest.model_validate(
        {
            "pact_version": 4,
            "target_pact_version": 4,
            "change": {
                "id": "demo",
                "title": "Demo",
                "status": "proposal",
                "tier": 2,
            },
            "requirements": [
                {
                    "id": "req.demo",
                    "title": "Demo",
                    "statement": "Demo must hold.",
                    "evidence_policy": {"required": ["planned_test"]},
                    "acceptance": [
                        {
                            "id": "acc.demo",
                            "description": "The observable check.",
                            "evidence": ["planned_test"],
                        }
                    ],
                }
            ],
            "tasks": [
                {
                    "id": "task.demo",
                    "title": "Do demo",
                    "verification": "pytest tests/pact -q",
                    "requirements": ["req.demo"],
                }
            ],
            "test_plan": [
                {
                    "id": "test.demo",
                    "requirement": "req.demo",
                    "test_id": (
                        "tests/pact/test_approval_survives_proof_edits.py"
                        "::TestProofEditsKeepTheApproval::test_state_survives"
                    ),
                }
            ],
        }
    )


def _attach_proof(manifest: PactManifest) -> PactManifest:
    """Everything an implementing agent writes while proving the change."""

    edited = deepcopy(manifest)
    edited.change.status = "verification"
    edited.tasks[0].status = "done"
    edited.tasks[0].evidence.append("ev.pytest.req.demo")
    edited.tasks[0].blocked_reason = None
    edited.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="pass",
            refs=["req.demo"],
            summary="3 passed, 0 failed, 0 skipped (junit.xml)",
        )
    )
    return edited


class TestProofEditsKeepTheApproval:
    def test_digest_is_unchanged(self) -> None:
        manifest = _manifest()
        assert intent_digest(_attach_proof(manifest)) == intent_digest(manifest)

    def test_state_survives(self) -> None:
        """The approval still applies, so the change does not fall back."""

        manifest = _manifest()
        approved_digest = intent_digest(manifest)
        approval = DecisionEvent(
            change_id="demo",
            verdict="approve",
            principal="riley",
            from_state="proposed",
            to_state="in_progress",
            intent_digest=approved_digest,
        )
        edited = _attach_proof(manifest)

        derived = derive_state(
            valid=True,
            tier=2,
            tasks_terminal=True,
            evidence_satisfied=True,
            decisions=[approval],
            intent_digest=intent_digest(edited),
        )

        assert derived.state == "awaiting_review"
        assert derived.state != "proposed"


class TestIntentEditsDropTheApproval:
    @pytest.mark.parametrize(
        "mutate",
        [
            pytest.param(
                lambda m: setattr(m.requirements[0], "statement", "Something else."),
                id="requirement_statement",
            ),
            pytest.param(
                lambda m: setattr(
                    m.requirements[0].acceptance[0], "description", "A new promise."
                ),
                id="acceptance",
            ),
            pytest.param(
                lambda m: setattr(m.tasks[0], "verification", "pytest -k other"),
                id="task_verification",
            ),
            pytest.param(
                lambda m: setattr(m.test_plan[0], "test_id", "tests/pact/other.py"),
                id="test_plan",
            ),
            pytest.param(
                lambda m: setattr(m.change, "tier", 3),
                id="tier",
            ),
            pytest.param(
                lambda m: setattr(m.change, "title", "A different change"),
                id="title",
            ),
        ],
    )
    def test_scope_change_changes_the_digest(self, mutate) -> None:
        manifest = _manifest()
        original = intent_digest(manifest)
        edited = deepcopy(manifest)
        mutate(edited)
        assert intent_digest(edited) != original

    def test_scope_change_returns_a_tier_2_change_to_proposed(self) -> None:
        manifest = _manifest()
        approval = DecisionEvent(
            change_id="demo",
            verdict="approve",
            principal="riley",
            from_state="proposed",
            to_state="in_progress",
            intent_digest=intent_digest(manifest),
        )
        edited = deepcopy(manifest)
        edited.requirements[0].statement = "Something else."

        derived = derive_state(
            valid=True,
            tier=2,
            tasks_terminal=True,
            evidence_satisfied=True,
            decisions=[approval],
            intent_digest=intent_digest(edited),
        )

        assert derived.state == "proposed"


class TestOperatorBriefOffersOnlyLegalVerdicts:
    """The brief must not name a verdict `decide` will then refuse.

    The brief used to print the whole vocabulary in every state, so an
    `in_progress` change advertised `revise` and `decide` answered "verdict
    revise is not legal in state in_progress" - the loop that consumed three
    operator approval rounds (issue #1476).

    These cases check the brief against `fsm.derive_state` and
    `fsm.legal_verdicts` - the same functions the refusal reads - rather than
    against the brief's own idea of the state, which would only prove it is
    self-consistent.
    """

    def _brief(self, manifest, *, tasks_terminal: bool, satisfied: bool, **kw):
        from okg.substrate.pact.markdown_view import operator_brief

        alignment = {
            "lifecycle": {},
            "requirements": {
                req.id: {
                    "satisfaction_status": "satisfied" if satisfied else "missing"
                }
                for req in manifest.requirements
            },
            "tasks": {
                task.id: {
                    "satisfaction_status": (
                        "satisfied" if tasks_terminal else "missing"
                    )
                }
                for task in manifest.tasks
            },
        }
        return operator_brief(manifest, alignment, **kw)

    def _fsm_state(self, manifest, *, tasks_terminal: bool, satisfied: bool) -> str:
        return derive_state(
            valid=True,
            tier=manifest.change.tier or 0,
            tasks_terminal=tasks_terminal,
            evidence_satisfied=satisfied,
            decisions=[],
        ).state

    @pytest.mark.parametrize("tier", [1, 2])
    @pytest.mark.parametrize(
        ("tasks_terminal", "satisfied"),
        [(False, False), (True, False), (True, True)],
    )
    def test_brief_state_is_the_state_the_machine_derives(
        self, tier: int, tasks_terminal: bool, satisfied: bool
    ) -> None:
        from okg.substrate.pact.fsm import legal_verdicts

        manifest = _manifest()
        manifest.change.tier = tier
        expected = self._fsm_state(
            manifest, tasks_terminal=tasks_terminal, satisfied=satisfied
        )
        brief = self._brief(
            manifest, tasks_terminal=tasks_terminal, satisfied=satisfied
        )

        assert brief["human_choices_state"] == expected
        assert set(brief["human_choices"]) <= set(legal_verdicts(expected))

    def test_a_tier_1_change_in_progress_is_not_told_to_revise(self) -> None:
        """The exact reported loop, at the shape `okg pact new` produces."""

        manifest = _manifest()
        manifest.change.tier = 1
        brief = self._brief(manifest, tasks_terminal=False, satisfied=False)

        assert brief["human_choices_state"] == "in_progress"
        assert set(brief["human_choices"]) == {"defer", "supersede"}
        assert "revise" not in brief["decision_needed"]

    def test_a_tier_2_change_awaiting_go_ahead_is_offered_approve(self) -> None:
        """The opposite error: hiding the approval the operator owes."""

        manifest = _manifest()
        manifest.change.tier = 2
        brief = self._brief(manifest, tasks_terminal=True, satisfied=False)

        assert brief["human_choices_state"] == "proposed"
        assert "approve" in brief["human_choices"]

    def test_a_caller_that_read_the_ledger_wins(self) -> None:
        """The CLI passes its derived state; the brief must not overrule it."""

        from okg.substrate.pact.fsm import legal_verdicts

        manifest = _manifest()
        manifest.change.tier = 1
        brief = self._brief(
            manifest,
            tasks_terminal=False,
            satisfied=False,
            state="awaiting_review",
        )

        assert brief["human_choices_state"] == "awaiting_review"
        assert "approve" in brief["human_choices"]
        assert set(brief["human_choices"]) <= set(legal_verdicts("awaiting_review"))

    def test_a_legacy_authored_status_is_not_taken_for_a_state(self) -> None:
        manifest = _manifest()
        manifest.change.tier = 1
        brief = self._brief(
            manifest, tasks_terminal=False, satisfied=False, state="implementation"
        )

        assert brief["human_choices_state"] == "in_progress"
