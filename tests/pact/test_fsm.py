"""PACT v4 lifecycle FSM: table validity, transition legality, derivation."""

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import fsm
from okg.substrate.pact.fsm import DecisionEvent, derive_state, legal_transition


def _d(verdict, *, from_state=None, to_state=None, target=None):
    return DecisionEvent(
        change_id="c",
        verdict=verdict,
        principal="riley",
        from_state=from_state,
        to_state=to_state,
        target=target,
    )


class TestTable:
    def test_shipped_table_is_valid(self):
        assert fsm.validate_table(fsm.load_table()) == []

    def test_orphan_state_detected(self):
        raw = {
            "states": {
                "draft": {"terminal": False},
                "limbo": {"terminal": False},
                "retired": {"terminal": True},
            },
            "verdicts": {
                "human": list(fsm.HUMAN_VERDICTS),
                "agent_only": list(fsm.AGENT_ONLY_VERDICTS),
                "operator_only": list(fsm.OPERATOR_ONLY_VERDICTS),
            },
            "transitions": [
                {"from": "draft", "event": "approve", "to": "retired"},
                {"from": "limbo", "event": "approve", "to": "retired"},
            ],
        }
        issues = fsm.validate_table(raw)
        assert any("orphan" in i for i in issues)

    def test_nonterminal_without_exit_detected(self):
        raw = {
            "states": {
                "draft": {"terminal": False},
                "stuck": {"terminal": False},
            },
            "verdicts": {
                "human": list(fsm.HUMAN_VERDICTS),
                "agent_only": list(fsm.AGENT_ONLY_VERDICTS),
                "operator_only": list(fsm.OPERATOR_ONLY_VERDICTS),
            },
            "transitions": [
                {"from": "draft", "event": "approve", "to": "stuck"},
            ],
        }
        issues = fsm.validate_table(raw)
        assert any("no exit" in i for i in issues)

    def test_terminal_exits_only_via_operator(self):
        # The shipped table's only terminal exit is retired --reopen-->.
        table = fsm.load_table()
        for t in table["transitions"]:
            if table["states"][t["from"]].get("terminal"):
                assert t["event"] in fsm.OPERATOR_ONLY_VERDICTS


class TestLegalTransition:
    def test_approve_at_review_retires(self):
        assert (
            legal_transition("awaiting_review", "approve", is_operator=True)
            == "retired"
        )

    def test_revise_at_review_goes_in_progress(self):
        assert (
            legal_transition("awaiting_review", "revise", is_operator=False)
            == "in_progress"
        )

    def test_defer_records_without_transition(self):
        assert legal_transition("awaiting_review", "defer", is_operator=True) is None

    def test_escalate_records_without_transition(self):
        assert legal_transition("proposed", "escalate", is_operator=False) is None

    def test_reopen_is_operator_only(self):
        with pytest.raises(SubstrateError) as exc:
            legal_transition("retired", "reopen", is_operator=False)
        assert exc.value.data["violated_guard"] == "operator_only"
        assert (
            legal_transition("retired", "reopen", is_operator=True)
            == "awaiting_review"
        )

    def test_supersede_requires_target(self):
        with pytest.raises(SubstrateError) as exc:
            legal_transition("in_progress", "supersede", is_operator=True)
        assert exc.value.data["violated_guard"] == "target_exists"
        assert (
            legal_transition(
                "in_progress", "supersede", is_operator=True, target="other"
            )
            == "superseded"
        )

    def test_illegal_verdict_names_guard_and_alternatives(self):
        with pytest.raises(SubstrateError) as exc:
            legal_transition("in_progress", "approve", is_operator=True)
        assert exc.value.data["violated_guard"] == "legal_transition"
        assert "supersede" in exc.value.data["legal_verdicts_here"]

    def test_unknown_verdict_refused(self):
        with pytest.raises(SubstrateError):
            legal_transition("in_progress", "lgtm", is_operator=True)


class TestDeriveState:
    def test_invalid_manifest_is_draft(self):
        d = derive_state(
            valid=False, tier=1, tasks_terminal=False,
            evidence_satisfied=False, decisions=[],
        )
        assert d.state == "draft" and d.pending_decision is None

    def test_tier1_skips_proposed(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=False,
            evidence_satisfied=False, decisions=[],
        )
        assert d.state == "in_progress"

    def test_tier2_waits_at_proposed(self):
        d = derive_state(
            valid=True, tier=2, tasks_terminal=False,
            evidence_satisfied=False, decisions=[],
        )
        assert d.state == "proposed" and d.pending_decision == "go-ahead"

    def test_tier2_approved_proceeds(self):
        d = derive_state(
            valid=True, tier=2, tasks_terminal=False, evidence_satisfied=False,
            decisions=[_d("approve", from_state="proposed", to_state="in_progress")],
        )
        assert d.state == "in_progress"

    def test_done_work_awaits_review(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=True,
            evidence_satisfied=True, decisions=[],
        )
        assert d.state == "awaiting_review" and d.pending_decision == "review"

    def test_terminal_from_ledger_beats_manifest(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=True, evidence_satisfied=True,
            decisions=[_d("approve", from_state="awaiting_review", to_state="retired")],
        )
        assert d.state == "retired"

    def test_reopen_cancels_terminal(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=True, evidence_satisfied=True,
            decisions=[
                _d("approve", from_state="awaiting_review", to_state="retired"),
                _d("reopen", from_state="retired", to_state="awaiting_review"),
            ],
        )
        assert d.state == "awaiting_review"

    def test_defer_never_transitions(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=True, evidence_satisfied=True,
            decisions=[_d("defer")],
        )
        assert d.state == "awaiting_review"

    def test_evidence_gap_keeps_in_progress(self):
        d = derive_state(
            valid=True, tier=1, tasks_terminal=True,
            evidence_satisfied=False, decisions=[],
        )
        assert d.state == "in_progress"
        assert any("evidence gaps" in r for r in d.reasons)
