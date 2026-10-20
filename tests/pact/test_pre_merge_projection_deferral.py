"""A missing graph projection defers the merge gate; it never blocks it.

Issue #1476. `gate pre-merge` treated "the graph projection could not be read"
exactly like "this change is not approved": verdict `refused`, exit 1, the
projection tokens listed in `blocked_by`. During the Aug-Sep sprint a head with
green CI and a recorded operator approval waited because the local graph worker
was absent. The projection is a derived read-only view; a projector that is not
running is a fact about the projector.
"""

from __future__ import annotations

import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact import envelope
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact import workflow_authority as wa


def _seed(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    pact_operations.create_fast_path_change(
        root,
        "demo",
        title="Demo",
        intent="Demo must hold.",
        acceptance="The observable check.",
        test_id=(
            "tests/pact/test_pre_merge_projection_deferral.py"
            "::test_projection_only_blockers_defer_the_merge_gate"
        ),
    )
    return root


def _cli(root: Path, argv: tuple[str, ...]) -> tuple[int, dict[str, Any]]:
    stream = io.StringIO()
    with redirect_stdout(stream):
        rc = main(["pact", "--root", str(root), *argv, "--json"])
    return rc, json.loads(stream.getvalue().splitlines()[-1])


def _decision_gate(monkeypatch: pytest.MonkeyPatch, blockers: list[str]) -> None:
    """An approved ledger answer whose only unmet items are the ones given."""

    def fake_gate(root, change_id, capture=None):
        if capture is not None:
            capture["ledger_consulted"] = True
            capture["decisions"] = []
        return envelope.component(
            ok=not blockers,
            status="approved",
            intent_digest="sha256:deadbeef",
            principal="riley",
            decided_at="2026-09-02T00:00:00Z",
            workflow_authority={
                "accepted": not blockers,
                "blockers": list(blockers),
                "projection_only": wa.projection_only(blockers),
                "required_live_checks": ["refresh graph projection"],
            },
        )

    monkeypatch.setattr(pact_cli, "_ledger_decision_gate", fake_gate)
    monkeypatch.setattr(
        pact_cli,
        "_simple_gate",
        lambda root, change, name, lifecycle_gate=None: envelope.recorded(
            text=f"gate {name} {change}: recorded"
        ),
    )


class TestProjectionOnlyRule:
    """The membership test, which the CLI arm consults."""

    def test_graph_facts_are_projection_only(self) -> None:
        assert wa.projection_only(["graph_not_ready"]) is True
        assert wa.projection_only(["graph_not_fresh", "generation_pin_mismatch"]) is True

    def test_change_facts_are_not(self) -> None:
        assert wa.projection_only(["no_ledger_approval"]) is False
        assert wa.projection_only(["ledger_dsn_not_configured"]) is False
        assert wa.projection_only(["worktree_not_clean"]) is False

    def test_one_change_fact_disqualifies_the_whole_set(self) -> None:
        assert wa.projection_only(["graph_not_ready", "no_ledger_approval"]) is False

    def test_no_blockers_is_not_a_deferral(self) -> None:
        # A clean pass must stay exit 0, not be downgraded to exit 3.
        assert wa.projection_only([]) is False
        assert wa.projection_only(None) is False


class TestAgainstTheRealAuthorityFunction:
    """The stubbed verdict tests assume a blocker set; this checks it.

    `TestPreMergeVerdict` replaces the ledger read with a hand-built payload,
    so on its own it would only prove the CLI dispatches on the shape of a
    dict. These cases run the real `evaluate_workflow_authority` and confirm
    that an absent graph really does produce a projection-only blocker set,
    and that anything else does not.
    """

    def _inputs(self, **overrides: Any) -> wa.WorkflowAuthorityInput:
        base: dict[str, Any] = {
            "file_fresh": True,
            "graph_required": True,
            "graph_ready": True,
            "graph_fresh": True,
            "ledger_fresh": True,
            "ledger_approval": wa.LEDGER_APPROVAL_APPROVED,
            "generation_pin_matches": True,
            "evidence_artifacts_fresh": True,
            "pact_coordination_agree": True,
        }
        base.update(overrides)
        return wa.WorkflowAuthorityInput(**base)

    def test_an_absent_graph_produces_only_projection_blockers(self) -> None:
        # What `_graph_projection_status` reports with no reachable graph:
        # not ready, not fresh, and so the generation pin cannot match.
        decision = wa.evaluate_workflow_authority(
            self._inputs(
                graph_ready=False, graph_fresh=False, generation_pin_matches=False
            )
        )

        assert decision.accepted is False
        assert wa.projection_only(decision.blockers) is True

    def test_a_stale_graph_produces_only_projection_blockers(self) -> None:
        decision = wa.evaluate_workflow_authority(
            self._inputs(graph_fresh=False, generation_pin_matches=False)
        )

        assert wa.projection_only(decision.blockers) is True

    def test_an_unreachable_ledger_is_not_projection_only(self) -> None:
        decision = wa.evaluate_workflow_authority(
            self._inputs(
                ledger_fresh=False,
                ledger_approval=wa.LEDGER_APPROVAL_NOT_JUDGED,
                graph_ready=False,
                graph_fresh=False,
                generation_pin_matches=False,
            )
        )

        assert wa.projection_only(decision.blockers) is False

    def test_a_missing_approval_is_not_projection_only(self) -> None:
        decision = wa.evaluate_workflow_authority(
            self._inputs(ledger_approval=wa.LEDGER_APPROVAL_ABSENT)
        )

        assert wa.projection_only(decision.blockers) is False

    def test_a_stale_intent_event_is_not_projection_only(self) -> None:
        """The event plane is the authority, not a copy of it."""

        decision = wa.evaluate_workflow_authority(
            self._inputs(
                projection_authority="event_plane_intent",
                event_plane_authority_allowed=True,
                intent_event_fresh=False,
            )
        )

        assert "intent_event_not_fresh" in decision.blockers
        assert wa.projection_only(decision.blockers) is False

    def test_a_satisfied_authority_has_nothing_to_defer(self) -> None:
        decision = wa.evaluate_workflow_authority(self._inputs())

        assert decision.accepted is True
        assert wa.projection_only(decision.blockers) is False


class TestPreMergeVerdict:
    def test_projection_only_blockers_defer_the_merge_gate(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _decision_gate(
            monkeypatch,
            ["graph_not_ready", "graph_not_fresh", "generation_pin_mismatch"],
        )
        rc, payload = _cli(_seed(tmp_path), ("gate", "pre-merge", "demo"))

        assert payload["verdict"] == "deferred"
        assert rc == 3
        # No blockers: nothing about the change is unfit.
        assert payload["blocked_by"] == []
        # What could not be read is still named, in its own field.
        assert payload["projection_deferrals"] == [
            "graph_not_ready",
            "graph_not_fresh",
            "generation_pin_mismatch",
        ]
        assert "graph_not_ready" in payload["message"]

    def test_a_real_blocker_still_refuses(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _decision_gate(monkeypatch, ["graph_not_ready", "no_ledger_approval"])
        rc, payload = _cli(_seed(tmp_path), ("gate", "pre-merge", "demo"))

        assert payload["verdict"] == "refused"
        assert rc == 1
        assert "decision_gate_unsatisfied" in payload["blocked_by"]
        assert "no_ledger_approval" in payload["blocked_by"]

    def test_a_satisfied_gate_still_records(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _decision_gate(monkeypatch, [])
        rc, payload = _cli(_seed(tmp_path), ("gate", "pre-merge", "demo"))

        assert payload["verdict"] == "recorded"
        assert rc == 0

    def test_exit_code_follows_the_verdict(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _decision_gate(monkeypatch, ["graph_not_fresh"])
        rc, payload = _cli(_seed(tmp_path), ("gate", "pre-merge", "demo"))

        assert rc == envelope.exit_code(payload)
        assert payload["ok"] is False
