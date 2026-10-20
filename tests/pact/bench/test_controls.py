"""Positive controls for the branches today's measurements never reach.

Every scored row in the baseline is currently negative: no refusal classifies
SELF_REPAIRING, no episode reaches green. A metric whose "good" branch is never
exercised is indistinguishable from one that CANNOT report good — the harness
would report the same zeros if the branch were simply broken.

So each dead branch gets a synthetic subject, asserted to reach it. These are
CONTROLS ONLY: they construct their own subjects, never touch the measured
corpus, and contribute no rows to `metrics.json`, so they cannot flatter the
ratchet.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okg.substrate.pact import envelope as pact_envelope
from okg.substrate.pact.bench import calls, commands, episodes, fixtures, refusals
from okg.substrate.pact.bench.calls import CallResult, Recorder
from okg.substrate.pact.bench.isolation import (
    FixedClock,
    RuntimeControls,
    isolated_runtime,
)
from okg.substrate.pact.bench.runner import BenchResult

UNPROVEN = "bench-honesty-unproven"
PROVEN = "bench-honesty-proven"

# A repair that really does clear a blocker: the change's one requirement wants
# `executed_test` proof and has none, so attaching it removes the missing-proof
# blocker. Fits the repo command guards (one statement, well under 500 chars).
REPAIR_COMMAND = (
    f"okg pact evidence attach {UNPROVEN} req.one "
    "--kind executed_test --ref tests/bench/test_probe.py"
)


@pytest.fixture()
def control_root(bench_result: BenchResult, tmp_path: Path) -> Path:
    """A private copy of the fixture repo. Never the measured tree."""
    root = tmp_path / "repo"
    shutil.copytree(bench_result.fixture_root, root, symlinks=True)
    return root


def test_control_a_genuinely_repairing_refusal_classifies_command_repairable(
    control_root: Path,
) -> None:
    """The repaired branch of COMMAND_REPAIRABLE is reachable, on a real repair.

    Only the CLI's *emission* of the command is synthetic. Everything the
    classifier does with it is real: the command is parsed, guard-checked,
    checked for coverage, EXECUTED against a copy of the fixture, and the
    refused call is then replayed to see whether `blocked_by` actually shrank.
    """
    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        recorder = Recorder(root=control_root)
        refusal = calls.gate_task_done(recorder, control_root, UNPROVEN, "task.one")

        # Precondition: this really is a refusal that names its offending
        # value. If PACT stops refusing here, the control is measuring nothing.
        assert refusal.ok is False
        assert refusal.payload.get("missing_proof")
        before = commands.blocked_by(refusal.payload)
        assert any(token.startswith("missing_proof:") for token in before)

        # Inject what an honest envelope would carry, and nothing else.
        synthetic = CallResult(
            verb=refusal.verb,
            payload={
                **refusal.payload,
                "message": "task.one has no executed_test evidence for req.one",
                # `next_commands` is the ENVELOPE key the classifier reads;
                # `recommended_commands` is the pre-envelope detail. Injecting
                # only the latter left the payload's own (non-repairing) first
                # command in play, so the control measured nothing.
                "next_commands": [REPAIR_COMMAND],
                "recommended_commands": [REPAIR_COMMAND],
            },
            raised=refusal.raised,
            size_bytes=refusal.size_bytes,
            origin=dict(refusal.origin),
        )

        row = refusals.classify(control_root, synthetic)

    assert row["population"] == refusals.COMMAND_REPAIRABLE, row
    assert row["repaired"] is True, row
    assert row["reason"] is None
    assert row["first_command_executable"] is True
    assert row["repair"]["status"] == "executed"
    assert row["repair"]["shrank_blocked_by"] is True
    assert row["repair"]["blocked_by_after"] < row["repair"]["blocked_by_before"]


def test_control_the_repair_leaves_the_measured_fixture_untouched(
    bench_result: BenchResult, control_root: Path
) -> None:
    """A control must not be able to edit the corpus the baseline measured."""
    measured = bench_result.fixture_root / "pact" / "changes" / UNPROVEN / "pact.yaml"
    before = measured.read_bytes()

    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        recorder = Recorder(root=control_root)
        refusal = calls.gate_task_done(recorder, control_root, UNPROVEN, "task.one")
        synthetic = CallResult(
            verb=refusal.verb,
            payload={
                **refusal.payload,
                "message": "synthetic",
                "next_commands": [REPAIR_COMMAND],
                "recommended_commands": [REPAIR_COMMAND],
            },
            raised=False,
            size_bytes=refusal.size_bytes,
            origin=dict(refusal.origin),
        )
        refusals.classify(control_root, synthetic)

    assert measured.read_bytes() == before


def test_control_a_closing_episode_scores_green_at_its_real_call_count(
    control_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The green branch of `_run_episode` is reachable, and scores `calls`.

    Today every episode stalls and therefore scores the cap, so the
    `scored_calls == calls` arm never runs. Here the gate is made to close on
    a genuinely proven change; the END-STATE GUARD is left entirely real, so a
    green that degraded proof would still fail.
    """
    episode = episodes.Episode(
        id="control_closes_immediately",
        change=PROVEN,
        task="task.one",
        audited_class="positive control — not a measured episode",
    )

    real_gate = calls.gate_task_done

    def _closing_gate(
        recorder: Recorder, root: Path, change_id: str, task_id: str
    ) -> CallResult:
        result = real_gate(recorder, root, change_id, task_id)
        # Simulate the ledger being reachable: everything else about the
        # payload — its proof obligation, its task status — is real. The
        # simulation goes through the envelope constructor so it produces a
        # COHERENT payload; stamping a bare `ok: True` beside the real
        # `verdict: refused` would be the exact incoherence the honesty
        # classifier exists to reject.
        return recorder.record(
            result.verb,
            {
                **pact_envelope.restate(
                    result.payload,
                    pact_envelope.RECORDED,
                    blocked_by=[],
                ),
                "marked": True,
            },
            raised=False,
            origin=result.origin,
        )

    monkeypatch.setattr(episodes.calls, "gate_task_done", _closing_gate)

    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        row = episodes._run_episode(Recorder(root=control_root), control_root, episode)

    assert row["green"] is True, row
    assert row["closed"] is True
    assert row["stalled"] is None
    assert row["harness_gap"] is None
    assert row["scored"] is True
    # The point of the control: a green episode scores its REAL call count,
    # not the cap.
    assert row["scored_calls"] == row["calls"] < episodes.CALL_CAP
    guard = row["end_state_guard"]
    assert guard["passed"] is True
    assert guard["task_done_ok"] is True
    assert guard["evidence_kinds_resolve"] is True
    assert guard["quality_not_worse"] is True


def test_control_a_green_episode_still_fails_the_guard_on_degraded_proof(
    control_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Closing the gate is not sufficient: the end-state guard still binds.

    Without this, the previous control would only prove that `ok: True` makes
    an episode green — including one that closed by throwing its proof away.
    """
    episode = episodes.Episode(
        id="control_closes_with_bad_evidence",
        change=PROVEN,
        task="task.one",
        audited_class="negative control — not a measured episode",
    )

    real_gate = calls.gate_task_done

    def _closing_gate(
        recorder: Recorder, root: Path, change_id: str, task_id: str
    ) -> CallResult:
        result = real_gate(recorder, root, change_id, task_id)
        return recorder.record(
            result.verb,
            {
                **pact_envelope.restate(
                    result.payload,
                    pact_envelope.RECORDED,
                    blocked_by=[],
                ),
                "marked": True,
            },
            raised=False,
            origin=result.origin,
        )

    monkeypatch.setattr(episodes.calls, "gate_task_done", _closing_gate)

    # Degrade the proof the way a gate-closing-at-any-cost agent would: swap
    # the evidence kind for one outside the registry.
    import yaml

    path = control_root / "pact" / "changes" / PROVEN / "pact.yaml"
    document = yaml.safe_load(path.read_text(encoding="utf-8"))
    document["evidence"][0]["kind"] = fixtures.UNREGISTERED_KIND
    path.write_text(yaml.safe_dump(document, sort_keys=False), encoding="utf-8")

    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        row = episodes._run_episode(Recorder(root=control_root), control_root, episode)

    assert row["closed"] is True
    assert row["end_state_guard"]["evidence_kinds_resolve"] is False
    assert row["end_state_guard"]["passed"] is False
    assert row["green"] is False
    assert row["scored_calls"] == episodes.CALL_CAP


def test_control_a_deferred_verdict_alone_does_not_close_an_episode() -> None:
    """The terminal rule checks the deferral's CLAIMS, not its token.

    A deferral asserts that the subject holds and only the durable write is
    queued. Trusting the token alone meant a verb that stamped `deferred` over
    an unmet subject closed an episode for free — the same "absence dressed as
    agreement" shape the honesty metric exists to catch, one layer up.
    """
    ledger_only = ["ledger_dsn_not_configured"]

    def _row(**payload: object) -> CallResult:
        return CallResult(
            verb="gate task-done",
            payload={
                "verdict": "deferred",
                "ok": False,
                "blocked_by": ledger_only,
                "closure_intent": {"intent_id": "ci.probe"},
                "missing_proof": [],
                "validation_errors": [],
                **payload,
            },
            raised=False,
            size_bytes=0,
        )

    assert episodes._reached_terminal_state(_row()) is True
    # No intent was queued: the proof was NOT preserved, so nothing was
    # deferred — it was simply refused with a friendlier word.
    assert episodes._reached_terminal_state(_row(closure_intent=None)) is False
    # A proof slot is still open.
    assert (
        episodes._reached_terminal_state(_row(missing_proof=["req.one:executed_test"]))
        is False
    )
    # The manifest does not validate; a deferral may not stand on it.
    assert (
        episodes._reached_terminal_state(
            _row(validation_errors=[{"severity": "error", "code": "x"}])
        )
        is False
    )
    # A non-ledger cause is not a deferral this harness may accept.
    assert (
        episodes._reached_terminal_state(_row(blocked_by=["graph_not_ready"])) is False
    )


def test_control_a_closing_episode_fails_the_guard_on_out_of_scope_proof(
    control_root: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The anti-fabrication arm: closing with SOMEONE ELSE'S proof is not green.

    A repair path that can attach evidence can also attach the wrong evidence,
    and the resulting manifest passes every count-based check — the gate
    closes, the kinds resolve, the quality score rises. This control writes a
    row attached to a requirement the refused task does not prove, exactly as a
    manifest-wide acceptance sweep did, and requires the guard to catch it.
    """
    import yaml

    change = "bench-control-fabricated"
    manifest = fixtures.proven_manifest(change)
    manifest["requirements"].append({
        "id": "req.two",
        "title": "Requirement two",
        "statement": "The behavior named by req.two must hold.",
        "evidence_policy": {"required": ["executed_test"]},
        "scenarios": [],
        "acceptance": [
            {"id": "acc.two", "description": "the other check", "evidence": []}
        ],
    })
    path = control_root / "pact" / "changes" / change / "pact.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")

    episode = episodes.Episode(
        id="control_fabricates_proof",
        change=change,
        task="task.one",
        audited_class="negative control — not a measured episode",
    )
    real_gate = calls.gate_task_done

    def _fabricating_gate(
        recorder: Recorder, root: Path, change_id: str, task_id: str
    ) -> CallResult:
        # The fabrication: proof for `req.two`, which `task.one` does not
        # prove, written as if a repair command had attached it.
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not any(row["id"] == "ev.fabricated" for row in document["evidence"]):
            document["evidence"].append(
                fixtures.evidence_row("ev.fabricated", refs=["req.two"])
            )
            path.write_text(
                yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
            )
        result = real_gate(recorder, root, change_id, task_id)
        return recorder.record(
            result.verb,
            {
                **pact_envelope.restate(
                    result.payload, pact_envelope.RECORDED, blocked_by=[]
                ),
                "marked": True,
            },
            raised=False,
            origin=result.origin,
        )

    monkeypatch.setattr(episodes.calls, "gate_task_done", _fabricating_gate)

    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        row = episodes._run_episode(Recorder(root=control_root), control_root, episode)

    assert row["closed"] is True
    guard = row["end_state_guard"]
    assert guard["evidence_in_refusal_scope"] is False, guard
    assert guard["fabricated_evidence"][0]["evidence"] == "ev.fabricated"
    assert guard["fabricated_evidence"][0]["reason"] == "ref_outside_refusal_scope"
    assert guard["passed"] is False
    assert row["green"] is False
    assert row["scored_calls"] == episodes.CALL_CAP


def test_controls_contribute_no_rows_to_the_measured_baseline(
    bench_result: BenchResult,
) -> None:
    """The synthetic subjects above must never appear in `metrics.json`."""
    import json

    metrics = bench_result.metrics["metrics"]
    episode_ids = {row["episode"] for row in metrics["round_trips_to_green"]["episodes"]}
    assert episode_ids == {episode.id for episode in episodes.EPISODES}
    assert not any(name.startswith("control_") for name in episode_ids)
    # The controls run against their own copies of the fixture and a
    # FABRICATED closing gate; none of that may reach the published numbers.
    # (This used to assert `self_repairing == 0` — a pin on the measured value
    # of the day, not on control leakage, so it reddened the moment refusals
    # started repairing themselves. The ratchet owns the values; this test owns
    # the isolation and the internal consistency of each published rate.)
    encoded = json.dumps(metrics)
    assert "control_" not in encoded
    refusal_metrics = metrics["refusal_self_sufficiency"]
    for name, population in refusal_metrics["populations"].items():
        assert population["repaired"] == sum(
            1
            for row in refusal_metrics["rows"]
            if row["population"] == name and row["repaired"]
        ), name
    for row in refusal_metrics["rows"]:
        if not row["repaired"]:
            continue
        # A repaired row is one whose repair was really CARRIED OUT against a
        # copy of the fixture — a command executed and the cause set shrunk, a
        # descriptor applied and the code cleared — never one asserted. The
        # operator population is the exception by construction: its repair is
        # a named owner and remedy, and there is nothing to execute.
        if row["population"] == refusals.COMMAND_REPAIRABLE:
            assert row["repair"]["status"] == "executed", row
            assert row["repair"]["shrank_blocked_by"] is True, row
        elif row["population"] == refusals.AUTHOR_ACTION:
            assert row["repair"]["status"] == "applied", row
            assert row["repair"]["cleared"] is True, row
        else:
            assert row["names_owner"] and row["remedy_is_cause_derived"], row
