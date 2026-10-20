"""Corrected re-invocation, and the fences on it.

`pact-self-repairing-refusals` req.refusal.executable. A bad-argument refusal
(unknown id, duplicate id) is not repaired by a NEW command — it is repaired by
the SAME command with the one wrong token corrected. That makes the correction
a derived thing, and a derived thing needs fences or it becomes a guess:

* B1 the candidates must be difflib hits, not a truncated listing of the legal
  set. `repair.near_misses` falls back to `sorted(pool)[:3]` for a totally
  unrelated id, and treating that first element as "what you meant" is the
  `legal_values[0]` guess in another costume;
* B4 exactly one argv token may differ;
* B5 a duplicate change id is repaired by USING the existing change, never by
  scaffolding a second one.
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from okg.substrate.pact.bench import calls, commands, refusals
from okg.substrate.pact.bench.calls import Recorder
from okg.substrate.pact.bench.isolation import (
    FixedClock,
    RuntimeControls,
    isolated_runtime,
)
from okg.substrate.pact.bench.runner import BenchResult

PROVEN = "bench-honesty-proven"


@pytest.fixture()
def control_root(bench_result: BenchResult, tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    shutil.copytree(bench_result.fixture_root, root, symlinks=True)
    return root


def test_an_unknown_task_is_repaired_by_re_invoking_with_the_near_miss(
    control_root: Path,
) -> None:
    """The audited shape: `gate task-done <change> task.on`.

    The refusal names the near miss; the repair is the same verb with that one
    token corrected, and it is judged against the ORIGINAL cause set.
    """
    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        recorder = Recorder(root=control_root)
        refusal = calls.gate_task_done(recorder, control_root, PROVEN, "task.on")
        assert refusal.payload["ok"] is False

        correction = refusals._correction(refusal.payload, refusal.origin)
        assert correction is not None, refusal.payload
        assert correction["kind"] == "corrected_argument"
        assert correction["origin"]["task"] == "task.one"
        # B4: the same verb, one token.
        assert (
            commands.differing_argv_tokens(
                commands.render_origin(refusal.origin),
                commands.render_origin(correction["origin"]),
            )
            == 1
        )

        row = refusals.classify(control_root, refusal)

    assert row["population"] == refusals.COMMAND_REPAIRABLE, row
    assert row["correction"] == "corrected_argument"
    assert row["repaired"] is True, row


def test_a_listing_fallback_never_becomes_a_correction(control_root: Path) -> None:
    """B1/B5: below the cutoff, the candidate list is a MENU.

    `near_misses` prints the legal set when nothing is close, so the payload
    always has candidates. Re-invoking with the first of them would run a
    command the caller never meant — against a real task id, it would close the
    WRONG task.
    """
    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        recorder = Recorder(root=control_root)
        refusal = calls.gate_task_done(
            recorder, control_root, PROVEN, "zzz-nothing-like-a-task-id"
        )
        data = refusal.payload["error"]["data"]
        # The precondition: candidates ARE published (the refusal is complete),
        # and they are labelled as a listing rather than a correction.
        assert data["candidates"] == ["task.one"]
        assert data["candidate_source"] == "listing"

        assert refusals._correction(refusal.payload, refusal.origin) is None


def test_a_duplicate_change_id_is_repaired_by_using_the_existing_change(
    control_root: Path,
) -> None:
    """Never a second scaffold — the audited hint's exact mistake."""
    controls = RuntimeControls(fixture_root=control_root)
    with isolated_runtime(controls, clock=FixedClock()):
        recorder = Recorder(root=control_root)
        refusal = calls.new_change(recorder, control_root, PROVEN)
        assert refusal.payload["ok"] is False

        data = refusal.payload["error"]["data"]
        assert data["cause"] == "duplicate_change_id"
        command = refusal.payload["next_commands"][0]
        assert "pact view" in command
        assert "pact new" not in command

        correction = refusals._correction(refusal.payload, refusal.origin)
        assert correction is not None
        assert correction["kind"] == "use_existing"
        assert correction["origin"]["change"] == PROVEN

        row = refusals.classify(control_root, refusal)

    assert row["population"] == refusals.COMMAND_REPAIRABLE, row
    assert row["repaired"] is True, row


def test_operator_action_tokens_are_subtracted_from_both_sides() -> None:
    """The harness offers no DSN, so the ledger token is in every cause set.

    Leaving it in the comparison made a genuine repair — one that cleared the
    only thing it could clear — read as "did not shrink", because the ledger
    token survived on both sides and the strict-subset test failed.
    """
    # The observed shape: completing the proof turns the refusal into a
    # DEFERRAL, and the deferral names the ledger with a different token than
    # the refusal did. Raw, that reads as a cause the repair INTRODUCED.
    before = ["ledger_dsn_not_configured", "missing_proof:req.one:executed_test"]
    after = ["ledger_not_fresh"]
    assert refusals._net(after) < refusals._net(before)
    assert not set(after) < set(before)
