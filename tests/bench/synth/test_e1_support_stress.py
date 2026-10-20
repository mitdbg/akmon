"""PACT ``concord-disclosure-budget``, task.batch.e1-harness — the E1 smoke
half: the induced support-omission arm on the ``synthetic-corpus-smoke``
deployment's live parity replay.

The truthful-declaration arm of this replay is the standing parity witness
(``tests/bench/synth/test_parity_replay.py``: EQUAL at every update of a
22-update history). This module witnesses the E1-specific claims on the SAME
live path, bounded to a short history:

* with truthful declarations, every update up to the mutation point grades
  EQUAL (or EQUAL-with-certified-differences) against the complete
  reference — no unsafe skip;
* with ONE declared dependency snapshot omitted (the victim document step's
  current facts stop seeing the input that actually changed), the REAL
  ladder approves the skip it is entitled to approve, the executor keeps a
  stale result, and the three-way grade CATCHES it as a MISMATCH naming the
  update — the harness catching the unsafe skip is the witness;
* the mutation refuses to hide nothing: aimed at a step whose declared
  inputs did not move, it raises instead of running a vacuous arm.
"""
from __future__ import annotations

from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.bench.synth.e1_support_stress import (  # noqa: E402
    E1StressError,
    SupportOmissionMutation,
)
from okg.bench.synth.governed import document_step_id  # noqa: E402
from okg.bench.synth.parity import (  # noqa: E402
    MODE_COMPLETE,
    MODE_DIFFERENTIAL,
    VERDICT_CERTIFIED,
    VERDICT_EQUAL,
    VERDICT_MISMATCH,
    grade_update,
    replay_mode,
)
from okg.substrate.consistency.reuse_scheduler import SKIP_CAUSES  # noqa: E402
from tests.bench.synth.test_parity_replay import (  # noqa: E402
    _DEP_DIR,
    _install_composed_catalog,
    _reset_between_modes,
    _seal_recorded_history,
)

# A short history bounds the cost: the standing 22-update truthful witness
# already exists; E1 needs the omission arm, not a second marathon.
_E1_UPDATES = 8


def _choose_mutation_point(parity) -> tuple[int, str]:
    """The first update (>= 1, so prior state exists) that touches exactly one
    PRE-EXISTING document AND moves the accepted answer. A document created by
    the update has no prior snapshot to freeze, and hiding a change that
    changes nothing would grade EQUAL — either way the arm would prove
    nothing."""
    for index in range(1, len(parity.updates)):
        step = parity.gold_step(index)
        if len(step.touched_docs) != 1 or not step.expected_diff.get("changed"):
            continue
        doc = step.touched_docs[0]
        previously = parity.gold_step(index - 1).corpus_digests
        if doc in previously:
            return index, document_step_id(doc)
    raise AssertionError(
        "no answer-moving update over a pre-existing document in the sealed "
        "history; lengthen the history"
    )


@pytest.mark.postgres
def test_omitted_support_snapshot_is_caught_as_a_mismatch(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    mutation_at, victim_step = _choose_mutation_point(parity)
    limit = mutation_at + 1

    _reset_between_modes(dsn)
    reference = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-ref",
        parity=parity, mode=MODE_COMPLETE, policies=policies, checks=checks,
        limit=limit,
    )
    _reset_between_modes(dsn)
    mutation = SupportOmissionMutation(prefer_step=victim_step)
    mutated = replay_mode(
        dsn=dsn, deployment_dir=_DEP_DIR, corpus_dir=tmp_path / "corpus-e1",
        parity=parity, mode=MODE_DIFFERENTIAL, policies=policies,
        checks=checks, mutations={mutation_at: mutation}, limit=limit,
    )

    verdicts = [
        grade_update(
            gold_step=parity.gold_step(i),
            reference=reference.executions[i],
            differential=mutated.executions[i],
        )
        for i in range(limit)
    ]

    # the truthful prefix (the mutation fires only at `mutation_at`) shows the
    # live path proposing no unsafe skip under complete declarations.
    for verdict in verdicts[:mutation_at]:
        assert verdict.verdict in (VERDICT_EQUAL, VERDICT_CERTIFIED), (
            f"update {verdict.update}: the truthful prefix diverged "
            f"({verdict.verdict})"
        )
        assert not verdict.gold_problems, verdict.gold_problems[:3]

    # the omission arm: the REAL ladder approved a skip for the victim...
    assert mutation.target_step == victim_step
    assert mutation.detail["hidden_input_keys"], (
        "the mutation hid no input — it proves nothing"
    )
    victim_skips = [
        (step, cause)
        for step, cause in mutated.executions[mutation_at].skip_outcomes
        if step == victim_step
    ]
    assert victim_skips, (
        "the victim step did not SKIP at the mutated update — the omission "
        "did not license the reuse it models"
    )
    assert victim_skips[0][1] in SKIP_CAUSES

    # ...and the harness CAUGHT the stale result as a mismatch at exactly the
    # mutated update. Zero tolerance means caught, not absent-by-luck.
    assert verdicts[mutation_at].verdict == VERDICT_MISMATCH, (
        f"the induced unsafe skip at update {mutation_at} was NOT caught "
        f"(verdict {verdicts[mutation_at].verdict}) — the E1 harness cannot "
        "demonstrate it catches what it exists to catch on the live path"
    )


@pytest.mark.postgres
def test_the_mutation_refuses_to_hide_an_unmoved_input(
    clean_runtime: str, tmp_path: Path, world, smoke_config
) -> None:
    """Aimed at an update where the preferred step's declared inputs did not
    move, the mutation raises — a vacuous omission arm is refused, never run."""
    dsn = clean_runtime
    _install_composed_catalog(dsn, tmp_path)
    parity, policies, checks, _direct = _seal_recorded_history(
        dsn, tmp_path, world, smoke_config, _E1_UPDATES
    )
    # find an update >= 1 and a document it does NOT touch.
    target_index = None
    untouched = None
    for index in range(1, len(parity.updates)):
        step = parity.gold_step(index)
        candidates = sorted(
            set(step.corpus_digests) - set(step.touched_docs)
        )
        if candidates:
            target_index = index
            untouched = document_step_id(candidates[0])
            break
    assert target_index is not None and untouched is not None

    _reset_between_modes(dsn)
    with pytest.raises(E1StressError):
        replay_mode(
            dsn=dsn, deployment_dir=_DEP_DIR,
            corpus_dir=tmp_path / "corpus-e1-vacuous",
            parity=parity, mode=MODE_DIFFERENTIAL, policies=policies,
            checks=checks,
            mutations={
                target_index: SupportOmissionMutation(prefer_step=untouched)
            },
            limit=target_index + 1,
        )
