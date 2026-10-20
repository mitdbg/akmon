"""task.gtir.formal-commit-witness -- the executable property model.

Bounded model-check that the checker reports no violation IFF a state is
well-formed, plus the named counterexamples (bypass / unwitnessed / unguarded)
and the adversarial provenance-corruption test that must flag the offending row.
No DB needed -- this is the formal (property) layer.
"""

from __future__ import annotations

from okg.substrate.transform.verify import (
    COMMIT_WITNESS,
    GUARD_DOMINANCE,
    SEAL_RUNTIME_WITNESSED,
    CommittedRow,
    RuntimeWriteAttempt,
    State,
    adversarial_corrupt_provenance,
    check_seal_runtime_witnessed,
    counterexample_bypass,
    counterexample_seal_disabled,
    counterexample_unguarded,
    counterexample_unwitnessed,
    db_admits,
    model_check_bounded,
    model_check_seal_bounded,
    run_report,
    run_seal_report,
    seal_committed_rows,
)


def test_bounded_model_check_checker_equals_well_formedness():
    report = model_check_bounded(max_rows=2)
    # Exhaustive: checker-empty IFF well-formed held across the whole space.
    assert report["states_checked"] > 1000


def test_bypass_yields_named_commit_witness_counterexample():
    v = counterexample_bypass()
    assert len(v) == 1
    assert v[0].invariant == COMMIT_WITNESS
    assert v[0].offending_row_id == 7


def test_unwitnessed_yields_named_commit_witness_counterexample():
    v = counterexample_unwitnessed()
    assert len(v) == 1 and v[0].invariant == COMMIT_WITNESS and v[0].offending_row_id == 8


def test_unguarded_yields_named_guard_dominance_counterexample():
    v = counterexample_unguarded()
    assert len(v) == 1 and v[0].invariant == GUARD_DOMINANCE and v[0].offending_row_id == 9


def test_adversarial_corruption_flags_the_offending_row():
    # A well-formed two-row state; corrupt exactly one row's provenance.
    state = State(
        (
            CommittedRow(0, "sha256:ledgered", True, False, None),
            CommittedRow(1, "sha256:ledgered", True, True, True),
        )
    )
    v = adversarial_corrupt_provenance(state, row_id=1)
    assert len(v) == 1
    assert v[0].invariant == COMMIT_WITNESS
    assert v[0].offending_row_id == 1   # named the corrupted row


def test_run_report_both_properties_pass():
    results = {r["property_id"]: r for r in run_report()}
    assert results[COMMIT_WITNESS]["status"] == "pass"
    assert results[GUARD_DOMINANCE]["status"] == "pass"
    assert results[COMMIT_WITNESS]["states_checked"] > 1000


# --- seal model (PACT seal-graph-writes) ----------------------------------- #


def test_seal_trigger_admits_only_witnessed_runtime_writes():
    ledgered = RuntimeWriteAttempt(0, "sha256:ledgered", "runtime")
    unwitnessed = RuntimeWriteAttempt(1, "sha256:absent", "runtime")
    nullw = RuntimeWriteAttempt(2, None, "runtime")
    owner = RuntimeWriteAttempt(3, "sha256:absent", "trusted_base")
    # runtime: only the ledgered write is admitted; owner bypasses regardless.
    assert db_admits(ledgered) is True
    assert db_admits(unwitnessed) is False
    assert db_admits(nullw) is False
    assert db_admits(owner) is True
    committed = seal_committed_rows((ledgered, unwitnessed, nullw, owner))
    assert {a.row_id for a in committed} == {0, 3}             # unwitnessed runtime writes dropped
    assert check_seal_runtime_witnessed(committed) == []      # invariant holds


def test_seal_model_unwitnessed_runtime_unreachable_but_reachable_unsealed():
    mc = model_check_seal_bounded()
    assert mc["sealed_reachable_violation"] is False          # trigger: bad state unreachable
    assert mc["unsealed_reachable_violation"] is True         # without trigger: reachable
    assert mc["states_checked"] > 10


def test_seal_disabled_counterexample_is_named():
    v = counterexample_seal_disabled()
    assert len(v) == 1 and v[0].invariant == SEAL_RUNTIME_WITNESSED


def test_run_seal_report_passes():
    results = {r["property_id"]: r for r in run_seal_report()}
    assert results[SEAL_RUNTIME_WITNESSED]["status"] == "pass"
