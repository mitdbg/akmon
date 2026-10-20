"""Executable property model of the commit boundary's safety envelope (task 7).

In lieu of TLA+, a bounded model-check of the two invariants the commit path must
satisfy:

  * commit-witness  -- every committed row went through the DeltaSet boundary AND
    its source_program_checksum resolves in the IR ledger (no durable mutation
    without a ledgered producing program; no path bypasses the boundary);
  * guard-dominance -- every mutation row had a passing dominating ASSERT.

The checker returns named ``Violation``s carrying the offending row id. The model
is exhaustively enumerated over a small state space so that "checker reports no
violation" is proven equivalent to "state is well-formed". Deliberately broken
variants (a direct write bypassing the boundary; an unguarded mutation; a
corrupted provenance checksum) each yield the named counterexample -- mirroring
the optimizer verification harness (named invariant + offending id).

This model is *refined* by the real commit path: the refinement tests in
``tests/substrate/transform/test_commit_witness.py`` exercise the actual
SQL commit boundary against the real okg.ir_programs / okg.ir_invocations ledger
and show it enforces exactly these invariants.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass

COMMIT_WITNESS = "gtir.commit_witness"
GUARD_DOMINANCE = "gtir.guard_dominance"

LEDGER = frozenset({"sha256:ledgered"})


@dataclass(frozen=True)
class CommittedRow:
    row_id: int
    source_checksum: str | None  # provenance stamped at commit
    via_boundary: bool  # did it pass through the commit boundary?
    is_mutation: bool  # a DELTA-produced row
    guard_passed: bool | None  # verdict of the dominating ASSERT (None = unguarded)


@dataclass(frozen=True)
class State:
    rows: tuple[CommittedRow, ...]
    ledger: frozenset[str] = LEDGER


@dataclass(frozen=True)
class Violation:
    invariant: str
    offending_row_id: int
    reason: str


def check_commit_witness(state: State) -> list[Violation]:
    out: list[Violation] = []
    for r in state.rows:
        if not r.via_boundary:
            out.append(
                Violation(
                    COMMIT_WITNESS,
                    r.row_id,
                    "row bypassed the DeltaSet commit boundary",
                )
            )
        elif r.source_checksum is None or r.source_checksum not in state.ledger:
            out.append(
                Violation(
                    COMMIT_WITNESS,
                    r.row_id,
                    "source_program_checksum does not resolve in the ledger",
                )
            )
    return out


def check_guard_dominance(state: State) -> list[Violation]:
    return [
        Violation(
            GUARD_DOMINANCE, r.row_id, "mutation row has no passing dominating ASSERT"
        )
        for r in state.rows
        if r.is_mutation and r.guard_passed is not True
    ]


def check(state: State) -> list[Violation]:
    return check_commit_witness(state) + check_guard_dominance(state)


def _is_well_formed(row: CommittedRow) -> bool:
    return (
        row.via_boundary
        and row.source_checksum in LEDGER
        and (not row.is_mutation or row.guard_passed is True)
    )


def model_check_bounded(max_rows: int = 2) -> dict:
    """Exhaustively enumerate committed-row states; assert the checker reports no
    violation IFF the state is well-formed. This equivalence over the full bounded
    space is the model-check."""
    checksums = [None, "sha256:ledgered", "sha256:absent"]
    row_space = [
        CommittedRow(0, cs, vb, mut, guard)
        for cs in checksums
        for vb in (True, False)
        for mut in (True, False)
        for guard in (True, False, None)
    ]
    states_checked = 0
    for n in range(0, max_rows + 1):
        for combo in itertools.product(row_space, repeat=n):
            rows = tuple(
                CommittedRow(
                    i, r.source_checksum, r.via_boundary, r.is_mutation, r.guard_passed
                )
                for i, r in enumerate(combo)
            )
            state = State(rows)
            empty = check(state) == []
            well_formed = all(_is_well_formed(r) for r in rows)
            if empty != well_formed:  # the model invariant
                raise AssertionError(
                    f"checker disagrees with well-formedness at {state}"
                )
            states_checked += 1
    return {
        "property_id": "model",
        "states_checked": states_checked,
        "max_rows": max_rows,
    }


# --- named counterexamples (the deliberately broken variants) --------------- #


def counterexample_bypass() -> list[Violation]:
    """A direct write bypassing the DeltaSet boundary."""
    return check_commit_witness(
        State((CommittedRow(7, "sha256:ledgered", False, False, None),))
    )


def counterexample_unwitnessed() -> list[Violation]:
    """A commit whose program is not in the ledger."""
    return check_commit_witness(
        State((CommittedRow(8, "sha256:absent", True, False, None),))
    )


def counterexample_unguarded() -> list[Violation]:
    """A mutation with no passing dominating ASSERT."""
    return check_guard_dominance(
        State((CommittedRow(9, "sha256:ledgered", True, True, False),))
    )


def adversarial_corrupt_provenance(state: State, row_id: int) -> list[Violation]:
    """Corrupt one row's provenance checksum; the checker must flag THAT row."""
    rows = tuple(
        CommittedRow(
            r.row_id, "sha256:absent", r.via_boundary, r.is_mutation, r.guard_passed
        )
        if r.row_id == row_id
        else r
        for r in state.rows
    )
    return check_commit_witness(State(rows, state.ledger))


# --- seal model (PACT seal-graph-writes-behind-the-ir) ---------------------- #
#
# The route-mutation model above asks "did the row pass through the commit
# boundary?" (via_boundary). The SEAL changes the enforcement: route-all-writers ships
# stamp-and-gate (writers issue direct DML + stamp a witness), and the DB trigger
# okg.enforce_commit_witness makes an un-witnessed durable write UNREACHABLE for the
# runtime role. This model proves that reachability property: enumerate every write
# attempt by every writer class; the trigger is modelled as an admission predicate;
# the invariant is that no reachable committed state has a runtime-written row whose
# checksum does not resolve in the ledger. The trusted base (superuser/owner)
# bypasses -- the accepted BOUND.

SEAL_RUNTIME_WITNESSED = "sealw.runtime_write_witnessed"


@dataclass(frozen=True)
class RuntimeWriteAttempt:
    row_id: int
    source_checksum: str | None
    writer: str  # 'runtime' | 'trusted_base'


def db_admits(
    attempt: RuntimeWriteAttempt,
    ledger: frozenset[str] = LEDGER,
    *,
    trigger_installed: bool = True,
) -> bool:
    """Model okg.enforce_commit_witness. With the trigger installed, a RUNTIME write
    becomes durable ONLY if its checksum resolves in the ledger; the trusted base
    always bypasses. Without the trigger (the pre-seal world) anything is admitted."""
    if not trigger_installed:
        return True
    if attempt.writer == "runtime":
        return attempt.source_checksum in ledger
    return True  # trusted-base bypass (accepted BOUND)


def seal_committed_rows(
    attempts: tuple[RuntimeWriteAttempt, ...],
    ledger: frozenset[str] = LEDGER,
    *,
    trigger_installed: bool = True,
) -> tuple[RuntimeWriteAttempt, ...]:
    """The durable rows that actually land = the attempts the DB admits."""
    return tuple(
        a for a in attempts if db_admits(a, ledger, trigger_installed=trigger_installed)
    )


def check_seal_runtime_witnessed(
    committed: tuple[RuntimeWriteAttempt, ...], ledger: frozenset[str] = LEDGER
) -> list[Violation]:
    """INVARIANT: every committed row attributable to the runtime role carries a
    resolvable witness. (Trusted-base rows may be un-witnessed -- the accepted BOUND.)"""
    return [
        Violation(
            SEAL_RUNTIME_WITNESSED,
            a.row_id,
            "runtime-written durable row lacks a resolvable witness",
        )
        for a in committed
        if a.writer == "runtime" and a.source_checksum not in ledger
    ]


def model_check_seal_bounded(max_attempts: int = 2) -> dict:
    """Exhaustively enumerate write-attempt sequences. With the trigger installed the
    runtime-un-witnessed durable-write state is UNREACHABLE (the invariant holds in
    every reachable committed state); WITHOUT it that state IS reachable -- which
    proves the trigger is load-bearing, not vacuous."""
    writers = ("runtime", "trusted_base")
    checksums = (None, "sha256:ledgered", "sha256:absent")
    attempt_space = [RuntimeWriteAttempt(0, cs, w) for w in writers for cs in checksums]
    states_checked = 0
    sealed_reachable_violation = False
    unsealed_reachable_violation = False
    for n in range(0, max_attempts + 1):
        for combo in itertools.product(attempt_space, repeat=n):
            attempts = tuple(
                RuntimeWriteAttempt(i, a.source_checksum, a.writer)
                for i, a in enumerate(combo)
            )
            if check_seal_runtime_witnessed(
                seal_committed_rows(attempts, trigger_installed=True)
            ):
                sealed_reachable_violation = True
            if check_seal_runtime_witnessed(
                seal_committed_rows(attempts, trigger_installed=False)
            ):
                unsealed_reachable_violation = True
            states_checked += 1
    return {
        "property_id": SEAL_RUNTIME_WITNESSED,
        "states_checked": states_checked,
        "sealed_reachable_violation": sealed_reachable_violation,  # MUST be False
        "unsealed_reachable_violation": unsealed_reachable_violation,  # MUST be True
    }


def counterexample_seal_disabled() -> list[Violation]:
    """Without the trigger, a runtime write of an un-witnessed row IS admitted to
    durable state -- the named counterexample that makes the trigger load-bearing."""
    attempts = (RuntimeWriteAttempt(11, "sha256:absent", "runtime"),)
    return check_seal_runtime_witnessed(
        seal_committed_rows(attempts, trigger_installed=False)
    )


def run_seal_report() -> list[dict]:
    """Run the seal reachability model; emit the formal result for the trigger."""
    mc = model_check_seal_bounded()
    ok = (not mc["sealed_reachable_violation"]) and mc["unsealed_reachable_violation"]
    return [
        {
            "property_id": SEAL_RUNTIME_WITNESSED,
            "status": "pass" if ok else "fail",
            "states_checked": mc["states_checked"],
            "counterexamples": ["seal_disabled_runtime_unwitnessed_reachable"],
            "summary": (
                "with okg.enforce_commit_witness installed, NO reachable committed state "
                "has a runtime-written un-witnessed durable row; without it such a state "
                "IS reachable -- the trigger is load-bearing. Trusted-base writes bypass "
                "(accepted BOUND)."
            ),
        }
    ]


def run_report() -> list[dict]:
    """Run the model-check + counterexamples; emit per-property formal results."""
    mc = model_check_bounded()
    cw_ok = (
        len(counterexample_bypass()) == 1
        and len(counterexample_unwitnessed()) == 1
        and len(
            adversarial_corrupt_provenance(
                State((CommittedRow(0, "sha256:ledgered", True, False, None),)), 0
            )
        )
        == 1
    )
    gd_ok = len(counterexample_unguarded()) == 1
    return [
        {
            "property_id": COMMIT_WITNESS,
            "status": "pass" if cw_ok else "fail",
            "states_checked": mc["states_checked"],
            "counterexamples": ["bypass", "unwitnessed", "adversarial_corruption"],
        },
        {
            "property_id": GUARD_DOMINANCE,
            "status": "pass" if gd_ok else "fail",
            "states_checked": mc["states_checked"],
            "counterexamples": ["unguarded_mutation"],
        },
    ]


__all__ = [
    "COMMIT_WITNESS",
    "GUARD_DOMINANCE",
    "CommittedRow",
    "State",
    "Violation",
    "check",
    "check_commit_witness",
    "check_guard_dominance",
    "model_check_bounded",
    "counterexample_bypass",
    "counterexample_unwitnessed",
    "counterexample_unguarded",
    "adversarial_corrupt_provenance",
    "run_report",
    "SEAL_RUNTIME_WITNESSED",
    "RuntimeWriteAttempt",
    "db_admits",
    "seal_committed_rows",
    "check_seal_runtime_witnessed",
    "model_check_seal_bounded",
    "counterexample_seal_disabled",
    "run_seal_report",
]
