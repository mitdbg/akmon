"""Uniform-governance gate for maintained-derivation-registry (#444 slice 3).

Proves the registry's UNIFORM governance at the row boundary (req.uniform-governance):
the determinism-APPROPRIATE oracle is enforced at registration; a registered row's
oracle CATCHES a drifted kept value; and a read of a registered row's table at a
generation it was never maintained at fails LOUD (never a silent scan / stale / zero).

Determinism-appropriate enforcement is also covered structurally in
test_maintained_derivation_registry; re-asserted here under the governance selector
the PACT test_plan names (`maintained_derivation_governance or
maintained_derivation_fallback`).
"""
from __future__ import annotations

import pytest

from okg.substrate.derived_artifacts.derivation_registry import (
    DeclaredDeps,
    DerivationRegistrationError,
    DerivationRegistry,
    DerivationRow,
    DeterminismClass,
    ExecutionPhase,
    GovernanceProfile,
    Materialization,
    OverridePath,
)
from okg.substrate.derived_artifacts.maintained_derivation import (
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    MaintainedDerivationUnavailable,
    MaintainResult,
    ProjectionParityOracle,
    SentinelSpec,
    read_maintained_rows,
)

pytestmark = pytest.mark.postgres


def _spec(derivation_id, oracle):
    def _noop(_ctx) -> MaintainResult:
        return MaintainResult([], [], 0, None, "full_recount")

    return DerivationSpec(
        derivation_id=derivation_id,
        sentinel=SentinelSpec(pk_columns=["k"], cadence_column="v"),
        oracle=oracle,
        force_full_predicates=[],
        full_recount=_noop,
        incremental=_noop,
        self_heal_every=25,
    )


def _mat():
    return Materialization(
        kind="side_table",
        storage=ChronosRegisteredTable(table="okg.x", ensure_fn=lambda store: None),
        sentinel=SentinelSpec(pk_columns=["k"], cadence_column="v"),
    )


_CORRECTNESS = CorrectnessOracle("SELECT 1 AS k, 1 AS v", ["k"], ["v"])
_PARITY = ProjectionParityOracle(lambda frozen, checkout: {}, ["k"])


# --- determinism-appropriate oracle enforced at the registry boundary -------
def test_maintained_derivation_governance_deterministic_needs_correctness_oracle() -> None:
    bad = DerivationRow(
        spec=_spec("d", _PARITY),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_mat()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    with pytest.raises(DerivationRegistrationError, match="CorrectnessOracle"):
        DerivationRegistry().register(bad)


def test_maintained_derivation_governance_judgment_needs_parity_oracle_and_override() -> None:
    no_override = DerivationRow(
        spec=_spec("j", _PARITY),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[_mat()],
        governance=GovernanceProfile(determinism=DeterminismClass.JUDGMENT),
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )
    with pytest.raises(DerivationRegistrationError, match="override"):
        DerivationRegistry().register(no_override)

    wrong_oracle = DerivationRow(
        spec=_spec("j", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[_mat()],
        governance=GovernanceProfile(
            determinism=DeterminismClass.JUDGMENT,
            override=OverridePath(prepare=lambda: None, commit=lambda: None),
        ),
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )
    with pytest.raises(DerivationRegistrationError, match="ProjectionParityOracle"):
        DerivationRegistry().register(wrong_oracle)


def test_maintained_derivation_governance_rollup_rows_are_deterministic() -> None:
    """Both registered rollup rows carry the determinism-appropriate governance:
    a CorrectnessOracle, the DETERMINISTIC class, and NO override path."""
    from okg.substrate.publisher.console_count_rollup import console_count_rollup_row
    from okg.substrate.publisher.ontology_rollup import ontology_rollup_row

    for row in (ontology_rollup_row(), console_count_rollup_row()):
        assert isinstance(row.spec.oracle, CorrectnessOracle), row.spec.derivation_id
        assert row.governance.determinism is DeterminismClass.DETERMINISTIC
        assert row.governance.override is None


# --- drift is caught by the row's oracle (present-but-wrong, not read-fallback) ---
def test_maintained_derivation_governance_drift_caught_by_oracle() -> None:
    """A present-but-DRIFTED kept value is caught by the row's CorrectnessOracle diff
    (NOT by the read fallback, which only covers ABSENCE). Uses the real ontology
    rollup row's oracle: a kept bucket whose count diverges from ground truth is
    flagged as a mismatch."""
    from okg.substrate.publisher.ontology_rollup import ontology_rollup_row

    oracle = ontology_rollup_row().spec.oracle
    assert isinstance(oracle, CorrectnessOracle)
    key = ("case", "reported_by", "person", "git")
    ground_truth = {key: (5,)}
    kept_clean = {key: (5,)}
    kept_drifted = {key: (4,)}  # a corrupted kept count
    assert oracle.diff(kept_clean, ground_truth).ok is True
    drifted = oracle.diff(kept_drifted, ground_truth)
    assert drifted.ok is False
    assert key in drifted.mismatched


# --- a read of an unmaintained generation fails LOUD, never a silent zero ---
def test_maintained_derivation_fallback_loud_on_absent_generation(
    clean_runtime: str,
) -> None:
    """Reading a registered derivation's table AS-OF a generation that was never
    checkpointed/maintained raises a typed MaintainedDerivationUnavailable -- the
    loud-fail-on-absence contract (never a silent scan, stale value, or zero)."""
    import psycopg

    from okg.substrate.graph_store import CONSOLE_COUNT_ROLLUP_TABLE

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(MaintainedDerivationUnavailable):
            read_maintained_rows(
                conn,
                table=CONSOLE_COUNT_ROLLUP_TABLE,
                generation_id=999_999_999,  # no such checkpoint
                select_columns=["dimension_kind", "dimension_value", "item_count"],
                sentinel_spec=SentinelSpec(
                    pk_columns=["dimension_kind", "dimension_value"],
                    cadence_column="item_count",
                ),
                row_mapper=dict,
            )
