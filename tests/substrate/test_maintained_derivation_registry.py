"""Step-1 gate for maintained-derivation-registry (#444 slice 3): the registry
surface + its admission contract.

Pure-logic (no DB): the registry row schema, the determinism-appropriate oracle
enforcement, the override-iff-judgment rule, the dependency-kind allowlist, the
deterministic iteration order, and the name-collision check against the LLM
narrative-view registry (``views/registry.py``).
"""
from __future__ import annotations

import pytest

import okg.substrate.derived_artifacts.derivation_registry as registry_mod
from okg.substrate.derived_artifacts.maintained_derivation import ForceInputs
from okg.substrate.derived_artifacts.maintained_derivation import (
    ChronosRegisteredTable,
    CorrectnessOracle,
    DerivationSpec,
    MaintainResult,
    ProjectionParityOracle,
    SentinelSpec,
)

DeclaredDeps = registry_mod.DeclaredDeps
DerivationRegistrationError = registry_mod.DerivationRegistrationError
DerivationRegistry = registry_mod.DerivationRegistry
DerivationRow = registry_mod.DerivationRow
DeterminismClass = registry_mod.DeterminismClass
ExecutionPhase = registry_mod.ExecutionPhase
FailureMode = registry_mod.FailureMode
FindingSet = registry_mod.FindingSet
GovernanceProfile = registry_mod.GovernanceProfile
Materialization = registry_mod.Materialization
OverridePath = registry_mod.OverridePath
maintain_one_row = registry_mod.maintain_one_row
maintain_registered_derivations = registry_mod.maintain_registered_derivations


def _spec(derivation_id: str, oracle) -> DerivationSpec:
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


_CORRECTNESS = CorrectnessOracle("SELECT 1 AS k, 1 AS v", ["k"], ["v"])
_PARITY = ProjectionParityOracle(lambda frozen, checkout: {}, ["k"])


def _materialization(table: str = "okg.test_rollup") -> Materialization:
    return Materialization(
        kind="side_table",
        storage=ChronosRegisteredTable(table=table, ensure_fn=lambda store: None),
        sentinel=SentinelSpec(pk_columns=["k"], cadence_column="v"),
    )


def _deterministic_row(derivation_id: str = "det_x") -> DerivationRow:
    return DerivationRow(
        spec=_spec(derivation_id, _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )


def _judgment_row(derivation_id: str = "judg_x") -> DerivationRow:
    return DerivationRow(
        spec=_spec(derivation_id, _PARITY),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node", "decision"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(
            determinism=DeterminismClass.JUDGMENT,
            override=OverridePath(prepare=lambda: None, commit=lambda: None),
        ),
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )


# --- name collision (the PACT flags it twice) ------------------------------
def test_registry_does_not_shadow_views_registry() -> None:
    import okg.substrate.derived_artifacts.derivation_registry as dr
    import okg.substrate.views.registry as vr

    assert dr is not vr
    assert dr.__name__ == "okg.substrate.derived_artifacts.derivation_registry"
    assert vr.__name__ == "okg.substrate.views.registry"
    # The view registry's entry type is unrelated to the derivation row.
    assert hasattr(vr, "ViewRegistryEntry")
    assert not hasattr(dr, "ViewRegistryEntry")


# --- happy path + deterministic iteration ----------------------------------
def test_register_deterministic_and_judgment_rows() -> None:
    reg = DerivationRegistry()
    reg.register(_deterministic_row("a_det"))
    reg.register(_judgment_row("z_judg"))
    assert "a_det" in reg and "z_judg" in reg
    assert reg.get("a_det") is not None


def test_iteration_order_is_sorted_by_id() -> None:
    reg = DerivationRegistry()
    reg.register(_deterministic_row("zeta"))
    reg.register(_deterministic_row("alpha"))
    reg.register(_judgment_row("mu"))
    assert [r.spec.derivation_id for r in reg.rows()] == ["alpha", "mu", "zeta"]


# --- determinism-appropriate oracle enforcement (req.uniform-governance) ----
def test_deterministic_row_with_parity_oracle_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _PARITY),  # parity oracle on a deterministic row
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    with pytest.raises(DerivationRegistrationError, match="CorrectnessOracle"):
        DerivationRegistry().register(bad)


def test_judgment_row_with_correctness_oracle_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _CORRECTNESS),  # correctness oracle on a judgment row
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(
            determinism=DeterminismClass.JUDGMENT,
            override=OverridePath(prepare=lambda: None, commit=lambda: None),
        ),
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )
    with pytest.raises(DerivationRegistrationError, match="ProjectionParityOracle"):
        DerivationRegistry().register(bad)


def test_judgment_row_without_override_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _PARITY),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_node"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.JUDGMENT),  # no override
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )
    with pytest.raises(DerivationRegistrationError, match="override"):
        DerivationRegistry().register(bad)


def test_deterministic_row_with_override_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(
            determinism=DeterminismClass.DETERMINISTIC,
            override=OverridePath(prepare=lambda: None, commit=lambda: None),
        ),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    with pytest.raises(DerivationRegistrationError, match="override"):
        DerivationRegistry().register(bad)


# --- materialization + dependency-kind admission ---------------------------
def test_unknown_dependency_kind_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"not_a_real_kind"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    with pytest.raises(DerivationRegistrationError, match="reverse index"):
        DerivationRegistry().register(bad)


def test_no_materialization_raises() -> None:
    bad = DerivationRow(
        spec=_spec("bad", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    with pytest.raises(DerivationRegistrationError, match="materialization"):
        DerivationRegistry().register(bad)


def test_schema_admits_a_materializations_list() -> None:
    """The row SCHEMA admits multiple materializations (mutual parity is the goal).
    The driver rejects len>1 LOUDLY until multi-table maintenance lands (a later
    slice) -- but the registration surface must not forbid declaring the list, so a
    multi-materialization row registers cleanly here.
    """
    row = DerivationRow(
        spec=_spec("multi", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization("okg.t1"), _materialization("okg.t2")],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    reg = DerivationRegistry()
    reg.register(row)
    assert len(reg.get("multi").materializations) == 2


# --- the driver: maintain_registered_derivations ----------------------------
def _recording_maintain(calls):
    def _fake(conn, *, spec, storage, branch, generation_id,
              prior_generation_checkpoint, inputs):
        calls.append(
            {
                "id": spec.derivation_id,
                "table": storage.table,
                "inputs": inputs,
                "generation_id": generation_id,
                "prior": prior_generation_checkpoint,
            }
        )
        return {"mode": "incremental", "id": spec.derivation_id}

    return _fake


def _row_with_inputs(derivation_id, recorder) -> DerivationRow:
    def _make(dirty_scope, force_full):
        recorder.append((derivation_id, dirty_scope, force_full))
        return ForceInputs(
            change_inputs={"id": derivation_id, "scope": dirty_scope},
            force_full=force_full,
            scope_available=dirty_scope is not None,
        )

    base = _deterministic_row(derivation_id)
    import dataclasses

    return dataclasses.replace(base, make_inputs=_make)


def test_driver_drives_each_row_through_maintain_with_built_inputs(monkeypatch) -> None:
    calls: list = []
    make_calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))
    reg = DerivationRegistry()
    reg.register(_row_with_inputs("zz", make_calls))
    reg.register(_row_with_inputs("aa", make_calls))

    out = maintain_registered_derivations(
        object(), reg, branch="default", generation_id=7,
        prior_generation_checkpoint="generation:6", dirty_scope={"e": [1]},
        force_full=True,
    )
    # sorted order; each row drives once with its built inputs + the cross-cutting force.
    assert [c["id"] for c in calls] == ["aa", "zz"]
    assert all(c["inputs"].force_full is True for c in calls)
    assert all(c["generation_id"] == 7 and c["prior"] == "generation:6" for c in calls)
    assert [m[0] for m in make_calls] == ["aa", "zz"]
    assert all(m[1] == {"e": [1]} and m[2] is True for m in make_calls)
    assert set(out) == {"aa", "zz"}


def test_missing_dependency_declaration_forces_scope_unavailable(monkeypatch) -> None:
    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))

    import dataclasses

    row = dataclasses.replace(
        _row_with_inputs("missing_deps", []),
        declared_deps=DeclaredDeps(kinds=frozenset()),
    )
    maintain_one_row(
        object(), row, branch="default", generation_id=1,
        prior_generation_checkpoint=None, dirty_scope={"graph_edge": [7]},
        force_full=False,
    )

    assert calls[-1]["inputs"].scope_available is False
    assert calls[-1]["inputs"].force_full is False


def test_unavailable_edge_dirty_scope_forces_scope_unavailable(monkeypatch) -> None:
    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))

    maintain_one_row(
        object(), _row_with_inputs("edge_consumer", []), branch="default",
        generation_id=1, prior_generation_checkpoint=None,
        dirty_scope=({"document": ["n1"]}, {}, False, {}, {}, None),
        force_full=False,
    )

    assert calls[-1]["inputs"].scope_available is False
    assert calls[-1]["inputs"].force_full is False


def test_driver_rejects_multi_materialization_loudly(monkeypatch) -> None:
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain([]))
    multi = DerivationRow(
        spec=_spec("multi", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization("okg.t1"), _materialization("okg.t2")],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    reg = DerivationRegistry()
    reg.register(multi)
    with pytest.raises(DerivationRegistrationError, match="one materialization"):
        maintain_registered_derivations(
            object(), reg, branch="default", generation_id=1,
            prior_generation_checkpoint=None, dirty_scope=None,
        )


def test_driver_phase_filter(monkeypatch) -> None:
    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))
    reg = DerivationRegistry()
    reg.register(_deterministic_row("pre"))  # PRE_CHECKPOINT
    post = DerivationRow(
        spec=_spec("post", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.POST_PUBLISH,
    )
    reg.register(post)
    maintain_registered_derivations(
        object(), reg, branch="default", generation_id=1,
        prior_generation_checkpoint=None, dirty_scope=None,
        phase=ExecutionPhase.PRE_CHECKPOINT,
    )
    assert [c["id"] for c in calls] == ["pre"]


def test_driver_failsoft_swallows_but_gating_reraises(monkeypatch) -> None:
    def _boom(conn, **kwargs):
        raise RuntimeError("maintain blew up")

    monkeypatch.setattr(registry_mod, "maintain", _boom)

    # NON_GATING (the default): summary is explicit unavailable, publish not aborted.
    soft = DerivationRegistry()
    soft.register(_deterministic_row("soft"))
    out = maintain_registered_derivations(
        object(), soft, branch="default", generation_id=1,
        prior_generation_checkpoint=None, dirty_scope=None,
    )
    assert out["soft"]["status"] == "unavailable"
    assert out["soft"]["reason"] == "maintain_failed"
    assert "maintain blew up" in out["soft"]["error"]

    # GATING: re-raises.
    hard_row = DerivationRow(
        spec=_spec("hard", _CORRECTNESS),
        declared_deps=DeclaredDeps(kinds=frozenset({"graph_edge"})),
        materializations=[_materialization()],
        governance=GovernanceProfile(determinism=DeterminismClass.DETERMINISTIC),
        execution_phase=ExecutionPhase.PRE_CHECKPOINT,
        failure_mode=FailureMode.GATING,
    )
    hard = DerivationRegistry()
    hard.register(hard_row)
    with pytest.raises(RuntimeError, match="maintain blew up"):
        maintain_registered_derivations(
            object(), hard, branch="default", generation_id=1,
            prior_generation_checkpoint=None, dirty_scope=None,
        )


def test_ontology_row_drives_through_driver(monkeypatch) -> None:
    """The real ontology row registers + drives: its make_inputs extracts the
    changed-edge ids from the dirty scope and the driver hands the kit spec +
    storage to maintain() -- the equivalence the live re-route relies on."""
    from okg.substrate.publisher.ontology_rollup import ontology_rollup_row

    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))
    reg = DerivationRegistry()
    reg.register(ontology_rollup_row())

    # dirty scope: (nodes_by_subtype, edges_by_type, edge_table_available, attr_changes)
    dirty_scope = ({}, {"references": [11, 22]}, True, {})
    maintain_registered_derivations(
        object(), reg, branch="default", generation_id=9,
        prior_generation_checkpoint="generation:8", dirty_scope=dirty_scope,
        force_full=False,
    )
    assert len(calls) == 1
    call = calls[0]
    assert call["id"] == "ontology_graph_rollup"
    assert call["table"] == "okg.ontology_rollup"
    # make_inputs extracted the changed edge ids from the scope.
    assert sorted(call["inputs"].change_inputs.changed_edge_ids) == [11, 22]
    assert call["inputs"].scope_available is True


def test_console_row_drives_through_driver(monkeypatch) -> None:
    """The real console-count row drives: its make_inputs extracts the three force
    detectors (changed nodes/edges + __subtype__ + __source__) from the dirty scope."""
    from okg.substrate.publisher.console_count_rollup import console_count_rollup_row

    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))
    reg = DerivationRegistry()
    reg.register(console_count_rollup_row())

    scope = (
        {"document": ["n1"]},          # nodes_by_subtype
        {"references": [7]},           # edges_by_type
        True,                          # edge_table_available
        {"document": {"n1": ["__source__"]}},  # attr changes: a source reattribution
    )
    maintain_registered_derivations(
        object(), reg, branch="default", generation_id=5,
        prior_generation_checkpoint="generation:4", dirty_scope=scope, force_full=False,
    )
    assert len(calls) == 1
    ci = calls[0]["inputs"].change_inputs
    assert calls[0]["table"] == "okg.console_count_rollup"
    assert ci.changed_node_ids == ["n1"]
    assert ci.changed_edge_ids == [7]
    assert ci.source_reattributed is True
    assert ci.node_subtype_changed is False
    assert ci.edge_type_changed is False


def test_rollup_rows_timing_stage() -> None:
    """The publisher timing-stage names the bench reads back are preserved: the
    ontology row's is "ontology_rollup" (NOT its id "ontology_graph_rollup"); the
    console row's is "console_count_rollup"."""
    from okg.substrate.publisher.console_count_rollup import console_count_rollup_row
    from okg.substrate.publisher.ontology_rollup import ontology_rollup_row

    assert ontology_rollup_row().timing_stage == "ontology_rollup"
    assert console_count_rollup_row().timing_stage == "console_count_rollup"


def _finding_set_row(coverage_complete) -> DerivationRow:
    base = _deterministic_row("findings")
    import dataclasses

    return dataclasses.replace(
        base,
        finding_set=FindingSet(
            universe_key=["entity_id"], coverage_complete=coverage_complete
        ),
        make_inputs=lambda scope, force: ForceInputs(
            change_inputs={"covered": scope}, force_full=force, scope_available=True
        ),
    )


def test_finding_set_incomplete_coverage_forces_full_pass(monkeypatch) -> None:
    """A FINDING-SET row whose changeset does NOT cover its universe
    (coverage_complete -> False) is forced to a full pass; a covered changeset is
    left incremental (force_full as passed)."""
    calls: list = []
    monkeypatch.setattr(registry_mod, "maintain", _recording_maintain(calls))

    # Incomplete coverage -> forced full even though the driver passed force_full=False.
    incomplete = _finding_set_row(coverage_complete=lambda change_inputs: False)
    maintain_one_row(
        object(), incomplete, branch="default", generation_id=1,
        prior_generation_checkpoint=None, dirty_scope={"e": [1]}, force_full=False,
    )
    assert calls[-1]["inputs"].force_full is True

    # Complete coverage -> NOT forced (force_full stays the passed False).
    complete = _finding_set_row(coverage_complete=lambda change_inputs: True)
    maintain_one_row(
        object(), complete, branch="default", generation_id=1,
        prior_generation_checkpoint=None, dirty_scope={"e": [1]}, force_full=False,
    )
    assert calls[-1]["inputs"].force_full is False
