"""Derived-artifact dependency rules, proved without a database.

Every rule here used to be reachable only inside the scheduler's
database-bound module. These tests take plain inputs and touch no
connection: no `postgres` mark, no fixture, nothing to start.

The golden hashes are the ones captured from unmodified dev source
before the planning owner was extracted, by
`pact/changes/extract-scheduler-dependency-planning/evidence/golden-hash-capture.py`.
The inputs below are the same literals that script uses; that pairing is
the point, because the payload is sequence-sensitive and a golden value
means nothing without its exact inputs.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from okg.substrate import derived_artifacts as derived_artifacts_package
from okg.substrate.derived_artifacts import dependency_planning as planning
from okg.substrate.derived_artifacts import scheduler as scheduler_module
from okg.substrate.derived_artifacts.dependency_planning import (
    DerivedArtifactDependency,
    DerivedArtifactScheduleError,
    build_task_map,
    graph_hash,
    plan_dependency_graph,
    require_known_dependencies,
    topological_order,
)


@dataclass(frozen=True)
class _Definition:
    definition_id: str


@dataclass(frozen=True)
class _Task:
    """The least a task can be and still be plannable."""

    definition: _Definition


def _task(definition_id: str) -> _Task:
    return _Task(definition=_Definition(definition_id=definition_id))


DEP_BETA_ON_ALPHA = DerivedArtifactDependency(
    downstream_definition_id="beta",
    upstream_definition_id="alpha",
    upstream_slot_name="relation",
    dependency_kind="manifest_slot",
    scope_key=None,
    required_compute_hash="c1",
    required_output_schema_hash="s1",
    required_renderer_hash="r1",
    required_dirty_rule_hash="d1",
    metadata={"ignored_by_hash": True},
)

DEP_GAMMA_ON_BETA = DerivedArtifactDependency(
    downstream_definition_id="gamma",
    upstream_definition_id="beta",
    upstream_slot_name="relation",
    dependency_kind="manifest_slot",
    scope_key="scope-1",
    required_compute_hash=None,
    required_output_schema_hash=None,
    required_renderer_hash=None,
    required_dirty_rule_hash=None,
    metadata={},
)

DEP_ALL_FIELDS_DISTINCT = DerivedArtifactDependency(
    downstream_definition_id="downstream-9",
    upstream_definition_id="upstream-9",
    upstream_slot_name="slot-9",
    dependency_kind="kind-9",
    scope_key="scope-9",
    required_compute_hash="compute-9",
    required_output_schema_hash="schema-9",
    required_renderer_hash="renderer-9",
    required_dirty_rule_hash="dirty-9",
    metadata={"excluded": "metadata is not part of the payload"},
)

GOLDEN_HASHES = {
    "two_node_single_dependency": (
        "05eec15f8996cc2bac846ed2d9f3b54e9f130611df54409a7be688b073e1361b"
    ),
    "three_node_two_dependencies": (
        "64ddd8fdecd516d166226b0d4a2c14b91179df0025358967ee3270ea4338ff53"
    ),
    "three_node_dependencies_permuted": (
        "f684e96cffc782c0aef542d5e78cf951681dea890924cca532816e75a34587b7"
    ),
    "no_dependencies": (
        "62b6c6a51213395c24a75d4e12b595974c88e71070732a618ed7ea9368f44056"
    ),
    "null_catalog_version": (
        "46ae6f63da23216d9359384b00cd43306bf2c5d264e4041d07338cfdf7706505"
    ),
    "pre_publish_gate_phase": (
        "82850c255f537fc12b8ead2d36c0e6e09db26814de45dd6678e53f3bf03928d0"
    ),
    "all_nine_fields_distinct": (
        "fd3693beb3aa66c22fcc5507c3e35618e9e6f17e7679557079b55a3ae67c6383"
    ),
}

GOLDEN_CASES = {
    "two_node_single_dependency": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": 7,
        "definition_ids": ["alpha", "beta"],
        "dependencies": [DEP_BETA_ON_ALPHA],
    },
    "three_node_two_dependencies": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": 7,
        "definition_ids": ["alpha", "beta", "gamma"],
        "dependencies": [DEP_BETA_ON_ALPHA, DEP_GAMMA_ON_BETA],
    },
    "three_node_dependencies_permuted": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": 7,
        "definition_ids": ["alpha", "beta", "gamma"],
        "dependencies": [DEP_GAMMA_ON_BETA, DEP_BETA_ON_ALPHA],
    },
    "no_dependencies": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": 7,
        "definition_ids": ["solo"],
        "dependencies": [],
    },
    "null_catalog_version": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": None,
        "definition_ids": ["alpha", "beta"],
        "dependencies": [DEP_BETA_ON_ALPHA],
    },
    "pre_publish_gate_phase": {
        "deployment_name": "golden_deployment",
        "execution_phase": "pre_publish_gate",
        "catalog_version_id": 7,
        "definition_ids": ["alpha", "beta"],
        "dependencies": [DEP_BETA_ON_ALPHA],
    },
    "all_nine_fields_distinct": {
        "deployment_name": "golden_deployment",
        "execution_phase": "post_publish",
        "catalog_version_id": 7,
        "definition_ids": ["upstream-9", "downstream-9"],
        "dependencies": [DEP_ALL_FIELDS_DISTINCT],
    },
}


@pytest.mark.parametrize("case_name", sorted(GOLDEN_CASES))
def test_graph_hash_matches_captured_golden_values(case_name: str) -> None:
    assert graph_hash(**GOLDEN_CASES[case_name]) == GOLDEN_HASHES[case_name]


def test_permuted_dependency_order_changes_graph_hash() -> None:
    ordered = graph_hash(**GOLDEN_CASES["three_node_two_dependencies"])
    permuted = graph_hash(**GOLDEN_CASES["three_node_dependencies_permuted"])

    assert ordered != permuted


def test_graph_hash_ignores_dependency_metadata() -> None:
    without_metadata = DerivedArtifactDependency(
        downstream_definition_id=DEP_ALL_FIELDS_DISTINCT.downstream_definition_id,
        upstream_definition_id=DEP_ALL_FIELDS_DISTINCT.upstream_definition_id,
        upstream_slot_name=DEP_ALL_FIELDS_DISTINCT.upstream_slot_name,
        dependency_kind=DEP_ALL_FIELDS_DISTINCT.dependency_kind,
        scope_key=DEP_ALL_FIELDS_DISTINCT.scope_key,
        required_compute_hash=DEP_ALL_FIELDS_DISTINCT.required_compute_hash,
        required_output_schema_hash=(
            DEP_ALL_FIELDS_DISTINCT.required_output_schema_hash
        ),
        required_renderer_hash=DEP_ALL_FIELDS_DISTINCT.required_renderer_hash,
        required_dirty_rule_hash=(
            DEP_ALL_FIELDS_DISTINCT.required_dirty_rule_hash
        ),
        metadata={},
    )

    assert graph_hash(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        definition_ids=["upstream-9", "downstream-9"],
        dependencies=[without_metadata],
    ) == GOLDEN_HASHES["all_nine_fields_distinct"]


def test_graph_hash_notices_a_swapped_field() -> None:
    swapped = DerivedArtifactDependency(
        downstream_definition_id="downstream-9",
        upstream_definition_id="upstream-9",
        upstream_slot_name="slot-9",
        dependency_kind="kind-9",
        scope_key="scope-9",
        # renderer and schema exchanged
        required_compute_hash="compute-9",
        required_output_schema_hash="renderer-9",
        required_renderer_hash="schema-9",
        required_dirty_rule_hash="dirty-9",
        metadata={},
    )

    assert graph_hash(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        definition_ids=["upstream-9", "downstream-9"],
        dependencies=[swapped],
    ) != GOLDEN_HASHES["all_nine_fields_distinct"]


def test_graph_hash_counts_a_duplicate_edge() -> None:
    """Ordering deduplicates a repeated edge; plan identity does not."""
    once = graph_hash(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        definition_ids=["alpha", "beta"],
        dependencies=[DEP_BETA_ON_ALPHA],
    )
    twice = graph_hash(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        definition_ids=["alpha", "beta"],
        dependencies=[DEP_BETA_ON_ALPHA, DEP_BETA_ON_ALPHA],
    )

    assert once != twice
    assert topological_order({"alpha", "beta"}, [DEP_BETA_ON_ALPHA]) == (
        topological_order(
            {"alpha", "beta"},
            [DEP_BETA_ON_ALPHA, DEP_BETA_ON_ALPHA],
        )
    )


def test_topological_order_tie_break_is_alphabetical() -> None:
    dependencies = [
        DerivedArtifactDependency(
            downstream_definition_id="d",
            upstream_definition_id="a",
        ),
        DerivedArtifactDependency(
            downstream_definition_id="d",
            upstream_definition_id="c",
        ),
    ]

    assert topological_order({"a", "b", "c", "d"}, dependencies) == [
        "a",
        "b",
        "c",
        "d",
    ]


def test_topological_order_puts_upstream_first() -> None:
    assert topological_order(
        {"alpha", "beta", "gamma"},
        [DEP_BETA_ON_ALPHA, DEP_GAMMA_ON_BETA],
    ) == ["alpha", "beta", "gamma"]


def test_topological_order_refuses_a_cycle() -> None:
    dependencies = [
        DerivedArtifactDependency(
            downstream_definition_id="left",
            upstream_definition_id="right",
        ),
        DerivedArtifactDependency(
            downstream_definition_id="right",
            upstream_definition_id="left",
        ),
    ]

    with pytest.raises(DerivedArtifactScheduleError) as excinfo:
        topological_order({"left", "right"}, dependencies)

    assert str(excinfo.value) == (
        "derived artifact dependency cycle: left -> right"
    )


def test_build_task_map_refuses_duplicate_and_empty_tasks() -> None:
    with pytest.raises(DerivedArtifactScheduleError) as duplicate:
        build_task_map([_task("same"), _task("same")])
    assert str(duplicate.value) == "duplicate schedule task for 'same'"

    with pytest.raises(DerivedArtifactScheduleError) as empty:
        build_task_map([])
    assert str(empty.value) == "schedule requires at least one task"


def test_build_task_map_keeps_the_caller_s_task_objects() -> None:
    first = _task("alpha")
    second = _task("beta")

    task_by_id = build_task_map([first, second])

    assert task_by_id["alpha"] is first
    assert task_by_id["beta"] is second


def test_require_known_dependencies_lists_every_missing_id_sorted() -> None:
    dependencies = [
        DerivedArtifactDependency(
            downstream_definition_id="beta",
            upstream_definition_id="zulu",
        ),
        DerivedArtifactDependency(
            downstream_definition_id="beta",
            upstream_definition_id="alpha",
        ),
    ]

    with pytest.raises(DerivedArtifactScheduleError) as excinfo:
        require_known_dependencies({"beta": _task("beta")}, dependencies)

    assert str(excinfo.value) == (
        "schedule is missing upstream tasks: alpha, zulu"
    )


def test_plan_dependency_graph_returns_order_and_hash() -> None:
    plan = plan_dependency_graph(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        tasks=[_task("beta"), _task("alpha")],
        dependencies=[DEP_BETA_ON_ALPHA],
    )

    assert plan.topological_order == ["alpha", "beta"]
    assert plan.graph_hash == GOLDEN_HASHES["two_node_single_dependency"]
    assert sorted(plan.task_by_id) == ["alpha", "beta"]


def test_plan_dependency_graph_reports_only_the_two_moved_labels() -> None:
    labels: list[str] = []

    plan_dependency_graph(
        deployment_name="golden_deployment",
        execution_phase="post_publish",
        catalog_version_id=7,
        tasks=[_task("alpha"), _task("beta")],
        dependencies=[DEP_BETA_ON_ALPHA],
        record_timing=lambda label, started: labels.append(label),
    )

    assert labels == ["topological_order", "graph_hash"]


def test_plan_dependency_graph_refuses_an_unknown_upstream() -> None:
    with pytest.raises(DerivedArtifactScheduleError) as excinfo:
        plan_dependency_graph(
            deployment_name="golden_deployment",
            execution_phase="post_publish",
            catalog_version_id=7,
            tasks=[_task("beta")],
            dependencies=[DEP_BETA_ON_ALPHA],
        )

    assert str(excinfo.value) == "schedule is missing upstream tasks: alpha"


def test_scheduler_facade_matches_planning_owner() -> None:
    owner = "okg.substrate.derived_artifacts.dependency_planning"

    assert planning.DerivedArtifactScheduleError.__module__ == owner
    assert planning.DerivedArtifactDependency.__module__ == owner
    assert planning.plan_dependency_graph.__module__ == owner
    assert planning.build_task_map.__module__ == owner

    # The scheduler re-exports the very same objects, so `except` and
    # `isinstance` in existing callers keep working.
    assert (
        scheduler_module.DerivedArtifactScheduleError
        is planning.DerivedArtifactScheduleError
    )
    assert (
        scheduler_module.DerivedArtifactDependency
        is planning.DerivedArtifactDependency
    )
    assert scheduler_module.build_task_map is planning.build_task_map
    assert (
        scheduler_module.plan_dependency_graph
        is planning.plan_dependency_graph
    )

    # The scheduler imports only what it calls. It kept no private
    # aliases of the moved rule functions, because nothing imported them.
    for retired_alias in (
        "_task_map",
        "_require_known_dependencies",
        "_topological_order",
        "_graph_hash",
    ):
        assert not hasattr(scheduler_module, retired_alias), retired_alias

    # ... and so does the package.
    assert (
        derived_artifacts_package.DerivedArtifactScheduleError
        is planning.DerivedArtifactScheduleError
    )
    assert (
        derived_artifacts_package.DerivedArtifactDependency
        is planning.DerivedArtifactDependency
    )
    assert (
        derived_artifacts_package.plan_dependency_graph
        is planning.plan_dependency_graph
    )
    assert derived_artifacts_package.build_task_map is planning.build_task_map


def test_stable_hash_is_reused_not_copied() -> None:
    from okg.substrate.derived_artifacts import hashing
    from okg.substrate.derived_artifacts import relations

    assert relations.stable_hash is hashing.stable_hash
    assert derived_artifacts_package.stable_hash is hashing.stable_hash
    assert relations.RELATION_SLOT_NAME is hashing.RELATION_SLOT_NAME
    assert hashing.stable_hash.__module__ == (
        "okg.substrate.derived_artifacts.hashing"
    )
