"""The conformance evidence producers build from measurements or refuse.

No database, no package and no deployment: every input here is a number, a
digest string or a flag, which is the whole point of the producers being pure.
The hermetic arm (`tests/substrate/e2e/test_external_distribution_hermetic_arm.py`)
supplies the same inputs from a real run.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from okg.substrate.contracts.deployment_product.conformance import (
    ConformanceMaterializationEvidence,
    ConformancePublishEvidence,
    ConformanceReadinessEvidence,
    ConformanceReapplyEvidence,
)
from okg.substrate.distributions.conformance_evidence import (
    ConformanceEvidenceError,
    MeasuredCount,
    build_count_signal_evidence,
    build_materialization_evidence,
    build_publish_evidence,
    build_readiness_evidence,
    build_reapply_evidence,
)


def _digest(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


_PUBLISH_AUTHORITY: dict[str, Any] = {
    "generation_id": "7",
    "generation_status": "published",
    "definition_digest": _digest("definition"),
    "lock_digest": _digest("lock"),
    "composition_digest": _digest("composition"),
    "plan_digest": _digest("plan"),
    "sealed_release_digest": _digest("sealed-release"),
    "applied_instance_id": "lab-hermetic",
}


def _publish(**overrides: Any) -> ConformancePublishEvidence:
    """The two-unit/three-run/three-edge fixture shape, measured."""

    arguments: dict[str, Any] = dict(
        _PUBLISH_AUTHORITY,
        node_counts={
            "lab_unit": MeasuredCount("lab_unit", 0, 2),
            "lab_run": MeasuredCount("lab_run", 0, 3),
        },
        edge_counts={"contains": MeasuredCount("contains", 0, 3)},
        claimed_nodes_live_delta=5,
        claimed_edges_live_delta=3,
        claimed_source_deltas={"lab-hermetic.lab_units": 5},
        source_counts={
            "lab-hermetic.lab_units": MeasuredCount("lab-hermetic.lab_units", 0, 5)
        },
    )
    arguments.update(overrides)
    return build_publish_evidence(**arguments)


def _reapply(**overrides: Any) -> ConformanceReapplyEvidence:
    arguments: dict[str, Any] = dict(
        generation_id_before="7",
        generation_id_after="7",
        lock_digest=_digest("lock"),
        plan_digest=_digest("plan"),
        node_counts={
            "lab_unit": MeasuredCount("lab_unit", 2, 2),
            "lab_run": MeasuredCount("lab_run", 3, 3),
        },
        edge_counts={"contains": MeasuredCount("contains", 3, 3)},
        generations_created=MeasuredCount("generations", 1, 1),
        active_generation_pointer_changes=0,
        claimed_source_deltas={"lab-hermetic.lab_units": 0},
        source_counts={
            "lab-hermetic.lab_units": MeasuredCount("lab-hermetic.lab_units", 5, 5)
        },
    )
    arguments.update(overrides)
    return build_reapply_evidence(**arguments)


def _materialization(**overrides: Any) -> ConformanceMaterializationEvidence:
    declared = {
        "lab:schema": _digest("schema-bytes"),
        "lab:connector": _digest("connector-bytes"),
    }
    arguments: dict[str, Any] = dict(
        materialization_digest=_digest("materialization"),
        declared_asset_digests=declared,
        materialized_asset_digests=dict(declared),
        authoring_tree_present=False,
    )
    arguments.update(overrides)
    return build_materialization_evidence(**arguments)


def _readiness(**overrides: Any) -> ConformanceReadinessEvidence:
    arguments: dict[str, Any] = dict(
        ready=True,
        doctor="clean",
        queue_depth=0,
        dlq_depth=0,
        mcp_graph_read=True,
    )
    arguments.update(overrides)
    return build_readiness_evidence(**arguments)


def _refusal(call: Any, **overrides: Any) -> dict[str, Any]:
    with pytest.raises(ConformanceEvidenceError) as excinfo:
        call(**overrides)
    assert excinfo.value.code == "conformance_evidence_refused"
    return dict(excinfo.value.data)


# --------------------------------------------------------------------------
# What the producers accept
# --------------------------------------------------------------------------


def test_the_publish_producer_records_the_counts_it_read_back() -> None:
    evidence = _publish()
    assert evidence.generation_id == "7"
    assert (evidence.nodes_live_delta, evidence.edges_live_delta) == (5, 3)
    assert [(item.source_id, item.delta) for item in evidence.source_deltas] == [
        ("lab-hermetic.lab_units", 5)
    ]
    assert evidence.sealed_release_digest == _digest("sealed-release")


def test_the_reapply_producer_records_a_measured_no_op() -> None:
    evidence = _reapply()
    assert evidence.generation_id_before == evidence.generation_id_after == "7"
    assert evidence.nodes_live_delta == evidence.edges_live_delta == 0
    assert evidence.generations_created_delta == 0
    assert [item.delta for item in evidence.source_deltas] == [0]


def test_the_readiness_and_materialization_producers_record_their_measurements(
) -> None:
    readiness = _readiness(queue_depth=2, dlq_depth=1)
    assert (readiness.ready, readiness.doctor) == (True, "clean")
    assert (readiness.queue_depth, readiness.dlq_depth) == (2, 1)
    assert _materialization().materialization_digest == _digest("materialization")
    # A deployment that is honestly not ready is recordable; only the
    # combination that contradicts itself is refused.
    unready = _readiness(ready=False, doctor="failed", mcp_graph_read=False)
    assert (unready.ready, unready.doctor, unready.mcp_graph_read) == (
        False,
        "failed",
        False,
    )


def test_the_signal_producer_binds_a_claim_to_the_read_back_count() -> None:
    signal = build_count_signal_evidence(
        signal_id="lab-unit-nodes",
        kind="node_count",
        target="lab_unit",
        claimed_count=2,
        measured_count=2,
    )
    assert (signal.observed_count, signal.positive) == (2, True)
    empty = build_count_signal_evidence(
        signal_id="lab-ghost-nodes",
        kind="node_count",
        target="lab_ghost",
        claimed_count=0,
        measured_count=0,
    )
    assert empty.positive is False


# --------------------------------------------------------------------------
# A claim that disagrees with the read-back
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("overrides", "path"),
    [
        ({"claimed_nodes_live_delta": 6}, "nodes_live_delta"),
        ({"claimed_edges_live_delta": 4}, "edges_live_delta"),
        (
            {"claimed_source_deltas": {"lab-hermetic.lab_units": 4}},
            "source_deltas.lab-hermetic.lab_units",
        ),
    ],
)
def test_a_publish_count_that_disagrees_with_the_read_back_refuses(
    overrides: dict[str, Any], path: str
) -> None:
    data = _refusal(_publish, **overrides)
    assert data["reason"] == "count_disagrees_with_readback"
    assert data["path"] == path
    assert data["block"] == "publish"


def test_a_signal_claim_that_disagrees_with_the_read_back_refuses() -> None:
    data = _refusal(
        build_count_signal_evidence,
        signal_id="lab-unit-nodes",
        kind="node_count",
        target="lab_unit",
        claimed_count=3,
        measured_count=2,
    )
    assert data["reason"] == "count_disagrees_with_readback"


def test_per_source_deltas_must_account_for_the_live_node_delta() -> None:
    data = _refusal(
        _publish,
        claimed_source_deltas={"lab-hermetic.lab_units": 4, "other.source": 1},
        source_counts={
            "lab-hermetic.lab_units": MeasuredCount("lab-hermetic.lab_units", 0, 4),
            "other.source": MeasuredCount("other.source", 0, 1),
        },
        node_counts={"lab_unit": MeasuredCount("lab_unit", 0, 6)},
        claimed_nodes_live_delta=6,
    )
    assert data["reason"] == "source_deltas_disagree_with_nodes"


def test_a_source_the_run_never_read_back_refuses() -> None:
    data = _refusal(
        _publish,
        claimed_source_deltas={
            "lab-hermetic.lab_units": 5,
            "never-measured.source": 0,
        },
    )
    assert data["reason"] == "source_coverage_mismatch"


# --------------------------------------------------------------------------
# A measurement the run never took
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "field",
    [
        "generation_id",
        "generation_status",
        "definition_digest",
        "lock_digest",
        "composition_digest",
        "plan_digest",
        "sealed_release_digest",
        "applied_instance_id",
        "node_counts",
        "edge_counts",
        "claimed_nodes_live_delta",
        "claimed_edges_live_delta",
        "claimed_source_deltas",
        "source_counts",
    ],
)
def test_every_absent_publish_measurement_refuses(field: str) -> None:
    data = _refusal(_publish, **{field: None})
    assert data["reason"] == "measurement_missing"
    assert data["block"] == "publish"


@pytest.mark.parametrize(
    "field", ["ready", "doctor", "queue_depth", "dlq_depth", "mcp_graph_read"]
)
def test_every_absent_readiness_measurement_refuses(field: str) -> None:
    data = _refusal(_readiness, **{field: None})
    assert data["reason"] == "measurement_missing"


def test_a_half_measured_count_refuses_rather_than_assuming_zero() -> None:
    data = _refusal(
        _publish,
        node_counts={"lab_unit": MeasuredCount("lab_unit", None, 2)},
    )
    assert (data["reason"], data["path"]) == ("measurement_missing", "lab_unit.before")


def test_no_read_back_count_at_all_refuses() -> None:
    data = _refusal(_publish, node_counts={})
    assert (data["reason"], data["path"]) == ("measurement_missing", "counts")


# --------------------------------------------------------------------------
# An outcome the measurements contradict
# --------------------------------------------------------------------------


def test_an_unpublished_generation_cannot_prove_first_publish() -> None:
    data = _refusal(_publish, generation_status="blocked")
    assert data["reason"] == "generation_not_published"


def test_a_publish_that_added_no_rows_refuses() -> None:
    data = _refusal(
        _publish,
        node_counts={"lab_unit": MeasuredCount("lab_unit", 2, 2)},
        edge_counts={"contains": MeasuredCount("contains", 3, 3)},
        claimed_nodes_live_delta=0,
        claimed_edges_live_delta=0,
        claimed_source_deltas={"lab-hermetic.lab_units": 0},
        source_counts={
            "lab-hermetic.lab_units": MeasuredCount("lab-hermetic.lab_units", 5, 5)
        },
    )
    assert data["reason"] == "publish_added_no_rows"


@pytest.mark.parametrize(
    ("overrides", "reason"),
    [
        ({"generation_id_after": "8"}, "reapply_changed_the_active_generation"),
        (
            {"node_counts": {"lab_unit": MeasuredCount("lab_unit", 2, 3)}},
            "reapply_mutated_the_graph",
        ),
        (
            {"edge_counts": {"contains": MeasuredCount("contains", 3, 4)}},
            "reapply_mutated_the_graph",
        ),
        (
            {"generations_created": MeasuredCount("generations", 1, 2)},
            "reapply_created_a_generation",
        ),
        (
            {"active_generation_pointer_changes": 1},
            "reapply_moved_the_active_pointer",
        ),
        (
            {"claimed_source_deltas": {"lab-hermetic.lab_units": 1}},
            "count_disagrees_with_readback",
        ),
    ],
)
def test_a_reapply_that_moved_anything_refuses(
    overrides: dict[str, Any], reason: str
) -> None:
    assert _refusal(_reapply, **overrides)["reason"] == reason


def test_ready_cannot_contradict_the_doctor_result() -> None:
    assert (
        _refusal(_readiness, ready=True, doctor="failed")["reason"]
        == "ready_contradicts_doctor"
    )
    assert (
        _refusal(_readiness, doctor="mostly-fine")["reason"] == "doctor_result_unknown"
    )


@pytest.mark.parametrize("field", ["queue_depth", "dlq_depth"])
def test_a_flag_is_not_a_queue_depth(field: str) -> None:
    """`True` is an `int` in Python; a depth of one is not "yes"."""

    data = _refusal(_readiness, **{field: True})
    assert (data["reason"], data["path"]) == ("measurement_not_an_integer", field)


def test_an_unread_queue_depth_refuses_rather_than_recording_zero() -> None:
    """A queue authority that reported itself unavailable read no depth.

    Its report still carries zeros. Passing those on would put a number in the
    receipt that nothing measured, so the caller passes None and this refuses.
    """

    assert (
        _refusal(_readiness, queue_depth=None)["reason"] == "measurement_missing"
    )
    assert _refusal(_readiness, dlq_depth=None)["reason"] == "measurement_missing"
    # The positive path stays proven with explicitly measured depths.
    measured = _readiness(queue_depth=7, dlq_depth=2)
    assert (measured.queue_depth, measured.dlq_depth) == (7, 2)


def test_a_mutated_materialized_asset_refuses_with_the_digest_mismatch() -> None:
    declared = {
        "lab:schema": _digest("schema-bytes"),
        "lab:connector": _digest("connector-bytes"),
    }
    data = _refusal(
        _materialization,
        materialized_asset_digests=dict(declared, **{"lab:schema": _digest("mutated")}),
    )
    assert data["reason"] == "asset_digest_mismatch"
    assert data["path"] == "materialized_asset_digests.lab:schema"


def test_a_declared_asset_that_never_materialized_refuses() -> None:
    data = _refusal(
        _materialization,
        materialized_asset_digests={"lab:connector": _digest("connector-bytes")},
    )
    assert data["reason"] == "asset_not_materialized"
    assert data["path"] == "materialized_asset_digests.lab:schema"


def test_an_undeclared_materialized_asset_refuses() -> None:
    declared = {
        "lab:schema": _digest("schema-bytes"),
        "lab:connector": _digest("connector-bytes"),
    }
    data = _refusal(
        _materialization,
        materialized_asset_digests=dict(declared, **{"lab:ghost": _digest("ghost")}),
    )
    assert data["reason"] == "asset_not_declared"


def test_a_materialization_that_could_still_read_the_authoring_tree_refuses() -> None:
    data = _refusal(_materialization, authoring_tree_present=True)
    assert data["reason"] == "checkout_not_independent"


def test_a_non_count_signal_kind_refuses_rather_than_inventing_a_count() -> None:
    data = _refusal(
        build_count_signal_evidence,
        signal_id="census-view",
        kind="view",
        target="lab_unit_census",
        claimed_count=2,
        measured_count=2,
    )
    assert data["reason"] == "signal_kind_not_counted"


def test_a_flag_is_not_a_row_count() -> None:
    data = _refusal(
        _publish, node_counts={"lab_unit": MeasuredCount("lab_unit", False, True)}
    )
    assert data["reason"] == "measurement_not_an_integer"


def test_the_refusal_reports_the_contract_diagnostic_shape() -> None:
    with pytest.raises(ConformanceEvidenceError) as excinfo:
        _publish(claimed_nodes_live_delta=6)
    assert excinfo.value.to_dict() == {
        "code": "conformance_evidence_refused",
        "path": "nodes_live_delta",
        "expected": "publish",
        "observed": "count_disagrees_with_readback",
    }
    assert excinfo.value.fix_hint
