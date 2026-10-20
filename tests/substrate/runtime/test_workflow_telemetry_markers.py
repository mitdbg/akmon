"""Database-free tests of the workflow-root ``unavailable`` marker merge (#2320).

A workflow root starts with ``unavailable`` markers for ``queue_wait_ms`` and
``recovery_attempts``. When DBOS status later supplies those values, the
marker must go; a later marker must never erase a value already recorded;
and a marker whose value is still missing must survive a partial update
(the source-sync admission marker from change ``issue-877-source-sync-spans``).
"""

from __future__ import annotations

from okg.substrate.runtime.workflow_telemetry import (
    _merge_workflow_attributes,
    _workflow_attributes,
)


def _root() -> dict:
    return _workflow_attributes(
        workflow_name="okg_source_sync",
        workflow_id="wf-1",
        queue_name=None,
        attributes=None,
    )


def test_later_value_clears_its_marker() -> None:
    target = _root()
    assert set(target["unavailable"]) == {"queue_wait_ms", "recovery_attempts"}

    _merge_workflow_attributes(
        target,
        {
            "dbos.workflow.queue_wait_ms": 4000,
            "dbos.workflow.recovery_attempts": 1,
            "unavailable": {},
        },
    )

    assert target["dbos.workflow.queue_wait_ms"] == 4000
    assert target["dbos.workflow.recovery_attempts"] == 1
    assert target["unavailable"] == {}


def test_later_marker_never_erases_a_present_value() -> None:
    target = _root()
    _merge_workflow_attributes(
        target,
        {"dbos.workflow.queue_wait_ms": 4000, "unavailable": {}},
    )

    # A second DBOS read that could not compute the wait brings a marker
    # and no value; the known value must win.
    _merge_workflow_attributes(
        target,
        {"unavailable": {"queue_wait_ms": "dbos_workflow_queue_wait_not_available"}},
    )

    assert target["dbos.workflow.queue_wait_ms"] == 4000
    assert "queue_wait_ms" not in target["unavailable"]
    assert target["unavailable"] == {
        "recovery_attempts": "dbos_workflow_recovery_attempts_not_available",
    }


def test_marker_for_a_still_missing_value_survives_a_partial_update() -> None:
    target = _root()
    target["unavailable"]["source_sync.admission_lock.wait"] = (
        "source_sync_trace_handoff_not_available_legacy_input"
    )

    _merge_workflow_attributes(
        target,
        {"dbos.workflow.recovery_attempts": 0, "unavailable": {}},
    )

    assert target["unavailable"] == {
        "queue_wait_ms": "dbos_workflow_queue_wait_not_available",
        "source_sync.admission_lock.wait": (
            "source_sync_trace_handoff_not_available_legacy_input"
        ),
    }


def test_null_value_does_not_clear_its_marker() -> None:
    target = _root()
    _merge_workflow_attributes(
        target,
        {"dbos.workflow.queue_wait_ms": None, "unavailable": {}},
    )

    assert "queue_wait_ms" in target["unavailable"]


def test_supplied_attributes_with_the_value_drop_the_default_marker() -> None:
    attrs = _workflow_attributes(
        workflow_name="okg_source_sync",
        workflow_id="wf-1",
        queue_name=None,
        attributes={"dbos.workflow.recovery_attempts": 2},
    )

    assert attrs["dbos.workflow.recovery_attempts"] == 2
    assert attrs["unavailable"] == {
        "queue_wait_ms": "dbos_workflow_queue_wait_not_available",
    }
