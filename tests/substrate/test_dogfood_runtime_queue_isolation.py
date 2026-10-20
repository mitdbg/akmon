from __future__ import annotations

from contextlib import contextmanager

from okg.substrate.runtime.dbos_enqueue import DBOSWorkflowEnqueueResult
from okg.substrate.runtime.partition_queue import (
    deployment_runtime_queue_names,
    partition_queue_name,
    register_source_sync_partition_queue,
    source_sync_enqueue_priority,
    source_sync_queue_name,
)
from okg.substrate.source_sync import SourceSyncJob
from okg.substrate.source_sync_enqueue import enqueue_source_sync_via_dbos


def test_runtime_queue_names_separate_control_and_source_sync_lanes() -> None:
    assert partition_queue_name("okg-workspace") == "okg-okg-workspace"
    assert (
        source_sync_queue_name("okg-workspace")
        == "okg-okg-workspace-source-sync"
    )
    assert deployment_runtime_queue_names("okg-workspace") == (
        "okg-okg-workspace",
        "okg-okg-workspace-source-sync",
        # okg#3112 step 2: the declared-attribute index maintenance lane.
        "okg-okg-workspace-index-maintenance",
    )


def test_source_sync_dbos_enqueue_uses_source_sync_lane(monkeypatch) -> None:
    captured: dict[str, object] = {}

    @contextmanager
    def noop_source_sync_admission_intent_lock(**_kwargs):
        yield

    def fake_enqueue_deployment_workflow(**kwargs):
        captured.update(kwargs)
        return DBOSWorkflowEnqueueResult("wf-source-sync")

    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.active_workflow_id_for_name",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.publisher_publish_lock_holder_id",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue."
        "source_sync_admission_intent_lock",
        noop_source_sync_admission_intent_lock,
    )
    monkeypatch.setattr(
        "okg.substrate.source_sync_enqueue.enqueue_deployment_workflow",
        fake_enqueue_deployment_workflow,
    )

    workflow_id = enqueue_source_sync_via_dbos(
        deployment="okg-workspace",
        dsn="postgres://unused",
        job=SourceSyncJob(
            deployment="okg-workspace",
            sources=("docs",),
            run_mode="scope_complete",
        ),
        deduplication_id=(
            "sync:okg-workspace:watch:scope_complete:"
            "docs:closure:bucket"
        ),
    )

    assert workflow_id == "wf-source-sync"
    assert captured["queue_name"] == "okg-okg-workspace-source-sync"
    assert captured["workflow_name"] == "okg_source_sync"
    assert captured["priority"] == 100
    assert captured["deduplication_id"] == (
        "sync:okg-workspace:watch:scope_complete:"
        "docs:closure:bucket"
    )


def test_source_sync_enqueue_priority_tiers() -> None:
    assert source_sync_enqueue_priority(profile_force_deep=True) == 1
    assert source_sync_enqueue_priority(
        profile_collectors=["rootless-speedscope"],
    ) == 1
    assert source_sync_enqueue_priority(operator_requested=True) == 10
    assert source_sync_enqueue_priority(readiness_recovery=True) == 20
    assert source_sync_enqueue_priority() == 100


def test_source_sync_queue_registration_enables_priority(monkeypatch) -> None:
    from okg.substrate.runtime import partition_queue

    captured: dict[str, object] = {}

    class FakeQueue:
        def __init__(self, name, **kwargs):
            captured["name"] = name
            captured.update(kwargs)

    monkeypatch.setattr("dbos.Queue", FakeQueue)
    monkeypatch.setattr(partition_queue, "_REGISTERED", {})

    register_source_sync_partition_queue(deployment="okg-workspace")

    assert captured == {
        "name": "okg-okg-workspace-source-sync",
        "concurrency": 1,
        "priority_enabled": True,
        "polling_interval_sec": 0.05,
    }


def test_generic_deployment_enqueue_defaults_to_control_lane(monkeypatch) -> None:
    import dbos

    from okg.substrate.runtime.dbos_enqueue import enqueue_deployment_workflow

    captured: dict[str, object] = {}

    class FakeHandle:
        workflow_id = "wf-publisher"

    class FakeClient:
        def __init__(self, **_kwargs):
            pass

        def enqueue(self, options, *_args):
            captured.update(options)
            return FakeHandle()

    monkeypatch.setattr(dbos, "DBOSClient", FakeClient)
    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.latest_registered_app_version",
        lambda _dsn: None,
    )

    @contextmanager
    def permit_fake_enqueue(**_kwargs):
        yield

    monkeypatch.setattr(
        "okg.substrate.runtime.dbos_enqueue.promotion_enqueue_permit",
        permit_fake_enqueue,
    )

    result = enqueue_deployment_workflow(
        deployment="okg-workspace",
        dsn="postgres://unused",
        workflow_name="okg_publisher_cycle",
    )

    assert result == DBOSWorkflowEnqueueResult("wf-publisher")
    assert captured == {
        "queue_name": "okg-okg-workspace",
        "workflow_name": "okg_publisher_cycle",
    }
